#!/usr/bin/env bash
# T3/EKS: does the Spark availableNow MICRO-BATCH tax exist on REAL S3 at 100M? (MinIO couldn't show it —
# localhost S3 has ~0 commit latency; the KB "~25× at 100M" tax is S3-commit-latency-bound: each trigger
# commits parquet + checkpoint over the network.) SAME maxOffsets sweep we ran on MinIO (there it was FLAT),
# now on real S3: fewer triggers (bigger maxOffsets = fewer S3 commits) should be FASTER iff the tax is real.
# Plus one CONTINUOUS run (one long-lived pipeline, RisingWave/Flink/Arroyo model). Single compute node is
# enough — the tax is per-trigger S3 commit latency, node-count-independent. Writes to a per-run bucket, $0.
# Assumes cluster UP (compute node has S3 access) + image :TAG in ECR. Usage: scripts/eks_microbatch_s3_sweep.sh [N] [TAG]
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${1:-100000000}"; TAG="${2:-bf6}"; REGION=ap-south-1; NS=stream
BUCKET="vajra-mbsweep-$(date +%s)"
ECR="$(aws ecr describe-repositories --region $REGION --repository-name vajra --query 'repositories[0].repositoryUri' --output text | tr -d '[:space:]')"; REG="${ECR%/vajra}"
kk() { kubectl -n "$NS" "$@"; }
wait_ready() { kk wait --for=condition=available --timeout=300s deployment/"$1"; }
gib() { awk -v b="$1" 'BEGIN{printf "%.2f", b/1073741824}'; }

echo "==== [0] S3 bucket ===="
aws s3 mb "s3://$BUCKET" --region "$REGION" >/dev/null && echo "bucket created"
cleanup() { echo "== emptying s3://$BUCKET =="; aws s3 rb "s3://$BUCKET" --force >/dev/null 2>&1; }
trap cleanup EXIT

echo "==== [1] Kafka + produce $N (contiguous 10-window data) ===="
kk apply -f k8s/stream/kafka.yaml >/dev/null 2>&1 || kubectl apply -f k8s/stream/kafka.yaml
wait_ready kafka
KPOD=$(kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')
CUR=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic events 2>/dev/null | awk -F: '{s+=$3} END{print s}')
if [ "${CUR:-0}" != "$N" ]; then
  kk delete job producer --ignore-not-found >/dev/null 2>&1
  sed -e "s|N_EVENTS, value: \"[0-9]*\"|N_EVENTS, value: \"$N\"|" k8s/stream/producer-job.yaml | kk apply -f -
  kk wait --for=condition=complete --timeout=1800s job/producer; kk logs job/producer | grep -a PRODUCED
  CUR=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic events 2>/dev/null | awk -F: '{s+=$3} END{print s}')
fi
[ "${CUR:-0}" = "$N" ] || { echo "ABORT: events=$CUR != $N"; exit 3; }
echo "TOPIC_CHECK events=$CUR"

echo "==== [2] Vajra ($TAG, COMPLETE_ON_END=1) + client ===="
sed -e "s|__ECR__|$REG|g" -e "s|vajra:eo-multipart|vajra:$TAG|g" k8s/stream/vajra-stream.yaml | kk apply -f -
kk patch deploy vajra-stream --type merge -p '{"spec":{"strategy":{"rollingUpdate":{"maxSurge":0,"maxUnavailable":1}}}}' >/dev/null
kk set env deploy/vajra-stream AWS_REGION="$REGION" VAJRA_COMPLETE_ON_END=1 >/dev/null
wait_ready vajra-stream
kk apply -f k8s/stream/vajra-client.yaml
kk wait --for=condition=ready --timeout=300s pod/vajra-client
until kk logs vajra-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
kk cp scripts/stream_windowed_agg.py vajra-client:/tmp/wagg.py
kk cp scripts/stream_windowed_agg_continuous.py vajra-client:/tmp/wagg_co.py
SR="sc://vajra-stream.$NS.svc.cluster.local:50051"; BOOT="kafka.$NS.svc.cluster.local:9092"

run_mx() { # $1=maxOffsets $2=subdir -> VAJRA_WAGG line + peak RSS
  kk rollout restart deploy/vajra-stream >/dev/null 2>&1; wait_ready vajra-stream >/dev/null 2>&1
  until kk logs vajra-client 2>/dev/null | grep -q CLIENT_READY; do sleep 2; done
  local R; R=$(kk exec vajra-client -- sh -c \
    "SPARK_REMOTE=$SR BOOT=$BOOT TOPIC=events N_EVENTS=$N MAXOFFSETS=$1 OUT=s3://$BUCKET/$2 CK=s3://$BUCKET/${2}_ck python3 /tmp/wagg.py" 2>&1 | grep -aoE 'VAJRA_WAGG.*' | tail -1)
  local VPOD MEM; VPOD=$(kk get pod -l app=vajra-stream --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')
  MEM=$(kk exec "$VPOD" -- cat /sys/fs/cgroup/memory.peak 2>/dev/null)
  local BATCHES=$(( (N + $1 - 1) / $1 ))
  printf "  maxOffsets=%-10s (~%3d triggers): %s  peakRSS=%sGiB\n" "$1" "$BATCHES" "$R" "$(gib "${MEM:-0}")"
}

echo "==== [3] availableNow maxOffsets sweep on REAL S3 ===="
run_mx 100000000 mx_1
run_mx 20000000  mx_5
run_mx 4000000   mx_25

echo "==== [4] CONTINUOUS mode (one long-lived pipeline) ===="
kk rollout restart deploy/vajra-stream >/dev/null 2>&1; wait_ready vajra-stream >/dev/null 2>&1
until kk logs vajra-client 2>/dev/null | grep -q CLIENT_READY; do sleep 2; done
kk exec vajra-client -- sh -c \
  "SPARK_REMOTE=$SR BOOT=$BOOT TOPIC=events N_EVENTS=$N OUT=s3://$BUCKET/co CK=s3://$BUCKET/co_ck RUN_SECS=150 python3 /tmp/wagg_co.py" >/tmp/eks_co.log 2>&1 &
COPID=$!
T0=$(date +%s)
for i in $(seq 1 30); do
  sleep 8
  TOT=$(kk exec vajra-client -- sh -c "SPARK_REMOTE=$SR python3 - <<PY
from pyspark.sql import SparkSession, functions as F
s=SparkSession.builder.remote('$SR').getOrCreate()
try:
  print(s.read.parquet('s3://$BUCKET/co').agg(F.sum('count')).collect()[0][0] or 0)
except Exception: print(0)
PY" 2>/dev/null | tail -1)
  EL=$(( $(date +%s) - T0 )); echo "  continuous t=${EL}s processed=${TOT:-0}"
  [ "${TOT:-0}" -ge 99000000 ] 2>/dev/null && { echo "  continuous drained ~$N at ~${EL}s => $(awk "BEGIN{printf \"%.2f\", $N/$EL/1e6}")M ev/s"; break; }
done
wait $COPID 2>/dev/null

echo ""; echo "######## MICRO-BATCH vs CONTINUOUS on REAL S3 (N=$N) ########"
echo "If bigger maxOffsets (fewer S3 commits) is much faster => S3-commit micro-batch tax is REAL => continuous wins."
echo "If flat (like MinIO) => tax is negligible even on real S3 => continuous-dataflow is NOT the throughput lever."
echo "Teardown: run scripts/aws_eks_teardown.sh (or eksctl delete cluster --name vajra-stream-ht --region $REGION --force --wait) — NEVER interrupt."
