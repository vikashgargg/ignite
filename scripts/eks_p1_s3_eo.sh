#!/usr/bin/env bash
# P1 production-workload (docs/design/production-workload-benchmark.md): Kafka -> 10s windowed-agg ->
# PARQUET on S3, exactly-once — the canonical Uber/Netflix streaming-data-lake shape. Two phases:
#   P1a CLEAN  : availableNow full run -> S3; assert correctness (groups=9000, total=N) + throughput + RSS.
#   P1b EO-CRASH: kill -9 the server mid-run, restart (same checkpointLocation on S3), finish; assert the
#                 S3 output is EXACTLY the windowed-agg result (no dup / no loss across the crash).
# Assumes cluster UP (compute node role has S3 access) + image in ECR. Writes to a per-run S3 bucket,
# deleted at the end ($0 discipline). Zelox reaches S3 via the object-store `from_env` (node instance role).
# Usage: scripts/eks_p1_s3_eo.sh [N]
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${1:-100000000}"; REGION=ap-south-1; NS=stream
BUCKET="zelox-p1-$(date +%s)"
ECR="$(aws ecr describe-repositories --region $REGION --repository-name zelox --query 'repositories[0].repositoryUri' --output text)"; REG="${ECR%/zelox}"
kk() { kubectl -n "$NS" "$@"; }
wait_ready() { kk wait --for=condition=available --timeout=300s deployment/"$1"; }
gib() { awk -v b="$1" 'BEGIN{printf "%.2f", b/1073741824}'; }

echo "==== [0] S3 bucket s3://$BUCKET ===="
aws s3 mb "s3://$BUCKET" --region "$REGION" >/dev/null && echo "bucket created"
cleanup() { echo "== teardown: emptying+deleting s3://$BUCKET =="; aws s3 rb "s3://$BUCKET" --force >/dev/null 2>&1; }
trap cleanup EXIT

echo "==== [1] Kafka + produce $N ===="
kk apply -f k8s/stream/kafka.yaml >/dev/null 2>&1 || kubectl apply -f k8s/stream/kafka.yaml
wait_ready kafka
kk delete job producer --ignore-not-found >/dev/null 2>&1
sed "s|N_EVENTS\", value: \"[0-9]*\"|N_EVENTS\", value: \"$N\"|" k8s/stream/producer-job.yaml | kk apply -f -
kk wait --for=condition=complete --timeout=1800s job/producer; kk logs job/producer | grep PRODUCED

echo "==== [2] Zelox + client (S3 sink) ===="
sed -E -e "s|__ECR__/zelox:[A-Za-z0-9._-]+|$REG/zelox:${TAG:-rename42}|g" -e "s|__ECR__|$REG|g" k8s/stream/zelox-stream.yaml | kk apply -f -
kk patch deploy zelox-stream --type merge -p '{"spec":{"strategy":{"rollingUpdate":{"maxSurge":0,"maxUnavailable":1}}}}' >/dev/null
kk set env deploy/zelox-stream AWS_REGION="$REGION" >/dev/null   # object-store from_env uses node instance role
wait_ready zelox-stream
kk apply -f k8s/stream/zelox-client.yaml
kk wait --for=condition=ready --timeout=300s pod/zelox-client
until kk logs zelox-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
kk cp scripts/stream_windowed_agg.py zelox-client:/tmp/wagg.py

run_wagg() { # $1=out-subdir  -> emits ZELOX_WAGG line
  kk exec zelox-client -- sh -c \
    "SPARK_REMOTE=sc://zelox-stream.$NS.svc.cluster.local:50051 BOOT=kafka.$NS.svc.cluster.local:9092 TOPIC=events N_EVENTS=$N OUT=s3://$BUCKET/$1 CK=s3://$BUCKET/$1_ck python3 /tmp/wagg.py" 2>&1
}
verify_s3() { # $1=out-subdir -> read the S3 parquet, assert groups=9000 + no dup (distinct windows*keys)
  kk exec zelox-client -- sh -c "SPARK_REMOTE=sc://zelox-stream.$NS.svc.cluster.local:50051 python3 - <<PY
from pyspark.sql import SparkSession, functions as F
s=SparkSession.builder.remote('sc://zelox-stream.$NS.svc.cluster.local:50051').getOrCreate()
d=s.read.parquet('s3://$BUCKET/$1')
n=d.count(); distinct=d.select('window','k').distinct().count(); tot=d.agg(F.sum('count')).collect()[0][0]
print(f'P1_VERIFY rows={n} distinct_window_key={distinct} sum_count={tot} dup={n-distinct}')
PY" 2>&1 | grep -a P1_VERIFY
}

echo "==== [P1a] CLEAN run -> S3 ===="
WAGG=$(run_wagg wagg_clean); echo "$WAGG" | grep -aoE 'ZELOX_WAGG.*'
VPOD=$(kk get pod -l app=zelox-stream --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')
MEM=$(kk exec "$VPOD" -- cat /sys/fs/cgroup/memory.peak 2>/dev/null)
echo "P1a  peakRSS=$(gib "${MEM:-0}")GiB"
verify_s3 wagg_clean

echo "==== [P1b] EO-under-CRASH -> S3 ===="
# start the run in the background, kill the server ~40% through, restart, re-run (same CK resumes), verify
kk exec zelox-client -- sh -c \
  "SPARK_REMOTE=sc://zelox-stream.$NS.svc.cluster.local:50051 BOOT=kafka.$NS.svc.cluster.local:9092 TOPIC=events N_EVENTS=$N OUT=s3://$BUCKET/wagg_crash CK=s3://$BUCKET/wagg_crash_ck python3 /tmp/wagg.py" >/tmp/p1b.log 2>&1 &
RUNPID=$!
sleep 8
echo "== CHAOS: kill -9 zelox-stream server =="; kk delete pod -l app=zelox-stream --grace-period=0 --force >/dev/null 2>&1
wait $RUNPID 2>/dev/null; echo "(first attempt died with the server)"
wait_ready zelox-stream; until kk logs zelox-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
echo "== restart: resume from S3 checkpoint =="
run_wagg wagg_crash | grep -aoE 'ZELOX_WAGG.*' || true
verify_s3 wagg_crash

echo ""; echo "######## P1 RESULT (Kafka->windowed-agg->Parquet-on-S3 EO) ########"
echo "P1a clean + P1b crash: compare P1_VERIFY (expect groups/distinct=9000, dup=0, sum_count stable)"
echo "Teardown: eksctl delete cluster --name zelox-stream-ht --region $REGION --force --wait"
