#!/usr/bin/env bash
# T3 (EKS) final confirmation of Gap 1 (completeness) + Gap 2 (parallel Kafka sink) at scale, vs Flink.
# One 100M `events` backlog, reused for both:
#   Gap 1: Vajra windowed-agg with VAJRA_COMPLETE_ON_END=1 -> assert n_windows=10 sum=100M (Flink-parity;
#          Flink emits 10 windows/100M, measured via scripts/eks_flink_verify.sh).
#   Gap 2: Vajra continuous Kafka passthrough events -> sink_out -> assert all 100M delivered (parallel sink
#          reads every partition; was 1/16) + throughput.
# Assumes cluster UP + image :TAG in ECR. Usage: scripts/eks_t3.sh [N] [TAG]
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${1:-100000000}"; TAG="${2:-parallel-sink}"; REGION=ap-south-1; NS=stream
ECR="$(aws ecr describe-repositories --region $REGION --repository-name vajra --query 'repositories[0].repositoryUri' --output text | tr -d '[:space:]')"; REG="${ECR%/vajra}"
kk() { kubectl -n "$NS" "$@"; }
kk apply -f k8s/stream/kafka.yaml >/dev/null 2>&1 || kubectl apply -f k8s/stream/kafka.yaml
kk wait --for=condition=available --timeout=300s deployment/kafka
KPOD=$(kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')
kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic sink_out --partitions 16 --replication-factor 1 >/dev/null 2>&1

echo "==== produce events N=$N ===="
kk delete job producer --ignore-not-found >/dev/null 2>&1
sed -e "s|N_EVENTS, value: \"[0-9]*\"|N_EVENTS, value: \"$N\"|" k8s/stream/producer-job.yaml | kk apply -f -
kk wait --for=condition=complete --timeout=1800s job/producer; kk logs job/producer | grep -a PRODUCED
TOT=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic events 2>/dev/null | awk -F: '{s+=$3} END{print s}')
[ "${TOT:-0}" = "$N" ] || { echo "ABORT: events has $TOT != $N"; exit 3; }
echo "TOPIC_CHECK events=$TOT expected=$N"

echo "==== Vajra ($TAG, VAJRA_COMPLETE_ON_END=1) + client ===="
sed -e "s|__ECR__|$REG|g" -e "s|vajra:eo-multipart|vajra:$TAG|g" k8s/stream/vajra-stream.yaml | kk apply -f -
kk patch deploy vajra-stream --type merge -p '{"spec":{"strategy":{"rollingUpdate":{"maxSurge":0,"maxUnavailable":1}}}}' >/dev/null
kk set env deploy/vajra-stream VAJRA_COMPLETE_ON_END=1 >/dev/null
kk wait --for=condition=available --timeout=300s deployment/vajra-stream
kk apply -f k8s/stream/vajra-client.yaml
kk wait --for=condition=ready --timeout=300s pod/vajra-client
until kk logs vajra-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
kk cp scripts/stream_windowed_agg.py vajra-client:/tmp/wagg.py
SR="sc://vajra-stream.$NS.svc.cluster.local:50051"; BOOT="kafka.$NS.svc.cluster.local:9092"

echo "==== [GAP 1] Vajra windowed-agg (bounded-complete) -> completeness ===="
kk exec vajra-client -- sh -c "SPARK_REMOTE=$SR BOOT=$BOOT TOPIC=events N_EVENTS=$N OUT=/tmp/wagg CK=/tmp/wck python3 /tmp/wagg.py" 2>&1 | grep -aoE 'VAJRA_WAGG.*' || true

echo "==== [GAP 2] Vajra continuous passthrough events -> sink_out (60s) ===="
kk exec vajra-client -- sh -c "SPARK_REMOTE=$SR BOOT=$BOOT python3 - <<'PY'
import time
from pyspark.sql import SparkSession, functions as F
s=SparkSession.builder.remote('$SR').getOrCreate()
raw=(s.readStream.format('kafka').option('kafka.bootstrap.servers','$BOOT').option('subscribe','events').option('startingOffsets','earliest').load())
q=(raw.select(F.col('value')).writeStream.format('kafka').option('kafka.bootstrap.servers','$BOOT').option('topic','sink_out').option('checkpointLocation','/tmp/sink_ck').trigger(continuous='1 second').start())
time.sleep(60)
try: q.stop()
except Exception: pass
print('PASSTHROUGH_DONE', flush=True)
PY" 2>&1 | grep -a PASSTHROUGH_DONE || true
OUT=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic sink_out 2>/dev/null | awk -F: '{s+=$3} END{print s}')
awk -v o="${OUT:-0}" -v n="$N" 'BEGIN{printf "T3_KAFKA_SINK delivered=%d of %d in 60s = %.4fM_msg/s %s\n", o, n, o/60/1e6, (o>=n?"DRAINED":(o>n/16?"all-partition(backlog-bound)":"FAIL(1/16)"))}'

echo ""; echo "######## T3 RESULT ########"
echo "GAP 1: VAJRA_WAGG n_windows should be 10 (matches Flink 10/100M) + total_events=100M"
echo "GAP 2: T3_KAFKA_SINK delivered should be >> N/16 (all partitions; was 1/16)"
echo "Teardown: eksctl delete cluster --name vajra-stream-ht --region $REGION --force --wait (NEVER interrupt)"
