#!/usr/bin/env bash
# T3 (EKS) confirmation of the PARALLEL Kafka sink (Gap 2) at scale: pre-load sink_in with N, run the Zelox
# CONTINUOUS Kafka->Kafka passthrough (sink_in -> sink_out), VERIFY all N delivered (parallel sink reads every
# partition) + report sustained throughput. Assumes cluster UP + image :TAG in ECR.
# Usage: scripts/eks_kafka_sink_test.sh [N] [TAG] [RUN]
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${1:-100000000}"; TAG="${2:-parallel-sink}"; RUN="${3:-60}"; REGION=ap-south-1; NS=stream
ECR="$(aws ecr describe-repositories --region $REGION --repository-name zelox --query 'repositories[0].repositoryUri' --output text | tr -d '[:space:]')"; REG="${ECR%/zelox}"
kk() { kubectl -n "$NS" "$@"; }
kk apply -f k8s/stream/kafka.yaml >/dev/null 2>&1 || kubectl apply -f k8s/stream/kafka.yaml
kk wait --for=condition=available --timeout=300s deployment/kafka
KPOD=$(kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')
for t in sink_in sink_out; do kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "$t" --partitions 16 --replication-factor 1 >/dev/null 2>&1; done

echo "==== pre-load sink_in with N=$N ===="
kk delete job producer --ignore-not-found >/dev/null 2>&1
sed -e "s|N_EVENTS, value: \"[0-9]*\"|N_EVENTS, value: \"$N\"|" -e "s|TOPIC, value: \"events\"|TOPIC, value: \"sink_in\"|" k8s/stream/producer-job.yaml | kk apply -f -
kk wait --for=condition=complete --timeout=1800s job/producer; kk logs job/producer | grep -a PRODUCED
TOT=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic sink_in 2>/dev/null | awk -F: '{s+=$3} END{print s}')
[ "${TOT:-0}" = "$N" ] || { echo "ABORT: sink_in has $TOT != $N"; exit 3; }
echo "TOPIC_CHECK sink_in=$TOT expected=$N"

echo "==== Zelox ($TAG) + client ===="
sed -E -e "s|__ECR__/zelox:[A-Za-z0-9._-]+|$REG/zelox:$TAG|g" -e "s|__ECR__|$REG|g" k8s/stream/zelox-stream.yaml | kk apply -f -
kk patch deploy zelox-stream --type merge -p '{"spec":{"strategy":{"rollingUpdate":{"maxSurge":0,"maxUnavailable":1}}}}' >/dev/null
kk wait --for=condition=available --timeout=300s deployment/zelox-stream
kk apply -f k8s/stream/zelox-client.yaml
kk wait --for=condition=ready --timeout=300s pod/zelox-client
until kk logs zelox-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done

echo "==== continuous Kafka passthrough sink_in -> sink_out (${RUN}s) ===="
SR="sc://zelox-stream.$NS.svc.cluster.local:50051"; BOOT="kafka.$NS.svc.cluster.local:9092"
kk exec zelox-client -- sh -c "SPARK_REMOTE=$SR BOOT=$BOOT RUN=$RUN python3 - <<'PY'
import os, time
from pyspark.sql import SparkSession, functions as F
s=SparkSession.builder.remote(os.environ['SPARK_REMOTE']).getOrCreate()
BOOT=os.environ['BOOT']
raw=(s.readStream.format('kafka').option('kafka.bootstrap.servers',BOOT).option('subscribe','sink_in').option('startingOffsets','earliest').load())
q=(raw.select(F.col('value')).writeStream.format('kafka').option('kafka.bootstrap.servers',BOOT).option('topic','sink_out').option('checkpointLocation','/tmp/sink_ck').trigger(continuous='1 second').start())
time.sleep(int(os.environ['RUN']))
try: q.stop()
except Exception: pass
print('PASSTHROUGH_DONE', flush=True)
PY" 2>&1 | grep -a PASSTHROUGH_DONE || true

OUT=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic sink_out 2>/dev/null | awk -F: '{s+=$3} END{print s}')
awk -v o="${OUT:-0}" -v n="$N" -v r="$RUN" 'BEGIN{printf "T3_KAFKA_SINK delivered=%d of %d = %.4fM_msg/s %s\n", o, n, o/r/1e6, (o>=n?"PASS(all delivered, DRAINED)":(o>n/16?"backlog-bound(all-partition)":"FAIL(1/16 single-partition)"))}'
echo "Teardown: eksctl delete cluster --name zelox-stream-ht --region $REGION --force --wait"
