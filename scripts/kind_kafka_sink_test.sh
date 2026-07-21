#!/usr/bin/env bash
# T2 (kind) end-to-end test for the PARALLEL Kafka sink (Gap 2). Real k8s: pre-load sink_in with N, run the
# Zelox CONTINUOUS Kafka->Kafka passthrough (sink_in -> sink_out), then VERIFY all N were delivered (parallel
# sink reads every partition, not just partition 0) + report throughput. Resources scaled for the kind VM.
# Self-checking. Usage: TAG=parallel-sink N=2000000 RUN=30 scripts/kind_kafka_sink_test.sh
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
TAG="${TAG:-parallel-sink}"; N="${N:-2000000}"; EPMS="${EPMS:-1000}"; RUN="${RUN:-30}"; NS=stream; CTX=kind-zelox-kind
kk() { kubectl --context "$CTX" -n "$NS" "$@"; }
kubectl --context "$CTX" get ns "$NS" >/dev/null 2>&1 || kubectl --context "$CTX" create ns "$NS"
scale_kind() { sed -E -e 's/cpu: "1[0-9]"/cpu: "1"/g' -e 's/cpu: "[6-9]"/cpu: "1"/g' -e 's/memory: "2[0-9]Gi"/memory: "2Gi"/g' -e 's/memory: "1[0-9]Gi"/memory: "1500Mi"/g' -e 's/"--workers", "4"/"--workers", "2"/g'; }

echo "==== [1] Kafka + topics sink_in/sink_out (16 part) ===="
scale_kind < k8s/stream/kafka.yaml | kk apply -f -
kk wait --for=condition=available --timeout=300s deployment/kafka
KPOD=$(kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')
for t in sink_in sink_out; do kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "$t" --partitions 16 --replication-factor 1 >/dev/null 2>&1; done

echo "==== [2] pre-load sink_in with N=$N ===="
kk delete job producer --ignore-not-found >/dev/null 2>&1
PJOB=$(sed -e "s|N_EVENTS, value: \"[0-9]*\"|N_EVENTS, value: \"$N\"|" -e "s|TOPIC, value: \"events\"|TOPIC, value: \"sink_in\"|" k8s/stream/producer-job.yaml | scale_kind)
echo "$PJOB" | grep -q "sink_in" || { echo "ABORT: producer TOPIC sed did not apply"; exit 3; }
echo "$PJOB" | kk apply -f -
kk wait --for=condition=complete --timeout=900s job/producer && kk logs job/producer | grep -a PRODUCED
TOT=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic sink_in 2>/dev/null | awk -F: '{s+=$3} END{print s}')
[ "${TOT:-0}" = "$N" ] || { echo "ABORT: sink_in has $TOT != $N"; exit 3; }
echo "TOPIC_CHECK sink_in=$TOT expected=$N"

echo "==== [3] Zelox ($TAG) + client ===="
ECR="$(aws ecr describe-repositories --region ap-south-1 --repository-name zelox --query 'repositories[0].repositoryUri' --output text | tr -d '[:space:]')"; REG="${ECR%/zelox}"
sed -e "s#__ECR__/zelox:eo-multipart#zelox:$TAG#g" k8s/stream/zelox-stream.yaml | scale_kind | kk apply -f -
kk patch deploy zelox-stream --type=json -p='[{"op":"add","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"Never"}]' >/dev/null 2>&1
kk wait --for=condition=available --timeout=300s deployment/zelox-stream
scale_kind < k8s/stream/zelox-client.yaml | kk apply -f -
kk wait --for=condition=ready --timeout=300s pod/zelox-client
until kk logs zelox-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done

echo "==== [4] continuous Kafka passthrough sink_in -> sink_out (${RUN}s) ===="
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

echo "==== [5] VERIFY all delivered (parallel sink = every partition) ===="
OUT=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic sink_out 2>/dev/null | awk -F: '{s+=$3} END{print s}')
awk -v o="${OUT:-0}" -v n="$N" -v r="$RUN" 'BEGIN{printf "T2_KAFKA_SINK delivered=%d of %d = %.4fM_msg/s %s\n", o, n, o/r/1e6, (o>=n?"PASS(all delivered)":(o>n/16?"partial":"FAIL(<=1/16 = single-partition bug)"))}'
