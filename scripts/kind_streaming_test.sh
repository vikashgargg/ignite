#!/usr/bin/env bash
# T2 tier gate (docs/design/three-tier-sdlc.md): run the streaming stack on the LOCAL kind cluster — REAL
# Kubernetes (scheduling, real Kafka broker, service networking, the vajra image) — to catch k8s-specific
# issues BEFORE EKS, for FREE. Deploys the SAME manifests as EKS with resource REQUESTS scaled to a laptop
# (8 CPU / ~7.75 GiB Docker VM); T2 proves topology/scheduling/correctness, not scale (that is T3/EKS).
# First target: the final-window COMPLETENESS gap (EKS: Vajra 9 windows / Flink 10). Self-checking.
# Usage: TAG=realtime-fix N=2000000 EPMS=100 scripts/kind_streaming_test.sh
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
TAG="${TAG:-realtime-fix}"; N="${N:-2000000}"; EPMS="${EPMS:-100}"; NS=stream; CTX=kind-vajra-kind
kk() { kubectl --context "$CTX" -n "$NS" "$@"; }
kubectl --context "$CTX" get ns "$NS" >/dev/null 2>&1 || kubectl --context "$CTX" create ns "$NS"
# Scale resource requests + vajra workers down to fit the kind Docker VM (keeps the SAME manifests/topology).
scale_kind() {
  sed -E \
    -e 's/cpu: "1[0-9]"/cpu: "1"/g' -e 's/cpu: "[6-9]"/cpu: "1"/g' \
    -e 's/memory: "2[0-9]Gi"/memory: "2Gi"/g' -e 's/memory: "1[0-9]Gi"/memory: "1500Mi"/g' \
    -e 's/"--workers", "4"/"--workers", "2"/g'
}

echo "==== [1] Kafka (scaled) ===="
scale_kind < k8s/stream/kafka.yaml | kk apply -f -
kk wait --for=condition=available --timeout=300s deployment/kafka

echo "==== [2] produce N=$N (EPMS=$EPMS, 16-part) ===="
kk delete job producer --ignore-not-found >/dev/null 2>&1
# NOTE: flow-style YAML is `name: KEY, value: "V"` (no quote after KEY) — the sed pattern MUST match that.
PJOB=$(sed -e "s|N_EVENTS, value: \"[0-9]*\"|N_EVENTS, value: \"$N\"|" -e "s|EVENTS_PER_MS, value: \"[0-9]*\"|EVENTS_PER_MS, value: \"$EPMS\"|" k8s/stream/producer-job.yaml | scale_kind)
# Self-check: the substitution actually took (else the test would silently use the default event density).
echo "$PJOB" | grep -q "N_EVENTS, value: \"$N\"" && echo "$PJOB" | grep -q "EVENTS_PER_MS, value: \"$EPMS\"" || { echo "ABORT: producer-job sed did not apply (N=$N EPMS=$EPMS)"; exit 3; }
echo "$PJOB" | kk apply -f -
kk wait --for=condition=complete --timeout=900s job/producer && kk logs job/producer | grep -a PRODUCED
KPOD=$(kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')
TOT=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic events 2>/dev/null | awk -F: '{s+=$3} END{print s}')
echo "TOPIC_CHECK events=$TOT expected=$N"
[ "${TOT:-0}" = "$N" ] || { echo "ABORT: producer self-check failed (events=$TOT != $N)"; exit 3; }

echo "==== [3] Vajra ($TAG, scaled) + client ===="
ECR_DUMMY=local  # image is loaded into kind as vajra:$TAG; replace the whole ref
sed -e "s#__ECR__/vajra:eo-multipart#vajra:$TAG#g" -e 's/imagePullPolicy: Always/imagePullPolicy: IfNotPresent/g' k8s/stream/vajra-stream.yaml | scale_kind | kk apply -f -
# ensure the loaded local image is used (never pulled) + bounded-complete flush (Flink-parity)
kk patch deploy vajra-stream --type=json -p='[{"op":"add","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"Never"}]' >/dev/null 2>&1
[ "${COMPLETE:-1}" = "1" ] && kk set env deploy/vajra-stream VAJRA_COMPLETE_ON_END=1 >/dev/null 2>&1
kk wait --for=condition=available --timeout=300s deployment/vajra-stream
scale_kind < k8s/stream/vajra-client.yaml | kk apply -f -
kk wait --for=condition=ready --timeout=300s pod/vajra-client
until kk logs vajra-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
kk cp scripts/stream_windowed_agg.py vajra-client:/tmp/wagg.py

echo "==== [4] windowed-agg (availableNow + bounded-complete) -> local sink; assert completeness ===="
SR="sc://vajra-stream.$NS.svc.cluster.local:50051"
kk exec vajra-client -- sh -c \
  "SPARK_REMOTE=$SR BOOT=kafka.$NS.svc.cluster.local:9092 TOPIC=events N_EVENTS=$N OUT=/tmp/wagg CK=/tmp/ck python3 /tmp/wagg.py" 2>&1 | grep -aoE 'VAJRA_WAGG.*' || true

echo "==== [5] assert n_windows + sum (Flink-parity completeness on real k8s) ===="
kk exec vajra-client -- sh -c "SPARK_REMOTE=$SR N=$N EPMS=$EPMS python3 - <<'PY'
import os, math
from pyspark.sql import SparkSession, functions as F
s=SparkSession.builder.remote(os.environ['SPARK_REMOTE']).getOrCreate()
N=int(os.environ['N']); EPMS=int(os.environ['EPMS'])
exp_win=math.floor(((N-1)//EPMS)/10000)+1
d=s.read.parquet('/tmp/wagg').where(F.col('k')>=0)
nwin=d.select('window').distinct().count(); tot=d.agg(F.sum('count')).collect()[0][0]
ok=(nwin==exp_win) and (tot==N)
print(f'T2_COMPLETENESS n_windows={nwin} sum_count={tot} expected_windows={exp_win} expected_sum={N} {\"PASS\" if ok else \"FAIL\"}')
PY" 2>&1 | grep -a T2_COMPLETENESS
