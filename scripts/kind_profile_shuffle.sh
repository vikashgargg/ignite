#!/usr/bin/env bash
# T2/kind CPU PROFILE of the distributed Flight shuffle — the definitive hotspot on REAL cross-pod pods
# (what local loopback can't show). Deploys Kafka+MinIO+distributed zelox with ZELOX_PPROF on the WORKER
# template (workers do the shuffle), runs the windowed-agg a few times to fill the profiler window, then
# pulls each worker's folded stacks and prints the top on-CPU functions. FREE.
# Assumes: TAG=pprof2 scripts/kind_up.sh (cluster up + zelox:pprof2 loaded). Usage: N=20000000 TAG=pprof2 scripts/kind_profile_shuffle.sh
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${N:-20000000}"; KEYS="${KEYS:-1000}"; PARTS="${PARTS:-4}"; TAG="${TAG:-pprof2}"; NS=stream; CTX="${CTX:-kind-zelox-kind}"
PPROF_SECS="${PPROF_SECS:-260}"; RUNS="${RUNS:-4}"
kk() { kubectl --context "$CTX" -n "$NS" "$@"; }
scale_kind() {
  sed -E -e 's/cpu: "1[0-9]"/cpu: "1"/g' -e 's/cpu: "[3-9]"/cpu: "1"/g' \
         -e 's/memory: "2[0-9]Gi"/memory: "2Gi"/g' -e 's/memory: "1[0-9]Gi"/memory: "1500Mi"/g' \
         -e 's/"cpu":"3"/"cpu":"1"/g' \
         -e 's/"memory":"4Gi"/"memory":"1Gi"/g' -e 's/"memory":"10Gi"/"memory":"1500Mi"/g'
}
kubectl --context "$CTX" get ns "$NS" >/dev/null 2>&1 || kubectl --context "$CTX" create ns "$NS"
MINIO_EP="http://minio.$NS.svc.cluster.local:9000"

echo "==== [1] Kafka + MinIO + produce $N ($PARTS parts) ===="
scale_kind < k8s/stream/kafka.yaml | kk apply -f -
kk apply -f k8s/kind/minio.yaml
kk wait --for=condition=available --timeout=300s deployment/kafka deployment/minio
until kk logs job/minio-mkbucket 2>/dev/null | grep -q BUCKET_READY; do sleep 3; done; echo "minio ready"
KPOD=$(kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')
CUR=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic events 2>/dev/null | awk -F: '{s+=$3} END{print s}')
PARTCHK=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic events 2>/dev/null | grep -c 'Partition:')
if [ "${CUR:-0}" != "$N" ] || [ "${PARTCHK:-0}" != "$PARTS" ]; then
  kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic events >/dev/null 2>&1
  for i in $(seq 1 30); do kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -qx events || break; sleep 2; done
  kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic events --partitions "$PARTS" --replication-factor 1 >/dev/null 2>&1
  kk delete job producer --ignore-not-found >/dev/null 2>&1
  sed -e "s|N_EVENTS, value: \"[0-9]*\"|N_EVENTS, value: \"$N\"|" -e "s|N_PARTS, value: \"[0-9]*\"|N_PARTS, value: \"$PARTS\"|" k8s/stream/producer-job.yaml | scale_kind | kk apply -f -
  kk wait --for=condition=complete --timeout=1200s job/producer && kk logs job/producer | grep -a PRODUCED
fi
echo "TOPIC events=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic events 2>/dev/null | awk -F: '{s+=$3} END{print s}')"

echo "==== [2] deploy distributed zelox with ZELOX_PPROF_SECS=$PPROF_SECS on WORKERS ===="
EXTRA='{"name":"ZELOX_WM_PROF","value":"1"},{"name":"ZELOX_COMPLETE_ON_END","value":"1"},{"name":"ZELOX_PPROF_SECS","value":"'"$PPROF_SECS"'"},{"name":"ZELOX_PPROF_OUT","value":"/tmp/prof.folded"},{"name":"AWS_ENDPOINT","value":"'"$MINIO_EP"'"},{"name":"AWS_ENDPOINT_URL","value":"'"$MINIO_EP"'"},{"name":"AWS_ACCESS_KEY_ID","value":"minioadmin"},{"name":"AWS_SECRET_ACCESS_KEY","value":"minioadmin"},{"name":"AWS_ALLOW_HTTP","value":"true"},{"name":"AWS_REGION","value":"us-east-1"},{"name":"ZELOX_SHUFFLE_BATCH_ROWS","value":"16384"}'
sed -e "s|__ECR__/zelox:bf6|zelox:$TAG|g" -e 's/imagePullPolicy: Always/imagePullPolicy: IfNotPresent/g' \
    -e "s|{\"name\":\"ZELOX_WM_PROF\",\"value\":\"1\"}|$EXTRA|" \
    k8s/stream/zelox-stream-dist.yaml | scale_kind | kk apply -f - >/dev/null
kk set env deploy/zelox-driver AWS_REGION=us-east-1 AWS_ENDPOINT="$MINIO_EP" AWS_ENDPOINT_URL="$MINIO_EP" \
   AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_ALLOW_HTTP=true \
   ZELOX_CLUSTER__WORKER_TASK_SLOTS="${SLOTS:-8}" ZELOX_COMPLETE_ON_END=1 ZELOX_SHUFFLE_BATCH_ROWS=16384 >/dev/null
kk wait --for=condition=available --timeout=240s deployment/zelox-driver || { echo "driver failed"; kk get pods -o wide | sed -E 's/[0-9]{12}/<ACCT>/g'; exit 1; }
T0=$(date +%s)
kk apply -f k8s/stream/zelox-client.yaml >/dev/null 2>&1
kk wait --for=condition=ready --timeout=200s pod/zelox-client >/dev/null 2>&1
until kk logs zelox-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
kk cp scripts/stream_windowed_agg.py zelox-client:/tmp/wagg.py 2>/dev/null
SR="sc://zelox-driver.$NS.svc.cluster.local:50051"; BOOT="kafka.$NS.svc.cluster.local:9092"

echo "==== [3] run windowed-agg x$RUNS to fill the profiler window ===="
for r in $(seq 1 "$RUNS"); do
  R=$(kk exec zelox-client -- sh -c "SPARK_REMOTE=$SR BOOT=$BOOT TOPIC=events N_EVENTS=$N MAXOFFSETS=500000 OUT=s3://zelox/prof$r CK=s3://zelox/prof${r}_ck python3 /tmp/wagg.py" 2>&1 | grep -aoE 'ZELOX_WAGG.*' | tail -1)
  echo "  run$r: $R"
done

echo "==== [4] wait for profiler dump (${PPROF_SECS}s from worker start) then pull folded stacks ===="
WORKER=$(kk get pods --no-headers 2>/dev/null | grep -E 'zelox-worker' | awk '{print $1}' | head -1)
while [ "$(( $(date +%s) - T0 ))" -lt "$((PPROF_SECS + 8))" ]; do sleep 5; done
mkdir -p /tmp/kprof; : > /tmp/kprof/all.folded
for p in $(kk get pods --no-headers 2>/dev/null | grep -E 'zelox-worker' | awk '{print $1}'); do
  kk cp "$p:/tmp/prof.folded" "/tmp/kprof/$p.folded" 2>/dev/null && cat "/tmp/kprof/$p.folded" >> /tmp/kprof/all.folded && echo "  pulled $p"
done
echo "==== [5] TOP on-CPU leaf functions (definitive shuffle hotspot) ===="
awk '{n=split($1,a,";"); leaf=a[n]; c[leaf]+=$2; tot+=$2} END{for(k in c) printf "%d\t%s\n", c[k], k; printf "TOTAL\t%d\n", tot}' /tmp/kprof/all.folded | sort -rn | head -30
echo "(folded stacks in /tmp/kprof/*.folded — full flamegraph input)"
