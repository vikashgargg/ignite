#!/usr/bin/env bash
# T2/kind distributed shuffle-coalescer A/B — the prod-grade gate BEFORE any EKS spend. REAL pods + REAL
# pod-to-pod Flight shuffle + MinIO S3 (the distributed sink's object store), same manifests as EKS. Runs
# the distributed windowed-agg twice: coalescing OFF (VAJRA_SHUFFLE_BATCH_ROWS=0) then ON (16384). Asserts
# pods come up (no ImagePullBackOff — what killed the last EKS run), counts EXACT OFF==ON (correctness), and
# shuffle_send_batches DROPS (mechanism). FREE. Assumes `TAG=shufcoal3 scripts/kind_up.sh` loaded the image.
# Usage: N=10000000 TAG=shufcoal3 scripts/kind_shuffle_coalesce_ab.sh
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${N:-2000000}"; KEYS="${KEYS:-1000}"; PARTS="${PARTS:-2}"; TAG="${TAG:-shufcoal3}"; NS=stream; CTX="${CTX:-kind-vajra-kind}"
kk() { kubectl --context "$CTX" -n "$NS" "$@"; }
scale_kind() {  # fit the kind Docker VM: shrink cpu/mem for BOTH requests AND limits (request<=limit!)
  sed -E -e 's/cpu: "1[0-9]"/cpu: "1"/g' -e 's/cpu: "[3-9]"/cpu: "1"/g' \
         -e 's/memory: "2[0-9]Gi"/memory: "2Gi"/g' -e 's/memory: "1[0-9]Gi"/memory: "1500Mi"/g' \
         -e 's/"cpu":"3"/"cpu":"1"/g' \
         -e 's/"memory":"4Gi"/"memory":"1Gi"/g' -e 's/"memory":"10Gi"/"memory":"1500Mi"/g'
}
kubectl --context "$CTX" get ns "$NS" >/dev/null 2>&1 || kubectl --context "$CTX" create ns "$NS"
MINIO_EP="http://minio.$NS.svc.cluster.local:9000"

echo "==== [1] Kafka + MinIO + produce $N ===="
scale_kind < k8s/stream/kafka.yaml | kk apply -f -
kk apply -f k8s/kind/minio.yaml
kk wait --for=condition=available --timeout=300s deployment/kafka deployment/minio
until kk logs job/minio-mkbucket 2>/dev/null | grep -q BUCKET_READY; do sleep 3; done; echo "minio bucket ready"
KPOD=$(kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')
CUR=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic events 2>/dev/null | awk -F: '{s+=$3} END{print s}')
PARTCHK=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic events 2>/dev/null | grep -c 'Partition:')
if [ "${CUR:-0}" != "$N" ] || [ "${PARTCHK:-0}" != "$PARTS" ]; then
  kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic events >/dev/null 2>&1
  # wait for the topic to be FULLY gone before re-producing (delete is async — else stale data races)
  for i in $(seq 1 30); do
    kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -qx events || break
    sleep 2
  done
  kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic events --partitions "$PARTS" --replication-factor 1 >/dev/null 2>&1
  kk delete job producer --ignore-not-found >/dev/null 2>&1
  sed -e "s|N_EVENTS, value: \"[0-9]*\"|N_EVENTS, value: \"$N\"|" -e "s|N_PARTS, value: \"[0-9]*\"|N_PARTS, value: \"$PARTS\"|" k8s/stream/producer-job.yaml | scale_kind | kk apply -f -
  kk wait --for=condition=complete --timeout=1200s job/producer && kk logs job/producer | grep -a PRODUCED
fi
echo "TOPIC events=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic events 2>/dev/null | awk -F: '{s+=$3} END{print s}')"

run() { # $1=rows $2=label
  local ROWS="$1" LABEL="$2"
  # inject MinIO S3 env + shuffle-batch-rows into the worker pod template (after VAJRA_WM_PROF) + driver
  # NOTE: the window runs on WORKERS, so VAJRA_COMPLETE_ON_END (bounded end-flush) + MinIO S3 env MUST be
  # on the worker template, not just the driver — else windows accumulate state but never emit (no output).
  local EXTRA='{"name":"VAJRA_WM_PROF","value":"1"},{"name":"VAJRA_COMPLETE_ON_END","value":"1"},{"name":"AWS_ENDPOINT","value":"'"$MINIO_EP"'"},{"name":"AWS_ENDPOINT_URL","value":"'"$MINIO_EP"'"},{"name":"AWS_ACCESS_KEY_ID","value":"minioadmin"},{"name":"AWS_SECRET_ACCESS_KEY","value":"minioadmin"},{"name":"AWS_ALLOW_HTTP","value":"true"},{"name":"AWS_REGION","value":"us-east-1"},{"name":"VAJRA_SHUFFLE_BATCH_ROWS","value":"'"$ROWS"'"}'
  sed -e "s|__ECR__/vajra:bf6|vajra:$TAG|g" -e 's/imagePullPolicy: Always/imagePullPolicy: IfNotPresent/g' \
      -e "s|{\"name\":\"VAJRA_WM_PROF\",\"value\":\"1\"}|$EXTRA|" \
      k8s/stream/vajra-stream-dist.yaml | scale_kind | kk apply -f - >/dev/null
  kk set env deploy/vajra-driver AWS_REGION=us-east-1 AWS_ENDPOINT="$MINIO_EP" AWS_ENDPOINT_URL="$MINIO_EP" \
     AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_ALLOW_HTTP=true \
     SAIL_CLUSTER__WORKER_TASK_SLOTS="${SLOTS:-8}" \
     VAJRA_COMPLETE_ON_END=1 VAJRA_SHUFFLE_BATCH_ROWS="$ROWS" >/dev/null
  if ! kk wait --for=condition=available --timeout=240s deployment/vajra-driver >/dev/null 2>&1; then
    echo "  [$LABEL] DRIVER FAILED TO START:"; kk get pods -o wide --no-headers | grep -viE 'kafka|minio|producer' | sed -E 's/[0-9]{12}/<ACCT>/g'
    return 1
  fi
  kk apply -f k8s/stream/vajra-client.yaml >/dev/null 2>&1
  kk wait --for=condition=ready --timeout=200s pod/vajra-client >/dev/null 2>&1
  until kk logs vajra-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
  kk cp scripts/stream_windowed_agg.py vajra-client:/tmp/wagg.py 2>/dev/null
  local SR="sc://vajra-driver.$NS.svc.cluster.local:50051" BOOT="kafka.$NS.svc.cluster.local:9092"
  local R; R=$(kk exec vajra-client -- sh -c \
    "SPARK_REMOTE=$SR BOOT=$BOOT TOPIC=events N_EVENTS=$N MAXOFFSETS=500000 OUT=s3://vajra/$LABEL CK=s3://vajra/${LABEL}_ck python3 /tmp/wagg.py" 2>&1 | grep -aoE 'VAJRA_WAGG.*' | tail -1)
  sleep 11
  local SB=0
  for p in $(kk get pods --no-headers 2>/dev/null | grep -E 'sail-worker' | awk '{print $1}'); do
    local L; L=$(kk logs "$p" --tail=400 2>/dev/null | grep -aoE 'shuffle_send_batches=[0-9]+' | tail -1 | cut -d= -f2)
    [ -n "$L" ] && SB=$((SB + L))
  done
  echo "  [$LABEL rows=$ROWS] $R"
  echo "  [$LABEL] shuffle_send_batches=$SB"
  kk delete -f k8s/stream/vajra-stream-dist.yaml --ignore-not-found >/dev/null 2>&1; sleep 3
}

echo "==== [2] A/B on real kind pods: OFF then ON ===="
run 0 off
run 16384 on
echo ""; echo "######## KIND T2 A/B ######## (counts EXACT off==on = correctness; on batches << off = mechanism)"
