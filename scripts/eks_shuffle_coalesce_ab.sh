#!/usr/bin/env bash
# T3/EKS decisive A/B for the Flight-shuffle COALESCER (lever B). Runs the SAME distributed windowed-agg
# TWICE on one 100M backlog — coalescing OFF (ZELOX_SHUFFLE_BATCH_ROWS=0) then ON (16384) — on a clean,
# unloaded cluster (the local nm_dist_gate is timing-flaky under desktop load; main flakes too). Reports,
# per run: counts (Σ window counts = CORRECTNESS, must match OFF==ON), throughput, and per-pod
# shuffle_send_batches (the MECHANISM: coalescing must cut the ~24k tiny Flight messages ~4×). If counts
# match AND throughput rises AND batches drop, the fix is proven. Writes to a per-run bucket, torn $0.
# Assumes cluster UP + image :TAG in ECR. Usage: scripts/eks_shuffle_coalesce_ab.sh [N] [TAG]
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
# Accept N/TAG via env OR positional (env wins) — avoids the "env ignored -> wrong default tag" foot-gun.
N="${N:-${1:-100000000}}"; TAG="${TAG:-${2:-shufcoal3}}"; REGION=ap-south-1; NS=stream
BUCKET="zelox-coal-$(date +%s)"
ECR="$(aws ecr describe-repositories --region $REGION --repository-name zelox --query 'repositories[0].repositoryUri' --output text | tr -d '[:space:]')"; REG="${ECR%/zelox}"
kk() { kubectl -n "$NS" "$@"; }
aws s3 mb "s3://$BUCKET" --region "$REGION" >/dev/null && echo "bucket s3://$BUCKET"
cleanup() { aws s3 rb "s3://$BUCKET" --force >/dev/null 2>&1; }
trap cleanup EXIT

echo "==== [1] Kafka + produce $N ===="
kk apply -f k8s/stream/kafka.yaml >/dev/null 2>&1 || kubectl apply -f k8s/stream/kafka.yaml
kk wait --for=condition=available --timeout=300s deployment/kafka
KPOD=$(kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')
CUR=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic events 2>/dev/null | awk -F: '{s+=$3} END{print s}')
if [ "${CUR:-0}" != "$N" ]; then
  kk delete job producer --ignore-not-found >/dev/null 2>&1
  sed -e "s|N_EVENTS, value: \"[0-9]*\"|N_EVENTS, value: \"$N\"|" k8s/stream/producer-job.yaml | kk apply -f -
  kk wait --for=condition=complete --timeout=1800s job/producer; kk logs job/producer | grep -a PRODUCED
fi
echo "TOPIC_CHECK events=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic events 2>/dev/null | awk -F: '{s+=$3} END{print s}')"

run_zelox() { # $1=shuffle_batch_rows $2=label -> prints RESULT line + per-pod shuffle batches
  local ROWS="$1" LABEL="$2"
  # Inject ZELOX_SHUFFLE_BATCH_ROWS into BOTH driver env and the worker pod template (coalescing runs in
  # the worker's Flight do_get). Worker template is a JSON string; add the env var after CREDIT.
  sed -e "s|__ECR__/zelox:bf6|$REG/zelox:$TAG|g" \
      -e "s|{\"name\":\"ZELOX_CREDIT_BACKPRESSURE\",\"value\":\"16\"}|{\"name\":\"ZELOX_CREDIT_BACKPRESSURE\",\"value\":\"16\"},{\"name\":\"ZELOX_SHUFFLE_BATCH_ROWS\",\"value\":\"$ROWS\"},{\"name\":\"ZELOX_COMPLETE_ON_END\",\"value\":\"1\"}|g" \
      k8s/stream/zelox-stream-dist.yaml | kk apply -f - >/dev/null
  # cap workers to fit the compute node (16-part -> ~32 tasks; 8 slots/worker -> ~4 workers = fits the
  # single c7g.4xlarge, still real cross-worker shuffle) — avoids the pending-worker churn.
  kk set env deploy/zelox-driver AWS_REGION="$REGION" ZELOX_CLUSTER__WORKER_TASK_SLOTS="${SLOTS:-8}" \
     ZELOX_COMPLETE_ON_END=1 ZELOX_SHUFFLE_BATCH_ROWS="$ROWS" >/dev/null
  kk wait --for=condition=available --timeout=300s deployment/zelox-driver >/dev/null 2>&1
  until kk logs zelox-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
  kk cp scripts/stream_windowed_agg.py zelox-client:/tmp/wagg.py 2>/dev/null
  local SR="sc://zelox-driver.$NS.svc.cluster.local:50051" BOOT="kafka.$NS.svc.cluster.local:9092"
  local R; R=$(kk exec zelox-client -- sh -c \
    "SPARK_REMOTE=$SR BOOT=$BOOT TOPIC=events N_EVENTS=$N MAXOFFSETS=4000000 OUT=s3://$BUCKET/$LABEL CK=s3://$BUCKET/${LABEL}_ck python3 /tmp/wagg.py" 2>&1 | grep -aoE 'ZELOX_WAGG.*' | tail -1)
  echo "  [$LABEL rows=$ROWS] $R"
  # per-pod shuffle_send_batches (mechanism)
  local sb=0
  for p in $(kk get pods --no-headers 2>/dev/null | grep -E 'sail-worker' | awk '{print $1}'); do
    local L; L=$(kk logs "$p" --tail=400 2>/dev/null | grep -aoE 'shuffle_send_batches=[0-9]+' | tail -1 | cut -d= -f2)
    [ -n "$L" ] && sb=$((sb + L))
  done
  echo "  [$LABEL] total shuffle_send_batches across workers = $sb"
  kk delete -f k8s/stream/zelox-stream-dist.yaml --ignore-not-found >/dev/null 2>&1
  kk apply -f k8s/stream/zelox-client.yaml >/dev/null 2>&1
  kk wait --for=condition=ready --timeout=120s pod/zelox-client >/dev/null 2>&1
  until kk logs zelox-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
}

echo "==== [2] client ===="
kk apply -f k8s/stream/zelox-client.yaml
kk wait --for=condition=ready --timeout=300s pod/zelox-client
until kk logs zelox-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done

echo "==== [3] A/B: coalescing OFF then ON ===="
run_zelox 0 off
run_zelox 16384 on

echo ""; echo "######## SHUFFLE-COALESCE A/B (N=$N) ########"
echo "CORRECTNESS: OFF and ON total_events must MATCH. MECHANISM: ON shuffle_send_batches << OFF."
echo "WIN: ON throughput > OFF throughput at equal counts => coalescing closes the distributed gap."
echo "Teardown: scripts/aws_eks_teardown.sh zelox-stream-ht $REGION (NEVER interrupt)."
