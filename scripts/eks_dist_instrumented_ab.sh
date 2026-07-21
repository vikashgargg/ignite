#!/usr/bin/env bash
# Flink-class DISTRIBUTED instrumented A/B — localize WHERE Zelox lags Flink in distributed streaming.
# Zelox runs distributed (ZELOX_DISTRIBUTED_STREAM=1: source+exchange+window across worker pods, Flight
# shuffle between them) with the :distprof image = per-pod WM_PROF_PROC dumper + Flight send/recv timing.
# We collect the per-pod stage breakdown from EVERY pod (driver + all workers) so the 3.6x is attributed
# to a concrete stage (source_read / from_json / exchange / shuffle_send / shuffle_recv / finalize), not a
# black box. Flink runs the same windowed-agg for the ratio. Writes to a per-run S3 bucket, torn down $0.
# Assumes cluster UP + image :TAG in ECR. Usage: scripts/eks_dist_instrumented_ab.sh [N] [TAG]
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${1:-100000000}"; TAG="${2:-distprof}"; REGION=ap-south-1; NS=stream
BUCKET="zelox-distprof-$(date +%s)"
ECR="$(aws ecr describe-repositories --region $REGION --repository-name zelox --query 'repositories[0].repositoryUri' --output text | tr -d '[:space:]')"; REG="${ECR%/zelox}"
kk() { kubectl -n "$NS" "$@"; }
gib() { awk -v b="$1" 'BEGIN{printf "%.2f", b/1073741824}'; }
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

echo "==== [2] ZELOX DISTRIBUTED ($TAG, WM_PROF per-pod) ===="
# distprof image; distributed + credit + WM_PROF already in the dist manifest (driver + worker template).
sed -e "s|__ECR__/zelox:bf6|$REG/zelox:$TAG|g" k8s/stream/zelox-stream-dist.yaml | kk apply -f -
kk set env deploy/zelox-driver AWS_REGION="$REGION" ZELOX_COMPLETE_ON_END=1 >/dev/null
kk wait --for=condition=available --timeout=300s deployment/zelox-driver
kk apply -f k8s/stream/zelox-client.yaml
kk wait --for=condition=ready --timeout=300s pod/zelox-client
until kk logs zelox-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
kk cp scripts/stream_windowed_agg.py zelox-client:/tmp/wagg.py
SR="sc://zelox-driver.$NS.svc.cluster.local:50051"; BOOT="kafka.$NS.svc.cluster.local:9092"
echo "-- distributed windowed-agg run --"
VWAGG=$(kk exec zelox-client -- sh -c \
  "SPARK_REMOTE=$SR BOOT=$BOOT TOPIC=events N_EVENTS=$N MAXOFFSETS=4000000 OUT=s3://$BUCKET/v CK=s3://$BUCKET/v_ck python3 /tmp/wagg.py" 2>&1 | grep -aoE 'ZELOX_WAGG.*' | tail -1)
echo "ZELOX: $VWAGG"

echo "==== [3] PER-POD WM_PROF breakdown (driver + ALL workers) ===="
# The distprof dumper logs WM_PROF_PROC to stderr on every pod every 10s. Grab the LAST line per pod.
for p in $(kk get pods -o name | grep -E 'zelox'); do
  LINE=$(kk logs "$p" --tail=400 2>/dev/null | grep -aE 'WM_PROF_PROC|WM_PROF\[' | tail -1)
  [ -n "$LINE" ] && echo "  ${p#pod/}: $LINE"
done
DPOD=$(kk get pod -l app=zelox-driver -o jsonpath='{.items[0].metadata.name}')
DMEM=$(kk exec "$DPOD" -- cat /sys/fs/cgroup/memory.peak 2>/dev/null)
echo "  driver peakRSS=$(gib "${DMEM:-0}")GiB"
kk delete -f k8s/stream/zelox-stream-dist.yaml --ignore-not-found >/dev/null 2>&1

echo "==== [4] FLINK baseline (same windowed-agg, for the ratio) ===="
kk apply -f k8s/stream/flink-session.yaml
kk wait --for=condition=available --timeout=600s deployment/flink-jm || echo WARN
kk wait --for=condition=available --timeout=600s deployment/flink-tm || echo WARN
kk create configmap flink-sql --from-file=flink-sql.sql=k8s/stream/flink-sql.sql --dry-run=client -o yaml | kk apply -f - >/dev/null 2>&1
kk delete job flink-runner --ignore-not-found >/dev/null 2>&1
kk apply -f k8s/stream/flink-runner-job.yaml
kk wait --for=condition=complete --timeout=600s job/flink-runner 2>/dev/null || echo "WARN flink job slow"
FWAGG=$(kk logs job/flink-runner 2>/dev/null | grep -aoE 'FLINK_WAGG.*' | tail -1)
kk delete -f k8s/stream/flink-session.yaml --ignore-not-found >/dev/null 2>&1

echo ""; echo "######## DISTRIBUTED INSTRUMENTED A/B (N=$N) ########"
echo "ZELOX: $VWAGG"
echo "FLINK: $FWAGG"
echo "=> Read the PER-POD WM_PROF_PROC above: the stage with the largest cpu-ms across the worker pods is"
echo "   where we lag. shuffle_send/shuffle_recv large => Flight IPC transport is the gap (Arroyo Shuffle-Edge"
echo "   batch+pool / zero-copy is the fix). source_read/from_json large => compute; exchange_wait => backpressure."
echo "Teardown: scripts/aws_eks_teardown.sh (NEVER interrupt)."
