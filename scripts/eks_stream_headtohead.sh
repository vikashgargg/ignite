#!/usr/bin/env bash
# Vajra-vs-Flink STREAMING head-to-head on EKS — reproducible orchestration.
#
# Assumes an EKS cluster is already up (k8s/stream/eks-stream-cluster.yaml) with
# nodes labeled role=kafka and role=compute, and the arm64 vajra image in ECR.
# Runs the IDENTICAL 10s event-time tumbling-window keyed COUNT over the SAME
# Kafka topic on both engines, sequentially (never concurrently), on the same
# dedicated compute node — true like-for-like. Captures throughput + peak RSS.
#
# Usage: scripts/eks_stream_headtohead.sh [N_EVENTS]   (default 100000000)
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${1:-100000000}"
REGION=ap-south-1
NS=stream
ECR="$(aws ecr describe-repositories --region $REGION --repository-name vajra --query 'repositories[0].repositoryUri' --output text)"
REG="${ECR%/vajra}"
echo "ECR=$(echo "$ECR" | sed -E 's/[0-9]{12}/<ACCT>/g')  N=$N"

kk() { kubectl -n "$NS" "$@"; }
wait_ready() { kk wait --for=condition=available --timeout=300s deployment/"$1"; }

echo "================ [1/6] Kafka ================"
kk apply -f k8s/stream/kafka.yaml >/dev/null 2>&1 || kubectl apply -f k8s/stream/kafka.yaml
wait_ready kafka
echo "Kafka up."

echo "================ [2/6] Produce $N events ================"
kk delete job producer --ignore-not-found >/dev/null 2>&1
sed "s|N_EVENTS\", value: \"[0-9]*\"|N_EVENTS\", value: \"$N\"|" k8s/stream/producer-job.yaml | kk apply -f -
kk wait --for=condition=complete --timeout=1200s job/producer
kk logs job/producer | grep PRODUCED

echo "================ [3/6] Flink baseline ================"
kk apply -f k8s/stream/flink-session.yaml
wait_ready flink-jm; wait_ready flink-tm
kk create configmap flink-sql --from-file=flink-sql.sql=k8s/stream/flink-sql.sql --dry-run=client -o yaml | kk apply -f -
kk delete job flink-runner --ignore-not-found
kk apply -f k8s/stream/flink-runner-job.yaml
kk wait --for=condition=complete --timeout=1200s job/flink-runner
FLINK_WALL=$(kk logs job/flink-runner | grep -oE 'FLINK_WAGG wall_s=[0-9.]+' | grep -oE '[0-9.]+')
FLINK_TM=$(kk get pod -l app=flink,component=tm -o jsonpath='{.items[0].metadata.name}')
FLINK_MEM=$(kk exec "$FLINK_TM" -- sh -c 'cat /sys/fs/cgroup/memory.peak 2>/dev/null || cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes' 2>/dev/null)
echo "FLINK wall_s=$FLINK_WALL peak_bytes=$FLINK_MEM"
kk delete -f k8s/stream/flink-session.yaml --ignore-not-found
kk delete job flink-runner --ignore-not-found

echo "================ [4/6] Vajra ================"
sed -e "s|__ECR__|$REG|g" -e "s|vajra:eo-multipart|vajra:${TAG:-realtime-fix}|g" k8s/stream/vajra-stream.yaml | kk apply -f -
# VAJ-T7 source-fusion opt-in + per-stage CPU profile (source_read drop is the beat).
[ "${VAJRA_T7_FUSE:-0}" = "1" ] && kk set env deploy/vajra-stream \
  VAJRA_T7_FUSE=1 VAJRA_WM_PROF=1 RUST_LOG="warn,sail_execution::task_runner=debug" >/dev/null 2>&1
wait_ready vajra-stream
kk apply -f k8s/stream/vajra-client.yaml
kk wait --for=condition=ready --timeout=300s pod/vajra-client
until kk logs vajra-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
kk cp scripts/stream_windowed_agg.py vajra-client:/tmp/wagg.py

echo "================ [5/6] Vajra windowed-agg run ================"
VPOD=$(kk get pod -l app=vajra-stream -o jsonpath='{.items[0].metadata.name}')
kk exec vajra-client -- sh -c \
  "SPARK_REMOTE=sc://vajra-stream.$NS.svc.cluster.local:50051 BOOT=kafka.$NS.svc.cluster.local:9092 TOPIC=events N_EVENTS=$N OUT=/data/wagg_out CK=/data/wagg_ck python3 /tmp/wagg.py" \
  2>&1 | grep VAJRA_WAGG
VAJRA_MEM=$(kk exec "$VPOD" -- sh -c 'cat /sys/fs/cgroup/memory.peak 2>/dev/null || cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes' 2>/dev/null)
echo "VAJRA peak_bytes=$VAJRA_MEM"

echo "================ [6/6] Scorecard ================"
awk -v fw="$FLINK_WALL" -v fm="$FLINK_MEM" -v vm="$VAJRA_MEM" -v n="$N" 'BEGIN{
  printf "Flink : wall=%.1fs  throughput=%.3fM ev/s  peakRSS=%.2f GiB\n", fw, n/fw/1e6, fm/1073741824;
  printf "Vajra : peakRSS=%.2f GiB (throughput from VAJRA_WAGG line above)\n", vm/1073741824;
}'
echo "Done. Run scripts/aws_eks_teardown.sh vajra-stream-ht $REGION when finished."
