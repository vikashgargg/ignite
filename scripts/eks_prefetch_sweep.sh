#!/usr/bin/env bash
# EKS memory fix validation: sweep the librdkafka prefetch bound (VAJRA_KAFKA_PREFETCH_KBYTES) over the
# SAME 100M windowed-agg and capture peak RSS + throughput at each — the RSS-vs-throughput curve that
# (a) confirms the prefetch queue is the streaming-memory driver (the 1 GiB run should reproduce ~10.34
# GiB) and (b) picks the prod-grade sweet spot vs Flink's 8.58 GiB. Assumes cluster UP + image in ECR.
# Usage: scripts/eks_prefetch_sweep.sh [N] [kbytes-list]
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${1:-100000000}"; REGION=ap-south-1; NS=stream
SWEEP="${2:-1048576 262144 65536}"   # 1 GiB, 256 MiB, 64 MiB per partition
ECR="$(aws ecr describe-repositories --region $REGION --repository-name vajra --query 'repositories[0].repositoryUri' --output text)"; REG="${ECR%/vajra}"
kk() { kubectl -n "$NS" "$@"; }
wait_ready() { kk wait --for=condition=available --timeout=300s deployment/"$1"; }
gib() { awk -v b="$1" 'BEGIN{printf "%.2f", b/1073741824}'; }

echo "==== [1] Kafka + produce $N ===="
kk apply -f k8s/stream/kafka.yaml >/dev/null 2>&1 || kubectl apply -f k8s/stream/kafka.yaml
wait_ready kafka
kk delete job producer --ignore-not-found >/dev/null 2>&1
sed "s|N_EVENTS\", value: \"[0-9]*\"|N_EVENTS\", value: \"$N\"|" k8s/stream/producer-job.yaml | kk apply -f -
kk wait --for=condition=complete --timeout=1800s job/producer; kk logs job/producer | grep PRODUCED

echo "==== [2] Vajra + client ===="
sed "s|__ECR__|$REG|g" k8s/stream/vajra-stream.yaml | kk apply -f -
wait_ready vajra-stream
kk apply -f k8s/stream/vajra-client.yaml
kk wait --for=condition=ready --timeout=300s pod/vajra-client
until kk logs vajra-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
kk cp scripts/stream_windowed_agg.py vajra-client:/tmp/wagg.py

echo "==== [3] PREFETCH SWEEP ===="
: > /tmp/sweep.txt
for PF in $SWEEP; do
  echo "-- prefetch=${PF}KiB/partition --"
  kk set env deploy/vajra-stream VAJRA_KAFKA_PREFETCH_KBYTES="$PF" >/dev/null
  kk rollout status deploy/vajra-stream --timeout=300s >/dev/null 2>&1
  until kk logs vajra-client 2>/dev/null | grep -q CLIENT_READY; do sleep 2; done
  VPOD=$(kk get pod -l app=vajra-stream -o jsonpath='{.items[0].metadata.name}')
  WAGG=$(kk exec vajra-client -- sh -c \
    "SPARK_REMOTE=sc://vajra-stream.$NS.svc.cluster.local:50051 BOOT=kafka.$NS.svc.cluster.local:9092 TOPIC=events N_EVENTS=$N OUT=/data/wo_$PF CK=/data/wc_$PF python3 /tmp/wagg.py" 2>&1 | grep -aoE 'throughput=[0-9.]+M_events/s|wall_s=[0-9.]+' | tr '\n' ' ')
  MEM=$(kk exec "$VPOD" -- sh -c 'cat /sys/fs/cgroup/memory.peak 2>/dev/null || cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes' 2>/dev/null)
  echo "PREFETCH=${PF}KiB  peakRSS=$(gib "${MEM:-0}")GiB  $WAGG" | tee -a /tmp/sweep.txt
done

echo ""; echo "######## PREFETCH SWEEP CURVE (vs Flink 8.58 GiB) ########"
cat /tmp/sweep.txt
echo "Teardown: eksctl delete cluster --name vajra-stream-ht --region $REGION --force --wait"
