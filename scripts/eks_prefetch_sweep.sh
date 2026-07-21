#!/usr/bin/env bash
# EKS memory fix validation: sweep the librdkafka prefetch bound (ZELOX_KAFKA_PREFETCH_KBYTES) over the
# SAME 100M windowed-agg and capture peak RSS + throughput at each — the RSS-vs-throughput curve that
# (a) confirms the prefetch queue is the streaming-memory driver (the 1 GiB run should reproduce ~10.34
# GiB) and (b) picks the prod-grade sweet spot vs Flink's 8.58 GiB. Assumes cluster UP + image in ECR.
# Usage: scripts/eks_prefetch_sweep.sh [N] [kbytes-list]
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${1:-100000000}"; REGION=ap-south-1; NS=stream
SWEEP="${2:-1048576 262144 65536}"   # 1 GiB, 256 MiB, 64 MiB per partition
ECR="$(aws ecr describe-repositories --region $REGION --repository-name zelox --query 'repositories[0].repositoryUri' --output text)"; REG="${ECR%/zelox}"
kk() { kubectl -n "$NS" "$@"; }
wait_ready() { kk wait --for=condition=available --timeout=300s deployment/"$1"; }
gib() { awk -v b="$1" 'BEGIN{printf "%.2f", b/1073741824}'; }

echo "==== [1] Kafka + produce $N ===="
kk apply -f k8s/stream/kafka.yaml >/dev/null 2>&1 || kubectl apply -f k8s/stream/kafka.yaml
wait_ready kafka
kk delete job producer --ignore-not-found >/dev/null 2>&1
sed "s|N_EVENTS\", value: \"[0-9]*\"|N_EVENTS\", value: \"$N\"|" k8s/stream/producer-job.yaml | kk apply -f -
kk wait --for=condition=complete --timeout=1800s job/producer; kk logs job/producer | grep PRODUCED

echo "==== [1b] Flink baseline (same session, self-contained) ===="
kk apply -f k8s/stream/flink-session.yaml >/dev/null 2>&1; wait_ready flink-jm; wait_ready flink-tm
kk create configmap flink-sql --from-file=flink-sql.sql=k8s/stream/flink-sql.sql --dry-run=client -o yaml | kk apply -f - >/dev/null
kk delete job flink-runner --ignore-not-found >/dev/null 2>&1; kk apply -f k8s/stream/flink-runner-job.yaml
kk wait --for=condition=complete --timeout=1800s job/flink-runner
FLINK_WALL=$(kk logs job/flink-runner | grep -oE 'FLINK_WAGG wall_s=[0-9.]+' | grep -oE '[0-9.]+')
FLINK_TM=$(kk get pod -l app=flink,component=tm -o jsonpath='{.items[0].metadata.name}')
FLINK_MEM=$(kk exec "$FLINK_TM" -- sh -c 'cat /sys/fs/cgroup/memory.peak 2>/dev/null || cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes' 2>/dev/null)
echo "FLINK  peakRSS=$(gib "${FLINK_MEM:-0}")GiB  wall_s=$FLINK_WALL  throughput=$(awk -v n="$N" -v w="$FLINK_WALL" 'BEGIN{printf "%.3fM/s", n/w/1e6}')" | tee /tmp/sweep.txt
kk delete -f k8s/stream/flink-session.yaml --ignore-not-found >/dev/null 2>&1; kk delete job flink-runner --ignore-not-found >/dev/null 2>&1

echo "==== [2] Zelox + client ===="
sed "s|__ECR__|$REG|g" k8s/stream/zelox-stream.yaml | kk apply -f -
# maxSurge=0: the pod requests ~15 CPU on a 16-vCPU node, so a rolling update that surges a 2nd pod
# DEADLOCKS (new Pending, old never terminates → env change never applies → sweep measures stale config).
# Terminate old BEFORE new so each prefetch value actually takes effect.
kk patch deploy zelox-stream --type merge -p '{"spec":{"strategy":{"rollingUpdate":{"maxSurge":0,"maxUnavailable":1}}}}' >/dev/null
wait_ready zelox-stream
kk apply -f k8s/stream/zelox-client.yaml
kk wait --for=condition=ready --timeout=300s pod/zelox-client
until kk logs zelox-client 2>/dev/null | grep -q CLIENT_READY; do sleep 3; done
kk cp scripts/stream_windowed_agg.py zelox-client:/tmp/wagg.py

echo "==== [3] PREFETCH SWEEP (RSS + direct prefetch + throughput + WM_PROF + correctness) ===="
for PF in $SWEEP; do
  echo "-- prefetch=${PF}KiB/partition --"
  # fresh pod per value (isolates memory.peak) + stats enabled (direct prefetch measurement)
  kk set env deploy/zelox-stream ZELOX_KAFKA_PREFETCH_KBYTES="$PF" ZELOX_KAFKA_STATS=1 >/dev/null
  kk rollout status deploy/zelox-stream --timeout=300s >/dev/null 2>&1; sleep 8
  until kk logs zelox-client 2>/dev/null | grep -q CLIENT_READY; do sleep 2; done
  WAGG=$(kk exec zelox-client -- sh -c \
    "SPARK_REMOTE=sc://zelox-stream.$NS.svc.cluster.local:50051 BOOT=kafka.$NS.svc.cluster.local:9092 TOPIC=events N_EVENTS=$N OUT=/data/wo_$PF CK=/data/wc_$PF python3 /tmp/wagg.py" 2>&1)
  TP=$(echo "$WAGG" | grep -aoE 'throughput=[0-9.]+M_events/s' | head -1)
  WALL=$(echo "$WAGG" | grep -aoE 'wall_s=[0-9.]+' | head -1)
  GRP=$(echo "$WAGG" | grep -aoE 'groups=[0-9]+' | head -1)   # correctness (expect groups=9000)
  # robust RSS: the RUNNING pod (not a terminating one), retried
  VPOD=$(kk get pod -l app=zelox-stream --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')
  MEM=""; for t in 1 2 3 4; do MEM=$(kk exec "$VPOD" -- cat /sys/fs/cgroup/memory.peak 2>/dev/null); [ -n "$MEM" ] && [ "$MEM" != "0" ] && break; sleep 2; done
  # DIRECT prefetch measurement (KAFKA_STATS logs fetchq_size = the C-side prefetch buffer)
  PFG=$(kk logs deploy/zelox-stream 2>/dev/null | grep -aoE 'prefetch_gib=[0-9.]+' | sed 's/prefetch_gib=//' | sort -rn | head -1)
  WMP=$(kk logs deploy/zelox-stream 2>/dev/null | grep -aoE 'STAGES\(summed-cpu-ms\):[^|]+' | tail -1)
  echo "PREFETCH=${PF}KiB  peakRSS=$(gib "${MEM:-0}")GiB  measured_prefetch=${PFG:-?}GiB  $TP $WALL $GRP" | tee -a /tmp/sweep.txt
  [ -n "$WMP" ] && echo "  wmprof: $WMP" | tee -a /tmp/sweep.txt
done

echo ""; echo "######## PREFETCH SWEEP CURVE (vs Flink 8.58 GiB) ########"
cat /tmp/sweep.txt
echo "Teardown: eksctl delete cluster --name zelox-stream-ht --region $REGION --force --wait"
