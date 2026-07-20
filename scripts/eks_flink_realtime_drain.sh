#!/usr/bin/env bash
# FLINK UNBOUNDED realtime-streaming drain of the pre-loaded `events` backlog (no scan.bounded.mode) —
# the true realtime comparison to Vajra's Trigger.RealTime. Submits the streaming SQL async
# (dml-sync=false), then measures the catch-up DRAIN wall by polling the Kafka consumer-group `flink-wagg`
# lag until it hits ~0 (all N consumed). Captures peak TM RSS. Cancels the job + tears the session.
# Usage: scripts/eks_flink_realtime_drain.sh [N]
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${1:-100000000}"; NS=stream
kk() { kubectl -n "$NS" "$@"; }
KPOD=$(kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')

echo "==== Flink session up ===="
kk apply -f k8s/stream/flink-session.yaml >/dev/null
kk wait --for=condition=available --timeout=600s deployment/flink-jm >/dev/null 2>&1 || echo "WARN jm slow"
kk wait --for=condition=available --timeout=600s deployment/flink-tm >/dev/null 2>&1 || echo "WARN tm slow"
JM=$(kk get pod -l app=flink,component=jm -o jsonpath='{.items[0].metadata.name}')
for i in $(seq 1 40); do kk exec "$JM" -- sh -c 'curl -sf localhost:8081/overview >/dev/null' 2>/dev/null && break; sleep 5; done

echo "==== submit UNBOUNDED streaming SQL (async) ===="
kk create configmap flink-sql-rt --from-file=flink-sql.sql=k8s/stream/flink-sql-realtime.sql --dry-run=client -o yaml | kk apply -f - >/dev/null
kk delete job flink-rt --ignore-not-found >/dev/null 2>&1
sed -e 's/name: flink-runner/name: flink-rt/' -e 's/name: flink-sql }/name: flink-sql-rt }/' k8s/stream/flink-runner-job.yaml | kk apply -f - >/dev/null
# runner exits fast (dml-sync=false submits + returns); the JOB runs on the cluster.
kk wait --for=condition=complete --timeout=300s job/flink-rt >/dev/null 2>&1 || echo "WARN runner submit slow"
sleep 5
JID=$(kk exec "$JM" -- sh -c 'curl -sf localhost:8081/jobs 2>/dev/null' | grep -oE '"id":"[a-f0-9]+","status":"RUNNING"' | grep -oE '[a-f0-9]{32}' | head -1)
echo "flink job id=$JID"
t0=$(date +%s)

echo "==== poll drain (consumer-group flink-wagg lag -> 0) ===="
drain_s=""
for i in $(seq 1 240); do
  sleep 3
  LAG=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group flink-wagg 2>/dev/null | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END{print s+0}')
  CUR=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group flink-wagg 2>/dev/null | awk 'NR>1 && $4 ~ /^[0-9]+$/ {s+=$4} END{print s+0}')
  now=$(date +%s); el=$((now-t0))
  echo "  t=${el}s consumed=${CUR:-?} lag=${LAG:-?}"
  if [ -n "$CUR" ] && [ "$CUR" -ge "$N" ] 2>/dev/null; then drain_s=$el; break; fi
  if [ -n "$LAG" ] && [ "$LAG" -le 0 ] 2>/dev/null && [ -n "$CUR" ] && [ "$CUR" -gt $((N/2)) ] 2>/dev/null; then drain_s=$el; break; fi
done
TM=$(kk get pod -l app=flink,component=tm -o jsonpath='{.items[0].metadata.name}')
MEM=$(kk exec "$TM" -- sh -c 'cat /sys/fs/cgroup/memory.peak 2>/dev/null || cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes' 2>/dev/null)
[ -z "$drain_s" ] && drain_s=$(( $(date +%s) - t0 ))
awk -v d="$drain_s" -v n="$N" -v m="$MEM" 'BEGIN{printf "FLINK_REALTIME_DRAIN drain_s=%d throughput=%.3fM_ev/s peakRSS=%.2f GiB\n", d, n/d/1e6, m/1073741824}'
[ -n "$JID" ] && kk exec "$JM" -- sh -c "curl -sf -XPATCH localhost:8081/jobs/$JID?mode=cancel >/dev/null 2>&1" || true
kk delete -f k8s/stream/flink-session.yaml --ignore-not-found >/dev/null 2>&1
kk delete job flink-rt --ignore-not-found >/dev/null 2>&1
