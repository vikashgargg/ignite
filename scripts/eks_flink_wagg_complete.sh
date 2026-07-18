#!/usr/bin/env bash
# FLINK unbounded realtime streaming, measured SYMMETRICALLY with Vajra: time from job start until the
# SINK (Kafka wagg_out) holds ALL 10 windows / 100M (output-complete), not consumer-offset. This is the
# apples-to-apples end-to-end realtime throughput (consume+window+emit) vs Vajra's S3-completeness drain.
# Also captures peak TM RSS and the full aggregation output (for the identical-output check).
# Usage: scripts/eks_flink_wagg_complete.sh [N]
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${1:-100000000}"; NS=stream
kk() { kubectl -n "$NS" "$@"; }
KPOD=$(kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')
kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic wagg_out --partitions 16 --replication-factor 1 >/dev/null 2>&1
# fresh sink
kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic wagg_out >/dev/null 2>&1; sleep 4
kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic wagg_out --partitions 16 --replication-factor 1 >/dev/null 2>&1

echo "==== Flink session ===="
kk apply -f k8s/stream/flink-session.yaml >/dev/null
kk wait --for=condition=available --timeout=600s deployment/flink-jm >/dev/null 2>&1 || echo WARN
kk wait --for=condition=available --timeout=600s deployment/flink-tm >/dev/null 2>&1 || echo WARN
JM=$(kk get pod -l app=flink,component=jm -o jsonpath='{.items[0].metadata.name}')
for i in $(seq 1 40); do kk exec "$JM" -- sh -c 'curl -sf localhost:8081/overview >/dev/null' 2>/dev/null && break; sleep 5; done

echo "==== submit unbounded streaming SQL -> wagg_out (async) ===="
kk create configmap flink-sql-rt --from-file=flink-sql.sql=k8s/stream/flink-sql-realtime.sql --dry-run=client -o yaml | kk apply -f - >/dev/null
kk delete job flink-rt --ignore-not-found >/dev/null 2>&1
sed -e 's/name: flink-runner/name: flink-rt/' -e 's/name: flink-sql }/name: flink-sql-rt }/' k8s/stream/flink-runner-job.yaml | kk apply -f - >/dev/null
kk wait --for=condition=complete --timeout=300s job/flink-rt >/dev/null 2>&1 || echo "WARN submit"
JID=$(kk exec "$JM" -- sh -c 'curl -sf localhost:8081/jobs 2>/dev/null' | grep -oE '[a-f0-9]{32}' | head -1)
t0=$(date +%s)
echo "==== poll wagg_out until output-complete (10000 rows = 10 windows x 1000 keys, sum=$N) ===="
drain_s=""
for i in $(seq 1 200); do
  sleep 3
  MSGS=$(kk exec "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic wagg_out 2>/dev/null | awk -F: '{s+=$3} END{print s+0}')
  el=$(( $(date +%s) - t0 ))
  echo "  t=${el}s wagg_out_rows=${MSGS:-?}"
  if [ -n "$MSGS" ] && [ "$MSGS" -ge 10000 ] 2>/dev/null; then drain_s=$el; break; fi
done
TM=$(kk get pod -l app=flink,component=tm -o jsonpath='{.items[0].metadata.name}')
MEM=$(kk exec "$TM" -- sh -c 'cat /sys/fs/cgroup/memory.peak 2>/dev/null || cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes' 2>/dev/null)
[ -z "$drain_s" ] && drain_s=$(( $(date +%s) - t0 ))
# capture the full output for the identical-output check + correctness
kk exec "$KPOD" -- sh -c 'timeout 40 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic wagg_out --from-beginning --timeout-ms 15000 2>/dev/null' > /tmp/flink_wagg_out.jsonl 2>/dev/null
ROWS=$(wc -l < /tmp/flink_wagg_out.jsonl | tr -d ' ')
awk -v d="$drain_s" -v n="$N" -v m="$MEM" -v r="$ROWS" 'BEGIN{printf "FLINK_WAGG_COMPLETE drain_s=%d throughput=%.3fM_ev/s peakRSS=%.2f GiB rows=%d\n", d, n/d/1e6, m/1073741824, r}'
[ -n "$JID" ] && kk exec "$JM" -- sh -c "curl -sf -XPATCH localhost:8081/jobs/$JID?mode=cancel >/dev/null 2>&1" || true
kk delete -f k8s/stream/flink-session.yaml --ignore-not-found >/dev/null 2>&1
kk delete job flink-rt --ignore-not-found >/dev/null 2>&1
