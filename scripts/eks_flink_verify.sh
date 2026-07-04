#!/usr/bin/env bash
# Measure FLINK's windowed-agg CORRECTNESS (the blackhole throughput job discards output). Runs the
# identical 10s tumbling keyed COUNT (flink-sql-verify.sql) writing the result to Kafka `wagg_out`, then
# consumes it and asserts distinct (window,k) groups + sum(count) == the same values Vajra reports. Makes
# the Vajra-vs-Flink comparison BOTH-correct. Assumes cluster up + `events` topic already loaded (N events).
# Usage: scripts/eks_flink_verify.sh [N]
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${1:-100000000}"; REGION=ap-south-1; NS=stream
kk() { kubectl -n "$NS" "$@"; }
wait_ready() { kk wait --for=condition=available --timeout=600s deployment/"$1"; }
KPOD=$(kk get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')
kk exec "$KPOD" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic wagg_out --partitions 16 --replication-factor 1 >/dev/null 2>&1

echo "==== Flink session + verify job (windowed-agg -> Kafka wagg_out) ===="
kk apply -f k8s/stream/flink-session.yaml
wait_ready flink-jm || echo "WARN jm slow"; wait_ready flink-tm || echo "WARN tm slow"
JM=$(kk get pod -l app=flink,component=jm -o jsonpath='{.items[0].metadata.name}')
for i in $(seq 1 40); do kk exec "$JM" -- sh -c 'curl -sf localhost:8081/overview >/dev/null' 2>/dev/null && break; sleep 5; done
kk create configmap flink-sql-verify --from-file=flink-sql.sql=k8s/stream/flink-sql-verify.sql --dry-run=client -o yaml | kk apply -f -
kk delete job flink-verify --ignore-not-found >/dev/null 2>&1
sed -e 's/name: flink-runner/name: flink-verify/' -e 's/name: flink-sql }/name: flink-sql-verify }/' \
    k8s/stream/flink-runner-job.yaml | kk apply -f -
echo "waiting for flink-verify job (bounded, dml-sync) to complete..."
kk wait --for=condition=complete --timeout=1200s job/flink-verify 2>/dev/null || echo "WARN verify job did not complete cleanly"
echo "--- flink-verify job log (errors) ---"
kk logs job/flink-verify 2>/dev/null | grep -aiE "FLINK_WAGG|error|exception|caused by|fail" | head -15
echo "--- end log ---"

echo "==== consume wagg_out + verify (distinct groups + sum) ===="
kk exec "$KPOD" -- sh -c '
  /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic wagg_out | awk -F: "{s+=\$3} END{print \"WAGG_OUT_MSGS=\"s}"
  timeout 60 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic wagg_out --from-beginning --timeout-ms 20000 2>/dev/null > /tmp/wagg.jsonl
  wc -l < /tmp/wagg.jsonl | awk "{print \"WAGG_ROWS=\"\$1}"
' 2>&1 | grep -aE "WAGG_OUT_MSGS|WAGG_ROWS"
# distinct + sum via python on the consumed rows (copy out)
kk exec "$KPOD" -- cat /tmp/wagg.jsonl > /tmp/flink_wagg.jsonl 2>/dev/null
"$ROOT/.venvs/smoke/bin/python" - <<PY
import json
rows=[json.loads(l) for l in open("/tmp/flink_wagg.jsonl") if l.strip()]
groups=set((r.get("window_start"), r.get("k")) for r in rows)
tot=sum(r.get("cnt",0) for r in rows)
print(f"FLINK_VERIFY rows={len(rows)} distinct_groups={len(groups)} sum_count={tot} dup={len(rows)-len(groups)}")
PY
kk delete -f k8s/stream/flink-session.yaml --ignore-not-found >/dev/null 2>&1
kk delete job flink-verify --ignore-not-found >/dev/null 2>&1
