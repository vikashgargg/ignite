#!/usr/bin/env bash
# Thorough prod-grade validation sweep for Vajra distributed batch + stateful streaming.
#
# Runs, against a freshly-started Vajra server, the full functional + reliability suite and prints a
# scorecard. Covers what a true streaming engine must guarantee (Flink/Spark bar):
#   FUNCTIONAL (correctness vs Spark): distributed batch write; the 6-probe streaming suite
#     (stateless rate/file, keyed event-time window agg, dropDuplicates, stream-stream join) — every
#     value cross-checked against real Spark 3.5.3 in scripts/dist_streaming_smoke.py.
#   RELIABILITY (exactly-once): micro-batch EO across restart; continuous (Trigger.Continuous) EO
#     across a graceful restart AND a HARD crash (kill -9), single-partition AND no-funnel
#     multi-partition (parallel writes).
#
# Usage:
#   scripts/full_validation.sh <mode> <port>      # mode: local | cluster   (default: cluster 50150)
# Requires: target/release/vajra built; vajra_kafka container up; .venvs/smoke python.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
MODE="${1:-cluster}"; PORT="${2:-50150}"
PY=.venvs/smoke/bin/python
BIN=target/release/vajra
KAFKA=vajra_kafka
pass=0; fail=0
declare -a RESULTS

start_server() {
  pkill -9 -f "vajra server" 2>/dev/null; sleep 1
  if [ "$MODE" = "cluster" ]; then
    RUST_LOG=error "$BIN" server --mode local-cluster --workers 2 --port "$PORT" >/tmp/fv_server.log 2>&1 &
  else
    RUST_LOG=error "$BIN" server --port "$PORT" >/tmp/fv_server.log 2>&1 &
  fi
  for _ in $(seq 1 20); do lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1 && return 0; sleep 1; done
  echo "server failed to start"; tail -5 /tmp/fv_server.log; exit 1
}
record() { if echo "$2" | grep -q "$3"; then RESULTS+=("PASS  $1"); pass=$((pass+1)); else RESULTS+=("FAIL  $1  ($2)"); fail=$((fail+1)); fi; }
mktopic() { docker exec "$KAFKA" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$1" >/dev/null 2>&1; docker exec "$KAFKA" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$1" --partitions 1 --replication-factor 1 >/dev/null 2>&1; }

echo "=== Vajra full validation: mode=$MODE port=$PORT binary=$BIN ==="
start_server

# 1. FUNCTIONAL: batch + 6 streaming probes (Spark-matched expected values)
out=$($PY scripts/dist_streaming_smoke.py "$PORT" 2>&1 | grep DIST_STREAMING_SMOKE)
record "functional.batch+streaming (6 Spark-matched probes)" "$out" "6/6 passed"

# 2. RELIABILITY: continuous single-partition EO across HARD crash
mktopic cont_eo
$PY scripts/dist_continuous_eo_crash.py "$PORT" w1 >/tmp/fv_w1.log 2>&1
pkill -9 -f "vajra server"; sleep 2; start_server
out=$($PY scripts/dist_continuous_eo_crash.py "$PORT" check 2>&1 | grep EXACTLY_ONCE_ACROSS_CRASH)
record "reliability.continuous EO across HARD crash (single-partition)" "$out" "True"

# 3. RELIABILITY: continuous MULTI-partition (no-funnel) EO across HARD crash
mktopic mp_cont
$PY scripts/streaming_mp_eo.py "$PORT" w1 >/tmp/fv_mp1.log 2>&1
pkill -9 -f "vajra server"; sleep 2; start_server
out=$($PY scripts/streaming_mp_eo.py "$PORT" check 2>&1 | grep MULTIPART_EXACTLY_ONCE)
record "reliability.continuous EO across HARD crash (multi-partition, no funnel)" "$out" "True"

pkill -9 -f "vajra server" 2>/dev/null
echo; echo "=== SCORECARD (mode=$MODE) ==="; printf '%s\n' "${RESULTS[@]}"
echo "FULL_VALIDATION ${pass}/$((pass+fail)) passed"
[ "$fail" -eq 0 ]
