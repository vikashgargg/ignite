#!/usr/bin/env bash
# F3-c gate orchestrator: continuous STATEFUL (windowed-agg) exactly-once across a HARD crash
# (kill -9) on a local-cluster. See scripts/f3c_stateful_crash.py. Exit 0 = PASS.
set -uo pipefail
PORT="${PORT:-50091}"; ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/debug/vajra"; [ -x "$BIN" ] || BIN="$ROOT/target/release/vajra"
PY="$ROOT/.venvs/smoke/bin/python"
KPOD=$(docker ps --format '{{.Names}}' | grep -i kafka | head -1)
docker exec "$KPOD" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic f3c_eo >/dev/null 2>&1; sleep 2
docker exec "$KPOD" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic f3c_eo --partitions 4 --replication-factor 1 >/dev/null 2>&1

start() { RUST_LOG=warn "$BIN" server --ip 127.0.0.1 --port "$PORT" --mode local-cluster --workers 2 >/tmp/f3c_server.log 2>&1 & echo $!; }
SRV=$(start); for i in $(seq 1 30); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 1; done
echo "=== phase w1 (server $SRV) ==="
"$PY" "$ROOT/scripts/f3c_stateful_crash.py" "$PORT" w1 2>&1 | grep -E "W1|Error|Traceback" | head
echo "=== HARD CRASH: kill -9 $SRV ==="
kill -9 "$SRV" 2>/dev/null; sleep 3
SRV=$(start); for i in $(seq 1 30); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 1; done
echo "=== restarted server $SRV; phase check ==="
"$PY" "$ROOT/scripts/f3c_stateful_crash.py" "$PORT" check 2>&1 | grep -E "CHECK|F3C_STATEFUL_EO|  \(|Error|Traceback" | head -20
RC=${PIPESTATUS[0]}
kill -9 "$SRV" 2>/dev/null; wait 2>/dev/null
exit $RC
