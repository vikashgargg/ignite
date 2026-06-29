#!/usr/bin/env bash
# Rescale-from-checkpoint gate (P0-2 / 3b validation): checkpoint a stateful windowed agg at
# parallelism M, HARD-crash, restart at M' != M (different spark.sql.shuffle.partitions) reading the
# SAME checkpointLocation, assert completeness + counts + no rescale-induced loss/dup. Exercises
# WindowAccumExec rescale wiring (restore_keyed_range_recompute_auto) — Flink key-groups (REFERENCES §2b).
# Requires docker `vajra_kafka`, target/debug/vajra, .venvs/smoke python. Reuses inc_ckpt_gate.py.
# Usage: M=4 MP=2 bash scripts/rescale_gate.sh   (scale down 4->2; also try M=2 MP=4 scale up)
set -uo pipefail
PORT="${PORT:-50093}"; ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/debug/vajra"; [ -x "$BIN" ] || BIN="$ROOT/target/release/vajra"
PY="$ROOT/.venvs/smoke/bin/python"
N="${N:-300}"; BUDGET="${BUDGET:-65536}"; M="${M:-4}"; MP="${MP:-2}"; PARTS="${PARTS:-4}"
KPOD=$(docker ps --format '{{.Names}}' | grep -i kafka | head -1)
[ -n "$KPOD" ] || { echo "FATAL: no kafka container"; exit 2; }
[ -x "$BIN" ] || { echo "FATAL: no vajra binary"; exit 2; }

docker exec "$KPOD" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic incckpt_eo >/dev/null 2>&1; sleep 2
docker exec "$KPOD" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic incckpt_eo --partitions "$PARTS" --replication-factor 1 >/dev/null 2>&1

# Server runs with rescale + incremental checkpointing ON.
start() {
  RUST_LOG=warn env VAJRA_RESCALE=1 VAJRA_INC_CKPT=1 SAIL_STREAMING_STATE_BUDGET_BYTES="$BUDGET" \
    "$BIN" server --ip 127.0.0.1 --port "$PORT" --mode local-cluster --workers 2 >/tmp/rescale_server.log 2>&1 & echo $!;
}

echo "=== rescale gate: ckpt at M=$M, crash, restart at M'=$MP (N=$N keys, $PARTS kafka parts) ==="
SRV=$(start); for i in $(seq 1 30); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 1; done
echo "--- phase w1 @ shuffle.partitions=$M (server $SRV) ---"
N="$N" SHUFFLE="$M" "$PY" "$ROOT/scripts/inc_ckpt_gate.py" "$PORT" w1 2>&1 | grep -E "W1|Error|Traceback" | head
echo "--- HARD CRASH: kill -9 $SRV ---"
kill -9 "$SRV" 2>/dev/null; sleep 3
SRV=$(start); for i in $(seq 1 30); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 1; done
echo "--- restarted $SRV @ shuffle.partitions=$MP; phase check (rescale restore) ---"
N="$N" SHUFFLE="$MP" "$PY" "$ROOT/scripts/inc_ckpt_gate.py" "$PORT" check 2>&1 | grep -E "CHECK|INC_CKPT_EO|Error|Traceback" | head
RC=${PIPESTATUS[0]}
kill -9 "$SRV" 2>/dev/null; wait 2>/dev/null
echo "exit=$RC (0=rescale PASS: complete + no loss/dup across M->M')"
exit "$RC"
