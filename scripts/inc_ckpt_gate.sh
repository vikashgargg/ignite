#!/usr/bin/env bash
# inc-ckpt.4 gate: incremental checkpointing in CONTINUOUS mode.
#   (a) EXACTLY-ONCE across a HARD crash (kill -9) with ZELOX_INC_CKPT=1 + a small spill budget — the
#       OPEN window at the crash survives only if incremental snapshot+restore (manifest→residual+
#       chunks) is correct.
#   (b) per-checkpoint bytes = O(delta): inc-mode writes a manifest (+small residual) per epoch
#       (independent of state size); full-mode writes the whole state per epoch. The harness prints
#       both for comparison.
# Requires: docker `zelox_kafka` up; target/debug/zelox (current); .venvs/smoke python.
# Usage: bash scripts/inc_ckpt_gate.sh            # inc ON (default), N=2000 keys, budget 64KiB
#        N=5000 BUDGET=131072 bash scripts/inc_ckpt_gate.sh
set -uo pipefail
PORT="${PORT:-50092}"; ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/debug/zelox"; [ -x "$BIN" ] || BIN="$ROOT/target/release/zelox"
PY="$ROOT/.venvs/smoke/bin/python"
N="${N:-2000}"; BUDGET="${BUDGET:-65536}"   # tiny budget -> force spill -> chunks -> incremental manifests
CK=/tmp/incckpt_ck
KPOD=$(docker ps --format '{{.Names}}' | grep -i kafka | head -1)
[ -n "$KPOD" ] || { echo "FATAL: no kafka container (start zelox_kafka)"; exit 2; }
[ -x "$BIN" ] || { echo "FATAL: no zelox binary (cargo build -p zelox-cli --bin zelox)"; exit 2; }

docker exec "$KPOD" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic incckpt_eo >/dev/null 2>&1; sleep 2
docker exec "$KPOD" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic incckpt_eo --partitions ${PARTS:-4} --replication-factor 1 >/dev/null 2>&1

# INC=1 (default) enables incremental checkpointing; INC=0 runs the full-snapshot baseline (A/B).
INC="${INC:-1}"
start() {
  if [ "$INC" = "1" ]; then INCENV="ZELOX_INC_CKPT=1"; else INCENV="ZELOX_INC_CKPT_OFF=1"; fi
  # RUST_LOG overridable for observability (default warn = quiet). Set e.g.
  # RUST_LOG=info,sail_execution=debug,sail_physical_plan=debug to capture the finalized streaming
  # physical plan (DisplayableExecutionPlan, debug!) + per-operator logs — local-cluster workers are
  # in-process threads so their stderr lands in the same server log.
  RUST_LOG="${RUST_LOG:-warn}" env $INCENV ZELOX_STREAMING_STATE_BUDGET_BYTES="$BUDGET" ZELOX_WM_PARTITIONS="${PARTS:-4}" \
    "$BIN" server --ip 127.0.0.1 --port "$PORT" --mode local-cluster --workers 2 >/tmp/incckpt_server.log 2>&1 & echo $!;
}

echo "=== inc-ckpt.4 gate: N=$N keys, budget=$BUDGET bytes, ZELOX_INC_CKPT=1 ==="
SRV=$(start); for i in $(seq 1 30); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 1; done
echo "--- phase w1 (server $SRV) ---"
N="$N" "$PY" "$ROOT/scripts/inc_ckpt_gate.py" "$PORT" w1 2>&1 | grep -E "W1|Error|Traceback" | head
if [ "${NOCRASH:-0}" = "1" ]; then
  echo "--- NOCRASH mode: same server, no kill (isolates multi-partition merge from crash-EO) ---"
else
  echo "--- HARD CRASH: kill -9 $SRV ---"
  kill -9 "$SRV" 2>/dev/null; sleep 3
  SRV=$(start); for i in $(seq 1 30); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 1; done
  echo "--- restarted $SRV; phase check (EO via incremental restore) ---"
fi
N="$N" "$PY" "$ROOT/scripts/inc_ckpt_gate.py" "$PORT" check 2>&1 | grep -E "CHECK|INC_CKPT_EO|Error|Traceback" | head
RC=${PIPESTATUS[0]}
kill -9 "$SRV" 2>/dev/null; wait 2>/dev/null

echo "--- (b) per-checkpoint bytes (O(delta) proof) ---"
# inc-mode per-epoch objects = manifest (refs chunks) + residual (small), independent of state size.
echo "manifests + residuals written per epoch (inc-mode):"
find "$CK" -path '*/epoch-*/manifest' -o -path '*/epoch-*/residual' 2>/dev/null \
  | sort | while read -r f; do printf "  %8s  %s\n" "$(wc -c <"$f")" "${f#$CK/}"; done | head -40
echo "immutable spill chunks (the bulk, written off-barrier during spill, REFERENCED not re-copied):"
find "$CK" -name 'spill-*' 2>/dev/null | sort | while read -r f; do printf "  %8s  %s\n" "$(wc -c <"$f")" "${f#$CK/}"; done | head -20
MAN=$(find "$CK" -path '*/epoch-*/manifest' 2>/dev/null | head -1)
[ -n "$MAN" ] && echo "=> per-epoch checkpoint write ≈ manifest($(wc -c <"$MAN")B) + residual (≤budget), NOT O(total state)."
echo "exit=$RC (0=EO PASS across crash via incremental restore)"
exit "$RC"
