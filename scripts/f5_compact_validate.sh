#!/usr/bin/env bash
# F5.3 compaction validation — RECURRING keys (each key appears in M batches → M partials/key before
# compaction). A/B: compaction ON (default) vs OFF (VAJRA_F5_NO_COMPACT=1), fixed small budget.
# Expect: out == N EXACT in BOTH (compaction is correctness-preserving), and spill_events(ON) <<
# spill_events(OFF) — compaction collapses the per-batch partial pile-up toward O(distinct groups).
set -uo pipefail
BIN=${VAJRA_BIN:-./target/release/vajra}
PY=${PY:-.venvs/smoke/bin/python}
PORT=${PORT:-50073}
BUDGET=${SAIL_STREAMING_STATE_BUDGET_BYTES:-2097152} # 2 MiB
N=${N:-100000}; M=${M:-20}   # N distinct keys, each repeated across M rounds -> N*M rows
ROOT=$(mktemp -d /tmp/f5cmp.XXXX)
echo "F5.3 compaction A/B: N=$N keys x M=$M rounds = $((N*M)) rows, budget=$BUDGET, root=$ROOT"

# Generate: M rounds, each round = all N keys at ts=1000ms (same window [0,10s)); a recurring key
# thus lands in many 8192-row batches -> many partials. + advancer rows to close the window.
DIR="$ROOT/in"; mkdir -p "$DIR"
"$PY" - "$N" "$M" "$DIR" <<'PY'
import sys
n,m,d=int(sys.argv[1]),int(sys.argv[2]),sys.argv[3]
with open(d+"/part.json","w") as f:
    for r in range(m):
        for k in range(n):
            f.write('{"k":%d,"ts":1000}\n'%k)
    for j in range(5):
        f.write('{"k":%d,"ts":50000}\n'%(n+j))   # advancer -> watermark>10s closes [0,10s)
PY

run() { # run LABEL  (EXTRA = extra "VAR=val" env pairs, passed via env(1) so they ARE assignments)
  local label="$1" OUT="$ROOT/out_$1" CK="$ROOT/ck_$1" LOG="$ROOT/srv_$1.log"
  env SAIL_STREAMING_STATE_BUDGET_BYTES=$BUDGET VAJRA_F5_DEBUG=1 ${EXTRA:-} \
    "$BIN" server --ip 127.0.0.1 --port "$PORT" >"$LOG" 2>&1 &
  local SRV=$!; sleep 4
  SPARK_REMOTE="sc://localhost:$PORT" DIR="$DIR" OUT="$OUT" CK="$CK" \
    "$PY" scripts/state_scale_stress.py 2>&1 | grep -E "STREAMING" | sed "s/^/[$label] /" || true
  local SP=$(grep -c F5_SPILL "$LOG" 2>/dev/null || echo 0)
  kill "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null
  echo "[$label] spill_events=$SP  (expect out_rows=$N)"
}

EXTRA="" run "compact_ON"
EXTRA="VAJRA_F5_NO_COMPACT=1" run "compact_OFF"
echo "root=$ROOT"
