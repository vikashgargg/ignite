#!/usr/bin/env bash
# F5.2 validation gate — bounded-peak streaming finalize.
# For each N (distinct keys, all in ONE 10s window) at a FIXED small state budget:
#   - correctness: streaming windowed-agg out_rows == N  (no silent loss; F5.1/F5.2 invariant)
#   - bounded peak RSS: server peak RSS grows ~flat in N (spill offloads state to object-store +
#     DataFusion spills the Final hash table under the bounded MemoryPool) — NOT linear in N.
# Compares the operator-level memory claim of docs/design/streaming-spillable-state-f5.md.
set -euo pipefail
BIN=${ZELOX_BIN:-./target/release/zelox}
PY=${PY:-.venvs/smoke/bin/python}   # Spark-Connect client venv (pyspark 3.5.3)
[ -x "$PY" ] || { echo "FATAL: Spark-Connect venv missing at $PY"; exit 2; }
PORT=${PORT:-50071}
BUDGET=${ZELOX_STREAMING_STATE_BUDGET_BYTES:-4194304} # 4 MiB default small budget
NS=${NS:-"200000 500000 1000000"}
ROOT=$(mktemp -d /tmp/f5val.XXXX)
echo "F5.2 validate: budget=$BUDGET bytes, Ns=[$NS], root=$ROOT"

gen() { # gen N dir  -> N keys all in window [0,10s) + a few advancer rows that push the
        # watermark past 10s so that window CLOSES and emits exactly N rows.
  python3 - "$1" "$2" <<'PY'
import sys, os
n=int(sys.argv[1]); d=sys.argv[2]; os.makedirs(d, exist_ok=True)
with open(os.path.join(d,"part.json"),"w") as f:
    for k in range(n):
        f.write('{"k":%d,"ts":1000}\n' % k)        # ts=1000ms -> window [0,10s)
    for j in range(5):                              # advancer rows -> watermark to ~50s,
        f.write('{"k":%d,"ts":50000}\n' % (n+j))    # closes [0,10s); their window stays open
PY
}

for N in $NS; do
  DIR="$ROOT/in_$N"; OUT="$ROOT/out_$N"; CK="$ROOT/ck_$N"
  gen "$N" "$DIR"
  # start server with the small budget
  ZELOX_STREAMING_STATE_BUDGET_BYTES=$BUDGET ZELOX_F5_DEBUG=1 \
    "$BIN" server --ip 127.0.0.1 --port "$PORT" >"$ROOT/server_$N.log" 2>&1 &
  SRV=$!
  sleep 4
  # sample RSS (KB) in the background
  ( PEAK=0; while kill -0 "$SRV" 2>/dev/null; do
      R=$(ps -o rss= -p "$SRV" 2>/dev/null | tr -d ' '); [ -n "${R:-}" ] && [ "$R" -gt "$PEAK" ] && PEAK=$R
      echo "$PEAK" > "$ROOT/peak_$N"; sleep 0.05
    done ) &
  SMP=$!
  SPARK_REMOTE="sc://localhost:$PORT" DIR="$DIR" OUT="$OUT" CK="$CK" \
    "$PY" scripts/state_scale_stress.py 2>&1 | grep -E "BATCH|STREAMING" | sed "s/^/[N=$N] /" || true
  SPILLS=$(grep -c "F5_SPILL" "$ROOT/server_$N.log" || true)
  # F5.4: max operator-resident state across partitions — the bounded-memory proof (should stay
  # ≈ budget regardless of N, independent of the O(N) parquet sink).
  PEAK_PEND=$(grep "F5_PEAK" "$ROOT/server_$N.log" 2>/dev/null | sed -E 's/.*peak_pending_bytes=([0-9]+).*/\1/' | sort -n | tail -1)
  kill "$SRV" 2>/dev/null || true; wait "$SRV" 2>/dev/null || true
  kill "$SMP" 2>/dev/null || true
  PEAK_KB=$(cat "$ROOT/peak_$N" 2>/dev/null || echo "?")
  echo "[N=$N] peak_RSS_MiB=$(( ${PEAK_KB:-0} / 1024 ))  OPERATOR_peak_pending_KiB=$(( ${PEAK_PEND:-0} / 1024 ))  spill_events=$SPILLS  budget_KiB=$(( BUDGET/1024 ))"
  echo "----"
done
echo "root=$ROOT (inspect server_*.log for spill traces)"
