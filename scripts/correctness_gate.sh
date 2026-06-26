#!/usr/bin/env bash
# Streaming correctness gate — standing adversarial harness (docs/design/streaming-correctness-gate.md).
# Runs curated cells, asserts the invariant contract (completeness, no-dup/no-partial-split, EO, bounded
# mem), prints a PASS/XFAIL/FAIL matrix. Exit 0 iff all GREEN cells pass AND no XFAIL unexpectedly passes.
# Needs: docker vajra_kafka + target/debug/vajra + .venvs/smoke. Reuses inc_ckpt_gate / state_scale /
# f5_validate / f3c_stateful_crash rather than rebuilding.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
PASS=0; XOK=0; FAIL=0; XBROKE=0
declare -a ROWS

# Ensure Kafka (continuous + f3c cells need it; availableNow f5_validate cells ignore it).
if ! docker ps --format '{{.Names}}' | grep -qi kafka; then
  docker run -d --name vajra_kafka -p 9092:9092 apache/kafka:latest >/dev/null 2>&1 || true
  for i in $(seq 1 40); do docker exec vajra_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && break; sleep 2; done
fi

# Shared invariant inspector: 0 double-emitted (window,key) across committed epochs in $OUT.
dup_count() {
  .venvs/smoke/bin/python - "$1" <<'PY'
import os,glob,collections,sys
import pyarrow.parquet as pq
OUT=sys.argv[1]
eds=[d for d in os.listdir(OUT) if d.isdigit()] if os.path.isdir(OUT) else []
seen=collections.defaultdict(int)
for e in eds:
    for f in glob.glob(f"{OUT}/{e}/*.parquet"):
        try:d=pq.read_table(f).to_pydict()
        except Exception:continue
        w=d.get("window") or [];ks=d.get("k") or []
        for i in range(len(ks)):seen[(str(w[i]['start'] if w else None),ks[i])]+=1
print(sum(1 for v in seen.values() if v>1))
PY
}

# record CELL EXPECT(GREEN|XFAIL) ACTUAL(pass|fail)
record() {
  local cell="$1" expect="$2" actual="$3"
  local mark
  if [ "$expect" = GREEN ]; then
    if [ "$actual" = pass ]; then mark="PASS "; PASS=$((PASS+1)); else mark="FAIL!"; FAIL=$((FAIL+1)); fi
  else # XFAIL
    if [ "$actual" = fail ]; then mark="xfail"; XOK=$((XOK+1)); else mark="XPASS"; XBROKE=$((XBROKE+1)); fi
  fi
  ROWS+=("  [$mark] $cell (expect $expect)")
}

# --- continuous cells via inc_ckpt_gate.sh (C5 single-partition GREEN, C6 multi-partition XFAIL) ---
run_continuous() { # PARTS  -> "pass" if 0 dups (no-dup invariant) else "fail"
  INC=0 NOCRASH=1 PARTS="$1" N="${N:-300}" BUDGET=16384 RUN="${RUN:-40}" \
    bash scripts/inc_ckpt_gate.sh >/tmp/cgate_c$1.log 2>&1
  local d; d=$(dup_count /tmp/incckpt_out 2>/dev/null || echo 99)
  [ "${d:-99}" = "0" ] && echo pass || echo fail
}
run_continuous_crash() { # PARTS -> "pass" if EO across crash (INC_CKPT_EO ... PASS) else "fail"
  INC=0 PARTS="$1" N="${N:-300}" BUDGET=16384 RUN="${RUN:-25}" \
    bash scripts/inc_ckpt_gate.sh >/tmp/cgate_crash$1.log 2>&1
  grep -q "INC_CKPT_EO_ACROSS_CRASH PASS" /tmp/cgate_crash$1.log && echo pass || echo fail
}

echo "=== streaming correctness gate ==="
record "C5 continuous, 1 partition, no-dup"            GREEN "$(run_continuous 1)"
record "C6 continuous, 4 partitions SCRAMBLED, no-dup" XFAIL "$(run_continuous 4)"
record "C7 continuous + crash, 4 partitions, EO"       XFAIL "$(run_continuous_crash 4)"

# --- availableNow completeness/bounded cells via f5_validate.sh (file-based, self-contained) ---
# C1: large budget (no spill) -> windowed_out_rows must == N (completeness).
an_complete() { # N BUDGET
  VAJRA_BIN=./target/debug/vajra NS="$1" SAIL_STREAMING_STATE_BUDGET_BYTES="$2" \
    bash scripts/f5_validate.sh >/tmp/cgate_an_$1.log 2>&1
  grep -q "windowed_out_rows=$1 " /tmp/cgate_an_$1.log && echo pass || echo fail
}
# C2: tiny budget -> completeness AND bounded operator state (peak_pending well under N-scaled, <2MiB).
an_bounded() { # N BUDGET
  VAJRA_BIN=./target/debug/vajra NS="$1" SAIL_STREAMING_STATE_BUDGET_BYTES="$2" \
    bash scripts/f5_validate.sh >/tmp/cgate_anb_$1.log 2>&1
  local peak; peak=$(grep -oE "OPERATOR_peak_pending_KiB=[0-9]+" /tmp/cgate_anb_$1.log | head -1 | cut -d= -f2)
  if grep -q "windowed_out_rows=$1 " /tmp/cgate_anb_$1.log && [ "${peak:-99999}" -lt 2048 ]; then echo pass; else echo fail; fi
}
record "C1 availableNow, 500k, no-dup/complete"        GREEN "$(an_complete 500000 134217728)"
record "C2 availableNow, 1M, tiny budget, bounded mem" GREEN "$(an_bounded 1000000 16384)"

# C4: availableNow + hard crash -> EO (f3c prints F3C_STATEFUL_EO_ACROSS_CRASH PASS).
c4() { bash scripts/f3c_stateful_crash.sh >/tmp/cgate_c4.log 2>&1; grep -q "F3C_STATEFUL_EO_ACROSS_CRASH PASS" /tmp/cgate_c4.log && echo pass || echo fail; }
record "C4 availableNow + crash, EO"                   GREEN "$(c4)"

ROWS+=("  [TODO ] C3 availableNow 8-part SCRAMBLED     -> needs availableNow-Kafka skew producer")

printf '%s\n' "${ROWS[@]}"
echo "GREEN pass=$PASS fail=$FAIL | XFAIL ok=$XOK unexpectedly-passing=$XBROKE"
# Green gate: all GREEN cells pass, and no XFAIL silently started passing (=fix landed, promote it).
[ "$FAIL" = 0 ] && [ "$XBROKE" = 0 ] && { echo "GATE: GREEN"; exit 0; } || { echo "GATE: RED"; exit 1; }
