#!/usr/bin/env bash
# FREE local validation of the progressive per-stage profiler (scripts/stream_stage_profile.py).
# Starts a release Zelox server, runs the SAME local Kafka topic through source -> parse -> full,
# prints each stage's throughput + the deltas that isolate from_json and shuffle+window+sink.
# This validates the HARNESS (stages isolate correctly, counts sane) before the one EKS run.
# Usage: TOPIC=bench_src N_EVENTS=10000000 bash scripts/stage_profile_local.sh
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
PORT="${PORT:-50090}"; BOOT="${BOOT:-localhost:9092}"; TOPIC="${TOPIC:-bench_src}"
N="${N_EVENTS:-10000000}"
BIN="$ROOT/target/release/zelox"; PY="$ROOT/.venvs/smoke/bin/python"
[ -x "$BIN" ] || { echo "FATAL: need release zelox"; exit 2; }

echo "=== start zelox server :$PORT (COMPLETE_ON_END=1 so every window flushes => equal row counts) ==="
rm -rf /tmp/stage_*; ZELOX_COMPLETE_ON_END=1 "$BIN" server --ip 127.0.0.1 --port "$PORT" >/tmp/stage_srv.log 2>&1 & SRV=$!
trap 'kill "$SRV" 2>/dev/null' EXIT
for i in $(seq 1 30); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 1; done

TP_nokey=0; TP_full=0
for STAGE in nokey full; do
  echo "=== stage=$STAGE ==="
  LINE=$(SPARK_REMOTE="sc://localhost:$PORT" BOOT="$BOOT" TOPIC="$TOPIC" N_EVENTS="$N" STAGE="$STAGE" \
         OUT="/tmp/stage_out_$STAGE" CK="/tmp/stage_ck_$STAGE" \
         "$PY" scripts/stream_stage_profile.py 2>&1)
  echo "$LINE" | grep -E "ZELOX_STAGE|Error|Exception|No files" | head -3
  TP_VAL=$(echo "$LINE" | grep -oE "throughput=[0-9.]+" | head -1 | cut -d= -f2)
  eval "TP_$STAGE=\"${TP_VAL:-0}\""
done

echo "================= STAGE PROFILE (N=$N, topic=$TOPIC) ================="
echo "source-only ceiling  : ~2.3 M/s  (micro-bench kafka_read_bench, 16-part local)"
awk -v nk="${TP_nokey:-0}" -v f="${TP_full:-0}" 'BEGIN{
  printf "from_json+window(nokey): %.3f M/s\n", nk;
  printf "+keyed exchange (full) : %.3f M/s   (keyed-shuffle cost: %+.1f%%)\n", f, (nk>0? (f-nk)/nk*100:0);
  print  "----";
  print  "nokey vs full delta = the keyed exchange (shuffle-by-k) cost = suspected distributed culprit.";
  print  "Compare each stage to Flink per-operator busy% (web UI) on the one EKS run.";
}'
