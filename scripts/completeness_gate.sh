#!/usr/bin/env bash
# T1 gate for the FINAL-WINDOW COMPLETENESS gap (docs/design/three-tier-sdlc.md Gap 1): a bounded backlog
# aggregated with trigger=availableNow must emit EVERY window (Flink flushes all at end-of-input via
# MAX_WATERMARK) — including the last boundary window whose end > max-event-time. EKS measured Zelox 9
# windows/90M where Flink emits 10/100M. This reproduces it locally + FREE and asserts:
#   n_windows == ceil((max_event_time_ms+1)/10000)   AND   sum_count == N.
# Self-checking (producer count verified). Usage: N=5500000 EPMS=100 WORKERS=2 bash scripts/completeness_gate.sh
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${N:-5500000}"; EPMS="${EPMS:-100}"; K="${K:-1000}"; NP="${NP:-16}"; WORKERS="${WORKERS:-2}"
PORT="${PORT:-50097}"; TOPIC=events_comp; OUT=/tmp/comp_out; CK=/tmp/comp_ck
BIN="$ROOT/target/debug/zelox"; PY="$ROOT/.venvs/smoke/bin/python"
KPOD=$(docker ps --format '{{.Names}}' | grep -i kafka | head -1); [ -n "$KPOD" ] || { echo "FATAL: no kafka"; exit 2; }
rm -rf "$OUT" "$CK"
docker exec "$KPOD" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$TOPIC" >/dev/null 2>&1; sleep 2
docker exec "$KPOD" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$TOPIC" --partitions "$NP" --replication-factor 1 >/dev/null 2>&1

echo "=== produce N=$N EPMS=$EPMS (self-checking) ==="
BOOT=localhost:9092 TOPIC=$TOPIC N=$N K=$K EPMS=$EPMS NP=$NP KPOD="$KPOD" "$PY" "$ROOT/scripts/scale_producer.py" 2>&1 | grep -aE "PRODUCED|TOPIC_CHECK|PRODUCER_OK|FATAL"
[ "${PIPESTATUS[0]}" = "0" ] || { echo "ABORT: producer self-check failed"; exit 3; }

echo "=== zelox server --workers $WORKERS (ZELOX_COMPLETE_ON_END=1 = bounded-complete / Flink-parity) ==="
pkill -9 -f 'target/debug/zelox' 2>/dev/null; sleep 1
RUST_LOG=warn ZELOX_COMPLETE_ON_END=1 "$BIN" server --ip 127.0.0.1 --port "$PORT" --mode local-cluster --workers "$WORKERS" >/tmp/comp_server.log 2>&1 &
SRV=$!; for i in $(seq 1 30); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 1; done

echo "=== availableNow windowed-agg + completeness assert ==="
SPARK_REMOTE="sc://localhost:$PORT" BOOT=localhost:9092 TOPIC=$TOPIC N_EVENTS=$N OUT=$OUT CK=$CK \
  "$PY" "$ROOT/scripts/stream_windowed_agg.py" 2>&1 | grep -aoE 'ZELOX_WAGG.*' || true

SPARK_REMOTE="sc://localhost:$PORT" OUT=$OUT N=$N EPMS=$EPMS "$PY" - <<'PY'
import os, math
from pyspark.sql import SparkSession, functions as F
s=SparkSession.builder.remote(f"sc://localhost:{os.environ['SPARK_REMOTE'].split(':')[-1]}").getOrCreate()
N=int(os.environ["N"]); EPMS=int(os.environ["EPMS"])
max_ts_ms=(N-1)//EPMS
expected_windows=math.floor(max_ts_ms/10000)+1
try:
    d=s.read.parquet(os.environ["OUT"]).where(F.col("k")>=0)
    n=d.count()
except Exception as e:
    print(f"COMPLETENESS rows=0 n_windows=0 sum_count=0 expected_windows={expected_windows} expected_sum={N} FAIL(no output: {e})"); raise SystemExit(1)
nwin=d.select("window").distinct().count()
tot=d.agg(F.sum("count")).collect()[0][0]
ok = (nwin==expected_windows) and (tot==N)
print(f"COMPLETENESS rows={n} n_windows={nwin} sum_count={tot} expected_windows={expected_windows} expected_sum={N} {'PASS' if ok else 'FAIL'}")
raise SystemExit(0 if ok else 1)
PY
RC=${PIPESTATUS[0]}
kill -9 "$SRV" 2>/dev/null; wait 2>/dev/null
exit "$RC"
