#!/usr/bin/env bash
# Local FREE reproduction of the EKS scale-dependent continuous-mode over-emit (clean run, NO crash,
# still dup>0 at 100M/16-part). Mirrors the EKS shape: 16-partition topic, producer scheme identical to
# k8s/stream/producer-job.yaml (k=i%K, ts=BASE+i//EPMS, partition=i%NP), vajra local-cluster --workers 4,
# continuous windowed-agg -> local parquet, verify dup = rows - distinct(window,k). No S3, no EKS, no cost.
# Usage: N=30000000 WORKERS=4 RUN=120 bash scripts/local_continuous_scale.sh
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${N:-30000000}"; K="${K:-1000}"; EPMS="${EPMS:-1000}"; NP="${NP:-16}"; WORKERS="${WORKERS:-4}"; RUN="${RUN:-120}"
PORT="${PORT:-50095}"; TOPIC=events_scale; OUT=/tmp/contscale_out; CK=/tmp/contscale_ck
BIN="$ROOT/target/debug/vajra"; PY="$ROOT/.venvs/smoke/bin/python"
KPOD=$(docker ps --format '{{.Names}}' | grep -i kafka | head -1); [ -n "$KPOD" ] || { echo "FATAL: no kafka"; exit 2; }
rm -rf "$OUT" "$CK"
docker exec "$KPOD" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$TOPIC" >/dev/null 2>&1; sleep 2
docker exec "$KPOD" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$TOPIC" --partitions "$NP" --replication-factor 1 >/dev/null 2>&1

echo "=== produce N=$N (K=$K keys, $NP parts) [verified, self-checking] ==="
# Closer DISABLED by default: a far-ahead closer jumps the watermark and triggers emitted_ends pruning
# re-emit (a SEPARATE watermark-jump issue, not the idle over-emit). Real time-ordered streams advance the
# watermark gradually. Completeness is covered by correctness_gate; this gate targets the scale over-emit.
CLOSER_TS="${CLOSER_TS:-}"
BOOT=localhost:9092 TOPIC=$TOPIC N=$N K=$K EPMS=$EPMS NP=$NP KPOD="$KPOD" CLOSER_TS="$CLOSER_TS" "$PY" "$ROOT/scripts/scale_producer.py" \
  2>&1 | grep -aE "PRODUCED|TOPIC_CHECK|PRODUCER_OK|FATAL"
[ "${PIPESTATUS[0]}" = "0" ] || { echo "ABORT: producer self-check failed (no valid data to test)"; exit 3; }

start_srv() {
  RUST_LOG=warn "$BIN" server --ip 127.0.0.1 --port "$PORT" --mode local-cluster --workers "$WORKERS" >/tmp/contscale_server.log 2>&1 &
  SRV=$!; for i in $(seq 1 30); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 1; done
}
run_query() { # background
  SPARK_REMOTE="sc://localhost:$PORT" BOOT=localhost:9092 TOPIC=$TOPIC N_EVENTS=$N OUT=$OUT CK=$CK RUN_SECS=$RUN \
    "$PY" "$ROOT/scripts/stream_windowed_agg_continuous.py" >/tmp/contscale_q.log 2>&1 &
}
echo "=== vajra server --workers $WORKERS (CRASH=${CRASH:-0}) ==="
pkill -9 -f 'target/debug/vajra' 2>/dev/null; sleep 1
start_srv
if [ "${CRASH:-0}" = "1" ]; then
  echo "=== continuous run (bg), kill -9 mid-run, restart, resume (prod-scale crash-EO) ==="
  run_query; QPID=$!
  sleep "${CRASH_AT:-25}"
  echo "== CHAOS: kill -9 server =="; kill -9 "$SRV" 2>/dev/null; wait "$QPID" 2>/dev/null; sleep 3
  start_srv
  echo "== restart: resume from checkpoint =="
  run_query; QPID=$!; wait "$QPID" 2>/dev/null
else
  echo "=== continuous run (RUN=${RUN}s, no crash) ==="
  run_query; QPID=$!; wait "$QPID" 2>/dev/null
fi
grep -aoE 'VAJRA_WAGG.*' /tmp/contscale_q.log || true

echo "=== verify (dup = rows - distinct; exclude closer sentinel k<0) ==="
SPARK_REMOTE="sc://localhost:$PORT" OUT=$OUT "$PY" - <<'PY'
import os
from pyspark.sql import SparkSession, functions as F
s=SparkSession.builder.remote(f"sc://localhost:{os.environ['SPARK_REMOTE'].split(':')[-1]}").getOrCreate()
try:
    d=s.read.parquet(os.environ["OUT"]).where(F.col("k") >= 0)  # drop closer sentinel
    n=d.count()
except Exception as e:
    print(f"FATAL: output unreadable/empty ({e}) — query did not commit; test INVALID"); raise SystemExit(3)
if n==0:
    print("FATAL: 0 rows committed — query did not run; test INVALID"); raise SystemExit(3)
distinct=d.select("window","k").distinct().count(); tot=d.agg(F.sum("count")).collect()[0][0]
print(f"LOCAL_SCALE rows={n} distinct_window_key={distinct} sum_count={tot} dup={n-distinct} "
      f"{'DUP' if n>distinct else 'NO_DUP'}")
PY
kill -9 "$SRV" 2>/dev/null; wait 2>/dev/null