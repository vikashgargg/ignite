#!/usr/bin/env bash
# T1 gate for Gap 2 (realtime Kafka SINK throughput). ISOLATED (no concurrent loadgen): pre-load a topic
# with N messages, then drain it through a Zelox Kafka->Kafka passthrough (availableNow, which terminates)
# and measure throughput = N / wall. Compares against the Parquet/S3 sink (~5M ev/s) to prove the Kafka
# sink is the bottleneck. Self-checking (produced count + sink_out count). Usage: N=2000000 WORKERS=2 ...
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${N:-2000000}"; K="${K:-1000}"; EPMS="${EPMS:-1000}"; NP="${NP:-16}"; WORKERS="${WORKERS:-2}"
PORT="${PORT:-50098}"; IN=sink_in; OUT=sink_out; CK=/tmp/sink_ck
BIN="$ROOT/target/debug/zelox"; PY="$ROOT/.venvs/smoke/bin/python"
KPOD=$(docker ps --format '{{.Names}}' | grep -i kafka | head -1); [ -n "$KPOD" ] || { echo "FATAL: no kafka"; exit 2; }
rm -rf "$CK"
for t in "$IN" "$OUT"; do
  docker exec "$KPOD" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$t" >/dev/null 2>&1
done; sleep 2
for t in "$IN" "$OUT"; do
  docker exec "$KPOD" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$t" --partitions "$NP" --replication-factor 1 >/dev/null 2>&1
done

echo "=== pre-load $IN with N=$N (isolated: produced BEFORE the drain) ==="
BOOT=localhost:9092 TOPIC=$IN N=$N K=$K EPMS=$EPMS NP=$NP KPOD="$KPOD" "$PY" "$ROOT/scripts/scale_producer.py" 2>&1 | grep -aE "PRODUCED|TOPIC_CHECK|PRODUCER_OK|FATAL"
[ "${PIPESTATUS[0]}" = "0" ] || { echo "ABORT: producer self-check failed"; exit 3; }

echo "=== zelox server --workers $WORKERS ==="
pkill -9 -f 'target/debug/zelox' 2>/dev/null; sleep 1
RUST_LOG=warn "$BIN" server --ip 127.0.0.1 --port "$PORT" --mode local-cluster --workers "$WORKERS" >/tmp/sink_server.log 2>&1 &
SRV=$!; for i in $(seq 1 30); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 1; done

echo "=== drain $IN -> $OUT via Zelox CONTINUOUS passthrough for ${RUN:-30}s; measure DELIVERED throughput ==="
RUN="${RUN:-30}"
SPARK_REMOTE="sc://localhost:$PORT" BOOT=localhost:9092 IN=$IN OUT=$OUT CK=$CK RUN=$RUN "$PY" - <<'PY' &
import os, time
from pyspark.sql import SparkSession, functions as F
s=SparkSession.builder.remote(os.environ['SPARK_REMOTE']).getOrCreate()
BOOT=os.environ['BOOT']
raw=(s.readStream.format('kafka').option('kafka.bootstrap.servers',BOOT)
     .option('subscribe',os.environ['IN']).option('startingOffsets','earliest').load())
out=raw.select(F.col('value'))
q=(out.writeStream.format('kafka').option('kafka.bootstrap.servers',BOOT)
   .option('topic',os.environ['OUT']).option('checkpointLocation',os.environ['CK'])
   .trigger(continuous='1 second').start())
time.sleep(int(os.environ['RUN']))
try: q.stop()
except Exception: pass
PY
QPID=$!
# Sample OUT delivery over the run to compute the SUSTAINED continuous throughput (isolated: no concurrent
# loadgen — the backlog was pre-loaded).
sleep "$RUN"; wait "$QPID" 2>/dev/null; sleep 2
OUTTOT=$(docker exec "$KPOD" /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$OUT" 2>/dev/null | awk -F: '{s+=$3} END{print s}')
awk -v o="${OUTTOT:-0}" -v r="$RUN" -v n="$N" 'BEGIN{printf "KAFKA_SINK_CONTINUOUS delivered=%d of %d in %ds = %.4fM_msg/s %s\n", o, n, r, o/r/1e6, (o>=n?"DRAINED":"backlog-bound")}'
kill -9 "$SRV" 2>/dev/null; wait 2>/dev/null
