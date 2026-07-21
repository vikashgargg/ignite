#!/usr/bin/env bash
# Realtime end-to-end latency gate — PRODUCTION_READINESS.md §4 (latency) / "beats Flink".
#
# Latency is Flink's defining property and where the no-JVM/no-GC architecture should win on
# the TAIL (no GC pauses). This measures produce -> Zelox realtime (Kafka->Kafka passthrough,
# continuous trigger, at-least-once = the low-latency per-flush path) -> output-visible, per
# record: latency_ms = consume_wall_ms - embedded produce_ts_ms. Reports p50/p99/p99.9/max.
#
# Local/free (local Kafka + zelox binary). For a "beats Flink" claim, run the same passthrough
# on Flink and compare tails. Usage: BOOT=localhost:9092 DURATION_S=60 RATE=20000 scripts/stream_latency.sh
set -uo pipefail
BOOT="${BOOT:-localhost:9092}"; IN_TOPIC="${IN_TOPIC:-lat_in}"; OUT_TOPIC="${OUT_TOPIC:-lat_out}"
PORT="${PORT:-50072}"; CK="${CK:-/tmp/lat_ck}"; RATE="${RATE:-20000}"; DURATION_S="${DURATION_S:-60}"
WORKERS="${WORKERS:-2}"; ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN=""; for c in "$ROOT/target/release/zelox" "$ROOT/target/debug/zelox"; do [ -x "$c" ] && BIN="$c" && break; done
[ -z "$BIN" ] && { echo "FATAL: build zelox (cargo build --release -p sail-cli)" >&2; exit 2; }
PY="$ROOT/.venvs/smoke/bin/python"; [ -x "$PY" ] || { echo "FATAL: .venvs/smoke missing" >&2; exit 2; }
KPOD=$(docker ps --format '{{.Names}}' | grep -i kafka | head -1); [ -z "$KPOD" ] && { echo "FATAL: no kafka" >&2; exit 2; }
echo "=== latency: binary=$BIN duration=${DURATION_S}s rate=${RATE}/s ==="
rm -rf "$CK"
for t in "$IN_TOPIC" "$OUT_TOPIC"; do
  docker exec "$KPOD" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$t" >/dev/null 2>&1
done; sleep 3
for t in "$IN_TOPIC" "$OUT_TOPIC"; do
  docker exec "$KPOD" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$t" --partitions 16 --replication-factor 1 >/dev/null 2>&1
done

RUST_LOG=warn "$BIN" server --ip 127.0.0.1 --port "$PORT" --mode local-cluster --workers "$WORKERS" >/tmp/lat_server.log 2>&1 &
SRV=$!; for i in $(seq 1 30); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 1; done
echo "server pid=$SRV"

# Start the passthrough query, give it a moment to subscribe (startingOffsets=latest).
SPARK_REMOTE="sc://localhost:$PORT" BOOT="$BOOT" IN_TOPIC="$IN_TOPIC" OUT_TOPIC="$OUT_TOPIC" CK="$CK" \
  "$PY" "$ROOT/scripts/stream_latency_query.py" >/tmp/lat_query.log 2>&1 &
QUERY=$!; sleep 12

# Producer (embeds wall-clock produce_ts ms) + latency consumer, concurrently for DURATION_S.
"$PY" - "$BOOT" "$IN_TOPIC" "$RATE" "$DURATION_S" <<'PY' &
import sys, time, json
from confluent_kafka import Producer
boot, topic, rate, dur = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
p = Producer({"bootstrap.servers": boot, "linger.ms": 5, "queue.buffering.max.messages": 2000000})
i, t0 = 0, time.time()
while time.time() - t0 < dur:
    s = time.time()
    for _ in range(rate):
        now_ms = int(time.time() * 1000)
        while True:
            try: p.produce(topic, value=json.dumps({"id": i, "ts": now_ms})); break
            except BufferError: p.poll(0.01)
        i += 1
    p.poll(0)
    dt = time.time() - s
    if dt < 1.0: time.sleep(1.0 - dt)
p.flush()
PY
PRODUCER=$!

"$PY" - "$BOOT" "$OUT_TOPIC" "$DURATION_S" <<'PY'
import sys, time, json
from confluent_kafka import Consumer
boot, topic, dur = sys.argv[1], sys.argv[2], int(sys.argv[3])
c = Consumer({"bootstrap.servers": boot, "group.id": f"lat-{time.time()}",
              "auto.offset.reset": "latest", "enable.auto.commit": False})
c.subscribe([topic])
lat = []
t0 = time.time()
while time.time() - t0 < dur + 8:
    m = c.poll(0.5)
    if m is None or m.error():
        continue
    try:
        v = json.loads(m.value())
        lat.append(int(time.time() * 1000) - int(v["ts"]))
    except Exception:
        pass
c.close()
lat = sorted(x for x in lat if x >= 0)
def pct(p):
    return lat[min(len(lat) - 1, int(len(lat) * p / 100))] if lat else -1
if lat:
    print(f"LATENCY_RESULT n={len(lat)} p50_ms={pct(50)} p99_ms={pct(99)} "
          f"p999_ms={pct(99.9)} max_ms={lat[-1]} min_ms={lat[0]}")
else:
    print("LATENCY_RESULT n=0 (no output observed)")
PY
RC=$?
kill "$PRODUCER" "$QUERY" 2>/dev/null; sleep 2; kill -9 "$SRV" 2>/dev/null; wait 2>/dev/null
echo "=== done (server log /tmp/lat_server.log, query log /tmp/lat_query.log) ==="
exit $RC
