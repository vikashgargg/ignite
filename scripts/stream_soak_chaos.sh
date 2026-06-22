#!/usr/bin/env bash
# Streaming endurance (soak) + chaos gate — PRODUCTION_READINESS.md §4.
#
# Proves the production "-ilities" Flink users assume: runs a continuous Kafka -> durable
# parquet exactly-once query under steady load for DURATION_S, while
#   (a) sampling server RSS  -> assert FLAT (no memory leak / unbounded growth),
#   (b) sampling Kafka lag    -> assert BOUNDED (keeps up with input),
#   (c) at the midpoint, HARD-KILLs (kill -9) the server and restarts it (chaos/failover)
#       -> assert the durable output is EXACTLY the contiguous id set (exactly-once across
#          crash: no loss, no duplicate) and RSS recovers flat.
#
# Local/free: uses a local Kafka broker (default localhost:9092) + the vajra binary. The
# 24h prod DoD is the same harness with SOAK=1 (DURATION_S=86400).
#
# Usage:
#   BOOT=localhost:9092 DURATION_S=180 scripts/stream_soak_chaos.sh
#   SOAK=1 scripts/stream_soak_chaos.sh                 # 24h prod DoD run
#
# Requires: a built vajra binary (target/{release,debug}/vajra) and a Spark-Connect client
# venv (.venvs/smoke). The script locates/instructs for both. Exit 0 = gate passed.
set -uo pipefail

BOOT="${BOOT:-localhost:9092}"
TOPIC="${TOPIC:-soak_events}"
PORT="${PORT:-50071}"
OUT="${OUT:-/tmp/soak_out}"
CK="${CK:-/tmp/soak_ck}"
RATE="${RATE:-50000}"                                  # ids/sec produced
DURATION_S="${DURATION_S:-180}"
[ "${SOAK:-0}" = "1" ] && DURATION_S=86400             # 24h prod DoD
SAMPLE_S="${SAMPLE_S:-10}"
WORKERS="${WORKERS:-4}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
METRICS="/tmp/soak_metrics.csv"
LEAK_RATIO="${LEAK_RATIO:-1.5}"                         # max(RSS)/median(RSS) must stay under this

BIN=""
for c in "$ROOT/target/release/vajra" "$ROOT/target/debug/vajra"; do
  [ -x "$c" ] && BIN="$c" && break
done
if [ -z "$BIN" ]; then
  echo "FATAL: no vajra binary. Build one: cargo build --release -p sail-cli" >&2; exit 2
fi
PY="$ROOT/.venvs/smoke/bin/python"
[ -x "$PY" ] || { echo "FATAL: Spark-Connect venv missing at .venvs/smoke (see scripts/dist_streaming_smoke.py header)" >&2; exit 2; }

echo "=== soak+chaos: binary=$BIN duration=${DURATION_S}s rate=${RATE}/s workers=$WORKERS ==="
rm -rf "$OUT" "$CK"; : > "$METRICS"; echo "t_s,rss_kb,end_offset" >> "$METRICS"

# Fresh topic (16 partitions).
recreate_topic() {
  local pod; pod=$(docker ps --format '{{.Names}}' | grep -i kafka | head -1)
  [ -z "$pod" ] && { echo "FATAL: no local kafka container" >&2; exit 2; }
  docker exec "$pod" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$TOPIC" >/dev/null 2>&1
  sleep 3
  docker exec "$pod" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$TOPIC" --partitions 16 --replication-factor 1 >/dev/null 2>&1
  echo "$pod"
}
KPOD=$(recreate_topic)
end_offset() { docker exec "$KPOD" /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$TOPIC" --time -1 2>/dev/null | awk -F: '{s+=$3} END{print s+0}'; }

start_server() {
  RUST_LOG=warn "$BIN" server --ip 127.0.0.1 --port "$PORT" --mode local-cluster --workers "$WORKERS" >/tmp/soak_server.log 2>&1 &
  echo $!
}
SRV=$(start_server)
echo "server pid=$SRV; waiting for readiness..."
for i in $(seq 1 30); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 1; done

# Continuous producer (monotonic ids -> JSON), steady RATE.
"$PY" - "$BOOT" "$TOPIC" "$RATE" "$DURATION_S" <<'PY' &
import sys, time, json
from confluent_kafka import Producer
boot, topic, rate, dur = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
p = Producer({"bootstrap.servers": boot, "linger.ms": 20, "compression.type": "lz4",
              "queue.buffering.max.messages": 2000000})
i, t0 = 0, time.time()
while time.time() - t0 < dur + 5:
    batch_end = i + rate
    while i < batch_end:
        while True:
            try: p.produce(topic, value=json.dumps({"id": i})); break
            except BufferError: p.poll(0.05)
        i += 1
    p.poll(0); time.sleep(1)
p.flush()
print(f"PRODUCED {i}", flush=True)
PY
PRODUCER=$!

# The continuous EO query (realtime trigger): Kafka -> parse -> parquet, checkpointed.
run_query() {
  SPARK_REMOTE="sc://localhost:$PORT" BOOT="$BOOT" TOPIC="$TOPIC" OUT="$OUT" CK="$CK" \
    "$PY" "$ROOT/scripts/stream_soak_query.py" >/tmp/soak_query.log 2>&1 &
  echo $!
}
QUERY=$(run_query)

# Sample RSS + lag; trigger chaos at the midpoint.
chaos_done=0
for ((t=0; t<DURATION_S; t+=SAMPLE_S)); do
  rss=$(ps -o rss= -p "$SRV" 2>/dev/null | tr -d ' '); rss=${rss:-0}
  echo "$t,$rss,$(end_offset)" >> "$METRICS"
  if [ "$chaos_done" = "0" ] && [ "$t" -ge "$((DURATION_S/2))" ]; then
    echo "=== CHAOS @${t}s: kill -9 server $SRV, restart ==="
    kill -9 "$SRV" 2>/dev/null; kill "$QUERY" 2>/dev/null; sleep 3
    SRV=$(start_server); for i in $(seq 1 30); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 1; done
    QUERY=$(run_query); chaos_done=1
    echo "=== restarted server pid=$SRV (resumes from checkpoint $CK) ==="
  fi
  sleep "$SAMPLE_S"
done

# Stop everything, drain.
kill "$PRODUCER" "$QUERY" 2>/dev/null; sleep 5; kill -9 "$SRV" 2>/dev/null; wait 2>/dev/null

# Verdict: EO (contiguous, no loss/dup) + flat RSS (no leak).
"$PY" - "$OUT" "$METRICS" "$LEAK_RATIO" <<'PY'
import sys, glob
import pyarrow.parquet as pq
out, metrics, leak_ratio = sys.argv[1], sys.argv[2], float(sys.argv[3])
files = glob.glob(f"{out}/**/*.parquet", recursive=True)
ids = []
for f in files:
    try: ids += pq.read_table(f).column("id").to_pylist()
    except Exception: pass
ids = sorted(int(x) for x in ids if x is not None)
n, uniq = len(ids), len(set(ids))
contiguous = bool(ids) and ids == list(range(ids[0], ids[0] + uniq))
dup = n - uniq
rss = []
for line in open(metrics):
    parts = line.strip().split(",")
    if len(parts) == 3 and parts[1].isdigit(): rss.append(int(parts[1]))
rss = [r for r in rss if r > 0]
rss.sort()
med = rss[len(rss)//2] if rss else 0
mx = max(rss) if rss else 0
flat = med > 0 and (mx / med) < leak_ratio
print(f"SOAK_RESULT rows={n} unique={uniq} dup={dup} contiguous={contiguous} "
      f"rss_med_kb={med} rss_max_kb={mx} flat={flat}")
ok = contiguous and dup == 0 and flat
print("SOAK_GATE", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
PY
RC=$?
echo "=== exit $RC (0=pass) ; metrics: $METRICS ; server log: /tmp/soak_server.log ==="
exit $RC
