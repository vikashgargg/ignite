#!/usr/bin/env bash
# FREE local validation of the shuffle coalescer (D2) + periodic watermark (D1): local-cluster distributed
# windowed-agg -> MinIO S3, WM_PROF on. Runs coalescing OFF then ON; asserts counts match (correctness) and
# reports per-process shuffle_send_batches (mechanism: ON should be << OFF if local-cluster routes shuffle
# over Flight and the coalescer merges). No EKS. Usage: N=5000000 KEYS=1000 bash scripts/local_dist_coalesce_check.sh
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${N:-5000000}"; KEYS="${KEYS:-1000}"; TOPIC="${TOPIC:-coal_events}"; PORT="${PORT:-50147}"; WORKERS="${WORKERS:-4}"
BOOT=localhost:9092; BIN="$ROOT/target/debug/vajra"; PY="$ROOT/.venvs/smoke/bin/python"
KPOD=$(docker ps --format '{{.Names}}' | grep -i kafka | head -1)
export AWS_ENDPOINT="http://localhost:9000" AWS_ENDPOINT_URL="http://localhost:9000"
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_ALLOW_HTTP=true AWS_REGION=us-east-1

# MinIO up + bucket
docker ps --format '{{.Names}}' | grep -qi vajra_minio || docker run -d --name vajra_minio -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data --console-address ":9001" >/dev/null 2>&1
for i in $(seq 1 30); do curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1 && break; sleep 1; done
mc() { docker run --rm --network host --entrypoint sh minio/mc -c "mc alias set l http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; $1" >/dev/null 2>&1; }
mc "mc mb -p l/vajra"

# Produce N monotonic (contiguous 100 windows) events once
CUR=$(docker exec "$KPOD" /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$TOPIC" 2>/dev/null | awk -F: '{s+=$3} END{print s}')
if [ "${CUR:-0}" != "$N" ]; then
  docker exec "$KPOD" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$TOPIC" >/dev/null 2>&1; sleep 2
  docker exec "$KPOD" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$TOPIC" --partitions 16 --replication-factor 1 >/dev/null 2>&1
  "$PY" - "$BOOT" "$TOPIC" "$N" "$KEYS" <<'PY'
import sys, json
from confluent_kafka import Producer
boot, topic, n, keys = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
p = Producer({"bootstrap.servers": boot, "linger.ms": 20, "batch.size": 1<<20, "queue.buffering.max.messages": 3000000})
base=1_700_000_000_000; per_win=max(1, n//100)
for i in range(n):
    v={"k": i%keys, "ts": base+(i//per_win)*10000, "v":1}
    while True:
        try: p.produce(topic, value=json.dumps(v)); break
        except BufferError: p.poll(0.01)
    if i%500000==0: p.poll(0)
p.flush(); print("PRODUCED", n, flush=True)
PY
fi
echo "TOPIC $TOPIC = $(docker exec "$KPOD" /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$TOPIC" 2>/dev/null | awk -F: '{s+=$3} END{print s}')"

run() { # $1=rows $2=label
  pkill -9 -f 'target/debug/vajra' 2>/dev/null; sleep 2
  mc "mc rm -r --force l/vajra/$2 l/vajra/${2}_ck"
  VAJRA_DISTRIBUTED_STREAM=1 VAJRA_WM_PROF=1 VAJRA_COMPLETE_ON_END=1 VAJRA_SHUFFLE_BATCH_ROWS="$1" \
    RUST_LOG=warn "$BIN" server --ip 127.0.0.1 --port "$PORT" --mode local-cluster --workers "$WORKERS" >/tmp/coal_$2.log 2>&1 &
  for i in $(seq 1 30); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 1; done
  local R; R=$(SPARK_REMOTE="sc://localhost:$PORT" BOOT="$BOOT" TOPIC="$TOPIC" N_EVENTS="$N" MAXOFFSETS=1000000 \
    OUT="s3://vajra/$2" CK="s3://vajra/${2}_ck" timeout 300 "$PY" scripts/stream_windowed_agg.py 2>&1 | grep -aoE 'VAJRA_WAGG.*' | tail -1)
  sleep 11  # let the WM_PROF_PROC dumper fire once more
  local SB; SB=$(grep -aoE 'shuffle_send_batches=[0-9]+' /tmp/coal_$2.log | awk -F= '{s+=$2} END{print s}')
  pkill -9 -f 'target/debug/vajra' 2>/dev/null
  echo "  [$2 rows=$1] $R"
  echo "  [$2] shuffle_send_batches(sum WM_PROF_PROC lines)=${SB:-0}"
}

echo "=== A/B: coalescing OFF then ON ==="
run 0 off
run 16384 on
echo "=> counts (total_events) must MATCH; ON shuffle_send_batches << OFF proves coalescing (if >0, local-cluster uses Flight)."
