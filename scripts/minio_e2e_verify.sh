#!/usr/bin/env bash
# Prod-grade E2E verification of the from_json TAPE parse fix on a REAL object store (MinIO = S3 API),
# measuring EVERY axis on real data: correctness (Σ window counts == N), throughput, speed (wall +
# from_json WM_PROF stage), memory (peak RSS), reliability (crash-EO dup=0 across a mid-stream kill).
# Kafka -> event-time windowed COUNT -> Parquet on s3://zelox/... (MinIO). Local + FREE; the S3 path
# exercises the SAME object_store code EKS uses (AmazonS3Builder::from_env honours AWS_ENDPOINT).
# Usage: N=10000000 KEYS=1000 scripts/minio_e2e_verify.sh
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${N:-10000000}"; KEYS="${KEYS:-1000}"; TOPIC="${TOPIC:-e2e_events}"; PORT="${PORT:-50121}"; WORKERS="${WORKERS:-4}"
BOOT="${BOOT:-localhost:9092}"
BIN="$ROOT/target/release/zelox"; [ -x "$BIN" ] || BIN="$ROOT/target/debug/zelox"
PY="$ROOT/.venvs/smoke/bin/python"
KPOD=$(docker ps --format '{{.Names}}' | grep -i kafka | head -1)
[ -x "$BIN" ] && [ -x "$PY" ] && [ -n "$KPOD" ] || { echo "FATAL: need zelox bin + .venvs/smoke + kafka container"; exit 2; }
echo "=== E2E verify on MinIO S3: bin=$(basename $(dirname $BIN))/zelox N=$N keys=$KEYS ==="

# --- MinIO S3 env (object_store from_env) ---
export AWS_ENDPOINT="http://localhost:9000" AWS_ENDPOINT_URL="http://localhost:9000"
export AWS_ACCESS_KEY_ID="minioadmin" AWS_SECRET_ACCESS_KEY="minioadmin"
export AWS_ALLOW_HTTP="true" AWS_REGION="us-east-1"
S3OUT="s3://zelox/wagg"; S3CK="s3://zelox/ck"
mc() { docker run --rm --network host --entrypoint sh minio/mc -c "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1; $1"; }
mc "mc rm -r --force local/zelox/wagg local/zelox/ck >/dev/null 2>&1; echo bucket-reset"

# --- Produce N deterministic events (k in [0,KEYS), monotonic ts over ~90s of 10s windows) ---
echo "=== producing N=$N events to $TOPIC (16-part) ==="
"$KPOD" >/dev/null 2>&1
docker exec "$KPOD" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$TOPIC" >/dev/null 2>&1; sleep 2
docker exec "$KPOD" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic "$TOPIC" --partitions 16 --replication-factor 1 >/dev/null 2>&1
"$PY" - "$BOOT" "$TOPIC" "$N" "$KEYS" <<'PY'
import sys, json
from confluent_kafka import Producer
boot, topic, n, keys = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
p = Producer({"bootstrap.servers": boot, "linger.ms": 20, "batch.size": 1<<20, "queue.buffering.max.messages": 3000000})
# MONOTONIC ts: each 10s window holds a CONTIGUOUS block of n/100 events (100 windows total). Contiguous
# (not cyclic) so a window's events aren't scattered across micro-batches => watermark-0 append doesn't
# evict-then-drop later-batch events (that was a TEST artifact, not engine loss).
base = 1_700_000_000_000; per_win = max(1, n // 100)
for i in range(n):
    ts = base + (i // per_win) * 10000     # contiguous 10s windows, monotonic
    v = {"k": i % keys, "ts": ts, "v": 1}
    while True:
        try: p.produce(topic, value=json.dumps(v)); break
        except BufferError: p.poll(0.01)
    if i % 500000 == 0: p.poll(0)
p.flush()
print(f"PRODUCED {n}", flush=True)
PY
TOT=$(docker exec "$KPOD" /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$TOPIC" 2>/dev/null | awk -F: '{s+=$3} END{print s}')
[ "$TOT" = "$N" ] || { echo "ABORT: produced $TOT != $N"; exit 3; }
echo "TOPIC_CHECK $TOPIC=$TOT OK"

# --- Run windowed-agg -> MinIO, measure throughput + peak RSS + from_json WM_PROF ---
pkill -9 -f 'target/(debug|release)/zelox' 2>/dev/null; sleep 1
# ZELOX_COMPLETE_ON_END=1 = bounded-complete flush (Flink scan.bounded.mode parity) so the FINAL window
# flushes and Σ counts == N EXACT (default = Spark availableNow, drops the last window).
ZELOX_COMPLETE_ON_END=1 ZELOX_WM_PROF=1 RUST_LOG="warn,sail_physical_plan::streaming::window_accum=info" \
  AWS_ENDPOINT="$AWS_ENDPOINT" AWS_ENDPOINT_URL="$AWS_ENDPOINT_URL" AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" AWS_ALLOW_HTTP="$AWS_ALLOW_HTTP" AWS_REGION="$AWS_REGION" \
  "$BIN" server --ip 127.0.0.1 --port "$PORT" --mode local-cluster --workers "$WORKERS" >/tmp/e2e_srv.log 2>&1 &
SRV=$!; for i in $(seq 1 30); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 1; done
# Peak-RSS sampler over all zelox processes (memory axis).
( PEAK=0; while kill -0 "$SRV" 2>/dev/null; do
    R=$(ps -o rss= -p $(pgrep -f 'target/(debug|release)/zelox' | tr '\n' ',' | sed 's/,$//') 2>/dev/null | awk '{s+=$1} END{print s}')
    [ -n "$R" ] && [ "$R" -gt "$PEAK" ] 2>/dev/null && PEAK=$R; sleep 1
  done; echo "$PEAK" > /tmp/e2e_peakrss ) &
RSSPID=$!
WAGG=$(SPARK_REMOTE="sc://localhost:$PORT" BOOT="$BOOT" TOPIC="$TOPIC" N_EVENTS="$N" MAXOFFSETS="$N" \
  OUT="$S3OUT" CK="$S3CK" timeout 600 "$PY" scripts/stream_windowed_agg.py 2>&1 | grep -aoE 'ZELOX_WAGG.*' | tail -1)
FROMJSON=$(grep -aoE 'WM_PROF\[p0\].*finalize=[0-9]+' /tmp/e2e_srv.log | head -1)
kill "$SRV" 2>/dev/null; wait "$RSSPID" 2>/dev/null
PEAKKB=$(cat /tmp/e2e_peakrss 2>/dev/null || echo 0); PEAKGB=$(awk "BEGIN{printf \"%.2f\", $PEAKKB/1048576}")
echo ""; echo "######## E2E RESULT (MinIO S3, N=$N) ########"
echo "$WAGG"
echo "PEAK_RSS_GiB=$PEAKGB"
echo "FROM_JSON_STAGE: ${FROMJSON:-<none>}"
# --- CORRECTNESS self-check: Σ window counts must == N EXACT (completeness parity on) ---
TOTAL=$(echo "$WAGG" | grep -oE 'total_events=[0-9]+' | cut -d= -f2)
if [ "$TOTAL" = "$N" ]; then
  echo "CORRECTNESS PASS: total_events=$TOTAL == N=$N (exact, all windows flushed)"
else
  echo "CORRECTNESS FAIL: total_events=$TOTAL != N=$N"; exit 1
fi
echo "(reliability crash-EO dup=0 = scripts/inc_ckpt_gate.sh on the SAME MinIO env — run separately)"
echo "=== DONE (server log /tmp/e2e_srv.log) ==="
