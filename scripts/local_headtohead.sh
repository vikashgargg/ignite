#!/usr/bin/env bash
# LOCAL Vajra-vs-Flink head-to-head ($0, file-based — no Kafka networking). Same JSON data dir, same
# logical query (10s event-time TUMBLE, GROUP BY window+k, COUNT), both BOUNDED → wall = catch-up
# throughput. Measures throughput (N/wall) + peak RSS. Fair: RELEASE vajra vs release Flink.
# Usage: N=10000000 K=10000 bash scripts/local_headtohead.sh
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${N:-10000000}"; K="${K:-10000}"; PORT="${PORT:-50080}"
BIN="$ROOT/target/release/vajra"; PY="$ROOT/.venvs/smoke/bin/python"
DIR=/tmp/h2h/events; FLINK_IMG=flink:1.19-scala_2.12
[ -x "$BIN" ] || { echo "FATAL: need release vajra (cargo build --release -p sail-cli --bin vajra)"; exit 2; }

echo "=== gen $N events, $K keys -> $DIR ==="
rm -rf /tmp/h2h; mkdir -p "$DIR"
"$PY" - "$N" "$K" "$DIR" <<'PY'
import sys
n,k,d=int(sys.argv[1]),int(sys.argv[2]),sys.argv[3]
base=1700000000000
with open(d+"/part.json","w") as f:
    for i in range(n):
        # k cycles 0..K; ts advances 1s every full round of K keys -> well-defined 10s windows
        f.write('{"k":%d,"ts":%d,"v":1}\n' % (i % k, base + (i // k) * 1000))
PY
echo "data: $(du -sh $DIR | cut -f1)"

# ---------------- Flink (container, batch, filesystem source, blackhole sink) ----------------
cat > /tmp/h2h/flink_job.sql <<'SQL'
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
SET 'parallelism.default' = '8';
CREATE TABLE events (
  k INT, ts BIGINT, v INT,
  event_time AS TO_TIMESTAMP_LTZ(ts, 3),
  WATERMARK FOR event_time AS event_time
) WITH ('connector'='filesystem','path'='/data/events','format'='json','json.ignore-parse-errors'='true');
CREATE TABLE sink (ws TIMESTAMP(3), k INT, cnt BIGINT) WITH ('connector'='blackhole');
INSERT INTO sink
SELECT window_start, k, COUNT(*)
FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
GROUP BY window_start, window_end, k;
SQL

echo "=== Flink run ==="
docker rm -f h2h_flink >/dev/null 2>&1
docker run -d --name h2h_flink -v /tmp/h2h:/data "$FLINK_IMG" bash -c '
  /opt/flink/bin/start-cluster.sh >/dev/null 2>&1; sleep 6
  S=$(date +%s.%N)
  /opt/flink/bin/sql-client.sh -f /data/flink_job.sql >/tmp/flink_out.log 2>&1
  E=$(date +%s.%N)
  echo "FLINK_WALL=$(echo "$E - $S" | bc)" >> /tmp/flink_out.log
  sleep 86400' >/dev/null
# wait for the job to finish
for i in $(seq 1 600); do docker exec h2h_flink grep -q "FLINK_WALL=" /tmp/flink_out.log 2>/dev/null && break; sleep 2; done
FLINK_WALL=$(docker exec h2h_flink sh -c 'grep -oE "FLINK_WALL=[0-9.]+" /tmp/flink_out.log | cut -d= -f2')
FLINK_MEM=$(docker exec h2h_flink sh -c 'cat /sys/fs/cgroup/memory.peak 2>/dev/null || echo 0')
docker exec h2h_flink sh -c 'grep -iE "Complete execution|Exception|error" /tmp/flink_out.log | head -3'
docker rm -f h2h_flink >/dev/null 2>&1

# ---------------- Vajra (release server + file windowed-agg via state_scale_stress) ------------
echo "=== Vajra run ==="
"$BIN" server --ip 127.0.0.1 --port "$PORT" >/tmp/h2h_vajra_srv.log 2>&1 & SRV=$!
for i in $(seq 1 30); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 1; done
( P=0; while kill -0 "$SRV" 2>/dev/null; do R=$(ps -o rss= -p "$SRV" 2>/dev/null | tr -d ' '); [ -n "${R:-}" ] && [ "$R" -gt "$P" ] && P=$R; echo "$P" >/tmp/h2h_vajra_peak; sleep 0.2; done ) & SMP=$!
VAJRA_LINE=$(SPARK_REMOTE="sc://localhost:$PORT" DIR="$DIR" OUT=/tmp/h2h/v_out CK=/tmp/h2h/v_ck \
  "$PY" scripts/state_scale_stress.py 2>&1 | grep -E "STREAMING")
kill "$SRV" 2>/dev/null; kill "$SMP" 2>/dev/null; wait 2>/dev/null
VAJRA_WALL=$(echo "$VAJRA_LINE" | grep -oE "wall_s=[0-9.]+" | cut -d= -f2)
VAJRA_MEM_KB=$(cat /tmp/h2h_vajra_peak 2>/dev/null || echo 0)

# ---------------- compare ----------------
echo "================= LOCAL HEAD-TO-HEAD (N=$N, K=$K) ================="
awk -v n="$N" -v fw="${FLINK_WALL:-0}" -v fm="${FLINK_MEM:-0}" -v vw="${VAJRA_WALL:-0}" -v vmk="${VAJRA_MEM_KB:-0}" 'BEGIN{
  printf "Flink : wall=%.1fs  throughput=%.2fM ev/s  peakRSS=%.2f GiB\n", fw, (fw>0?n/fw/1e6:0), fm/1073741824;
  printf "Vajra : wall=%.1fs  throughput=%.2fM ev/s  peakRSS=%.2f GiB\n", vw, (vw>0?n/vw/1e6:0), vmk/1048576;
  if(fw>0&&vw>0) printf "Vajra throughput vs Flink: %.2fx ; memory: %.2fx\n", fw/vw, (vmk/1048576)/(fm/1073741824);
}'
echo "(note: Flink wall includes ~5s cluster+client startup; use large N so it is negligible)"
