#!/usr/bin/env bash
# VAJ-BF2 T-BF2.3 T1 gate: distributed N→M streaming windowed-agg correctness.
#
# A multi-partition (N) file source → keyed event-time window agg (StreamExchange N→M) → parquet,
# on a local-cluster (driver + WORKERS in-process workers). With ZELOX_DISTRIBUTED_STREAM=1 the
# keyed exchange is CUT into a cross-network Hash shuffle whose receiver MIN-merges distinct-source
# watermarks + aligns Chandy-Lamport barriers (T-BF2.3b) and whose N window instances spread across
# workers (T-BF2.5). This gate asserts the distributed cut is EXACTLY-EQUAL to the in-process
# baseline (gate OFF) AND deterministic across repeats — i.e. the cut introduces no dup/loss.
#
# SELF-CHECKING + prod-representative: a FRESH unique input each run (no cross-run accumulation — the
# confound that once masqueraded as a 2× "dup"), identical input for both gates.
#   Usage: WORKERS=4 ROWS=4000 KEYS=50 PARTS=8 bash scripts/nm_dist_gate.sh
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
WORKERS="${WORKERS:-4}"; ROWS="${ROWS:-4000}"; KEYS="${KEYS:-50}"; PARTS="${PARTS:-8}"
BIN="$ROOT/target/debug/zelox"; PY="$ROOT/.venvs/smoke/bin/python"
STAMP="$$_$RANDOM"; INP="/tmp/nm_gate_in_$STAMP"
[ -x "$BIN" ] || { echo "FATAL: build first: cargo build -p zelox-cli --bin zelox"; exit 2; }
rm -rf "$INP"

start_srv() { # $1=gate $2=port
  pkill -9 -f 'target/debug/zelox' 2>/dev/null; sleep 1
  ZELOX_DISTRIBUTED_STREAM="$1" RUST_LOG=warn "$BIN" server --ip 127.0.0.1 --port "$2" \
    --mode local-cluster --workers "$WORKERS" >/tmp/nm_gate_srv_$1.log 2>&1 &
  for i in $(seq 1 30); do nc -z 127.0.0.1 "$2" 2>/dev/null && break; sleep 1; done
}

# Write the input ONCE (fresh), then read it with both gates against the IDENTICAL files.
start_srv 0 50190
"$PY" - "$INP" "$ROWS" "$KEYS" "$PARTS" <<'PY'
import sys
from pyspark.sql import SparkSession
inp, rows, keys, parts = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
s=SparkSession.builder.remote("sc://localhost:50190").getOrCreate()
(s.range(0, rows, 1, parts)
  .selectExpr("CAST(id AS TIMESTAMP) AS ts", f"id % {keys} AS k")
  .write.mode("overwrite").parquet(inp))
print("INPUT_WRITTEN", flush=True)
PY

probe() { # $1=gate $2=port -> prints "groups sum distinct"
  start_srv "$1" "$2"
  OUT="/tmp/nm_gate_out_${1}_$STAMP"; CK="/tmp/nm_gate_ck_${1}_$STAMP"; rm -rf "$OUT" "$CK"
  SPARK_REMOTE="sc://localhost:$2" INP="$INP" OUT="$OUT" CK="$CK" timeout 120 "$PY" - <<'PY'
import os
from pyspark.sql import SparkSession, functions as F
s=SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).getOrCreate()
df=s.readStream.schema("ts timestamp, k long").parquet(os.environ["INP"])
win=(df.withWatermark("ts","2 seconds")
       .groupBy(F.window("ts","10 seconds"), F.col("k")).count())
q=(win.writeStream.format("parquet").option("path",os.environ["OUT"])
     .option("checkpointLocation",os.environ["CK"]).outputMode("append")
     .trigger(availableNow=True).start())
q.awaitTermination()
r=s.read.parquet(os.environ["OUT"])
g=r.count(); tot=r.agg(F.sum("count")).collect()[0][0]; d=r.select("window","k").distinct().count()
print(f"RESULT {g} {tot} {d}", flush=True)
PY
}

echo "=== gate OFF (in-process exchange baseline) ==="
OFF=$(probe 0 50191 | grep -aoE 'RESULT .*' | tail -1); echo "  $OFF"
echo "=== gate ON (N→M cut + aligning shuffle + even-spread) ==="
ON1=$(probe 1 50192 | grep -aoE 'RESULT .*' | tail -1); echo "  $ON1"
echo "=== gate ON repeat (determinism) ==="
ON2=$(probe 1 50193 | grep -aoE 'RESULT .*' | tail -1); echo "  $ON2"
pkill -9 -f 'target/debug/zelox' 2>/dev/null; rm -rf "$INP" /tmp/nm_gate_out_* /tmp/nm_gate_ck_* 2>/dev/null

[ -n "$OFF" ] && [ "$OFF" != "RESULT 0 0 0" ] || { echo "NM_DIST_GATE FAIL: baseline produced no output"; exit 3; }
if [ "$ON1" = "$OFF" ] && [ "$ON2" = "$OFF" ]; then
  echo "NM_DIST_GATE PASS: distributed N→M == in-process baseline, deterministic ($OFF)"
else
  echo "NM_DIST_GATE FAIL: off=[$OFF] on=[$ON1] on2=[$ON2] — the cut changed results (dup/loss)"; exit 1
fi
