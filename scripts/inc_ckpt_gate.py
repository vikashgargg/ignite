#!/usr/bin/env python3
"""inc-ckpt.4 gate: incremental checkpointing in CONTINUOUS mode — EXACTLY-ONCE across a hard crash
+ per-checkpoint bytes = O(delta). Driven by scripts/inc_ckpt_gate.sh.

Continuous stateful windowed COUNT over N distinct keys (env N, default 2000) so the operator's keyed
state is non-trivial (with a small ZELOX_STREAMING_STATE_BUDGET_BYTES it SPILLS → immutable chunks).
Under ZELOX_INC_CKPT=1 each Checkpoint{epoch} writes only a MANIFEST (references the chunks) + a small
residual — independent of total state size — instead of a full re-copy. Recovery restores the committed
epoch's manifest → residual + chunks (restore_epoch_incremental). The crash probe (an OPEN window at
kill -9) only survives if that incremental snapshot+restore is correct.

Phases (server start -> w1 -> kill -9 -> restart -> check):
  w1   : produce windows W0,W1,W2 (10 events/(window,key)). Producing W2 advances the 0s-watermark to
         close+commit W0,W1; W2 stays OPEN at the crash.
  check: produce W3,W4,W5 + flush -> close W2..W5; verify durable output: 6 windows × N keys, each
         count == 10, no dup/loss == exactly-once across the crash (via incremental restore).
"""
import sys, time, json, os, subprocess
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, IntegerType

PORT, PHASE = sys.argv[1], sys.argv[2]
N = int(os.environ.get("N", "2000"))
RUN_SECS = int(os.environ.get("RUN", "10"))
OUT, CK, TOPIC, BOOT = "/tmp/incckpt_out", "/tmp/incckpt_ck", "incckpt_eo", "localhost:9092"
BASE = 1700000000000
_b = SparkSession.builder.remote(f"sc://localhost:{PORT}")
_shuf = os.environ.get("SHUFFLE")
if _shuf:
    _b = _b.config("spark.sql.shuffle.partitions", _shuf)
s = _b.getOrCreate()
schema = StructType([StructField("k", IntegerType()), StructField("ts", LongType())])  # k Long so `partition` is the lone Int32 (prove-it detection)


def produce(windows, extra_ts=None):
    lines = []
    for w in windows:
        for k in range(N):
            for _ in range(10):
                lines.append(json.dumps({"k": k, "ts": BASE + w * 10000 + 1000}))
    if extra_ts is not None:
        lines.append(json.dumps({"k": 0, "ts": extra_ts}))
    subprocess.run(
        ["docker", "exec", "-i", "zelox_kafka", "/opt/kafka/bin/kafka-console-producer.sh",
         "--bootstrap-server", BOOT, "--topic", TOPIC],
        input=("\n".join(lines) + "\n").encode(), capture_output=True, check=True)


def run(seconds):
    raw = (s.readStream.format("kafka").option("kafka.bootstrap.servers", BOOT)
           .option("subscribe", TOPIC).option("startingOffsets", "earliest").load())
    # GENERAL query (drops partition): rewriter now auto-preserves the source partition column.
    parsed = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
              .select(F.col("e.k").alias("k"), (F.col("e.ts") / 1000).cast("timestamp").alias("et")))
    agg = parsed.withWatermark("et", "0 seconds").groupBy(F.window("et", "10 seconds"), "k").count()
    q = (agg.writeStream.format("parquet").option("path", OUT).option("checkpointLocation", CK)
         .outputMode("append").trigger(continuous="1 second").start())
    time.sleep(seconds)
    try:
        q.stop()
    except Exception:
        pass


if PHASE == "w1":
    import shutil
    shutil.rmtree(OUT, ignore_errors=True); shutil.rmtree(CK, ignore_errors=True)
    produce([0, 1, 2])
    run(RUN_SECS)
    print("W1 committed rows=", s.read.parquet(OUT).count())
elif PHASE == "check":
    produce([3, 4, 5], extra_ts=BASE + 70000)
    run(RUN_SECS)
    rows = s.read.parquet(OUT).select(F.col("window.start").alias("ws"), "k", "count").collect()
    keyset = set((str(r["ws"]), r["k"]) for r in rows)
    n = len(rows)
    distinct_windows = len(set(p[0] for p in keyset))
    counts_ok = all(r["count"] == 10 for r in rows)
    no_dup = len(keyset) == n
    ok = n == 6 * N and distinct_windows == 6 and counts_ok and no_dup
    print(f"CHECK rows={n} expected={6*N} distinct_windows={distinct_windows} "
          f"all_counts_10={counts_ok} no_dup={no_dup}")
    print("INC_CKPT_EO_ACROSS_CRASH", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)
