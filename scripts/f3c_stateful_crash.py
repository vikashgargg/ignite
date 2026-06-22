#!/usr/bin/env python3
"""F3-c gate: continuous STATEFUL (windowed-agg) exactly-once across a hard crash.

Two phases driven by scripts/f3c_stateful_crash.sh (start server -> w1 -> kill -9 -> restart -> check):
  w1   : produce windows W0,W1,W2 (10 events/(window,key), 2 keys). Producing W2 advances the
         0s-watermark to close W0,W1 (committed during the continuous run); W2 stays OPEN.
  check: produce W3,W4,W5 + a flush event -> closes W2..W5. Verify the durable windowed-agg output.

W2 is the F3-c probe: its events were produced PRE-CRASH and its window was OPEN at the kill -9, so
its count (10/key) only survives if the operator's per-epoch keyed state was snapshotted + restored
from the committed epoch. EO ⇒ exactly 12 rows (W0..W5 × 2 keys), each count == 10, no dup/loss.
"""
import sys, time, json, subprocess
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, IntegerType

PORT, PHASE = sys.argv[1], sys.argv[2]
OUT, CK, TOPIC, BOOT = "/tmp/f3c_out", "/tmp/f3c_ck", "f3c_eo", "localhost:9092"
BASE = 1700000000000
s = SparkSession.builder.remote(f"sc://localhost:{PORT}").getOrCreate()
schema = StructType([StructField("k", IntegerType()), StructField("ts", LongType())])


def produce(windows, extra_ts=None):
    lines = []
    for w in windows:
        for k in range(2):
            for _ in range(10):
                lines.append(json.dumps({"k": k, "ts": BASE + w * 10000 + 1000}))
    if extra_ts is not None:
        lines.append(json.dumps({"k": 0, "ts": extra_ts}))
    subprocess.run(
        ["docker", "exec", "-i", "vajra_kafka", "/opt/kafka/bin/kafka-console-producer.sh",
         "--bootstrap-server", BOOT, "--topic", TOPIC],
        input=("\n".join(lines) + "\n").encode(), capture_output=True, check=True)


def run(seconds):
    raw = (s.readStream.format("kafka").option("kafka.bootstrap.servers", BOOT)
           .option("subscribe", TOPIC).option("startingOffsets", "earliest").load())
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
    produce([0, 1, 2])      # W2 advances watermark -> closes W0,W1; W2 open
    run(8)
    print("W1 committed rows=", s.read.parquet(OUT).count())
elif PHASE == "check":
    produce([3, 4, 5], extra_ts=BASE + 70000)   # closes W2,W3,W4,W5
    run(8)
    rows = s.read.parquet(OUT).select(F.col("window.start").alias("ws"), "k", "count").collect()
    pairs = sorted((str(r["ws"]), r["k"], r["count"]) for r in rows)
    n = len(pairs)
    distinct_windows = len(set(p[0] for p in pairs))
    counts_ok = all(p[2] == 10 for p in pairs)
    no_dup = len(set((p[0], p[1]) for p in pairs)) == n
    ok = n == 12 and distinct_windows == 6 and counts_ok and no_dup
    print(f"CHECK rows={n} distinct_windows={distinct_windows} all_counts_10={counts_ok} no_dup={no_dup}")
    for p in pairs:
        print("  ", p)
    print("F3C_STATEFUL_EO_ACROSS_CRASH", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)
