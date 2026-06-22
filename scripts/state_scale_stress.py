#!/usr/bin/env python3
"""Large-state stress / correctness gate — the prod-grade-vs-Flink test.

Streaming event-time windowed COUNT with N distinct keys all in ONE window (forces the operator to
hold N partial-agg rows in memory simultaneously + emit N groups). Compares streaming vs batch.

FINDING 2026-06-22 (BUG): streaming windowed-agg SILENTLY CAPS distinct groups at 65536 (2^16) and
drops the rest, while batch groupBy handles the same keys perfectly:
  input 50,000  -> streaming out 50,000   (ok, below cap)
  input 70,000  -> streaming out 65,536   (LOST 4,464)
  input 200,000 -> streaming out 65,536   (LOST 134,464);  batch groupBy(k).count() = 200,001 (ok)
Parallelism-independent (shuffle.partitions=1 also caps) => the cap is in WindowAccumExec / its
streaming input, NOT the exchange/merge. Flink handles billions of keys. This is a P0 correctness
gap (silent data loss at cardinality > 64K). See docs/PRODUCTION_READINESS.md / STREAMING_ARCHITECTURE.md.

Usage (server running on $PORT, file of N {"k":i,"ts":..} rows in $DIR):
  SPARK_REMOTE=sc://localhost:PORT DIR=/tmp/sscale OUT=/tmp/ss_out CK=/tmp/ss_ck python state_scale_stress.py
"""
import os, time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType

s = SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).getOrCreate()
DIR, OUT, CK = os.environ["DIR"], os.environ["OUT"], os.environ["CK"]
schema = StructType([StructField("k", LongType()), StructField("ts", LongType())])

# batch reference (correct at any cardinality)
b = s.read.schema(schema).json(DIR)
print(f"BATCH distinct_k={b.select('k').distinct().count()} groupby_rows={b.groupBy('k').count().count()}")

# streaming windowed-agg (the bug surfaces here)
raw = s.readStream.format("json").schema(schema).load(DIR)
parsed = raw.select("k", (F.col("ts") / 1000).cast("timestamp").alias("et"))
agg = parsed.withWatermark("et", "0 seconds").groupBy(F.window("et", "10 seconds"), "k").count()
t0 = time.time()
q = (agg.writeStream.format("parquet").option("path", OUT).option("checkpointLocation", CK)
     .outputMode("append").trigger(availableNow=True).start())
q.awaitTermination()
print(f"STREAMING windowed_out_rows={s.read.parquet(OUT).count()} wall_s={time.time()-t0:.1f}")
