#!/usr/bin/env python3
"""P4 batch-on-S3 benchmark (vs Spark): write a dataset to Parquet on S3, read it back + aggregate,
timing each phase + emitting count/sum for a like-for-like correctness check. Runs against any Spark
Connect remote (Zelox OR Spark local[*]) so the SAME code measures both on the same S3 data.

Emits: BATCH_RESULT engine=.. rows=N write_s=.. read_agg_s=.. total_s=.. sum_v=.. distinct_k=..
Usage: SPARK_REMOTE=sc://.. S3_PATH=s3://bucket/p4 N_ROWS=200000000 ENGINE=zelox|spark python batch_s3_bench.py
"""
import os, time
from pyspark.sql import SparkSession, functions as F

# NOTE: `SPARK_REMOTE` is a MAGIC pyspark env — if set (to anything), getOrCreate() forces Spark
# Connect mode (which needs pandas) and IGNORES .master(). So the local Spark baseline passes its
# master via the non-magic `BENCH_REMOTE` and must NOT set SPARK_REMOTE. Zelox sets SPARK_REMOTE=sc://.
REMOTE = os.environ.get("SPARK_REMOTE") or os.environ.get("BENCH_REMOTE", "sc://localhost:50051")
S3 = os.environ["S3_PATH"]
N = int(os.environ.get("N_ROWS", "200000000"))
ENGINE = os.environ.get("ENGINE", "?")
K = int(os.environ.get("KEYS", "1000"))

# sc:// = Spark Connect (Zelox); anything else (e.g. local[16]) = a local Spark master (baseline).
_b = SparkSession.builder
s = (_b.remote(REMOTE) if REMOTE.startswith("sc://") else _b.master(REMOTE)).getOrCreate()

# 1) generate + WRITE parquet to S3 (id, k=id%KEYS, v=id*2)
t0 = time.time()
df = (s.range(0, N)
      .withColumn("k", (F.col("id") % K))
      .withColumn("v", (F.col("id") * F.lit(2))))
df.write.mode("overwrite").parquet(S3)
write_s = time.time() - t0

# 2) READ back + AGGREGATE (count, sum(v), groupBy k) — the correctness + read path
t1 = time.time()
r = s.read.parquet(S3)
rows = r.count()
sum_v = r.agg(F.sum("v")).collect()[0][0]
distinct_k = r.groupBy("k").count().count()
read_agg_s = time.time() - t1

total = write_s + read_agg_s
print(f"BATCH_RESULT engine={ENGINE} rows={rows} write_s={write_s:.2f} read_agg_s={read_agg_s:.2f} "
      f"total_s={total:.2f} sum_v={sum_v} distinct_k={distinct_k}")
