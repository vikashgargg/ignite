#!/usr/bin/env python3
"""Distributed-streaming smoke harness (F3 gate).

Runs probes against a Vajra server in local-cluster mode (driver + N in-process
workers) through real Spark Connect. Start the server first:

    target/debug/vajra server --mode local-cluster --workers 2 --port 50081

then:

    .venvs/smoke/bin/python scripts/dist_streaming_smoke.py 50081

Probes (expected values cross-checked against real Spark 3.5.3 on the same inputs):
  1. batch.write          — distributed read -> compute -> parquet write          (1000)
  2. stream.rate          — stateless rate -> filter -> parquet (availableNow)     (>0)
  3. stream.file          — stateless FILE -> filter -> parquet (availableNow)     (1000)
  4. stream.windowed_file — keyed event-time window agg over a FILE source         (97)

Probe 4's input spans 100s of event-time over 5 keys with a 2s watermark; the
watermark closes 97 (window,key) groups — Spark produces exactly 97 too (the
earlier rate+availableNow windowed probe correctly produced 0 because a single
batch + 2s watermark closes no window; that matched Spark and was NOT a bug).
"""
import sys, shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PORT = sys.argv[1] if len(sys.argv) > 1 else "50081"
s = SparkSession.builder.remote(f"sc://localhost:{PORT}").getOrCreate()
results = []


def check(name, ok, detail=""):
    results.append((name, ok))
    print(("PASS" if ok else "FAIL"), name, detail)


def reset(*dirs):
    for d in dirs:
        shutil.rmtree(d, ignore_errors=True)


# 1. distributed batch write
try:
    out = "/tmp/dss_batch"; reset(out)
    s.range(0, 1000).selectExpr("id AS v").write.mode("overwrite").parquet(out)
    n = s.read.parquet(out).count()
    check("batch.write", n == 1000, f"rows={n}")
except Exception as e:
    check("batch.write", False, f"EXC {str(e)[:140]}")

# 2. stateless streaming write — rate source (single partition, no shuffle)
try:
    out, ck = "/tmp/dss_rate", "/tmp/dss_rate_ck"; reset(out, ck)
    df = s.readStream.format("rate").option("rowsPerSecond", "20000").load().selectExpr("value AS v")
    q = df.writeStream.format("parquet").option("path", out).option("checkpointLocation", ck).trigger(availableNow=True).start()
    q.awaitTermination(timeout=30)
    n = s.read.parquet(out).count()
    check("stream.rate", n > 0, f"rows={n}")
except Exception as e:
    check("stream.rate", False, f"EXC {str(e)[:140]}")

# 3. stateless streaming write — FILE source
try:
    inp, out, ck = "/tmp/dss_f_in", "/tmp/dss_f_out", "/tmp/dss_f_ck"; reset(inp, out, ck)
    s.range(0, 1000).selectExpr("id AS v").coalesce(1).write.mode("overwrite").parquet(inp)
    df = s.readStream.schema("v long").parquet(inp).filter("v >= 0")
    q = df.writeStream.format("parquet").option("path", out).option("checkpointLocation", ck).trigger(availableNow=True).start()
    q.awaitTermination(timeout=30)
    n = s.read.parquet(out).count()
    check("stream.file", n == 1000, f"rows={n} (expect 1000)")
except Exception as e:
    check("stream.file", False, f"EXC {str(e)[:140]}")

# 4. keyed event-time windowed aggregation over a FILE source (event-time spans 100s)
try:
    inp, out, ck = "/tmp/dss_w_in", "/tmp/dss_w_out", "/tmp/dss_w_ck"; reset(inp, out, ck)
    s.range(0, 100).selectExpr("CAST(id AS TIMESTAMP) AS ts", "id % 5 AS k").coalesce(1).write.mode("overwrite").parquet(inp)
    df = s.readStream.schema("ts timestamp, k long").parquet(inp)
    win = df.withWatermark("ts", "2 seconds").groupBy(F.window("ts", "1 second"), F.col("k")).count()
    q = win.writeStream.format("parquet").option("path", out).option("checkpointLocation", ck).trigger(availableNow=True).start()
    q.awaitTermination(timeout=60)
    n = s.read.parquet(out).count()
    check("stream.windowed_file", n == 97, f"rows={n} (expect 97, Spark-matched)")
except Exception as e:
    check("stream.windowed_file", False, f"EXC {str(e)[:140]}")

# 5. stateful dedup — dropDuplicates over a file source carrying a non-key column
try:
    inp, out, ck = "/tmp/dss_d_in", "/tmp/dss_d_out", "/tmp/dss_d_ck"; reset(inp, out, ck)
    s.range(0, 1000).selectExpr("id % 50 AS k", "id AS v").coalesce(1).write.mode("overwrite").parquet(inp)
    df = s.readStream.schema("k long, v long").parquet(inp).dropDuplicates(["k"])
    q = df.writeStream.format("parquet").option("path", out).option("checkpointLocation", ck).trigger(availableNow=True).start()
    q.awaitTermination(timeout=60)
    d = s.read.parquet(out).select("k").distinct().count()
    check("stream.dedup", d == 50, f"distinct_k={d} (expect 50, Spark-matched)")
except Exception as e:
    check("stream.dedup", False, f"EXC {str(e)[:140]}")

# 6. stream-stream join over two file sources
try:
    lin, rin, out, ck = "/tmp/dss_jl", "/tmp/dss_jr", "/tmp/dss_j_out", "/tmp/dss_j_ck"
    reset(lin, rin, out, ck)
    s.range(0, 200).selectExpr("id AS ka", "id*10 AS va").coalesce(1).write.mode("overwrite").parquet(lin)
    s.range(0, 200).selectExpr("id AS kb", "id*100 AS vb").coalesce(1).write.mode("overwrite").parquet(rin)
    a = s.readStream.schema("ka long, va long").parquet(lin)
    b = s.readStream.schema("kb long, vb long").parquet(rin)
    j = a.join(b, F.expr("ka = kb"))
    q = j.writeStream.format("parquet").option("path", out).option("checkpointLocation", ck).trigger(availableNow=True).start()
    q.awaitTermination(timeout=60)
    n = s.read.parquet(out).count()
    check("stream.join", n == 200, f"rows={n} (expect 200, Spark-matched)")
except Exception as e:
    check("stream.join", False, f"EXC {str(e)[:140]}")

passed = sum(1 for _, ok in results if ok)
print(f"\nDIST_STREAMING_SMOKE {passed}/{len(results)} passed")
