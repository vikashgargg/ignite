#!/usr/bin/env python3
"""Distributed-streaming smoke harness (F3-c gate).

Runs probes against a Vajra server started in local-cluster mode (driver + N
in-process workers), through real Spark Connect — the prerequisite gate for
distributed stateful streaming. Start the server first, e.g.:

    target/debug/vajra server --mode local-cluster --workers 2 --port 50081

then:

    .venvs/smoke/bin/python scripts/dist_streaming_smoke.py 50081

Probes (each isolates one capability so a failure points at the exact gap):
  1. batch.write     — distributed read -> compute -> parquet write
  2. stream.write    — stateless streaming (rate -> filter -> parquet, availableNow)
  3. stream.windowed — keyed event-time window agg -> parquet (exchange/align/window)

Findings (2026-06-15, --workers 2): probe 1 PASS; probes 2/3 produce no output and
the streaming query goes inactive immediately. Distributed BATCH is solid; the gap
is that the streaming execution model (single-node long-lived StreamDriver) is not
integrated with the distributed cluster runner (stage-based JobGraph) — codec is
necessary but not sufficient. See docs/design/distributed-streaming-f2f3.md.
"""
import sys, time, shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PORT = sys.argv[1] if len(sys.argv) > 1 else "50081"
s = SparkSession.builder.remote(f"sc://localhost:{PORT}").getOrCreate()
results = []


def check(name, ok, detail=""):
    results.append((name, ok))
    print(("PASS" if ok else "FAIL"), name, detail)


# 1. distributed batch write
try:
    out = "/tmp/dss_batch"
    shutil.rmtree(out, ignore_errors=True)
    s.range(0, 1000).selectExpr("id AS v").write.mode("overwrite").parquet(out)
    n = s.read.parquet(out).count()
    check("batch.write", n == 1000, f"rows={n}")
except Exception as e:
    check("batch.write", False, f"EXC {str(e)[:140]}")

# 2. stateless streaming write (availableNow micro-batch)
try:
    out, ck = "/tmp/dss_stream", "/tmp/dss_stream_ck"
    for d in (out, ck):
        shutil.rmtree(d, ignore_errors=True)
    df = s.readStream.format("rate").option("rowsPerSecond", "20000").load().selectExpr("value AS v")
    q = df.writeStream.format("parquet").option("path", out).option("checkpointLocation", ck).trigger(availableNow=True).start()
    q.awaitTermination(timeout=30)
    n = s.read.parquet(out).count()
    check("stream.write", n > 0, f"rows={n}")
except Exception as e:
    check("stream.write", False, f"EXC {str(e)[:140]}")

# 3. keyed event-time windowed aggregation -> parquet (exchange/align/window)
try:
    out, ck = "/tmp/dss_win", "/tmp/dss_win_ck"
    for d in (out, ck):
        shutil.rmtree(d, ignore_errors=True)
    df = s.readStream.format("rate").option("rowsPerSecond", "20000").load().withColumn("k", F.col("value") % 5)
    win = df.withWatermark("timestamp", "2 seconds").groupBy(F.window("timestamp", "1 second"), F.col("k")).count()
    q = win.writeStream.format("parquet").option("path", out).option("checkpointLocation", ck).trigger(availableNow=True).start()
    q.awaitTermination(timeout=30)
    n = s.read.parquet(out).count()
    check("stream.windowed", n > 0, f"rows={n}")
except Exception as e:
    check("stream.windowed", False, f"EXC {str(e)[:140]}")

passed = sum(1 for _, ok in results if ok)
print(f"\nDIST_STREAMING_SMOKE {passed}/{len(results)} passed")
