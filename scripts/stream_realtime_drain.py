#!/usr/bin/env python3
"""Vajra REALTIME (Spark 4.2 Trigger.RealTime) side of the EKS head-to-head vs Flink streaming.

Runs the IDENTICAL 10s event-time tumbling keyed COUNT over the pre-loaded Kafka `events` backlog (+16
closers), driven by `.trigger(realTime=<dur>)` — the native Spark 4.2 Real-Time Mode trigger we wired to
Vajra's realtime engine. Measures the CATCH-UP DRAIN throughput apples-to-apples with Flink: poll the
REAL S3 output sum(count) until it reaches N (all 10 windows closed = backlog fully drained, since the
closers advance the watermark past the last data window), record the wall. Then report completeness
(windows / sum / per-group) read back from S3 — never `find`, always the S3 object listing (pyarrow).

Env: SPARK_REMOTE, BOOT, TOPIC, N_EVENTS, OUT (s3://...), CK (s3://...), RT_DUR (default "5 seconds"),
     MAX_SECS, S3_ENDPOINT_REGION (for readback).
"""
import os, time, threading
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, IntegerType
import pyarrow.dataset as ds, pyarrow.fs as pafs, pyarrow.compute as pc

REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:50051")
BOOT = os.environ.get("BOOT", "kafka.stream.svc.cluster.local:9092")
TOPIC = os.environ.get("TOPIC", "events")
N = int(os.environ.get("N_EVENTS", "100000000"))
OUT = os.environ.get("OUT", "/data/rt_out")           # s3://bucket/rt_out
CK = os.environ.get("CK", "/data/rt_ck")
RT_DUR = os.environ.get("RT_DUR", "5 seconds")
MAX_SECS = int(os.environ.get("MAX_SECS", "600"))
REGION = os.environ.get("AWS_REGION", "ap-south-1")
BUCKET = OUT.replace("s3://", "").split("/", 1)[0]
PREFIX = OUT.replace("s3://", "").split("/", 1)[1]

# MinIO (box/kind) sets AWS_ENDPOINT; real S3 (EKS) does not. The readback MUST honour the endpoint or it
# silently queries real AWS S3 and reads 0 (this made a valid 90M box run look like 0 — a harness bug).
_ENDPOINT = os.environ.get("AWS_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL")
def _s3fs():
    if _ENDPOINT:
        return pafs.S3FileSystem(endpoint_override=_ENDPOINT, allow_bucket_creation=False,
                                 scheme="http" if _ENDPOINT.startswith("http://") else "https")
    return pafs.S3FileSystem(region=REGION)

def s3_sum():
    """Read the S3 output committed so far: (n_windows, sum_count, min_cnt, max_cnt). 0s if nothing yet."""
    try:
        s3 = _s3fs()
        sel = pafs.FileSelector(f"{BUCKET}/{PREFIX}", recursive=True)
        files = [f.path for f in s3.get_file_info(sel)
                 if f.path.endswith(".parquet") and "_spark_metadata" not in f.path]
        if not files:
            return (0, 0, 0, 0)
        t = ds.dataset(files, filesystem=s3, format="parquet").to_table()
        t = t.filter(pc.greater_equal(t.column("k"), 0))
        ws = pc.struct_field(t.column("window"), "start")
        return (pc.count_distinct(ws).as_py(), pc.sum(t.column("count")).as_py(),
                pc.min(t.column("count")).as_py(), pc.max(t.column("count")).as_py())
    except Exception:
        return (0, 0, 0, 0)

s = SparkSession.builder.remote(REMOTE).getOrCreate()
schema = StructType([StructField("k", IntegerType()), StructField("ts", LongType()), StructField("v", IntegerType())])
raw = (s.readStream.format("kafka").option("kafka.bootstrap.servers", BOOT)
       .option("subscribe", TOPIC).option("startingOffsets", "earliest").load())
parsed = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
          .select(F.col("e.k").alias("k"), (F.col("e.ts") / 1000).cast("timestamp").alias("event_time")))
agg = (parsed.withWatermark("event_time", "0 seconds")
       .groupBy(F.window("event_time", "10 seconds"), F.col("k")).count())

t0 = time.time()
q = (agg.writeStream.format("parquet").option("path", OUT).option("checkpointLocation", CK)
     .outputMode("append").trigger(realTime=RT_DUR).start())
print(f"RT_STARTED trigger=realTime dur='{RT_DUR}' -> {OUT}", flush=True)

drain_s = None       # OUTPUT-completeness (cadence-gated — kept for completeness, NOT throughput)
consume_s = None     # CONSUMPTION-rate (RFC-observability: the REAL throughput — how fast N is read)
seen_batches = {}    # batchId -> numInputRows (dedup; sum = rows consumed)
while time.time() - t0 < MAX_SECS:
    time.sleep(4)
    el = time.time() - t0
    # Consumption metric: accumulate the query's own numInputRows per epoch (StreamingQueryProgress).
    # This measures read/compute rate, unaffected by the commit cadence that gates the S3 output.
    try:
        for p in (q.recentProgress or []):
            bid = p.get("batchId")
            if bid is not None:
                seen_batches[bid] = max(seen_batches.get(bid, 0), int(p.get("numInputRows", 0) or 0))
    except Exception:
        pass
    consumed = sum(seen_batches.values())
    if consume_s is None and consumed >= N:
        consume_s = el
    w, tot, mn, mx = s3_sum()
    print(f"RT_DRAIN_PROGRESS consumed={consumed} windows={w} sum={tot} t={el:.1f}s", flush=True)
    if tot >= N:
        drain_s = el
        break
try:
    q.stop()
except Exception:
    pass
time.sleep(3)
w, tot, mn, mx = s3_sum()
if drain_s is None:
    drain_s = time.time() - t0
consumed = sum(seen_batches.values())
if consume_s is None and consumed >= N:
    consume_s = drain_s
thr = N / drain_s / 1e6 if drain_s > 0 else 0.0
if consume_s and consume_s > 0:
    print(f"VAJRA_CONSUME_RATE consumed={consumed} consume_s={consume_s:.1f} "
          f"throughput={N/consume_s/1e6:.3f}M_ev/s (REAL read/compute rate, cadence-independent)", flush=True)
print(f"VAJRA_REALTIME_DRAIN events={N} drain_s={drain_s:.1f} throughput={thr:.3f}M_ev/s trigger=realTime", flush=True)
print(f"VAJRA_COMPLETENESS windows={w} sum={tot} per_group[min={mn} max={mx}] "
      f"EXACT={'YES' if (w==10 and tot==N and mn==mx==10000) else 'NO'}", flush=True)
