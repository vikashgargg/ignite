#!/usr/bin/env python3
"""CONTINUOUS-trigger variant of stream_windowed_agg.py — the like-for-like EKS test of the aligned
checkpoint-barrier crash-EO fix (docs/design/distributed-eo-coordinator-wiring.md §4e).

Identical logical query (10s event-time tumbling keyed COUNT, Kafka -> Parquet) but driven by
`.trigger(continuous="1 second")` so it exercises the REALTIME N-reader multi-partition path (one
reader per Kafka partition -> keyed StreamExchange -> WindowAccum -> aligned barrier -> sink) — the
exact path whose exchange dropped 15/16 Checkpoint barriers before the fix. availableNow (the sibling
script) uses the micro-batch path and does NOT exercise it.

Continuous queries never self-terminate, so this runs for RUN_SECS then stops (the crash harness kills
the server mid-run and re-invokes this to resume from the same S3 checkpoint).

Env: SPARK_REMOTE, BOOT, TOPIC, N_EVENTS (throughput calc only), OUT (s3://...), CK (s3://...),
     RUN_SECS (default 90).
"""
import os, time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, IntegerType

REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:50051")
BOOT = os.environ.get("BOOT", "kafka.stream.svc.cluster.local:9092")
TOPIC = os.environ.get("TOPIC", "events")
N = int(os.environ.get("N_EVENTS", "100000000"))
OUT = os.environ.get("OUT", "/data/wagg_out")
CK = os.environ.get("CK", "/data/wagg_ck")
RUN_SECS = int(os.environ.get("RUN_SECS", "90"))

s = SparkSession.builder.remote(REMOTE).getOrCreate()

schema = StructType([
    StructField("k", IntegerType()),
    StructField("ts", LongType()),
    StructField("v", IntegerType()),
])

raw = (s.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOT)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")
       .load())

parsed = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
          .select(F.col("e.k").alias("k"),
                  (F.col("e.ts") / 1000).cast("timestamp").alias("event_time")))

agg = (parsed.withWatermark("event_time", "0 seconds")
       .groupBy(F.window("event_time", "10 seconds"), F.col("k"))
       .count())

t0 = time.time()
q = (agg.writeStream.format("parquet")
     .option("path", OUT).option("checkpointLocation", CK)
     .outputMode("append")
     .trigger(continuous="1 second").start())
# Run for a bounded window (continuous never self-terminates), then stop cleanly so the committed
# offsets/state land at an epoch boundary.
time.sleep(RUN_SECS)
try:
    q.stop()
except Exception:
    pass
dt = time.time() - t0
print(f"ZELOX_WAGG_CONTINUOUS events={N} ran_s={dt:.1f} trigger=continuous", flush=True)
