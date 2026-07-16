#!/usr/bin/env python3
"""Progressive per-stage throughput profiler (Vajra) — pinpoint WHERE the Flink gap is.

Runs the SAME Kafka topic through three successive cut points, each `availableNow`
(bounded catch-up), reporting throughput = N / wall for each. The DROP between
successive stages isolates that stage's cost:

    STAGE=source : raw Kafka           -> COUNT           (source fetch+build ceiling)
    STAGE=parse  : + from_json(value)  -> COUNT           (adds JSON parse)
    STAGE=full   : + TUMBLE window/k   -> S3/parquet       (adds keyBy-shuffle + window + sink)

  source->parse delta = from_json cost ; parse->full delta = shuffle+window+sink cost.

Compare each Vajra stage to Flink's per-operator busy%/records-in (web UI) or the
matching Flink progressive job (scripts stage_profile runner). Grounded in AIM:
measure each step, map to Flink, find where we lag — no guessing.

Env: SPARK_REMOTE, BOOT, TOPIC, N_EVENTS, STAGE (source|parse|full), OUT, CK, WINDOW_S.
"""
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StructField, StructType

REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:50051")
BOOT = os.environ.get("BOOT", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "bench_src")
N = int(os.environ.get("N_EVENTS", "10000000"))
STAGE = os.environ.get("STAGE", "source")
OUT = os.environ.get("OUT", "/tmp/stage_out")
CK = os.environ.get("CK", "/tmp/stage_ck")
WINDOW_S = os.environ.get("WINDOW_S", "10")
MAXOFFSETS = int(os.environ.get("MAXOFFSETS", "4000000"))

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
       .option("maxOffsetsPerTrigger", MAXOFFSETS)
       .load())


def run_agg(agg, out, ck):
    """Run `agg` (a windowed aggregation, whose windows CLOSE as event_time advances so the
    append+parquet sink emits) to completion; read back sum(count) = rows processed."""
    t0 = time.time()
    q = (agg.writeStream.format("parquet")
         .option("path", out).option("checkpointLocation", ck)
         .outputMode("append").trigger(availableNow=True).start())
    q.awaitTermination()
    dt = time.time() - t0
    got = s.read.parquet(out).agg(F.sum("count").alias("c")).collect()[0]["c"]
    return dt, got


# All SQL stages parse (event_time comes from from_json); source-only ceiling is the micro-bench
# (kafka_read_bench, ~2.3M/16-part local). `nokey` window-only vs `full` window+key isolates the
# KEYED EXCHANGE (shuffle-by-k) — the suspected distributed culprit (Flight IPC per REFERENCES).
parsed = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
          .select(F.col("e.k").alias("k"),
                  (F.col("e.ts") / 1000).cast("timestamp").alias("event_time"))
          .withWatermark("event_time", "0 seconds"))

if STAGE in ("source", "nokey", "parse"):
    # from_json + 10s tumbling window, NO key (group by window only) = minimal exchange.
    agg = parsed.groupBy(F.window("event_time", f"{WINDOW_S} seconds")).count()
    dt, got = run_agg(agg, OUT, CK)
    got = got or 0
    print(f"VAJRA_STAGE stage=nokey events={N} wall_s={dt:.2f} "
          f"throughput={got/dt/1e6:.3f}M/s rows={got}", flush=True)

elif STAGE == "full":
    # + keyBy-shuffle + tumbling window + parquet sink (the real workload).
    parsed = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
              .select(F.col("e.k").alias("k"),
                      (F.col("e.ts") / 1000).cast("timestamp").alias("event_time")))
    agg = (parsed.withWatermark("event_time", "0 seconds")
           .groupBy(F.window("event_time", f"{WINDOW_S} seconds"), F.col("k"))
           .count())
    t0 = time.time()
    q = (agg.writeStream.format("parquet")
         .option("path", OUT).option("checkpointLocation", CK)
         .outputMode("append")
         .trigger(availableNow=True).start())
    q.awaitTermination()
    dt = time.time() - t0
    out = s.read.parquet(OUT)
    row = out.agg(F.sum("count").alias("total"),
                  F.countDistinct("window").alias("n_win")).collect()[0]
    total = row["total"] or 0
    print(f"VAJRA_STAGE stage=full events={N} wall_s={dt:.2f} "
          f"throughput={total/dt/1e6:.3f}M/s total_events={total} n_windows={row['n_win']}",
          flush=True)

else:
    raise SystemExit(f"unknown STAGE={STAGE} (source|parse|full)")
