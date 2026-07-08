#!/usr/bin/env python3
"""Vajra side of the Vajra-vs-Flink streaming head-to-head.

Identical logical query to the Flink SQL job (k8s/stream/flink-sql.sql):
    SELECT window_start, k, COUNT(*)
    FROM events
    GROUP BY TUMBLE(event_time, INTERVAL '10' SECOND), k

Reads the SHARED Kafka topic `events` (same one Flink consumes), runs a 10s
event-time tumbling-window keyed COUNT over the full backlog, and reports
throughput = N_events / wall_time. `availableNow` processes all currently
available data and stops, giving a clean catch-up throughput directly comparable
to Flink's published "events/s" windowed-aggregation number.

Env:
  SPARK_REMOTE  sc://<vajra>:50051
  BOOT          Kafka bootstrap (kafka.stream.svc.cluster.local:9092)
  TOPIC         events
  N_EVENTS      total produced (for throughput calc)
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
       .option("maxOffsetsPerTrigger", int(os.environ.get("MAXOFFSETS","4000000")))
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
     .trigger(availableNow=True).start())
q.awaitTermination()
dt = time.time() - t0

out = s.read.parquet(OUT)
windows = out.count()
# total events that landed in windows == records actually read (under-read check);
# distinct window/key cardinality == correctness check vs the known workload shape.
row = out.agg(F.sum("count").alias("total"),
              F.countDistinct("window").alias("n_win"),
              F.countDistinct("k").alias("n_keys")).collect()[0]
total, n_win, n_keys = row["total"], row["n_win"], row["n_keys"]
print(f"VAJRA_WAGG events={N} wall_s={dt:.2f} throughput={N/dt/1e6:.3f}M_events/s "
      f"groups={windows} total_events={total} n_windows={n_win} n_keys={n_keys}", flush=True)
