#!/usr/bin/env python3
"""Realtime passthrough query for the latency harness (scripts/stream_latency.sh):
Kafka input -> (value passthrough) -> Kafka output, continuous/realtime trigger. Minimal
transform so the measured number is the engine's produce->output pipeline latency, not
query compute. The producer embeds a wall-clock produce_ts in each value; the latency
consumer (in the orchestrator) computes now - produce_ts on the output topic.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:50072")
BOOT = os.environ.get("BOOT", "localhost:9092")
IN_TOPIC = os.environ.get("IN_TOPIC", "lat_in")
OUT_TOPIC = os.environ.get("OUT_TOPIC", "lat_out")
CK = os.environ.get("CK", "/tmp/lat_ck")

s = SparkSession.builder.remote(REMOTE).getOrCreate()
raw = (s.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOT)
       .option("subscribe", IN_TOPIC)
       .option("startingOffsets", "latest")
       .load())
# Pass the original value bytes straight through to the output topic.
out = raw.select(F.col("value"))
q = (out.writeStream.format("kafka")
     .option("kafka.bootstrap.servers", BOOT)
     .option("topic", OUT_TOPIC)
     .option("checkpointLocation", CK)
     .trigger(realTime="5 seconds")
     .start())
q.awaitTermination()
