#!/usr/bin/env python3
"""Continuous Kafka -> parquet exactly-once query for the soak+chaos gate
(scripts/stream_soak_chaos.sh). Realtime/continuous trigger with a checkpoint, so a
hard server kill resumes from the committed offset (exactly-once across crash). Blocks
in awaitTermination until the harness kills it; re-running resumes from the checkpoint.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType

REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:50071")
BOOT = os.environ.get("BOOT", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "soak_events")
OUT = os.environ.get("OUT", "/tmp/soak_out")
CK = os.environ.get("CK", "/tmp/soak_ck")

s = SparkSession.builder.remote(REMOTE).getOrCreate()
schema = StructType([StructField("id", LongType())])

raw = (s.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOT)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")
       .load())
parsed = raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e")).select("e.id")

q = (parsed.writeStream.format("parquet")
     .option("path", OUT).option("checkpointLocation", CK)
     .trigger(continuous="2 seconds")   # realtime/continuous EO; per-epoch offset commit
     .start())
q.awaitTermination()
