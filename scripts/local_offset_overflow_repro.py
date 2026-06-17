#!/usr/bin/env python3
"""Local reproduction of the EKS windowed-agg Arrow i32 offset overflow.

An i32 OffsetBuffer overflow is byte-driven (a Utf8/Binary array > 2 GiB), so we
trigger the SAME bug with ~1.5M LARGE (~2 KiB) Kafka values instead of 100M small
ones — same `value` Binary column, far fewer rows, reproducible on a laptop.

Phase:
  produce : write N messages {"k","ts","v","pad":<~2KB>} to topic `bigval`
  query   : run the identical 10s tumbling-window keyed COUNT (availableNow)

Run the server first in the SAME mode as EKS:
  target/debug/vajra server --mode local-cluster --workers 4 --port 50099
Then: produce, then query (against a debug/unstripped binary -> real backtrace).
"""
import sys, os, json, time
PHASE = sys.argv[1]
PORT = sys.argv[2] if len(sys.argv) > 2 else "50099"
BOOT = "localhost:9092"
TOPIC = "bigval"
N = int(os.environ.get("N", "1500000"))
PAD = "x" * int(os.environ.get("PAD_BYTES", "2000"))
EPMS = int(os.environ.get("EVENTS_PER_MS", "1000"))   # event-time density -> window span

if PHASE == "produce":
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": BOOT, "linger.ms": 50, "batch.size": 1048576,
                  "compression.type": "none", "queue.buffering.max.messages": 1000000,
                  "queue.buffering.max.kbytes": 2097152, "message.max.bytes": 10485760})
    base = 1700000000000
    t0 = time.time()
    for i in range(N):
        rec = {"k": i % 1000, "ts": base + (i // EPMS), "v": 1}
        if PAD:
            rec["pad"] = PAD
        v = json.dumps(rec)
        while True:
            try:
                p.produce(TOPIC, key=str(i % 1000), value=v); break
            except BufferError:
                p.poll(0.1)
        if (i & 0x1FFFF) == 0:
            p.poll(0)
    p.flush()
    print(f"PRODUCED {N} msgs (~{N*2000/1e9:.1f} GB value bytes) in {time.time()-t0:.1f}s")

elif PHASE == "query":
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, LongType, IntegerType
    s = SparkSession.builder.remote(f"sc://localhost:{PORT}").getOrCreate()
    schema = StructType([StructField("k", IntegerType()), StructField("ts", LongType()),
                         StructField("v", IntegerType())])
    raw = (s.readStream.format("kafka").option("kafka.bootstrap.servers", BOOT)
           .option("subscribe", TOPIC).option("startingOffsets", "earliest").load())
    parsed = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
              .select(F.col("e.k").alias("k"), (F.col("e.ts")/1000).cast("timestamp").alias("event_time")))
    agg = (parsed.withWatermark("event_time", "0 seconds")
           .groupBy(F.window("event_time", "10 seconds"), F.col("k")).count())
    q = (agg.writeStream.format("parquet").option("path", "/tmp/repro_out")
         .option("checkpointLocation", "/tmp/repro_ck").outputMode("append")
         .trigger(availableNow=True).start())
    q.awaitTermination()
    print("QUERY_DONE windows=", s.read.parquet("/tmp/repro_out").count())
