#!/usr/bin/env python3
"""Multi-partition (no-funnel) continuous exactly-once gate.

A continuous Kafka->parquet query with `repartition(4, key)` — the data fans into 4
partitions processed in parallel, each sink task writes its own `part-<i>.parquet`
per epoch, and the last task to finish an epoch writes the single global commit
(object-store coordination). Validates exactly-once across restart / hard crash.

Driver sequence (see scripts/full_validation.sh):
  w1     : produce ids 0..4999 -> continuous repartition(4,key)->parquet, ~8s
  (crash/restart the server)
  check  : produce 5000..9999 -> re-run -> assert durable output is exactly 0..9999
"""
import sys, time, json, subprocess, shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PORT, PHASE = sys.argv[1], sys.argv[2]
OUT, CK, TOPIC, BOOT = "/tmp/mpeo_out", "/tmp/mpeo_ck", "mp_cont", "localhost:9092"
s = SparkSession.builder.remote(f"sc://localhost:{PORT}").getOrCreate()


def produce(lo, hi):
    lines = [json.dumps({"id": i}) for i in range(lo, hi)]
    p = subprocess.run(
        ["docker", "exec", "-i", "zelox_kafka", "/opt/kafka/bin/kafka-console-producer.sh",
         "--bootstrap-server", BOOT, "--topic", TOPIC],
        input=("\n".join(lines) + "\n").encode(), capture_output=True)
    assert p.returncode == 0, p.stderr[-200:]


def run(seconds):
    raw = (s.readStream.format("kafka").option("kafka.bootstrap.servers", BOOT)
           .option("subscribe", TOPIC).option("startingOffsets", "earliest").load())
    df = raw.selectExpr("CAST(value AS STRING) AS v").repartition(4, F.col("v"))
    q = (df.writeStream.format("parquet").option("path", OUT).option("checkpointLocation", CK)
         .trigger(continuous="1 second").start())
    time.sleep(seconds)
    q.stop()


if PHASE == "w1":
    shutil.rmtree(OUT, ignore_errors=True)
    shutil.rmtree(CK, ignore_errors=True)
    produce(0, 5000)
    run(8)
    print("W1 rows=", s.read.parquet(OUT).count())
elif PHASE == "check":
    produce(5000, 10000)
    run(8)
    ids = sorted(int(json.loads(r.v)["id"]) for r in s.read.parquet(OUT).select("v").collect())
    ok = len(set(ids)) == 10000 and ids == list(range(10000))
    print(f"CHECK total={len(ids)} distinct={len(set(ids))} contiguous_0_9999={ids == list(range(10000))}")
    print("MULTIPART_EXACTLY_ONCE", ok)
