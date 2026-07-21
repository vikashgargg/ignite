#!/usr/bin/env python3
"""Apple-container continuous (Trigger.Continuous) exactly-once + HARD-CRASH gate.

Same contract as dist_continuous_eo_crash.py, adapted for Zelox running INSIDE an
Apple container (1.0.0):
  * the Zelox server reaches host Kafka via the vmnet gateway external listener
    (192.168.64.1:9093); the producer still injects via the broker's INTERNAL
    listener through `docker exec` (localhost:9092).
  * OUT/CK live under /tmp/zelox, the volume mounted into the container, so the
    crash-safe object-store checkpoint survives `container kill` + restart.

Sequence (see scripts/container_validation.sh):
  w1     : produce 0..4999 -> continuous Kafka->parquet ~8s
  (container kill zelox-cluster ; container run ... again)
  check  : produce 5000..9999 -> re-run -> assert durable output is exactly 0..9999
"""
import sys, time, json, subprocess, shutil
from pyspark.sql import SparkSession

PORT, PHASE = sys.argv[1], sys.argv[2]
OUT, CK, TOPIC = "/tmp/zelox/ceo_out", "/tmp/zelox/ceo_ck", "cont_eo"
PRODUCE_BOOT = "localhost:9092"          # broker INTERNAL listener (docker exec, in-container)
SERVER_BOOT = "192.168.64.1:9093"        # broker EXTERNAL listener (reachable from Apple container)
s = SparkSession.builder.remote(f"sc://localhost:{PORT}").getOrCreate()


def produce(lo, hi):
    lines = [json.dumps({"id": i}) for i in range(lo, hi)]
    p = subprocess.run(
        ["docker", "exec", "-i", "zelox_kafka", "/opt/kafka/bin/kafka-console-producer.sh",
         "--bootstrap-server", PRODUCE_BOOT, "--topic", TOPIC],
        input=("\n".join(lines) + "\n").encode(), capture_output=True)
    assert p.returncode == 0, p.stderr[-200:]


def run(seconds):
    raw = (s.readStream.format("kafka").option("kafka.bootstrap.servers", SERVER_BOOT)
           .option("subscribe", TOPIC).option("startingOffsets", "earliest").load())
    q = (raw.selectExpr("CAST(value AS STRING) AS v").writeStream.format("parquet")
         .option("path", OUT).option("checkpointLocation", CK)
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
    print("EXACTLY_ONCE_ACROSS_CRASH", ok)
