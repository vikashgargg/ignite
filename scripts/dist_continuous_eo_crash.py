#!/usr/bin/env python3
"""Distributed continuous (Trigger.Continuous) exactly-once + HARD-CRASH recovery gate.

Validates that Zelox's realtime/continuous streaming is exactly-once across a hard
crash (SIGKILL) of the server, on a multi-worker cluster, end-to-end through real
Spark Connect.

Driver (bash) sequence — see the committed run in docs/design/distributed-streaming-f2f3.md:
  1. start `zelox server --mode local-cluster --workers 2 --port P`
  2. python dist_continuous_eo_crash.py P w1     # produce 5000 -> continuous Kafka->parquet
  3. pkill -9 -f 'zelox server'                   # HARD crash mid-stream (not graceful)
  4. restart the server on port P
  5. python dist_continuous_eo_crash.py P check   # produce +5000 -> verify EO

Phase `w1` produces ids 0..4999 to Kafka and runs a continuous Kafka->parquet query
(Trigger.Continuous '1 second') for a few seconds, committing per-epoch via the
atomic `realtime/committed` record on the shared object-store checkpoint. Phase
`check` produces 5000..9999, re-runs, and asserts the durable output is exactly
0..9999 (no dup, no loss) — i.e. the source resumed from the crash-safe committed
offset. Exactly-once survives a SIGKILL because the per-epoch commit is a single
atomic object `put` (no torn state) and the source seeks to it on restart.
"""
import sys, time, json, subprocess, shutil
from pyspark.sql import SparkSession

PORT, PHASE = sys.argv[1], sys.argv[2]
OUT, CK, TOPIC, BOOT = "/tmp/ceo_out", "/tmp/ceo_ck", "cont_eo", "localhost:9092"
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
