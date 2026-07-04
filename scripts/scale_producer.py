#!/usr/bin/env python3
"""Prod-representative Kafka load producer for the local scale gate. Single-process confluent-kafka with
large batches (avoids the macOS multiprocessing-spawn silent-no-op). Producer scheme is IDENTICAL to
k8s/stream/producer-job.yaml: k=i%K, ts=BASE+i//EPMS, routed to partition i%NP (time-ordered per partition).
SELF-CHECKING: after flush it re-reads the topic's summed high-watermarks and ASSERTS == N (fails loudly if
the produce silently did nothing). Usage: BOOT= TOPIC= N= K= EPMS= NP= python3 scripts/scale_producer.py
"""
import os, json, time, sys, subprocess
from confluent_kafka import Producer

BOOT = os.environ.get("BOOT", "localhost:9092")
TOPIC = os.environ["TOPIC"]
N = int(os.environ["N"]); K = int(os.environ.get("K", "1000"))
EPMS = int(os.environ.get("EPMS", "1000")); NP = int(os.environ.get("NP", "16"))
BASE = 1700000000000

p = Producer({"bootstrap.servers": BOOT, "linger.ms": 100, "batch.size": 1 << 20,
              "compression.type": "lz4", "queue.buffering.max.messages": 4000000,
              "queue.buffering.max.kbytes": 1 << 21})
t0 = time.time()
for i in range(N):
    v = json.dumps({"k": i % K, "ts": BASE + (i // EPMS), "v": 1})
    while True:
        try:
            p.produce(TOPIC, partition=i % NP, key=str(i % K), value=v); break
        except BufferError:
            p.poll(0.05)
    if (i & 0x7FFFF) == 0:
        p.poll(0)
# Optional closer: one high-event-time record per partition so the watermark advances past the last
# real window and ALL windows close (mirrors a live stream advancing; verify excludes this sentinel ts).
closer_ts = os.environ.get("CLOSER_TS")
if closer_ts:
    cts = int(closer_ts)
    for part in range(NP):
        p.produce(TOPIC, partition=part, key="closer", value=json.dumps({"k": -1, "ts": cts, "v": 1}))
p.flush()
dt = time.time() - t0
print(f"PRODUCED N={N} in {dt:.1f}s = {N/dt/1e6:.2f}M msg/s", flush=True)

# SELF-CHECK: sum the topic's per-partition high-watermarks == N (else the produce silently failed).
kpod = os.environ.get("KPOD") or subprocess.check_output(
    "docker ps --format '{{.Names}}' | grep -i kafka | head -1", shell=True).decode().strip()
out = subprocess.check_output(
    ["docker", "exec", kpod, "/opt/kafka/bin/kafka-get-offsets.sh",
     "--bootstrap-server", "localhost:9092", "--topic", TOPIC], text=True)
total = sum(int(line.rsplit(":", 1)[1]) for line in out.strip().splitlines() if ":" in line)
expected = N + (NP if closer_ts else 0)  # closer adds one sentinel per partition
print(f"TOPIC_CHECK topic={TOPIC} summed_high_watermarks={total} expected={expected}", flush=True)
if total != expected:
    print(f"FATAL: producer self-check FAILED (topic has {total}, expected {expected})", flush=True)
    sys.exit(3)
print("PRODUCER_OK", flush=True)
