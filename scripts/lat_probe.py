#!/usr/bin/env python3
"""Engine-agnostic streaming latency probe (dimension S3, tri-engine matrix).

Produces JSON {id, ts} to IN_TOPIC at RATE/s for DURATION_S while a consumer reads OUT_TOPIC and records
now_ms - ts per record. Whatever engine does the IN->OUT raw passthrough is measured IDENTICALLY ->
fair p50/p99/p99.9/max:
  - Vajra: scripts/stream_latency_query.py  (Kafka value passthrough, continuous trigger)
  - Flink: k8s/stream/flink-sql-latency.sql (raw passthrough, continuous)
Latency is Flink's defining property + where no-JVM/no-GC should win on the TAIL (no GC pauses).

Usage: BOOT=.. IN_TOPIC=lat_in OUT_TOPIC=lat_out RATE=20000 DURATION_S=60 ENGINE=vajra|flink \
       python lat_probe.py
"""
import os, time, json, threading

from confluent_kafka import Consumer, Producer

BOOT = os.environ.get("BOOT", "localhost:9092")
IN = os.environ.get("IN_TOPIC", "lat_in")
OUT = os.environ.get("OUT_TOPIC", "lat_out")
RATE = int(os.environ.get("RATE", "20000"))
DUR = int(os.environ.get("DURATION_S", "60"))
ENGINE = os.environ.get("ENGINE", "?")

lat: list[int] = []


def produce() -> None:
    p = Producer({"bootstrap.servers": BOOT, "linger.ms": 5,
                  "queue.buffering.max.messages": 2000000})
    i, t0 = 0, time.time()
    while time.time() - t0 < DUR:
        s = time.time()
        for _ in range(RATE):
            now = int(time.time() * 1000)
            while True:
                try:
                    p.produce(IN, value=json.dumps({"id": i, "ts": now}))
                    break
                except BufferError:
                    p.poll(0.01)
            i += 1
        p.poll(0)
        dt = time.time() - s
        if dt < 1.0:
            time.sleep(1.0 - dt)
    p.flush()


def consume() -> None:
    c = Consumer({"bootstrap.servers": BOOT, "group.id": f"lat-{time.time()}",
                  "auto.offset.reset": "latest", "enable.auto.commit": False})
    c.subscribe([OUT])
    t0 = time.time()
    while time.time() - t0 < DUR + 8:
        m = c.poll(0.5)
        if m is None or m.error():
            continue
        try:
            v = json.loads(m.value())
            lat.append(int(time.time() * 1000) - int(v["ts"]))
        except Exception:
            pass
    c.close()


ct = threading.Thread(target=consume)
ct.start()
time.sleep(2)  # consumer subscribed (auto.offset.reset=latest) before producing
pt = threading.Thread(target=produce)
pt.start()
pt.join()
ct.join()

xs = sorted(x for x in lat if x >= 0)


def pct(p: float) -> int:
    return xs[min(len(xs) - 1, int(len(xs) * p / 100))] if xs else -1


if xs:
    print(f"LATENCY_RESULT engine={ENGINE} n={len(xs)} p50_ms={pct(50)} p99_ms={pct(99)} "
          f"p999_ms={pct(99.9)} max_ms={xs[-1]} min_ms={xs[0]}")
else:
    print(f"LATENCY_RESULT engine={ENGINE} n=0 (no output observed)")
