# Vajra streaming latency — baseline & path to Flink-class

Validating **where Vajra streaming latency is today** (2026-06-09), with the
research-backed targets it must reach to be "on par with Flink." Honest baseline +
the concrete gaps to close.

## Targets (industry, researched)
| Engine | End-to-end latency | Model |
|---|---|---|
| **Apache Flink** | **~tens of ms** (p99 ~74 ms @ 50k ev/s; can be sub-ms) | event-at-a-time (pipelined) |
| Spark Structured Streaming (micro-batch) | ~100 ms default → **0.5–1.3 s** real-world | micro-batch (trigger interval is the bottleneck) |
| Spark Continuous Processing | ~1 ms (limited operators) | continuous |

Sources: Decodable, Confluent, Onehouse, and a 2024 multi-cloud Flink-vs-Spark
benchmark (Flink p99 74±3 ms vs Spark 231±8 ms under exactly-once @ 50k ev/s).

## What Vajra is today
- **Architecture: micro-batch** (flow events + markers + per-micro-batch operators) —
  the same *class* as Spark Structured Streaming, **not** Flink's event-at-a-time.
- **The engine itself is fast.** Measured locally (debug build, rate source):
  - In-batch processing latency (generation→output within a micro-batch):
    **≈ 0 (|Δ| ~3.5 ms, i.e. clock noise)** — the engine adds negligible latency
    inside a batch.
  - `availableNow` query client round-trip: **~1.8 ms**.
  - Throughput: **~28M rows/s** (see [STREAMING.md](STREAMING.md)).

## The honest gap: latency is not yet *measurable* end-to-end
Two findings block a real continuous-latency number today (both are the actual work
items, not just measurement annoyances):
1. **Latency is not instrumented.** The protocol defines a `LatencyTracker` flow
   marker ("emitted by each data source … measured by downstream operators"), but
   **no source emits it and no operator measures it** — so Vajra cannot report
   p50/p99 streaming latency or `processedRowsPerSecond` (streaming progress metrics
   are absent).
2. **Continuous output-commit cadence.** A continuous `rate → memory` query surfaces
   **0 rows** until a checkpoint/EndOfData boundary, so sustained end-to-end latency
   under load can't be observed via the sink. Output visibility is gated on the
   commit boundary, whose cadence isn't configurable/observable yet.

So the defensible statement today: **Vajra's streaming *compute* is very low latency
(sub-5 ms per batch), but end-to-end latency is micro-batch-class and currently
un-instrumented; we cannot yet quote a Flink-comparable p99.**

## Path to Flink-class (prioritized)
1. **Instrument latency first (so we can drive it):** emit `LatencyTracker` from
   sources on a cadence; measure `now() − marker_ts` at the sink/a downstream
   operator; expose it (and input/processed rows-per-second) via
   `StreamingQuery.lastProgress` — Vajra reports no progress today. *This is the
   immediate next step — you can't optimize what you can't measure.*
2. **Make continuous output prompt + observable:** configurable micro-batch trigger
   interval + commit cadence; ensure sinks surface data each batch (the memory sink
   commit-on-checkpoint gap above).
3. **Drive the micro-batch interval down** and measure p50/p99 at fixed event rates
   (Nexmark / Yahoo Streaming Benchmark methodology) on a release build.
4. **Evaluate a continuous (event-at-a-time) execution path** for the operators where
   tens-of-ms p99 matters — the architectural step to truly rival Flink, mirroring
   Spark's separate Continuous Processing mode.

## How these numbers were measured (reproduce)
Rate source stamps each row with `now()` at generation; `current_timestamp()` at the
sink is processing time, so `proc_ts − timestamp` is end-to-end latency. Run via the
pyspark `sc://` client against `vajra server` (see [STREAMING.md](STREAMING.md) for
the harness). Continuous-mode measurement is blocked on items #1–#2 above.
