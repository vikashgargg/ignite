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

## The honest gap (root cause, sharpened 2026-06-09)
The blocker is deeper than "not instrumented" — **continuous streaming queries do not
drive the sink locally**:
- A **bounded** query (`trigger(availableNow)`) runs the full pipeline: the sink
  (`MemorySinkExec`) executes, processes batches, commits — verified (1000 rows; the
  sink's batch loop fires).
- A **continuous** query (default trigger) is reported *active*, but the sink's batch
  loop **never runs** — instrumented with an unconditional log at the top of the
  sink loop, **zero batches arrived** in multiple seconds at 100–1000 rows/s. This is
  why a continuous `rate → memory` query surfaces **0 rows**: the driver's
  `stream.next()` loop never yields a batch for an unbounded plan.

Consequences:
1. **Continuous output doesn't work** (0 rows) — the primary issue to fix.
2. **Latency can't be measured** end-to-end: a `LatencyTracker` flow marker is defined
   in the protocol but, even after wiring a source to emit it and the sink to measure
   it, the markers are never processed because the continuous pipeline isn't driven.
   (That instrumentation was prototyped and reverted — it's correct but unobservable
   until continuous execution is fixed; bounded execution regression-tested OK.)
3. No streaming **progress metrics** (`lastProgress`/`processedRowsPerSecond`).

So the defensible statement today: **Vajra's streaming *compute* is very low latency
(sub-5 ms per batch, bounded), but *continuous* streaming execution does not yet drive
sinks locally — so continuous output and any end-to-end latency number are blocked on
that. We cannot quote a Flink-comparable p99 yet.**

## Path to Flink-class (prioritized — re-sequenced after the root-cause finding)
1. **Fix continuous execution driving the sink (THE blocker).** Make an unbounded
   streaming plan actually run the pipeline continuously so the driver's
   `stream.next()` yields per-micro-batch output (today only bounded/availableNow
   does). Without this, continuous output and latency are both dead. *Immediate next
   step.*
2. **Then instrument latency:** emit `LatencyTracker` from sources on a cadence;
   measure `now() − marker_ts` at the sink; expose it (and input/processed
   rows-per-second) via `StreamingQuery.lastProgress` (Vajra reports no progress
   today). The source+sink wiring is already understood/prototyped — it just needs #1.
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
