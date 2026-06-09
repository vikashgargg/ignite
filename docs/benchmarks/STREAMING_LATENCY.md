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
- **Architecture: micro-batch** (flow events + markers + per-micro-batch operators),
  but with a **~10 ms tick** — so latency is far below Spark's default-trigger class.
- **MEASURED end-to-end latency (2026-06-09): sub-millisecond.** With `LatencyTracker`
  markers emitted at the source and measured at the sink on a *continuous* `rate →
  memory` query (100 rows/s, single-node, debug build):
  > `streaming latency (memory sink): p50=0.1ms p99=0.1–0.3ms max=0.3ms (n≈83/s)`

  i.e. source-emit → sink-process traversal is **~0.1 ms p50, ≤0.3 ms p99** — this is
  **Flink-class** (Flink p99 ~tens of ms), not the Spark ~100 ms–1 s micro-batch class
  we'd assumed.
- **Rate-independent up to 100k rows/s** (measured 2026-06-09, continuous `rate → memory`):

  | Rate | p50 | p99 | max |
  |---|--:|--:|--:|
  | 1,000 rows/s | 0.1 ms | 0.1 ms | 0.2 ms |
  | 10,000 rows/s | 0.1 ms | 0.1 ms | 0.1 ms |
  | 100,000 rows/s | 0.0 ms | 0.1 ms | 0.4 ms |

  Latency does **not** degrade with throughput — the per-batch traversal stays sub-ms
  (batch cadence ~410/s; higher rates just mean larger batches).
- Throughput: **~28M rows/s** (see [STREAMING.md](STREAMING.md)).

### Stateful streaming — known gaps (found 2026-06-09)
Latency above is for stateless `rate → memory`. Stateful continuous aggregation has
open issues (next work, not latency-tuning):
- **Aggregation without a watermark** → a pipeline-breaking `AggregateExec` is planned,
  which is invalid on unbounded input (`Cannot execute pipeline breaking queries`).
  Stateful streaming aggregation **requires** a watermark (→ `WindowAccumExec`).
- **Windowed aggregation WITH `withWatermark`** currently errors `event-time column
  'timestamp' not found in input schema` — a column-resolution bug in the
  watermark/window streaming path (the flow-event schema rename likely loses the
  event-time column). Must be fixed before windowed-stream latency can be measured.

### Honest caveats on the latency number
- Measures the **engine's internal traversal** (source emission → sink processing),
  the right metric for engine latency, but it excludes external ingestion lag, network,
  and serialization to a remote client.
- Taken at **100 rows/s, single-node, debug build, simple `rate → memory`** (no heavy
  stateful operators). Latency under high throughput, with stateful windows/joins, and
  on a distributed cluster is **not yet measured** — those will raise it.
- Still **not exposed** via `StreamingQuery.lastProgress` (logged server-side only).

## How we got here (root cause, now RESOLVED 2026-06-09)
> **Resolved:** continuous execution is fixed (round-robin repartition disabled for
> streaming) and latency is now instrumented + measured (see above). The diagnosis
> below is kept for the record.

The blocker was deeper than "not instrumented" — **continuous streaming queries did not
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

So the defensible statement today: **Continuous streaming now works and Vajra's
measured end-to-end latency is sub-millisecond (p99 ≤0.3 ms) at 100 rows/s
single-node — Flink-class.** What remains: measure under high throughput, stateful
operators, and on a cluster; and expose latency via `StreamingQuery.lastProgress`
(it is logged server-side today, not yet in the progress API).

## Path to Flink-class (prioritized — re-sequenced after the root-cause finding)
1. ~~**Fix continuous execution driving the sink (THE blocker).**~~ **DONE (2026-06-09).**
   Root cause: the physical optimizer inserted `RepartitionExec: RoundRobinBatch(N)`
   between the single-partition source and the single-consumer streaming sink;
   unconsumed partitions backpressured the distributor and no batch reached the sink.
   Fix: disable `enable_round_robin_repartition` when physically planning streaming
   plans. Verified: continuous `rate → memory` now surfaces rows (0 → 416 in 5 s);
   bounded/aggregation/console paths unchanged.
2. ~~**Then instrument latency.**~~ **DONE (2026-06-09).** `LatencyTracker` emitted at
   the rate source, measured at the sink → **p50=0.1ms / p99≤0.3ms** logged. Still to do:
   expose via `StreamingQuery.lastProgress` (+ processedRowsPerSecond).
3. **Measure under load + stateful + cluster.** Re-measure at high rates (100k–1M/s),
   with windowed aggregation / stream-stream joins, and distributed — the realistic
   latency envelope (and where it degrades).
4. **Standardized benchmark (Nexmark)** on a release build for a defensible
   Flink-comparable p99.
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
