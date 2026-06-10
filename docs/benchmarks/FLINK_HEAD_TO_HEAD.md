# Vajra vs Flink — head-to-head (Phase 1: local de-risk)

Phase 1 validates the comparison harness locally and yields **directional** numbers
before a credible EKS single-node run (Phase 2). Setup: 8-core MacBook Air, Flink 1.17.2
(Java 8), parallelism = 1, generator sources. **Directional only** — not a published result.

## Results (single node, parallelism 1)

| Workload | Vajra | Flink 1.17 | Read |
|---|--:|--:|---|
| **Stateless** (generator → filter) | ~436k rows/s | **~3.25M rows/s** (datagen) | Flink ~7.5× — but Vajra is **source-limited** (`rate` micro-batch tick), not necessarily engine |
| **Windowed agg** (tumbling 1s count) | ~275k rows/s (event-time) | **~4.65M rows/s** (proctime) | Flink ~17× faster |
| **Peak memory (RSS)** | **~66 MB** | ~537 MB (TaskManager JVM) | **Vajra ~8× less** |
| **Latency (continuous)** | **sub-ms p50/p99 (measured)** | not measured (needs latency markers) | Vajra strong; Flink TBD |

## Honest reading (this is the important part)
- **Throughput: Flink wins decisively** (~17× on windowed). Drivers: (a) Vajra's `rate`
  source caps at ~436k/s (micro-batch tick), bottlenecking measured throughput; (b) even
  past that, Flink's **continuous pipelined** engine fundamentally out-throughputs Vajra's
  **micro-batch** model (Vajra is Spark-class here, not Flink-class) + per-batch
  `AggregateExec` construction overhead.
- **Memory: Vajra wins decisively** (~8×, no JVM). Consistent with the batch results.
- **Latency:** Vajra measured sub-ms; Flink not yet measured here.
- **Caveats:** different generators (`rate` vs `datagen`); Vajra event-time vs Flink
  proctime windowing (proctime is cheaper, favors Flink); single laptop; parallelism 1.

## What this means for the product claim (corrected, honest)
Vajra does **not** currently out-throughput Flink on streaming — Flink is substantially
faster there (its pipelined architecture vs Vajra's micro-batch). Vajra's real edges are
**memory (~8× lighter, no JVM)**, **competitive/strong latency**, and the **unified
batch+streaming, single-binary** story. The "outperform Flink" framing holds for
**memory/footprint and operational simplicity**, not raw streaming throughput.

## Optimization path (to narrow the throughput gap)
1. Faster streaming source (the `rate` micro-batch tick is the first bottleneck).
2. Persistent per-window accumulators (avoid per-batch plan construction — Flink
   `AggregateFunction` style).
3. Pipelined (non-micro-batch) execution is the deeper, architectural lever — a larger bet.

## Next
- **Phase 2 (EKS, one large node, tear down):** rerun on real hardware with matched
  parallelism + event-time on both for cleaner, publishable numbers. Direction is already
  clear; Phase 2 refines magnitudes.
- Measure Flink latency (latency markers) for a like-for-like latency row.
