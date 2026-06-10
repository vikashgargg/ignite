# Intra-node streaming parallelism (closing the per-node throughput gap)

## Why this exists
Phase-2 AWS head-to-head (`docs/benchmarks/FLINK_HEAD_TO_HEAD.md`): per **core**, Vajra
beats Flink (stateless 1.34×, windowed ~3×, memory ~16×). But Vajra streaming runs on a
**single core** regardless of node size, so per **node** Flink can scale past us via
`parallelism=N`. Closing that is the one remaining throughput lever.

## Phase 0 finding (measured 2026-06-10) — multi-partition currently *regresses*
Local release build, stateless `rate → filter → memory`, `rowsPerSecond=80M`:

| `numPartitions` | throughput |
|--:|--:|
| 1 | **28.9M/s** |
| 4 | 8.18M/s (**~3.5× slower**) |

**Root cause (code-confirmed):**
- The rate source *can* emit N partitions (`num_partitions` option), and streaming
  `FilterExec` preserves partitioning.
- But every **sink** (`memory_sink_exec.rs`, `collector.rs`) is `UnknownPartitioning(1)`
  with a `partition != 0` guard and calls `self.input.execute(0, …)`.
- So DataFusion's `EnforceDistribution` inserts a **single-threaded
  `CoalescePartitionsExec`** to funnel N→1 before the sink, and the streaming driver only
  executes partition 0. Result: N small-batch partitions merged on one thread is *slower*
  than one big-batch partition.
- The rewriter also treats `LogicalPlan::Repartition` as a **no-op** ("data arrives as a
  single flow-event stream") — the whole model assumes one stream end-to-end.

So parallelism isn't "almost working" — it's architecturally single-stream. This is the
build.

## Design (grounded in references)
- **DataFusion `RepartitionExec`** (docs.rs): maps N→M partitions by a `Partitioning`
  scheme (`RoundRobinBatch`, `Hash`); output partitions pull from per-(in,out) channels.
  We reuse the *pattern*, but markers need special handling (below).
- **Spark Structured Streaming / Flink:** stateless ops parallelize freely (one task per
  partition, one output file per task — no coalesce); stateful ops use a **keyed** exchange
  (hash by key) so each task owns disjoint state, with **watermark = min across input
  channels** and **aligned checkpoint barriers**.

### The marker rule (the crux for flow-events)
A flow-event stream carries control markers (`Watermark`, `Checkpoint{id}`, `EndOfData`,
`LatencyTracker`) interleaved with data. Across partitions:
- **Data**: routed by scheme (round-robin for stateless, **hash-by-key** for stateful).
- **Markers**: **broadcast to all output partitions.**
- **Watermark** at a multi-input operator = **min** over per-input watermarks.
- **Checkpoint** = **barrier alignment**: wait for the marker on *all* input channels
  before snapshotting (preserves the exactly-once we already built).
- **EndOfData**: the driver/coalesce completes only after **all N** inputs signal it.

## Build phases
- **Phase 0 (this doc): measure + root-cause.** ✅ Done — multi-partition regresses; cause known.
- **Phase 1 — parallel stateless sink + driver (no keyed state, no exactly-once risk):**
  multi-partition file sink (one writer/file per partition, Spark-style) + driver executes
  all N partitions concurrently; per-partition `EndOfData`. Target: stateless `source →
  map/filter → write` scales ~N×. *This is the safe first build — it does not touch the
  stateful exactly-once operators.*
- **Phase 2 — keyed exchange** (hash-by-key `StreamRepartitionExec`, markers broadcast).
- **Phase 3 — multi-partition stateful operators** (`WindowAccumExec`/`StreamJoinExec`,
  per-partition keyed state) + **watermark min-merge**.
- **Phase 4 — checkpoint barrier alignment** so exactly-once survives parallelism.

## Correctness gates (write tests first)
- Stateless N-partition output = single-partition output (same rows, no loss/dup).
- `availableNow` terminates only after all N partitions reach `EndOfData`.
- Stateful (Phase 3): windows still emit **once** with correct counts under hash exchange.
- Exactly-once (Phase 4): kill-restart across N partitions → no gap/dup.
