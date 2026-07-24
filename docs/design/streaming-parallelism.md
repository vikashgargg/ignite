# Intra-node streaming parallelism (closing the per-node throughput gap)

## Why this exists
Phase-2 AWS head-to-head (`docs/benchmarks/FLINK_HEAD_TO_HEAD.md`): per **core**, Zelox
beats Flink (stateless 1.34×, windowed ~3×, memory ~16×). But Zelox streaming runs on a
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

## Architecture (v2, post-review) — reuse the engine we already have
**Zelox already ships a distributed shuffle** (`zelox-execution`: `ShuffleWriteExec`,
`ShuffleReadExec`, `RepartitionExec` with `Partitioning::Hash`/`RoundRobinBatch`, job-graph
planner, task runner, spill). Streaming currently **bypasses** it (`zelox-plan/src/lib.rs`
disables repartition for streaming "so the pipeline runs unbroken").

**Decision: do NOT build a bespoke parallel driver. Make the existing partitioned-execution
machinery flow-event/marker-aware, and express parallelism in the PLAN.** The streaming
driver stays thin (drives the sink's N partitions via `execute(i)`). Consequence —
**intra-node parallelism is the single-node case of distributed execution: scale-up and
scale-out are one mechanism.** (On-goal: one engine, reuse mature DataFusion/Apache
machinery; distributed streaming later for nearly free.)

### Grounded references
- **DataFusion `RepartitionExec`** (docs.rs): N→M by `Partitioning` (`RoundRobinBatch`,
  `Hash`); **bounded** per-(in,out) channels (preserves our memory edge). Reuse + wrap for
  markers.
- **Flink:** keyed exchange + **watermark = min across inputs with idleness detection**
  (`withIdleness` — else an idle input stalls all windows); **Chandy–Lamport / aligned
  checkpoint barriers** via a **CheckpointCoordinator**; **credit-based flow control**.
- **Spark:** stateless parallelizes freely (one output file per task, no coalesce);
  stateful uses a hash (shuffle) exchange; skew handled via partial-agg/salting.

### Non-negotiable principles (protect the differentiators)
1. **Cost-based, not always-on.** Phase 0 showed 1 big-batch partition (28.9M/s) **beats**
   4 small-batch partitions (8.18M/s). Repartition only when a single core is CPU-bound;
   otherwise bigger batches on one core win (no shuffle, ordering kept, less memory). Never
   trade per-core efficiency (our real edge) for per-node vanity numbers.
2. **Bounded + backpressured exchange** (credit-based) — an unbounded buffer would destroy
   the 7.5–16× memory win, which is *the* differentiator.
3. **Markers are control-plane:** data is routed (round-robin / hash-by-key); markers are
   **broadcast**; watermark = **min with idleness**; checkpoint = **barrier-aligned via a
   coordinator**; `EndOfData` = all-N. Get this wrong and stateful correctness/exactly-once
   silently breaks.

### Advancements (toward outperforming both, not just parity)
- **Unified scale-up == scale-out** (one exchange for intra-node and distributed) — leaner
  than engines that maintain separate paths.
- **NUMA / core-affinity for partition workers** — a native, no-JVM advantage.
- Later: **unaligned checkpoints** (low latency under backpressure) + **reactive
  autoscaling** (load-adaptive parallelism).

### The marker rule (the crux for flow-events)
A flow-event stream carries control markers (`Watermark`, `Checkpoint{id}`, `EndOfData`,
`LatencyTracker`) interleaved with data. Across partitions:
- **Data**: routed by scheme (round-robin for stateless, **hash-by-key** for stateful).
- **Markers**: **broadcast to all output partitions.**
- **Watermark** at a multi-input operator = **min** over per-input watermarks.
- **Checkpoint** = **barrier alignment**: wait for the marker on *all* input channels
  before snapshotting (preserves the exactly-once we already built).
- **EndOfData**: the driver/coalesce completes only after **all N** inputs signal it.

## Phase 1 root-cause (confirmed 2026-06-10)
Tracing the multi-partition stateless write end-to-end:
- The source emits N partitions; `FilterExec`, `FlowEventToDataExec`, `EmptySinkAdapterExec`
  all **preserve** partitioning; Zelox's `create_writer` does **not** reject multi-partition.
- **The hard stop is DataFusion's `DataSinkExec`** — `datafusion-datasource/src/sink.rs:103`:
  *"DataSinkExec requires its input to have a single partition."* For batch the optimizer
  coalesces N→1; for an **unbounded (streaming) multi-partition** input it rejects →
  surfaced as `cannot write streaming data to listing table`. (Memory sink doesn't reject —
  it just coalesces N→1 single-threaded, the Phase-0 regression.)

**So Phase 1 ≠ tweak the existing sink.** It needs a **dedicated parallel streaming file
sink** that bypasses `DataSinkExec`'s single-partition rule:
- `output_partitioning() == 1` (driver unchanged — sees one completion stream).
- `execute(0)` spawns **N writer tasks**; task `i` runs `input.execute(i)`, decodes
  flow-events, writes **its own file** (`part-i.parquet`) via the Arrow/Parquet writer,
  tracks per-partition `EndOfData`.
- Completes only when **all N** tasks finish (all-N `EndOfData`) → then the driver's
  offset/state commit fires (exactly-once unaffected — commit still after all durable).
- Tests first: N-file output == 1-file output (same rows, no loss/dup); `availableNow`
  waits for all N; cost-gate (don't engage below CPU-bound).

## Build phases (re-sequenced around reuse + the exchange primitive)
- **Phase 0: measure + root-cause.** ✅ Done — multi-partition regresses; cause known.
- **Phase 1 — parallel stateless file sink.** ✅ **Done 2026-06-10.** `PartitionSelectExec`
  (input partition i → partition 0) + `ParallelStreamSinkExec` (drive N single-partition
  DataFusion sinks concurrently on tokio tasks, one file per source partition, all-N
  `EndOfData`); `create_writer` fans into N sinks when streaming + N>1. Also fixed a
  pre-existing rate-source bug (all partitions emitted identical values → now stride-by-N,
  Spark round-robin). **Verified:** NP=4 parquet → 4 files, output == NP=1, no loss/dup,
  exactly-once + batch unaffected. **Throughput:** write-to-disk is **I/O-bound**, so the
  NP gain is modest (~1.12× debug) — consistent with the cost-based principle. The real
  parallelism win is the **stateful** operators (Phase 2–3), where Flink scales and we don't
  yet. (The marker-aware `StreamExchangeExec` is deferred to Phase 2, where keyed routing
  actually needs it; Phase 1 rode the source's native N partitions, no exchange required.)
- **Phase 2 — keyed (hash) exchange** for stateful ops + **multi-partition
  `WindowAccumExec`/`StreamJoinExec`** (per-partition keyed state) + **watermark min-merge
  WITH idleness detection** (idle input must not stall windows). Document **key-skew**
  handling (partial-agg / salting).
  - **Step 1 ✅ (merged):** `StreamExchangeExec` (hash data, broadcast markers), unit-tested.
  - **Step 2 ⛔ (attempted, reverted — needs focused debugging):** wired multi-partition
    `WindowAccumExec` (per-partition state op-id) + planner inserts
    `WatermarkExec → StreamExchange(hash keys) → WindowAccum(N) → CoalescePartitions(N→1)`,
    gated to keyed aggs at `target_partitions`. **It compiles + does not regress** no-key /
    batch, but two issues block it:
    1. **Pre-existing (not parallelism):** keyed windowed agg with an *inline-expression*
       key (`value % 10`) fails `?table?.#1` column resolution; a *pre-projected column*
       key works. Fails identically at N=1 — independent of parallelism. Root cause is in
       the rewriter/resolver qualifier handling, not the planner's `create_physical_expr`
       (stripping qualifiers there did not fix it). **Fix this first, separately.**
    2. **Step-2 integration bug:** with a column key, the parallel path *runs without error
       but emits 0 windows* — window emission breaks through the exchange/coalesce. Suspect
       the broadcast **watermark not reaching the window instances** through the exchange,
       or `CoalescePartitionsExec` not draining the unbounded flow-event partitions. Needs a
       focused debug (likely a dedicated marker-aware coalesce, mirroring the exchange).
  - **Conclusion:** the exchange primitive is sound; step-2 integration is a real, multi-bug
    effort. Do not ship until both are fixed and the gate (parallel == single) passes.

### ⚠️ Deeper prerequisite found (2026-06-11): keyed windowed aggregation is broken
Investigating the inline-expression-key resolution error led to a bigger discovery — **keyed
windowed aggregation (`groupBy(window, key).count()`) does not produce correct results**, and
this is **independent of parallelism** (reproduces at N=1) and of the key form:
- **Resolution crash** for inline-expression keys (`value % 10` → `?table?.#1` not found).
  A one-line rewriter fix (`strip_qualifiers` on the agg group/aggr exprs, matching the
  stream-join path) removes the crash — but is **not enough**, because:
- **Wrong results for *all* keyed windows** (column keys too): the distinct group key
  **collapses to a single value (k=0)** and counts are **severely under-reported** (~5–10% of
  rows). The `final_group_by` remap looks correct, so the bug is deeper in the partial
  aggregate / window-struct grouping / emission path of `WindowAccumExec`.

**Implication:** Zelox's windowed aggregation today is correct for the **window-only** case
(`groupBy(window).count()`) but **not** for the **keyed** case. Phase 2 parallelism *targets*
keyed windowed aggs, so it is blocked until keyed windowed aggregation is correct.

**Required fix (dedicated, tests-first), in order:**
1. ✅ **Done (2026-06-11):** Rewriter `strip_qualifiers` on agg group/aggr exprs (fixes the
   inline-expression-key crash; matches join path).
2. ✅ **Done (2026-06-11):** `WindowAccumExec` keyed-window correctness. Root cause was
   `window_emit_mask`: `emitted.insert(window_end)` per row suppressed every group row after
   the first for a given window end → only `k=0` emitted, counts ~1/K. Fixed to test
   membership without mutating (all group rows of a newly-closed window emit together), then
   record ends after. Verified: all keys present, exactly K rows/window, keyed total ≈
   no-key total (counts complete); no-key + windowed exactly-once + batch unaffected.
3. ✅ **Done (2026-06-11) — Phase 2 step 2:** parallel keyed windowed aggregation.
   Multi-partition `WindowAccumExec` + planner inserts `Watermark → StreamExchange(hash keys,
   broadcast markers, N) → WindowAccum(N) → StreamCoalesceExec(N→1)`, gated to keyed aggs at
   `target_partitions`. The **marker-aware `StreamCoalesceExec`** (drains all N partitions
   concurrently) fixed the 0-output — DataFusion's generic coalesce left the exchange's
   bounded broadcast channels blocked. **Verified correct:** per-`(window,key)` counts
   exactly uniform (`value%5` → 3530×5, spread 0.00), all keys, no split. **Throughput**
   (keyed windowed, `value%100`, rate 5M): parallelism 1→372k/s, 8→**951k/s (~2.55×)**.

### Why ~2.55×, not ~8× (and the next lever)
The keyed window **aggregation** parallelizes across N cores, but the **source (1 partition)
and the exchange's single distributor task (read + hash + route)** are serial — so the serial
front-end caps the speedup. To approach linear scaling: a **multi-partition source** feeding
**per-partition distributors** (so hashing/routing also parallelizes), i.e. push the keyed
exchange closer to the source. That's the next throughput lever; correctness + the operator
infrastructure (exchange/coalesce/multi-partition stateful) are now in place and also pave
the way to **distributed** streaming (same exchange over the network shuffle).
- **Phase 3 — CheckpointCoordinator** (Chandy–Lamport, aligned barriers): trigger → align →
  ack → single commit wired to the existing offset-WAL + state-commit, so **exactly-once
  survives parallelism**.
- **Phase 4 — unify with distributed** (same exchange over the network shuffle) +
  observability (per-partition throughput, watermark lag, backpressure, checkpoint
  align-time). Later: unaligned checkpoints, reactive autoscaling, NUMA affinity.

## Correctness gates (write tests FIRST — this is exactly-once-adjacent)
- Stateless N-partition output == single-partition output (same rows, no loss/dup); order
  contract documented (repartition breaks row order).
- `availableNow` terminates only after **all N** partitions reach `EndOfData`.
- Parallelism is **not** engaged below the CPU-bound threshold (perf regression guard —
  don't lose to the 1-big-batch case).
- Stateful (P2): windows emit **once** with correct counts under hash exchange; **idle
  partition does not stall** watermark/window close.
- Exactly-once (P3): kill-restart across N partitions → no gap/dup; commit is atomic across
  all partitions (single coordinator cut).
