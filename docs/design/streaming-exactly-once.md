# Design: exactly-once streaming recovery

Status: **in progress.** Today (verified 2026-06-10): a `checkpointLocation` query persists
a **batch-id counter** and resumes it on restart (no crash, query continues) — but that's
*batch-id continuity*, not exactly-once: source offsets and operator state are **not**
checkpointed, so a restart reprocesses from the source start and loses stateful results.
This is the #1 reliability gap vs Spark/Flink. This doc is the plan to close it.

**Landed (this pass):** the rate source now supports a `startOffset` option and resumes
deterministically from it (row N has value N + a timestamp derived from N — verified:
`startOffset=1000` → values 1000,1001,…). That's the foundational primitive the recovery
loop uses to replay the exact same data after a restart.

## Model (grounded in Spark Structured Streaming / Flink)
Both engines checkpoint, per micro-batch/epoch: **(1) source offsets** (the input range
consumed) and **(2) operator state** (aggregation accumulators, join buffers). On restart
they restore both and resume — replaying uncommitted input exactly once. Without a
time-range/offset abstraction the state is unbounded and recovery impossible.

Vajra already has the right substrate: the **flow-event marker** stream
([streaming-watermark.md](streaming-watermark.md)) and a per-batch checkpoint writer in
`StreamingQuery::run` (`crates/sail-spark-connect/src/streaming.rs`).

## Plan

### A. Source-offset recovery (stateless exactly-once) — foundational
1. **Source emits its committed offset.** Extend the existing `FlowMarker::Checkpoint` (or
   add `FlowMarker::SourceOffset { source_id, offset }`) — the rate source emits its
   row-offset with each batch; it rides the flow-event stream to the query runner.
2. **Runner persists offsets.** `StreamingQuery::run` already writes `<loc>/offsets/<batch>`
   per output batch — decode the source-offset marker and write the **real offset** (not
   just `{batchId,timestamp}`) into that file.
3. **Restore on restart.** On start, read the latest committed offset from `<loc>/offsets/`
   and inject it into the source as `startOffset` (the primitive that now exists). The
   source replays from there — exactly-once for stateless pipelines (rate → map/filter →
   sink). Threading: same path as the `bounded` flag (rewriter → `StreamSourceWrapperNode`
   → `StreamSource::scan`), plus proto/codec for distributed.
4. **Kafka**: the same shape — commit Kafka partition offsets; restore on restart.

### B. Operator-state recovery (stateful exactly-once)
1. **Snapshot state on checkpoint.** On a `Checkpoint`/watermark boundary, serialize each
   stateful operator's state: `WindowAccumExec` partial states (`pending_rows`),
   `StreamJoinExec` buffered batches, `StreamDeduplicateExec` keys — to `<loc>/state/<op>/`.
2. **Restore on restart.** Rebuild each operator's state from its snapshot before
   resuming. Operators already hold their state in plain `Vec<RecordBatch>` /
   `HashSet`, so snapshot = write batches; restore = read them back into the unfold state.
3. **Atomic commit.** A batch is "committed" only when source offset **and** all operator
   states are persisted — so restore is consistent (exactly-once, not at-least/at-most).

## Correctness tests (write FIRST)
- Stateless: rate→filter→file sink, kill mid-stream, restart → output is the exact
  continuation (no gap, no duplicate) — needs (A).
- Stateful: windowed count, kill mid-window, restart → window counts are correct
  (pre-restart rows included) — needs (A)+(B).
- Idempotent sink / dedup on replay of the last uncommitted batch.

## Scope / honesty
Source-offset recovery (A) gives exactly-once for **stateless** streaming and is the
tractable next milestone. Operator-state recovery (B) is the larger half (state
serialization for each operator). Until both land, Vajra is **batch-id-continuity +
deterministic-replay capable**, not exactly-once — and we must not claim reliability
parity with Flink/Spark until A+B are done and the tests above pass.
