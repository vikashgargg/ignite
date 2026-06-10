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

## Model (grounded in Spark / Flink / Fluss — researched 2026-06-10)
- **Spark Structured Streaming** (micro-batch — Vajra's model): a checkpoint dir with an
  **`offsets/` WAL** (the offset range of batch N, written *before* processing) and a
  **`commits/` log** (written *after* batch N is durable in the sink). Recovery: resume
  from the latest `offsets/`; if `commits/N` is missing, **reprocess batch N**; sinks must
  be **idempotent**. [ref: Spark internals]
- **Flink**: checkpoint **barriers** + **`TwoPhaseCommitSinkFunction`** — the sink
  *pre-commits* (flush in an open transaction) on the barrier and *commits* only when the
  coordinator confirms all operators snapshotted. Exactly-once needs a **transactional sink**.
- **Apache Fluss**: the state snapshot is **tagged with the next-unread log offset** — the
  exact replay point; restart compares checkpoint offsets vs log-end and replays the delta.

**Unifying principle: the committed (source offset + operator state) must be tied to
*durable sink output*.** That's what makes replay exactly-once rather than at-least/at-most.

### Critical-path consequence for Vajra (important)
End-to-end exactly-once is **gated by a durable/transactional sink**, which Vajra does not
have yet (memory sink = in-process/non-durable; file/listing sink rejects streaming input
— see STREAMING.md). So the honest sequencing is:
1. **Durable sink** (file/Delta streaming write, idempotent or 2-phase) — *prerequisite*.
2. **Source-offset commit coordination** (offsets WAL + commits log, below).
3. **Operator-state snapshot/restore**.

Until #1 exists, the achievable robust milestone is **resume-from-offset** (a restart
replays from the last *generated* offset instead of from 0 — at-least-once with an
idempotent sink), not strict exactly-once. The `startOffset` primitive (landed) + the
offset WAL below deliver that; strict exactly-once follows once the sink lands.

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
