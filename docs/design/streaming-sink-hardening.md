# Prod-grade streaming-sink hardening checklist

Distilled from hardening the Iceberg streaming sink against the incumbent gold standards
(Flink `IcebergFilesCommitter`, Spark Structured Streaming committers, the Iceberg/Delta specs).
**Lesson: a freshly-shipped sink is not prod-grade until it has passed a hardening pass against
the incumbent.** Apply every item below to each new sink (Delta, Kafka, …) and re-audit existing
ones.

## C1 — Idempotency must scan committed *history*, not just current state
A crashed-then-replayed micro-batch must be recognized and skipped. The dedup key (batch/epoch id)
must be looked up across the **commit history**, because a *foreign* commit (compaction, an
unrelated writer) can become the "current" state and hide the marker.
- Iceberg: walk the snapshot **ancestry** matching `app-id` (Flink `getMaxCommittedCheckpointId`),
  not just the current snapshot's summary.
- Delta (next): scan the transaction log for the app's max committed `txnVersion`, not just the
  latest commit.

## C2 — Recovery pointers are LOWER BOUNDS; reconcile against the authoritative listing
Any "latest pointer" that is updated in a **separate, non-atomic step** from the data it points to
can be stale after a crash between the two writes. Never trust it blindly.
- Iceberg: `version-hint.text` is written *after* the metadata file. A crash in between leaves a
  stale hint → blindly trusting it hides the just-committed metadata and *deadlocks* the next
  commit against the orphaned file. Fix: honor the hint only when `>=` the max version actually
  listed (`hint_is_current`); else use the listed max. (Iceberg `HadoopTableOperations` semantics.)
- Delta (next): `_last_checkpoint` is a hint; the real latest version is the max `*.json` listed.

## C3 — The dedup key must be STABLE across replay
The batch/epoch id used for idempotency must come from a record that advances **atomically** with
the source offset, so a replay reuses the same id. In Vajra this is
`file_stream::current_batch_id` (batch id embedded in the source's `staged`/`committed` offset
record, promoted by one atomic rename). A separately-maintained counter would renumber the replay
and defeat idempotency.

## C4 — Skip empty commits
Do not commit a snapshot/transaction for a micro-batch with no new data (metadata bloat). The
source offset still advances, so the empty batch is not reprocessed. (Flink skips empty commits;
Spark's file sink writes an empty manifest entry — match the relevant incumbent.)

## C5 — Idempotency check INSIDE the commit retry loop
Optimistic-concurrency commits retry on conflict by re-reading the latest state. The "already
committed?" check must run **after each re-read**, or a racing/duplicate commit slips through.

## C6 — Write complete, spec-standard metadata
Downstream tooling, metadata tables, snapshot/version expiration, and incremental reads depend on
the standard summary/commit metrics (`added-records`, `total-records`, `added-data-files`,
`total-data-files`, file sizes, operation). Populate them like Flink/Spark do — for batch writes
too, not only streaming.

## Audit status
- **Iceberg sink** — ✅ C1–C6 (commits 0c72dbf9, cd51cf89, 3044dcf4).
- **File `_spark_metadata` sink** — C1 ✓ (reader folds the full commit log + compaction, not a
  single pointer), C2 ✓ (no separately-updated latest pointer — the log dir *is* the source of
  truth), C3 ✓ (`current_batch_id`), C5 ✓ (commit is a single atomic `put`, no retry window), C6 ✓
  (Spark `SinkFileStatus`). C4: writes empty-batch metadata (matches Spark's file sink, its
  incumbent) — acceptable.
- **Delta sink** — to build with C1–C6 baked in (`Txn(appId, version)` + log-scan dedup +
  `_last_checkpoint`-as-hint).
- **Kafka source EO** — to build; C3 applies (per-partition offsets in the atomic offset record).
