# File-sink commit log — absolute exactly-once for file output

## Goal
Close the **few-millisecond orphan window**: a crash *after* output parquet is durable but
*before* the source offset commits leaves an orphan file that the reader (which scans the
output dir) would include → a duplicate on replay. Verified empirically as **unhittable**
(see `docs/benchmarks/STREAMING_FAILURE_RECOVERY.md`) — this is for an **absolute**
guarantee, not a live bug.

## Reference design — Spark `FileStreamSink` / `_spark_metadata`
(grounded in Spark Structured Streaming + Flink two-phase-commit-sink)
- Per micro-batch, after the output files are durable, Spark writes a **manifest** listing
  the committed files to `<output>/_spark_metadata/<batchId>`.
- Recovery uses a **deterministic, offset-derived `batchId`**: a replay of an uncommitted
  batch reuses the **same** `batchId` → overwrites the same manifest slot → idempotent.
- A separate **commit log** (`commits/<batchId>`) is the single atomic "this batch is done"
  marker. Manifest is written *before* the commit marker; if the crash is in between, the
  re-run reuses the same `batchId` and overwrites — no duplicate.
- **Readers** of a path that contains `_spark_metadata` use the manifests and **ignore
  orphan files** not listed.

## What Zelox already has (helps) and what's missing
**Have:**
- Offset WAL + commit-after-durable + restore (`sources/<id>/{staged,committed}`), verified
  under SIGKILL for stateless + windowed + joins.
- The reader's listing glob **already excludes `_`/`.`-prefixed paths** (`listing/source.rs`)
  — so a `_zelox_metadata` dir is auto-excluded as data, and the read-side honoring can be
  **gated on its presence** (zero blast-radius for normal reads).
- `batch_id` is already derived from the committed offset log (`read_latest_batch_id + 1`),
  so a replay of an uncommitted batch **reuses the same id** — the idempotency enabler.

**Missing (the build):**
1. **Plumb the output path + the deterministic batch-id to the sink/commit step.** Today the
   sink path is fixed at plan time and the sink doesn't know the offset/batch-id at write
   time. `plan_executor` *does* have `start.options["path"]` (next to `checkpointLocation`),
   so thread it to `StreamingQuery::new → run`.
2. **Write-side manifest at commit.** In the runner's commit step (after durable, with the
   offset commit), write `<output>/_zelox_metadata/<batchId>` listing this batch's new files
   (discovered by diffing the output dir against existing manifests). Batch-id-keyed →
   replay overwrites the same slot.
3. **Atomic commit marker.** Reuse/extend the offset commit as the single atomic point so
   "batch committed" ⟺ manifest + offset both durable; a crash in between re-runs the same
   batch-id idempotently.
4. **Read-side honoring (gated).** In `listing/source.rs`, if `<output>/_zelox_metadata/`
   exists, read the manifests → committed file set → scan only those (ignore orphans). Gated
   on the dir's presence → no effect on normal reads.

## Recommended implementation order (tests-first, dedicated effort)
1. Read-side honoring + write-side manifest (no offset coupling yet) — verify a manually
   planted orphan file is ignored on read.
2. Wire the manifest into the runner commit step (batch-id-keyed) + the output-path plumbing.
3. Crash-harness gate: inject a crash in the durable→commit window (or simulate an orphan) →
   verify the replay produces no duplicate (orphan ignored, same batch-id overwritten).

## Implementation findings (2026-06-11 — traced the write path)
- **The sink discards file paths.** `ParallelStreamSinkExec` has each child write one file and
  emit a *count* row (then discarded); DataFusion's file writer returns **row counts, not
  paths**. A correct manifest must list **exactly the files this batch wrote**, so the sink
  must **report its written paths** to the runner — there is no path to harvest today.
- **Discovery is NOT a valid shortcut.** Diffing `<output>` against prior manifests to find
  "this batch's files" includes a crashed run's **orphans** in the replay's manifest → they
  become "committed" → duplicate. (Same flaw bites a post-write rename-to-deterministic-name.)
- **`batch_id` is known only post-plan.** It's derived from the committed offset log in
  `StreamingQuery::new`, *after* the sink plan (with its fixed path) is built — so a
  deterministic `batch_id`-keyed filename can't be set at plan time without re-planning.
- **Two viable correct approaches (each a real build, each with a wrinkle to resolve):**
  1. **Sink reports paths** → runner writes `<output>/_zelox_metadata/<batchId>` from the
     reported set (Spark `FileStreamSink`). Wrinkle: thread paths out of the
     count-emitting sink + the output path into the runner.
  2. **Atomic per-batch staging dir**: write to `<output>/_staging-<runId>/` (hidden →
     reader-excluded), then a single atomic dir rename to a committed location at commit.
     Wrinkle: a *visible* committed subdir risks Hive partition-inference; a flat layout
     needs N-file atomicity.
- **Crash-test gate before "done":** SIGKILL after files durable, before commit → restart →
  **no duplicate** in the output (orphans ignored / overwritten).

## Risk / priority note
The window is **few-ms and empirically unhittable**; this is a correctness-*completeness*
item. It touches the **shared read path** (gated, but still), so it warrants a careful,
tested, dedicated build — not a rushed one. Recommended **after** the all-in-one validation
pass, and can land alongside the continuous-EO / checkpoint-barrier-alignment work (which
shares the same commit-protocol machinery).
