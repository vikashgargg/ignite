# Streaming file source — road to prod-grade (vs Flink `FileSource` / Spark `FileStreamSource`)

`spark.readStream.format("parquet"|"csv"|"json").load(dir)`. Goal: match **and beat** Flink
`FileSource` + Spark `FileStreamSource` on throughput, while matching their exactly-once and
operational semantics. Built on DataFusion's `ListingTable` (which already enumerates
file/row-group splits) + Zelox's flow-event streaming.

## Status
| Capability | State | Notes |
|---|---|---|
| Read parquet/CSV/JSON as a stream | ✅ done | `FileStreamSource` wraps the batch reader |
| Correct over multiple files | ✅ done | reads all input partitions |
| **Parallel-per-file source + sink** | ✅ **done (prod-grade)** | Root cause of the earlier data loss: DataFusion's `repartition_file_scans` row-group byte-range splitting (target_partitions > file count) produces split partitions the streaming sink drained incorrectly. Fix: **disable row-group splitting for streaming scans** (sail-plan/lib.rs) → whole-file partitions (verified-correct regime); `FileSourceExec` exposes one output partition per file group (each with its own `EndOfData`), the parallel sink writes one file per partition. Verified par=1&8 read all 10M; scales 1→2→4 (4.84→2.91→2.64s, ~1.83× at 4 files; caps at file count); all-in-one 12/12; windowed consistent across parallelism. **Intra-file (row-group) split parallelism beyond file count** remains — needs the split-partition streaming-path fix (deeper lever). |
| **Cross-run exactly-once** (processed-files log) | ✅ **done** | re-run processes 0 new; add-files → only new; WAL commit verified |
| **Continuous new-file polling** | ✅ **done (prod-grade)** | `ProcessingTime` trigger → micro-batch re-plan loop; SIGKILL-mid-continuous crash-EO verified (no loss/dup) |
| Schema evolution / merge | ⬜ future | |
| `maxFilesPerTrigger` backpressure | ✅ **done** | cap new files/micro-batch (FIFO by mod-time), backlog drained in slices; verified 8 files @2/trigger → no loss/dup |

## Reference design
- **Spark `FileStreamSource`**: a per-source **metadata log** (`<ck>/sources/<id>/`) records,
  per batch, the files belonging to that batch. Each trigger lists the dir, computes
  *new* files (not in the log), processes them, and (on commit) appends them to the log.
  Restart replays from the log → each file is processed exactly once.
- **Flink `FileSource`**: a `SplitEnumerator` tracks processed paths and assigns new splits
  to parallel readers; checkpointed enumerator state gives exactly-once.

## Build plan — cross-run exactly-once (next)
Reuse Zelox's existing offset-WAL commit (`commit_source_offsets` already promotes
`<ck>/sources/0/staged → committed` atomically, content-agnostic) to store the **processed-files
set** instead of a row offset:

1. **Refactor `create_source` (streaming branch)**: instead of building the batch
   `ListingTable`, carry the pieces into `FileStreamSource` — `urls` (dir), `listing_options`
   (Clone), `schema`, `constraints`, `extension_with_compression`.
2. **`FileStreamSource::scan`**:
   a. Enumerate current files: `crate::listing::utils::list_all_files(url, ctx, store, "", ext)`
      → `ObjectMeta { location, last_modified, .. }` per file.
   b. Read committed seen-set: `<ck>/sources/0/committed` (one identifier per line).
   c. `new = current \ seen` (identifier = the object-store path; carry `last_modified` for
      future ordering / `latestFirst`).
   d. **Build the scan over only `new`**: `ListingTableConfig::new_with_multi_paths(new_urls)`
      `.with_listing_options(self.listing_options.clone()).with_schema(self.schema.clone())`
      → `ListingTable` → `.scan(...)` → wrap in the (parallel) `FileSourceExec`.
      If `new` is empty → an empty input (emits only `EndOfData`).
   e. Stage `seen ∪ new` to `<ck>/sources/0/staged`; the runner promotes it after the batch
      output is durable.
3. **The hard part to get right (why this is a careful build, not a shortcut):**
   reconstructing a correct `ListingTableUrl` per new file **across object stores**
   (local / S3 / GCS) from `ObjectMeta.location` + the base URL's scheme/authority. A
   local-only shortcut would silently mis-read on S3 — unacceptable per the prod-grade bar.
   Validate against `LocalFileSystem` **and** an S3-style store.
4. **Tests (must pass before "done")**: (a) run an `availableNow` job twice → 2nd run
   processes **0** new files; (b) add files between runs → only new ones processed;
   (c) SIGKILL mid-run → restart reprocesses only the uncommitted batch's files, no loss;
   (d) combined with the file-sink commit log, no duplicate output.

## Build plan — THROUGHPUT LEVER: aligned multi-partition EndOfData (next)
Profiling found stateless ETL scaling flattens early and that exposing N source-output
partitions loses data because the bounded-query **termination cancels split partitions before
they drain**. The lever: make the framework terminate a bounded multi-partition stream **only
after every partition's `EndOfData`** (aligned, like Flink barrier alignment), so the source
can expose N partitions → the parallel sink writes N files concurrently → near-linear. This is
the same alignment needed for correct multi-partition event-time watermarks.

## Build plan — continuous new-file polling (grounded findings, 2026-06-12)
For a non-`availableNow` trigger: a poll loop (interval = trigger) that re-lists the dir, emits
new files as flow-event micro-batches, never emitting `EndOfData`. **Scope finding (traced the
runner):** this is NOT just a poll loop — Zelox uses a **continuous-dataflow** model (one plan
runs forever, à la Flink), and `StreamingQuery::run` commits source offsets **only on clean
stream-end** (`commit_source_offsets` fires when the stream `task` resolves; markers are
stripped by `FlowEventToDataExec` before the runner). So:
- A naive poll loop gives **at-least-once on crash** (the processed-files log commits only on
  graceful stop) — **below Spark**, which is exactly-once per batch.
- Committing the file log per micro-batch naively **races**: the source would stage poll N+1's
  files before the runner commits poll N → files marked processed before their output is
  durable → loss on crash.
- **Prod-grade requires continuous exactly-once**: in-stream checkpoint **barriers** (Flink
  asynchronous barrier snapshotting) delineating micro-batches + a stage→durable→commit→next
  ordering (Spark `MicroBatchExecution` offset-log/commit-log protocol), so the file log
  commits only after each micro-batch's output is durable, with no race.

### LOCKED design (2026-06-12) — Spark micro-batch **re-plan loop**, reusing proven EO
Chosen over Flink barrier-snapshotting because it **reuses Zelox's crash-tested machinery**
with no new EO protocol:
- **Each trigger = a fresh *bounded* micro-batch.** Re-resolve+execute the plan with
  `bounded=true` (so the source lists, processes only *new* files via the metadata log, emits
  `EndOfData`, and the runner's existing `commit_source_offsets`-on-clean-end commits it).
- **State + offset continuity for free**: stateful operators already `restore_state` at
  execute start and snapshot on `EndOfData` (confirmed in `window_accum.rs`/`state_io.rs`);
  the source restores the committed processed-files set. So each micro-batch resumes from the
  previous one's committed checkpoint — **state continuity + per-batch exactly-once +
  crash-EO, by construction** (no new commit-cadence, no race).
- **Low blast radius — scope to the `ProcessingTime` trigger only.** `AvailableNow`/`Once`
  (bounded) and the default-trigger path stay **byte-identical** (no existing test uses
  `ProcessingTime`), so the all-in-one suite is the blast-radius guard.

**Plumbing:** `runner.execute(ctx, plan)` re-executes a physical plan, but the file list is
baked at plan time → must **re-plan** per trigger. So `StreamingQuery` (continuous variant)
holds `(ctx.clone(), config, spec_plan, checkpoint)` + interval and loops:
`resolve_and_execute_plan_with_options(bounded=true) → runner.execute → consume to EndOfData →
(commit on clean end) → sleep(interval)` until the stop signal. Bounded path unchanged.

**Gate (all must pass):** all-in-one 12/12 (default trigger unaffected); availableNow EO; a
`ProcessingTime` continuous query ingests files added between triggers; graceful stop →
restart → no reprocess; **SIGKILL mid-continuous → restart → no loss, no duplicate**.

> Status: design locked + de-risked (reuses proven EO/state recovery; ProcessingTime-scoped =
> low blast radius). Implementation is a contained streaming-lifecycle refactor
> (`StreamingQuery` + `plan_executor`), to be done with the full gate above.

## Throughput note
Parallel split reading (done) is the main lever to **beat** Flink/Spark: file/row-group splits
are read concurrently across `target_partitions`, then transformed/written in parallel
(Zelox's parallel streaming sink). The metadata log adds correctness without serializing I/O.
