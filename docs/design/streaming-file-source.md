# Streaming file source — road to prod-grade (vs Flink `FileSource` / Spark `FileStreamSource`)

`spark.readStream.format("parquet"|"csv"|"json").load(dir)`. Goal: match **and beat** Flink
`FileSource` + Spark `FileStreamSource` on throughput, while matching their exactly-once and
operational semantics. Built on DataFusion's `ListingTable` (which already enumerates
file/row-group splits) + Vajra's flow-event streaming.

## Status
| Capability | State | Notes |
|---|---|---|
| Read parquet/CSV/JSON as a stream | ✅ done | `FileStreamSource` wraps the batch reader |
| Correct over multiple files | ✅ done | reads all input partitions |
| **Parallel split reading** | ⚠️ **corrected to concurrent-read, single-output** | Profiling (20M, par=8) caught a silent **data-loss** bug: exposing N output partitions let the downstream sink cancel a **row-group-split** partition before it drained (par≤file-count was fine; par>files split files → loss). Fixed: `FileSourceExec` now drains **all** input partitions **concurrently** (`select_all`) into **one** output + a single `EndOfData` — correct at any parallelism (par=1 & par=8 both read all 10M). Concurrent read I/O kept; windowed-agg parallelism unaffected (keyed exchange). Fully-parallel **sink** output is the throughput lever below. |
| **Cross-run exactly-once** (processed-files log) | ✅ **done** | re-run processes 0 new; add-files → only new; WAL commit verified |
| **Continuous new-file polling** | ⬜ designed (below) | currently `availableNow`/one-shot only |
| Schema evolution / merge | ⬜ future | |
| `maxFilesPerTrigger` backpressure | ⬜ future | |

## Reference design
- **Spark `FileStreamSource`**: a per-source **metadata log** (`<ck>/sources/<id>/`) records,
  per batch, the files belonging to that batch. Each trigger lists the dir, computes
  *new* files (not in the log), processes them, and (on commit) appends them to the log.
  Restart replays from the log → each file is processed exactly once.
- **Flink `FileSource`**: a `SplitEnumerator` tracks processed paths and assigns new splits
  to parallel readers; checkpointed enumerator state gives exactly-once.

## Build plan — cross-run exactly-once (next)
Reuse Vajra's existing offset-WAL commit (`commit_source_offsets` already promotes
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

## Build plan — continuous new-file polling (after the log)
For a non-`availableNow` trigger: a poll loop (interval = trigger) that re-lists the dir,
emits new files as flow-event micro-batches, never emitting `EndOfData` (the query runs
continuously). Reuses the metadata log from above for new-file detection + recovery.

## Throughput note
Parallel split reading (done) is the main lever to **beat** Flink/Spark: file/row-group splits
are read concurrently across `target_partitions`, then transformed/written in parallel
(Vajra's parallel streaming sink). The metadata log adds correctness without serializing I/O.
