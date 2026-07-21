# Zelox Streaming — capability audit & "master of streaming" roadmap

> **Update 2026-07-04 (on `main`):** crash-EO exactly-once (16-part continuous `kill -9`, EKS-confirmed dup=0
> exact), final-window completeness (`ZELOX_COMPLETE_ON_END` = Flink `scan.bounded.mode` parity), and a
> **parallel Kafka sink** (fixed a 15/16-partition data-loss bug + ~300× throughput) landed, validated
> T1 local → T2 `kind` → T3 EKS. Open streaming items are tracked in
> [docs/design/spark-parity-and-upgrade-plan.md](design/spark-parity-and-upgrade-plan.md) §4.

Zelox's biggest differentiation vs LakeSail (which ships **no** Structured Streaming)
is streaming. This is the honest map of what works today, what's partial, and what's
needed to be a true Spark Structured Streaming replacement — with the code locations,
so each gap is actionable. Audited 2026-06-08 against the engine source.

Legend: ✅ works · 🟡 partial / per-micro-batch only · ⬜ missing.

## Sources
| Source | Status | Notes |
|---|---|---|
| Kafka | ✅ | `StreamSource` + `StreamSourceWrapperNode` |
| Rate | ✅ | `test_rate.py` coverage |
| Range | ✅ | via `StreamSourceAdapterNode` |
| File / table (incremental) | ✅ | `source_as_provider` → stream wrapper |
| Kafka **sink** (write to Kafka) | ⬜ | sources only today |

## Sinks
| Sink | Status | Notes |
|---|---|---|
| `console` | ✅ | verified working sink |
| **continuous output (any sink)** | ✅ | **fixed 2026-06-09** — continuous queries now drive the sink (was: round-robin repartition stalled the single-consumer pipeline → 0 rows). |
| `memory` (queryable) | ✅ | **fixed 2026-06-09** — buffer registered as a temp view via CatalogManager + handle carried on the node (was: `failed to resolve catalog: datafusion`). `SELECT … FROM <queryName>` now returns written rows. |
| `foreachBatch` (Python) | 🟡 | `ForeachBatchSinkNode`; needs server-side Python 3.12 env; Scala/Java foreachBatch unsupported |
| File / data source (parquet/csv listing) | ✅ | **Durable + sink-side exactly-once (2026-06-13):** `writeStream.format("parquet")` persists a stream to files. `create_writer` decodes the flow-event input (`FlowEventToDataExec`) and feeds the normal file writer. With a `checkpointLocation`, the sink now writes each micro-batch into a per-batch subdir `<out>/<batchId>/` and atomically commits a Spark-format `_spark_metadata/<batchId>` log (`StreamingSinkCommitExec` + `streaming_sink_log`); both the batch reader (`ListingTable`) and the streaming file source honor the log, reading **only committed files** so orphan/partial files from a crashed-then-retried batch are invisible. The batch id is embedded in the file source's offset record so it advances **atomically** with the source position (one rename of `staged`→`committed` carrying both the id and the processed-files set): a crash anywhere around commit replays the batch at the **same id**, idempotently overwriting `<out>/<N>/` + `_spark_metadata/<N>` — no duplicate **and** no silent-loss window. Verified: SIGKILL mid-batch → restart → `total==distinct` (no dup/loss); deterministic crash-mid-commit (W3) simulation (replay reuses id N, no `metadata/N+1`); planted-orphan exclusion; stream-reads-stream-output exactly-once; compaction every 10 batches; unit test on the recovery numbering. **Remaining:** non-file (non-replayable) sources fall back to offset-marker numbering (file→file is the exactly-once target); table/Delta/Iceberg streaming sinks not yet wired. |
| Iceberg streaming sink | ✅ | **Transactional streaming sink with idempotent exactly-once (2026-06-13):** `writeStream.format("iceberg").option("checkpointLocation",…)` decodes the flow-event stream and commits one Iceberg **append snapshot per micro-batch**. Exactly-once is via the snapshot summary (`zelox.streaming.batch-id`/`app-id`, Flink `max-committed-checkpoint-id` pattern): the commit records the batch id and **skips** if a batch `<=` it was already committed, so a crashed-then-replayed batch never double-appends. The batch id comes from the source's atomic offset record (`current_batch_id`), so source position + sink commit stay in lockstep — no `_spark_metadata` needed (Iceberg's commit is itself ACID, no orphan files). **Prod-hardened vs Flink (2026-06-13):** (1) idempotency walks the **snapshot ancestry** (Flink `getMaxCommittedCheckpointId`) not just the current snapshot, so it stays correct when a foreign snapshot (compaction/unrelated append) is interleaved; (2) **standard Iceberg summary metrics** (`added-records`/`total-records`/`added-data-files`/`total-data-files`/`added-files-size`/`total-files-size`) are written so tooling, the `snapshots` metadata table, expiration, and incremental reads work (batch writes too); (3) **empty micro-batches commit no snapshot** (Flink-style, avoids metadata bloat). Verified: file→Iceberg availableNow + continuous (multi-snapshot) read back exactly; deterministic replay (staged batch present, snapshot already committed) → commit skipped (3000 not 6000); ancestry-traversal unit test (interleaved foreign snapshot); 64 crate tests + batch write unaffected. Requires `file://`-scheme path (pre-existing Iceberg requirement). |
| Delta streaming sink | ⬜ | same shape as Iceberg (crate has the `Txn(appId,version)` idempotent-commit primitive); not yet wired. |
| `foreach` (row writer) | ⬜ | explicitly rejected — use `foreachBatch` |
| `console` | ⬜ | not implemented |

## Operators (`crates/sail-plan/src/streaming/rewriter.rs`)
| Operator | Status | Notes |
|---|---|---|
| Projection / Filter | ✅ | flow-event schema threaded through |
| Stateless window (rank/lag/row_number) | ✅ | per-micro-batch |
| Aggregation — append | ✅ | stateless per-micro-batch |
| Aggregation — **event-time window** | ✅ | **works 2026-06-09** via marker-based watermarks: `WatermarkExec` emits `FlowMarker::Watermark`; `WindowAccumExec` evicts windows on watermark advance (emit-once + bounded retention). `withWatermark(...).groupBy(window(...)).count()` → `SELECT *` returns `struct<window:struct<start,end>,count>` with correct contiguous windows + counts, fully consumable from a Spark client (window bounds cast to micros). Without a watermark, continuous aggregation correctly rejects (pipeline-breaking `AggregateExec`). Cosmetic: SQL column is named `window(timestamp, …)` not `window`. |
| Deduplication | ✅ | `StreamDeduplicateNode` (watermark-aware) |
| Union / Repartition / Limit | ✅ | repartition is a no-op in micro-batch |
| Join — stream × static | ✅ | per-micro-batch |
| Join — **stream × stream (stateful/windowed)** | 🟡 | **per-micro-batch only** → cross-batch matches are silently missed; no watermark-bounded state. Biggest correctness gap. |
| Sort | ⬜ | `plan_err` "sort is not supported for streaming" (fails loud — good) |

## Semantics
| Capability | Status | Notes |
|---|---|---|
| Event-time watermarks | ✅ | **marker-based** (`WatermarkExec` emits in-band `FlowMarker::Watermark`; Flink-style) — drives windowed-aggregation eviction; multi-input min-merge is the future hook for stream-stream joins |
| Output mode — append | ✅ | default |
| Output mode — **complete / update** | 🟡 | `output_mode` is intentionally ignored (proto/plan.rs); the sink picks append-vs-upsert. Not driven by query semantics → not Spark-equivalent for complete-mode aggregations. |
| `availableNow` / `once` triggers | ✅ | end-to-end: trigger → `bounded` flag → rewriter → `StreamSourceWrapperNode` → `StreamSource::scan(bounded)`. The **rate** source now scans available rows + `EndOfData` and **the query terminates** (verified: `availableNow` terminates in ~0s vs continuous runs forever). Bounded reads for **Kafka/socket** are the remaining per-source follow-up. |
| `processingTime` / `continuous` interval pacing | ⬜ | trigger captured + logged; interval pacing not yet honored (source-driven). |
| `mapGroupsWithState` / `flatMapGroupsWithState` | ⬜ | arbitrary keyed state not exposed |
| Checkpoint + recovery | 🟡 | **Stateless exactly-once for `availableNow`/`once` (2026-06-10):** source-offset checkpoint/restore via the Spark `MicroBatchExecution` protocol — the source **stages** its end offset write-ahead to `<loc>/sources/N/staged`, the runner **commits** it (atomic rename → `committed`) only after the output is durable, and `scan` **restores** `startOffset` from `committed` on the next run. Verified: two `availableNow` runs (incl. across a real **process restart**) → Parquet output `0..199` **contiguous, no gaps/dupes**. Plus **operator-state snapshot/restore (stateful, 2026-06-10):** `WindowAccumExec` snapshots open-window partials (+ watermark + emitted ends) via Arrow IPC to `<loc>/state/<op>/staged`, the runner commits after durable, restored on the next run — windowed agg across `availableNow` runs accumulates correctly (verified across 4 runs, no loss/crash). **Remaining:** continuous (re-plan loop), Kafka offset commit, `StreamJoinExec` state snapshot (same helper), distributed (carry checkpoint loc through the proto). |
| `dropDuplicatesWithinWatermark` | 🟡 | dedup exists; verify exact Spark semantics |

## Prioritized roadmap to "master of streaming"
Ordered by leverage (correctness/parity first, then breadth):

1. **P0 — Stateful stream–stream joins** (watermark-bounded interval joins). Today a
   stream×stream join only matches rows within the same micro-batch (silently
   incomplete). This is the highest-impact correctness gap. Needs buffered join
   state keyed by join key + watermark-based eviction. *(`rewriter.rs` `Join` arm.)*
2. **P0 — Triggers**: ✅ `availableNow`/`once` implemented end-to-end and verified for
   the **rate** source (bounded flag threaded rewriter → `StreamSourceWrapperNode` →
   `StreamSource::scan` → reader; the query terminates). Remaining: bounded reads for
   **Kafka/socket** (read to current end offsets then stop) and `processingTime` pacing.
3. **P1 — Explicit output modes**: make complete/update first-class (driven by query
   semantics + retraction), not sink-inferred; reject invalid mode/query combinations
   the way Spark does (e.g. append + non-windowed aggregation).
4. **P1 — Kafka sink + console sink**: close the sink matrix (we have the source).
5. **P2 — Arbitrary keyed state** (`flatMapGroupsWithState`) and state TTL.
6. **Throughout — proof**: an end-to-end harness (rate/Kafka → windowed agg + join →
   sink, with a forced restart) asserting **exactly-once / correctness after
   recovery**, plus a streaming throughput/latency benchmark. This is also the
   reliability evidence GA needs (Kafka→Delta 24 h soak).

## How to validate what exists today
`python/pysail/tests/spark/streaming/test_streaming_basic.py` and
`datasource/test_rate.py` exercise the live pipeline (rate source → transform →
sink). Run via the pysail/maturin test harness (CI). Extending these into the P0
correctness + recovery assertions above is the next concrete step.
