# Vajra Streaming — capability audit & "master of streaming" roadmap

Vajra's biggest differentiation vs LakeSail (which ships **no** Structured Streaming)
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
| File / data source (parquet/csv listing) | ⬜ | rejected: `cannot write streaming data to listing table` (`listing/source.rs:266`) — the listing sink gets a flow-event-schema input it can't write. Needs an incremental write path: decode flow events → per-micro-batch file writes + checkpoint commit. |
| Delta / Iceberg streaming sink | ⬜ | the production-relevant file sink (transactional); not yet wired for streaming input. |
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
| Checkpoint + recovery | ✅ | offsets/state persisted; recovery on restart |
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
