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
| File / data source | ✅ | `WriteMode::Append` |
| `foreachBatch` (Python) | ✅ | `ForeachBatchSinkNode`; Scala/Java foreachBatch unsupported |
| `memory` (queryable) | ✅ | `MemorySinkNode` registers an in-process table |
| `foreach` (row writer) | ⬜ | explicitly rejected — use `foreachBatch` |
| `console` | ⬜ | not implemented |

## Operators (`crates/sail-plan/src/streaming/rewriter.rs`)
| Operator | Status | Notes |
|---|---|---|
| Projection / Filter | ✅ | flow-event schema threaded through |
| Stateless window (rank/lag/row_number) | ✅ | per-micro-batch |
| Aggregation — append | ✅ | stateless per-micro-batch |
| Aggregation — **event-time window** | ✅ | stateful `WatermarkNode` + `WindowAccumNode` → `WindowAccumExec` |
| Deduplication | ✅ | `StreamDeduplicateNode` (watermark-aware) |
| Union / Repartition / Limit | ✅ | repartition is a no-op in micro-batch |
| Join — stream × static | ✅ | per-micro-batch |
| Join — **stream × stream (stateful/windowed)** | 🟡 | **per-micro-batch only** → cross-batch matches are silently missed; no watermark-bounded state. Biggest correctness gap. |
| Sort | ⬜ | `plan_err` "sort is not supported for streaming" (fails loud — good) |

## Semantics
| Capability | Status | Notes |
|---|---|---|
| Event-time watermarks | ✅ | drives window aggregation + dedup eviction |
| Output mode — append | ✅ | default |
| Output mode — **complete / update** | 🟡 | `output_mode` is intentionally ignored (proto/plan.rs); the sink picks append-vs-upsert. Not driven by query semantics → not Spark-equivalent for complete-mode aggregations. |
| Triggers (processingTime / once / availableNow / continuous) | 🟡 | trigger is now **captured** end-to-end (`spec::StreamTrigger`, proto→spec→resolver; no longer discarded) and logged. **Bounded** sources (file/table via the source adapter) already drain + `EndOfData` + stop, so `availableNow`/`once` terminate correctly for them. Making **unbounded** sources (rate/Kafka) stop under `availableNow` (per-source bounded reads) + `processingTime` pacing are the remaining runtime work. |
| `mapGroupsWithState` / `flatMapGroupsWithState` | ⬜ | arbitrary keyed state not exposed |
| Checkpoint + recovery | ✅ | offsets/state persisted; recovery on restart |
| `dropDuplicatesWithinWatermark` | 🟡 | dedup exists; verify exact Spark semantics |

## Prioritized roadmap to "master of streaming"
Ordered by leverage (correctness/parity first, then breadth):

1. **P0 — Stateful stream–stream joins** (watermark-bounded interval joins). Today a
   stream×stream join only matches rows within the same micro-batch (silently
   incomplete). This is the highest-impact correctness gap. Needs buffered join
   state keyed by join key + watermark-based eviction. *(`rewriter.rs` `Join` arm.)*
2. **P0 — Triggers**: the trigger is now **modeled + captured** (spec/proto/resolver);
   bounded sources already honor `availableNow`/`once`. Remaining: per-source **bounded
   reads** so unbounded sources (rate/Kafka) also stop under `availableNow`/`once`
   (thread a `bounded` flag rewriter → `StreamSourceWrapperNode` → `StreamSource::scan`
   → reader), plus `processingTime` pacing.
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
