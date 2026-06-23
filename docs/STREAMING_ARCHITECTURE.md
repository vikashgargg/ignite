# Vajra Streaming Architecture (authoritative spec)

**Purpose.** Single source of truth for Vajra's streaming engine: the component
contracts, the full feature matrix, the honest gap register, and the validation gates.
We build to *fill this matrix* — not to chase bugs. Every change must cite the cell it
advances and meet that cell's done-criteria. Grounded in the Apache references, not copied
from them: we take the proven designs and implement them prod-grade for a no-JVM, Arrow-native
engine under the Spark API. See [[feedback_prod_grade_bar]], [[feedback_no_workarounds]].

## 0. North-star correctness contract

From Flink's dynamic-table model: **a continuous query's output must be semantically
equivalent to the same query run in batch on a snapshot of the input.** Every streaming
operator + sink is judged against this. Vajra's differential primitive is
`FlowEvent::Data{ batch, retracted: BooleanArray }` — Flink's *retract stream*
(`retracted=false` = INSERT / UPDATE_AFTER; `retracted=true` = UPDATE_BEFORE / DELETE).

References (consulted, not copied):
- Flink dynamic tables / changelog streams (append vs retract vs upsert; INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE; upsert sinks need a unique key). https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/concepts/dynamic_tables/
- Flink fault tolerance: Chandy-Lamport async barrier snapshotting; alignment ⇒ exactly-once; replayable source + transactional/idempotent sink ⇒ end-to-end EO. https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/learn-flink/fault_tolerance/
- Flink event time / watermarks: `watermark = maxEventTime − boundedOutOfOrderness`; allowedLateness re-fires windows; `sideOutputLateData` keeps too-late records. FLIP-27 (one split→watermark per subtask), FLIP-182 (watermark alignment).
- RisingWave emit-on-window-close vs Flink emit-on-update (changelog). Materialize/differential-dataflow: retract+insert ⇒ eventual convergence, zero loss.
- DataFusion: `ExecutionPlan`/`AggregateMode::{Partial,Final}`/`RowConverter`; physical codec for distributed plans.

## 1. Component map (file ↔ responsibility ↔ contract)

| Component | File | Responsibility | Contract |
|---|---|---|---|
| FlowEvent model | `sail-common-datafusion/src/streaming/event/` | `Data{batch,retracted}` + `Marker(Watermark/Checkpoint/EndOfData/...)`; encode/decode | Markers never overtake their data; retract row == a prior insert row verbatim |
| Kafka source | `sail-data-source/src/formats/kafka/reader.rs` | bounded/realtime/unbounded reads; 1 instance per partition (FLIP-27) | Bounded read reaches captured end offsets (replayable); per-partition event-time order |
| Watermark | `sail-physical-plan/src/streaming/watermark.rs` | emit `Watermark(maxTs−delay)`, monotonic, per-partition | Never regress; delay = bounded out-of-orderness |
| Keyed exchange | `streaming/exchange.rs` | hash-shuffle by key N→M; MIN-merge watermarks at receiver | Same key→same partition; downstream watermark = MIN over inputs (FLIP-182) |
| Barrier align | `streaming/barrier_align.rs` | N→1 Chandy-Lamport barrier alignment | Collect barrier from all N before forwarding one (EO) |
| Window agg | `streaming/window_accum.rs` | event-time window agg; append + update(changelog)+allowedLateness | Append: emit-once-on-close. Update: retract+insert, retain until `end+L≤wm`, zero loss within L |
| Dedup | `streaming/dedup.rs` | keyed dedup with watermark eviction | Exactly-once per key within watermark horizon |
| Stream join | `streaming/stream_join.rs` | interval/equi join, per-side state | Bounded state via watermark; no spurious drops |
| Collector | `streaming/collector.rs` | materialize changelog→table (bounded) | Net by row-identity: insert +1, retract −1; survivors = batch-equivalent result |
| File sink | `sail-data-source` file write + `_spark_metadata` | append-only durable sink + EO commit log | Append-only; cannot represent retractions (see gaps) |
| Kafka sink | `kafka/sink.rs` | EO Kafka producer (txn) | Transactional; upsert mode = key'd changelog (gap) |
| Realtime file sink | `RealtimeFileSinkExec` | per-epoch atomic commit (realtime EO) | One atomic object per epoch (F4) |
| State snapshot | `streaming/state_io.rs` | operator state stage/restore | Write-ahead; commit after output durable |
| Distributed codec | `sail-execution/src/codec.rs` | (de)serialize physical plan across workers | Every exec field round-trips (else local-cluster/distributed diverges) |
| Planner | `sail-session/src/planner.rs` | logical streaming node → physical exec | Preserve all node options onto exec |
| Rewriter | `sail-plan/src/streaming/rewriter.rs` | optimized plan → streaming operators | Thread bounded/checkpoint/realtime/output-mode/lateness |
| Executor | `sail-spark-connect/.../plan_executor.rs` | writeStream spec → streaming run options | Map trigger/outputMode/options to engine flags |

## 2. Feature matrix (operator × output mode × sink × distribution)

Status: ✅ done+validated · 🟡 built, validation pending · ⛔ gap (not built) · — n/a

| Operator | append / file-sink / single | append / distributed | update(changelog) / collector / single | update / kafka-upsert | update / distributed |
|---|---|---|---|---|---|
| Windowed agg | ✅ (EKS vs Flink) | ✅ (EKS local-cluster) | ✅ operator e2e: out-of-order→batch-truth, 0 loss vs append drops (`update_mode_e2e_tests`); pyspark-vs-Spark diff gated on collect-path wiring | ⛔ upsert sink | ✅ codec round-trips update_mode+lateness (`test_round_trip_window_accum_exec`) |
| Non-windowed agg | ✅ complete-mode | 🟡 | 🟡 | ⛔ | ⛔ |
| Dedup | ✅ | ✅ | — | — | 🟡 |
| Stream-stream join | ✅ + F5 spill (buffer→object-store, streamed probe; `inner_join_streaming_probe_emits_all_pairs`, `join_probes_spilled_buffer`) | 🟡 | n/a (append) | — | 🟡 |
| Kafka EO sink | ✅ (realtime EO) | ✅ | — | ⛔ | 🟡 |

## 3. Gap register (severity: P0 blocks "replace both", P1 important, P2 nice)

- ~~P0 — streaming windowed-agg caps at 65536 distinct keys~~ **FIXED 2026-06-22** (commit): root cause
  was `window_emit_mask` marking a window's `end` emitted after the FIRST agg batch, suppressing the
  2nd+ batches of the SAME window in one finalize (a >8192-group window spans multiple batches) ⇒
  8 partitions × 8192 = 65536. Fix: `window_emit_mask` is now PURE; `mark_emitted_ends` records ends
  ONCE after all batches of the finalize. Verified input==output at 70k/200k; regression test
  `append_emits_all_keys_above_batch_size_no_cap`. **This restores Flink-class CORRECTNESS (no silent
  loss). But NOT yet Flink-class for LARGE/LONG state — two gaps remain (below).**
- **P1 — `emitted_ends` grows unbounded** (never pruned; one `i64` per closed window forever). A
  months-long stream with many windows leaks memory. Flink GCs window state past watermark+lateness.
  Fix needs care (pruning too early ⇒ re-emit duplicates on late data — EO-critical): prune only ends
  `< watermark − allowedLateness − window_span`. Tracked, not yet fixed.
- **P0 — windowed-agg state is fully IN-MEMORY** (`pending_rows` + `run_final_aggregate` materializes
  the whole output): a window larger than RAM OOMs. This is F5 — Flink spills keyed state to
  RocksDB/ForSt (object-store + cache + async, REFERENCES §3). The 64k fix did NOT change this; it's
  THE remaining gap for "Flink-class large state". (Original cap note kept below for history.)
- **P0(hist) — streaming windowed-agg silently caps at 65536 (2¹⁶) distinct keys (found 2026-06-22, now fixed above).**
  `scripts/state_scale_stress.py`: streaming event-time windowed COUNT drops every group past 65536
  (input 70k→out 65536; 200k→out 65536; 50k→out 50k ok), while **batch `groupBy(k).count()` on the
  same data = correct (200001)**. Parallelism-independent (`shuffle.partitions=1` also caps) ⇒ the cap
  is in `WindowAccumExec` / its streaming input, NOT the exchange/merge. **Silent data loss at
  cardinality > 64k — Flink handles billions of keys.** This is THE gap vs "prod-grade like Flink".
  Likely a u16/`2^16` group-capacity in the partial/final aggregate or row-format path inside the
  streaming window operator. Owner: `streaming/window_accum.rs`. Must fix before any large-state claim.
  (Compounds with F5: even once uncapped, state is in-memory — no spill — so very large state OOMs.)

- **P0 — throughput**: windowed agg ~2.5× slower than Flink wall (EKS 100M, 2026-06-21:
  Flink 17.4s/8.7GiB vs Vajra **44s**/2.4GiB). LOCALIZED by elimination at EKS scale:
  (a) `from_json` = 3.67M rows/s single-thread (×16 ≫ 2.3M/s aggregate) ⇒ not it;
  (b) `shuffle.partitions=1` (no exchange) = 43.6s ≈ 44s ⇒ exchange/parallelism not it;
  (c) larger batch (128Ki) = worse (44s, 2.4GiB) ⇒ per-batch overhead not it;
  (d) Flink reads 100M in ~10s ⇒ **broker serves ≥10M/s, not the cap**.
  ⇒ **ROOT CAUSE: the Kafka *consumer* read path** (~2.3M/s vs Flink ~10M/s) — per-message
  `to_vec` allocs (key+value) + `KafkaRow` + `build_batch` + rdkafka `StreamConsumer::next()`
  per-message. **FIX IMPLEMENTED + LOCALLY VALIDATED (2026-06-21): 1.5M/s → 3.22M/s (2.1×)** via
  (1) `KafkaArrowBuilders` — append message bytes straight into Arrow builders, no `to_vec`/`KafkaRow`;
  (2) `apply_consumer_throughput_defaults` — rdkafka prefetch/fetch tuning (the dominant lever: the
  local 1.5M/s was *fetch-config*-limited, not broker-capped — default `fetch.wait.max.ms=500` idled).
  `KAFKA_BENCH` micro-bench on local 10M. **EKS-CONFIRMED 2026-06-22: 44s → 19.72s** (2.2×,
  throughput 2.27→5.07M ev/s) on 100M — now **near-parity with Flink (17.4s)**, was 2.5× behind.
  All 3 read paths (bounded/realtime/unbounded) share `KafkaArrowBuilders`. Tradeoff: peak mem
  2.4→6.0 GiB from the 1 GiB×16 prefetch buffers (still < Flink's 8.7 GiB; `kafka.queued.max.messages.kbytes`
  tunes it down). Correctness unchanged (every group 10000, 0 loss). Remaining ~13% wall gap to Flink
  = window/sink compute (next lever, optional). Owner: `kafka/reader.rs`. **Largely RESOLVED.**
- ~~P0 — update-mode distributed codec~~ **RESOLVED 2026-06-21**: `update_mode`/`allowed_lateness`
  round-trip in `codec.rs` (proto fields 6/7 + `test_round_trip_window_accum_exec`). Update mode now
  survives local-cluster/distributed planning.
- **P1 — upsert/changelog sinks**: file sink is append-only; update mode needs upsert-kafka /
  Delta-merge / collector(done) to land retractions. Blocks update@external-sink.
- **P1 — late side output**: records later than `wm−L` are dropped (like append), not routed to a
  `_late/` side output (Flink `sideOutputLateData`).
- **P1 — changelog state checkpoint**: `last_emitted` (update mode) not snapshotted on EndOfData;
  cross-run update recovery incomplete (append partial-state recovery is fine).
- **P2 — complete mode on windows**: only append/update specialized; `complete` falls back to append.

## 4. Validation gates (every feature must pass its tier before "done")

1. **Unit** (in-crate, no I/O): operator logic, e.g. `window_accum::update_mode_tests` proves
   retract+insert convergence; `collector` netting test. Fast, runs in CI.
2. **Local Spark-diff** (`scripts/diff_test`, local Kafka in docker): same query on Vajra vs real
   Spark 3.5.3; assert bit-equal (append) or converged-equal (update). No cloud cost.
3. **EKS vs Flink** (bundled, paid): 100M head-to-head — correctness (per-group exact, 0 loss),
   memory, throughput vs official Flink 1.19. One spend per milestone, torn down to $0 after.

A cell is ✅ only when its tier-3 (or tier-2 for cloud-irrelevant features) gate is green and the
result is recorded in [[project_streaming_vs_flink_eks2]].

## 5. Prod-grade bar (per change)

- Cites the matrix cell it advances + meets that cell's done-criteria.
- Grounded in a named reference section above (not invented).
- No regression: append path stays bit-identical (default), full suite + clippy `-D warnings` green.
- Honest labeling: any MVP names its gap in §3 with severity.
- Distributed-aware: if it adds an exec field, it round-trips in `codec.rs` (or is explicitly
  single-node-only with a P0/P1 gap logged).
