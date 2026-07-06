# Vajra — Spark-parity gap list + DataFusion/Arrow upgrade plan (STANDING, updated 2026-07-04)

The maintained list of what remains to make Vajra a **true drop-in Spark replacement** ([charter](../../MEMORY.md)),
plus the safe path to adopt the latest DataFusion/Arrow via LakeSail v0.6.5 as a proven reference. Work this
architect-first, T1→T2→T3 ([three-tier-sdlc.md](three-tier-sdlc.md)); update this doc as items land.

## 1. Current state (2026-07-04)

- **Versions:** DataFusion **54.0.0**, Arrow **58.3.0** (`Cargo.toml`) — upgraded 2026-07-06, T2-validated. (Note: Arrow-rs is at 58.x — "Arrow
  25" was a version mix-up; the real target is 58.3.0.)
- **Streaming (just landed, merged to main cfae68f1):** crash-EO exactly-once (aligned barriers + exact
  PartitionEOF idle + emit floor) EKS-confirmed; final-window completeness (opt-in `VAJRA_COMPLETE_ON_END`,
  Flink scan.bounded.mode parity); **parallel Kafka sink** (fixed a 15/16 data-loss bug + ~300× throughput,
  100M/100M @ 1.67M msg/s on EKS). All T1→T2→T3 validated. 3-tier SDLC + kind tier established.
- **SQL compat:** 105/105 scorecard; TPC-H SF-1 ~36× vs Spark; TPC-DS 97/99. Batch-on-S3 6.2× vs Spark.

## 2. DataFusion 53.1 → 54.0 + Arrow 58.1 → 58.3 upgrade (reference: LakeSail v0.6.5)

LakeSail (our upstream fork base) shipped v0.6.5 on **DataFusion 54.0.0 + Arrow 58.3.0** — a PROVEN, stable
combination, which de-risks our upgrade. Plan:
- **Arrow 58.1 → 58.3** — trivial (same major, patch bump). Bump the `arrow*` + `serde_arrow`/`arrow-58`
  pins in `Cargo.toml`; expect ~zero API breakage. Do FIRST (isolate any Arrow-only fallout).
- **DataFusion 53.1 → 54.0** — one major. Bump all `datafusion*` pins to 54.0.0. Expect API churn
  (physical-plan/expr trait signatures, `AggregateExec`/`WindowAgg` APIs the streaming execs use, proto/codec
  helpers). Diff LakeSail v0.6.4→v0.6.5 for the exact call-site migrations (they already did it). Our
  streaming operators (`window_accum`, `exchange`, `barrier_align`, kafka `reader`/`sink`, `codec.rs`) are the
  highest-risk surfaces — they subclass DataFusion `ExecutionPlan`/`AggregateExec`.
- **Gate:** `cargo clippy --all-targets -D warnings` + `correctness_gate` GREEN 6/6 + `inc_ckpt_gate` crash
  dup=0 + TPC-H/TPC-DS scorecard unchanged. Then T2 kind + one T3 EKS smoke. **No behavior change** is the bar.
- **Sequencing:** Arrow patch first (own PR) → DataFusion major (own PR) → then adopt features (below).
- **Migration surface (scoped 2026-07-05, branch `upgrade/datafusion54-arrow583`):** Arrow 58.3 DONE (green).
  DataFusion 54 bump surfaced the FIRST break on resolve: **`datafusion-common` 54.0.0 dropped the `avro`
  feature** (Avro moved to the `arrow-avro` crate — 54.0.0 blog). Fix: drop `avro` from the
  `datafusion-common` pin (Cargo.toml ~L171) + wire Avro via `arrow-avro` where used. Then rebuild to surface
  the next breaks (expected: `ExecutionPlan`/`AggregateExec`/window-exec trait sigs used by
  `window_accum`/`exchange`/kafka `sink`, and `datafusion-proto`/`codec.rs` helpers). **Systematic approach:
  `git diff` LakeSail v0.6.4→v0.6.5** (they already migrated 53→54) to get the exact call-site changes rather
  than discovering each on rebuild. Gate: clippy -D warnings + correctness_gate 6/6 + inc_ckpt crash dup=0 +
  TPC-H/TPC-DS scorecard unchanged (NO behavior change), then T2 kind + one T3 EKS smoke. Best as a focused
  dedicated cycle (multi-point migration; do NOT interleave with unrelated work).

## 2b. What DataFusion 54.0.0 buys us — batch (Spark) + streaming (from the official 54.0.0 blog)

**Two different Arrows:** `arrow-rs` (Rust, v58.x — what our build compiles; bumped to 58.3.0) vs
`apache/arrow` (C++, **v25.0.0** = milestone 74, Q3 2026 — matters only for **pyarrow interop** via
`arrow-pyarrow`, NOT the Rust engine). So the engine upgrade = arrow-rs 58.3 + DataFusion 54.

**Batch / Spark wins (free on upgrade):**
- **Sort-merge join**: per-row bitset for semi/anti/mark joins + batched deferred filtering = **20–50× faster**
  near-unique LEFT/FULL joins; `DynComparator` = **~5% faster TPC-H** overall. (Directly lifts TPC-H/TPC-DS.)
- **`RepartitionExec` coalesces batches before distributing = up to 50% faster** on repartition-heavy
  workloads. (Our shuffle/exchange path — TPC-H/TPC-DS shuffles.)
- **Parquet scan morsel-driven parallelism = up to ~2× on skewed scans (ClickBench).** (We match LakeSail on
  ClickBench today — this could pull ahead.)
- Hashing `ahash → foldhash` (faster group-by/join keys); `first_value`/`last_value` GroupsAccumulator
  speedups; redundant sort-key pruning; statistics-driven file/row-group ordering; struct-field pushdown into
  the Parquet decoder; NestedLoopJoin **spilling** (memory robustness).
- New Spark-parity SQL: **LATERAL joins**, **lambda functions** (`x -> expr`) + higher-order array UDFs
  (`array_transform`/`array_filter`/`array_any_match`) — overlaps LakeSail v0.6.5 (§3), reduces our reimpl.

**Streaming optimization opportunities (the throughput lever vs Flink):**
- **`RepartitionExec` batch-coalescing** is the same idea our `StreamExchangeExec` needs — the streaming
  exchange emits tiny per-flush batches (the realtime throughput cost). Port the coalesce-before-distribute
  pattern into the streaming exchange → fewer, bigger batches downstream = higher throughput at the same
  latency bound. **Candidate fix for the realtime throughput gap.**
- **`foldhash`** speeds the keyed-exchange + windowed-agg group-by hashing (our hot path at 5.5M ev/s).
- **GroupsAccumulator `first_value`/`last_value`** speedups apply to `WindowAccumExec`'s aggregation.
- **Parquet content-defined chunking (CDC)** — page boundaries aligned to data → better dedup/incremental
  storage: directly useful for the **streaming Parquet/S3 sink + inc-ckpt** (O(delta) chunks).
- Extension-type registry + vector ops (`cosine_distance`/`inner_product`) → AI-native lakehouse (charter).

## 2c. DataFusion 54 migration — progress + precise remaining map (branch upgrade/datafusion54-arrow583)

**DONE (committed 94253edb):** Arrow 58.3 (green); Cargo pins 53.1→54.0 + `avro` dropped from
datafusion-common (`arrow-avro` auto-added); **sail-common-datafusion** + **sail-cache** fully migrated
(`TableScopedPath` re-key + new `cache_limit`/`update_cache_limit`/`drop_table_entries`, ported from LakeSail
v0.6.5); **ALL 254 `as_any` trait-impl overrides removed** (DF54 made `Any` a supertrait; downcasting now via
inherent `dyn X::downcast_ref::<T>()`); **13 `.as_any()` call-sites → `.downcast_ref`**.

**WORKSPACE LIB COMPILES GREEN ON DF54** (`cargo build --workspace` = 0 errors). Landed the full remaining map:
- *Mechanical (done):* `partition_statistics -> Result<Arc<Statistics>>` (delta relaxed_tz/scan_by_adds + 6 in
  sail-physical-plan: merge_cardinality_check, monotonic_id, repartition, spark_partition_id, streaming
  filter/limit — deref-clone `Arc` where the body mutates or calls `with_fetch`); `PartitionedFile.table_reference:
  None` + `extensions: Default::default()` (DF54 changed `extensions` from `Option<Arc<dyn Any>>` to
  `FileExtensions`); `CastColumnExpr → CastExpr::new_with_target_field` (iceberg expr_adapter, mirroring DF's own
  `schema_rewriter`).
- *Semantic (done):* logical `Cast`/`TryCast` construction → `Cast::new(expr, data_type)` (still takes `DataType`,
  converts to `field: FieldRef` internally) across sail-function (3) + sail-plan (window/aggregate/misc/time_travel/
  read/stat) + delta metadata_predicate destructure `Cast { expr, field }` using `field.data_type()`; reading
  `cast.data_type` → `cast.field.data_type()` (values.rs); `PruningStatistics::row_counts(&self)` arg-drop —
  delta impl reworked to **container-level** (per-Add `num_records`, column-independent, removed dead per-column
  `row_counts` storage), iceberg + snapshot/stats trivially dropped the arg.
- *DF54 API shifts caught:* `ScalarValue::ListView`/`LargeListView` new variants (formatter match arms);
  `Ident`/`ObjectName` moved to `datafusion_expr::sql` and `ExcludeSelectItem` now holds `ObjectName` not `Ident`
  (wildcard.rs); `TaskContext::new` gained `higher_order_functions` (window_accum); `CacheManagerConfig::
  with_files_statistics_cache → with_file_statistics_cache`; **167 `.as_any()` call-sites removed** (DF54 inherent
  downcast on `dyn ExecutionPlan`/`TableProvider`/`DataSource`/`DataSink`/`FileSource`/`PhysicalExpr`/UDFs — driven
  by exact compiler spans so arrow-array `.as_any()` was never touched) + **177 now-unused `use std::any::Any`**
  imports cleaned.
- **sail-execution/codec.rs (the distributed round-trip — highest care):** DF54 merged `&TaskContext` + `&dyn
  PhysicalExtensionCodec` into **`PhysicalPlanDecodeContext::new(task_ctx, codec)`** for every decode
  (`parse_protobuf_file_scan_config` / `parse_physical_sort_exprs` / `parse_protobuf_partitioning`); every
  serialize (`serialize_file_scan_config` / `serialize_physical_sort_exprs`) now takes `(items, codec,
  proto_converter)` — re-threaded `self` (the codec) at both sides.
- **DONE + VALIDATED (2026-07-06):** `clippy --all-targets -D warnings` green; `cargo test --workspace` **860/0**;
  gold data byte-identical to DF53; `inc_ckpt` crash-EO **dup=0 PASS**. **CRITICAL DF54 BUG root-caused + fixed:**
  the morsel-driven scan pools all files into a shared work-source for in-process sibling work-stealing, so an
  isolated distributed task drained the whole pool → **every file read once per partition (silent N× row
  duplication)**. Fix = per-task plan rewrite sets `partitioned_by_file_group=true` (DF54's own opt-out →
  `WorkSource::Local(file_groups[partition])`); see `docs/REFERENCES.md`. **T2 kind (real k8s, image built on a
  throwaway c7g EC2 → ECR, AWS $0): streaming n_windows=5 / sum=5M EXACT + Kafka-sink 2M/2M delivered.** Adopted
  DF54's new optimizer rules (WindowTopN/TopKRepartition/HashJoinBuffering) + coercion improvements. Merged to
  `main`. FOLLOW-UP: all-in-one DF54 metric-gain run (RepartitionExec batch-coalesce, sort-merge join, morsel
  scan, foldhash) on EC2/EKS — local release benchmark blocked by this laptop's LTO swap-thrashing.

## 3. LakeSail v0.6.5 features to adopt (mapped to Spark parity)

Each is a Spark-compat win; cherry-pick from LakeSail v0.6.5 (same fork lineage) or reimplement to our bar.

**AUDIT 2026-07-06 (codebase grep):** MOST already adopted via fork lineage + our work — PIVOT, lambda higher-order (filter/transform/exists/forall/array_sort), window_time, to_xml, schema_of_json, percentile_disc, HLL/theta sketches, variant (base), Delta identity cols/check constraints/default values, to_csv, try_parse_json, file write modes, DF54/Arrow58.3. **REMAINING GAPS (5): (1) lambda AGGREGATES (`reduce`/`aggregate` higher-order agg); (2) catalog-managed Delta/Iceberg tables + catalog exec context (charter unified-catalog — HIGH value); (3) Variant SHREDDING for Delta/Iceberg; (4) `grouping_id` aggregate (GROUPING SETS/CUBE/ROLLUP); (5) `timestampadd` fn.** Cherry-pick from LakeSail v0.6.5. Priority: (2) catalog > (1) lambda-agg > (4) grouping_id > (3) shredding > (5) timestampadd. NOTE the bigger "beat Flink" lever is the DF54 RepartitionExec batch-coalesce ported into StreamExchangeExec (realtime throughput gap), independent of these.
- **SQL:** `PIVOT` operator (rewrite → aggregate with per-value FILTER); **named window clauses**; lambda
  expressions `filter`/`transform`/`exists`/`forall`/`array_sort` + **lambda aggregates** (big Spark
  higher-order-function gap); `window_time` + more window fns.
- **Functions:** `to_xml`, enhanced `schema_of_json`, unified `to_timestamp`/`try_to_timestamp` (ANSI),
  `try_to_time`/`to_time` (SparkTime), `percentile_disc` (ANSI), `timestampadd`.
- **Catalog/lakehouse:** **catalog-managed Delta Lake + Iceberg tables** + catalog execution context (moves us
  toward a real unified catalog — charter "unified storage abstraction"); Windows local paths for Iceberg.
- **Writes:** additional file-sink modes; **Parquet content-defined chunking** (dedup-friendly writes).
- **Semantics:** `EXPLAIN EXTENDED`/`COST` aligned to Spark; `get_json_object` bracket paths;
  `array_position`/`array_sort` fixes.

## 4. "True Spark replacement" gap list (the maintained todo)

**Streaming (Flink-class):**
- [x] crash-EO exactly-once at scale (EKS-confirmed)
- [x] final-window completeness (bounded-complete flush)
- [x] parallel Kafka sink (throughput + no data loss)
- [ ] **EO + parallel Kafka sink**: per-task transactional offset commit (each sink task commits its
      partition's offsets) — the at-least-once path is done; the EXACTLY-once transactional path with N
      parallel producers needs per-partition offset handling.
- [ ] realtime **latency** vs Flink measured clean (the earlier passthrough number was skewed by the 1/16
      sink bug — now fixed; re-measure p50/p99/p999 vs Flink on EKS)
- [ ] stateful **stream-stream joins**; multiple explicit output modes (complete/update) hardened; CEP
- [ ] 24 h **soak** (Kafka→Delta) without OOM/restart; chaos/endurance

**Batch / SQL parity:**
- [ ] LakeSail v0.6.5 SQL features above (PIVOT, lambdas, named windows, functions)
- [ ] TPC-DS Q5/Q9 compat gaps (97/99 → 99/99)
- [ ] Official Apache Spark test suite ≥ 95% sustained on all 3 deploy modes

**Lakehouse / catalog:**
- [ ] catalog-managed Delta + Iceberg (from v0.6.5); streaming Iceberg sink; batch Iceberg vs Spark

**Ops / scale (production-first):**
- [ ] TPC-H SF-100 distributed < 60s (10-node K8s)
- [ ] autoscaling / elasticity; rescale-from-checkpoint on EKS (mechanism done locally)
- [ ] observability (metrics/traces) + Grafana; zero-downtime upgrade; multi-region
- [ ] `pip install vajra-pyspark` one-liner works unchanged

**Platform upgrade:**
- [ ] DataFusion 54.0 + Arrow 58.3 (§2)

## 5. Sequencing (architect-first, per the charter)
1. **Arrow 58.3** (safe, isolate) → **DataFusion 54.0** (major; diff LakeSail v0.6.4→v0.6.5) — gate: no
   behavior change, all gates green.
2. **Adopt v0.6.5 SQL features** (PIVOT, lambdas, named windows, functions) — each with a scorecard test.
3. **EO parallel-sink offset commit** + **clean realtime-latency-vs-Flink** re-measure.
4. **Catalog-managed Delta/Iceberg** + streaming Iceberg sink.
5. **Soak/chaos** + **SF-100 distributed** + observability.
