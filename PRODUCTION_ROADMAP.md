# Zelox — Production Spark Replacement Roadmap

> Last updated: 2026-07-04
> Goal: **True drop-in Apache Spark replacement** (+ Flink-class streaming).
>
> **➡️ THE maintained gap list + the DataFusion 54 / Arrow 58.3 upgrade plan + LakeSail v0.6.5 feature
> adoption now live in [docs/design/spark-parity-and-upgrade-plan.md](docs/design/spark-parity-and-upgrade-plan.md)** — read that first; this file is the older sprint-level roadmap.
>
> **Current state (2026-07-04):** streaming milestone landed on `main` — crash-EO exactly-once (EKS-confirmed),
> final-window completeness (`ZELOX_COMPLETE_ON_END`, Flink `scan.bounded.mode` parity), and a **parallel Kafka
> sink** (fixed a 15/16 data-loss bug + ~300× throughput; 100M/100M @ 1.67M msg/s on EKS). The **3-tier SDLC**
> (T1 local → T2 `kind` → T3 EKS) + [kind tier](k8s/kind/) are established. Versions: DataFusion 54.0.0 (upgraded 2026-07-06),
> Arrow 58.3.0 (DF54 upgrade COMPLETE + T2-kind validated 2026-07-06; morsel-scan distributed fix).

---

## Definition of Done — "True Spark Replacement"

A user can `pip install zelox-pyspark`, point their existing PySpark code at Zelox, and it works. Specifically:

- [ ] 105/105 scorecard on all three deployment modes (already done ✅)
- [x] VARIANT / parse_json / variant_get (Spark 4.x semi-structured type)
- [x] GroupedMap / CoGroupedMap / applyInPandas UDFs (Spark 4.1)
- [x] Delta time travel (AT VERSION / AT TIMESTAMP)
- [x] Delta V2 checkpointing — production tables compact correctly
- [x] Iceberg V3 spec + REST catalog
- [x] Official Apache Spark test suite ≥ 95% pass rate
- [ ] TPC-H SF-100 distributed < 60s (10-node K8s cluster)
- [ ] Kafka → Delta pipeline runs 24 h without OOM or restart
- [ ] Apple Container cluster: `make container-run-cluster` → same score as K8s
- [ ] `pip install zelox-pyspark && python -c "from zelox_pyspark import ZeloxSession; s=ZeloxSession.local(); s.sql('SELECT 1').show()"`

---

## Current Baseline (2026-05-27)

| Metric | Value |
|---|---|
| SQL compat scorecard | **105/105 (100%)** — local / local-cluster / kubernetes-cluster |
| TPC-H SF-1 (22 queries) | **22/22 PASS, 1.515s total (40× faster than Spark JVM warm)** |
| Phase 1 (SQL parity) | ✅ Complete |
| Phase 2 (streaming + auth + HA + infra) | ✅ Complete |
| Phase 3 (Spark 4.x + true parity) | 🔄 In progress (branch: `phase3/true-parity`) |

---

## Competitive Position vs LakeSail v0.6.3 (2026-05-27)

### Where we lead

| Feature | Zelox | LakeSail |
|---|---|---|
| Spark compat scorecard | **100% (105/105)** | ~95% |
| Kafka streaming source | ✅ rdkafka | ❌ issue #1969 open |
| foreachBatch | ✅ | ❌ |
| memory sink | ✅ | ❌ |
| Streaming checkpoint + recovery | ✅ | ❌ |
| JWT bearer auth | ✅ | ❌ |
| mTLS | ✅ | ❌ |
| Apple Container (macOS 26, arm64) | ✅ only one | ❌ |
| K8s Helm chart + HPA | ✅ | ❌ |
| Scheduler HA (K8s Lease) | ✅ | ❌ |
| Web UI on :4040 | ✅ | ❌ |
| TPC-H SF-1 speed vs Spark | **1.78s (~36× vs Spark)** | ~15s — ~parity (shared DataFusion core) |
| TPC-H SF-100 vs Spark (EKS, measured) | **347s / 51.7 GiB (~3.2× faster, ~2.2× less RAM)** | not published per-scale |
| Binary size | **80 MB Linux / 105 MB macOS** | ~300 MB |
| Cold start | **~200 ms** | ~2 s |

> Zelox is forked from `lakehq/sail` → shared analytical core → query perf vs
> Spark is ~parity with LakeSail; we don't claim "faster than LakeSail." Honest
> per-scale read: [docs/benchmarks/COMPETITIVE.md](docs/benchmarks/COMPETITIVE.md).

### Where LakeSail leads (our Sprint 4-5 targets)

| Feature | Their version | Our sprint |
|---|---|---|
| VARIANT type (Spark 4.x) | v0.6.3 ✅ | **Sprint 4** |
| GroupedMap/CoGroupedMap UDFs (Spark 4.1) | v0.6.3 ✅ | **Sprint 4** |
| variant_explode / to_variant_object | v0.6.3 ✅ | **Sprint 4** |
| Delta time travel (AT VERSION / TIMESTAMP) | v0.6.0 ✅ | **Sprint 4** |
| Delta V2 checkpointing + log compaction | v0.6.0 ✅ | **Sprint 4** |
| Delta type widening | v0.6.3 ✅ | Already in codebase ✅ |
| Iceberg V3 spec | v0.6.3 ✅ | **Sprint 4** |
| ClickBench 43-query benchmark | v0.6.3 ✅ | **Sprint 4** |
| dbt integration guide | v0.6.3 ✅ | **Sprint 4** |
| bitmap_and_agg | v0.6.2 ✅ | **Sprint 4** |
| HMS table metadata | v0.6.3 ✅ | **Sprint 5** |
| Provider-agnostic catalog caching | v0.6.3 ✅ | **Sprint 5** |
| Vortex data source | v0.6.0 ✅ | **Sprint 5** |
| Official Spark test suite | partial | **Sprint 5** |
| TPC-H SF-100 distributed | unknown | **Sprint 5** |

---

## Sprint 4 — Spark 4.x Feature Parity (2026-05-27 → 2026-06-07)

### 4.1 VARIANT Type  `[x]` P0 · ~3 days

The `VARIANT` semi-structured type is Spark 4.0's biggest new type. Required for any Spark 4.x workload.

**Functions to implement:** `parse_json`, `try_parse_json`, `is_variant_null`, `variant_get`, `try_variant_get`, `variant_explode`, `variant_explode_outer`, `to_variant_object`, `schema_of_variant_agg`.

**Files:**
- `crates/zelox-plan/src/resolver/data_type.rs` — `DataType::Variant` → internal Struct{value:Binary, metadata:Binary}
- `crates/zelox-plan/src/function/scalar/variant.rs` — all variant functions
- SQL parser — `VARIANT` keyword in type grammar

**Test:**
```python
spark.sql("SELECT parse_json('{\"k\":1}')").printSchema()
spark.sql("SELECT variant_get(parse_json('{\"a\":42}'), '$.a', 'INT')").collect()
# → [Row(42)]
```

---

### 4.2 Delta Time Travel  `[x]` P0 · ~2 days

`SELECT * FROM t VERSION AS OF 5` and `TIMESTAMP AS OF '2024-01-01'`.

**Files:**
- `crates/zelox-plan/src/resolver/` — detect `FOR SYSTEM_VERSION AS OF` / `AT VERSION` / `AT TIMESTAMP` on table scan
- `crates/zelox-delta-lake/src/table_format.rs` — pass version/timestamp to `open_table_with_object_store_and_table_config_at_version`
- DeltaReadOptions — add `version: Option<i64>`, `timestamp: Option<i64>`

**Test:**
```python
spark.sql("CREATE TABLE t USING delta LOCATION '/tmp/t' AS SELECT 1 AS v")
spark.sql("INSERT INTO t VALUES (2)")
assert spark.sql("SELECT * FROM t VERSION AS OF 0").collect() == [Row(v=1)]
```

---

### 4.3 GroupedMap / applyInPandas (Spark 4.1)  `[x]` P0 · ~3 days

`df.groupBy("k").applyInPandas(fn, schema)` — each group lands as a Pandas DataFrame in Python.

**Files:**
- `crates/zelox-spark-connect/src/proto/plan.rs` — handle `ApplyInPandas` / `CoGroupMap` plan nodes
- `crates/zelox-python-udf/src/udf/pyspark_group_map_udf.rs` — already has skeleton, wire execution
- `crates/zelox-plan/src/resolver/query/udf.rs` — GroupedMapUDF logical node

**Test:**
```python
def normalize(key, pdf):
    pdf["v"] = pdf["v"] / pdf["v"].mean()
    return pdf
df = spark.createDataFrame([(1, 2.0), (1, 3.0)], ["k", "v"])
df.groupBy("k").applyInPandas(normalize, schema="k long, v double").show()
```

---

### 4.4 Delta V2 Checkpointing  `[x]` P1 · ~2 days

Delta V2 checkpoint (multi-part Parquet sidecars) prevents thousands of JSON log files from accumulating. Critical for production tables.

**Files:**
- `crates/zelox-delta-lake/src/kernel/checkpoints.rs` — write V2 format (already partially done)
- `crates/zelox-delta-lake/src/kernel/checkpoint_augment.rs` — sidecar metadata
- Trigger: compact when `_delta_log/` has > 10 JSON files since last checkpoint

---

### 4.5 Iceberg V3 + OverwriteIf  `[x]` P1 · ~2 days

**V3 spec:** position delete files, new stats encoding, improved row-level deletes.

**OverwriteIf for Iceberg:** same pattern as Delta fix — route to overwrite plan instead of `not_impl_err!`.

**Files:**
- `crates/zelox-iceberg/src/table_format.rs` — remove `not_impl_err!` for `OverwriteIf` / `OverwritePartitions`
- `crates/zelox-iceberg/src/spec/` — V3 format changes
- REST catalog: improve sort transform parsing

---

### 4.6 ClickBench Benchmark  `[x]` P1 · ~1 day

43 OLAP queries on a 100M-row web analytics dataset. LakeSail shipped results in v0.6.3.

**Steps:**
1. `scripts/clickbench.py` — download Parquet from ClickHouse S3, run 43 queries, emit timing JSON
2. Add `BENCHMARKS.md` with results vs LakeSail and Spark
3. Target: run all 43 correctly, beat LakeSail total time

---

### 4.7 bitmap_and_agg + variant_explode + to_csv improvements  `[x]` P1 · ~1 day

- `bitmap_and_agg` / `bitmap_or_agg` / `bitmap_count` — Apache DataSketches HLL-compatible
- `variant_explode` / `variant_explode_outer` — depend on 4.1 VARIANT type
- `to_csv` improvements — delimiter, null value, quote handling parity

---

### 4.8 dbt Integration Guide  `[x]` P2 · ~4 hours

Test `dbt-spark` connector against Zelox. Write `docs/integrations/dbt.md`.

LakeSail has this — it's an important adoption channel.

---

## Sprint 4.2 — Trust + Perf Proof (2026-06-03 → )

The honest gating items between "feature-complete" and "true production Spark
replacement." LakeSail's latest remains **v0.6.3 (2026-05-21)** — no new release.
Zelox leads on the operational axis (streaming, JWT/mTLS, K8s HA, Apple Container,
Web UI); rough SQL/lakehouse parity; the open gap is **proven scale performance**.

### Done this sprint
- [x] **Merged to `main`** (2026-06-04, `fc6ec9e2`) after the local scorecard gate.
  Multi-mode verification with the fresh release binary: `local` **105/105**,
  `local-cluster` (4-worker distributed) **105/105**, **Apple Container** (fresh image,
  `ZELOX_MODE=local-cluster`, 4 workers) **105/105**, **K8s (kind, kubernetes-cluster mode,
  driver spawns worker pods)** **105/105**. Fixes landed: 5 GB Apple builder VM (default 2 GB
  OOM'd `hive_metastore`); `docker/Dockerfile` Rust `1.86→1.95` + `ARG CARGO_JOBS`
  (`71e9bdf2`); scorecard `SCORECARD_REMOTE_TMP` for the K8s `/tmp/zelox` mount + the two
  `_metadata` tests moved to the shared `tmp` root. **All four modes 105/105.**
- [x] **Workspace clippy lane green** — `cargo clippy --all-targets --all-features -- -D warnings`
  exits 0 for the first time (commit `90f69f22`). Complied with the strict lints
  (test modules use `#[expect(...)]`) rather than loosening `clippy.toml`, matching
  upstream LakeSail/DataFusion. 302 unit tests pass.
- [x] **Delta declared-nullability fix** (commit `2d1147d6`) — metaData now records the
  declared catalog column nullability (was inheriting non-null from the insert plan).
  Threaded declared schema resolver→sink→Delta writer. Delta feature suite 134→144,
  column-mapping 4/4, zero regressions. MERGE metric logic confirmed already correct.

### Remaining — trust
- [x] **Differential trust harness broadened 37→124 workloads** (commits `e5c23806`,
  `a4704d4a`, `b4088b78`, …) — byte-for-byte vs Apache Spark 3.5.3, **124/124 match**.
  Surfaced and **fixed four real Spark issues**: `log(x)` 1-arg natural log; `array_position`
  → BIGINT (was decimal(20,0)); `get_json_object` array-index JSONPath `$.arr[1]` (was NULL);
  **`to_utc_timestamp`/`from_utc_timestamp` were converting to/from the session timezone
  instead of UTC** (off by the session-tz offset) — now session-tz-independent. Remaining
  documented value-correct type diffs (allowlisted): `round`/`bround`/`percentile_approx`
  precision-or-type, `array_position`→fixed, `array_transform_index`/`width_bucket` int-vs-
  bigint. The `differential-spark` CI job fails on any divergence.
- [ ] **Make `differential-spark` a required check** `P0` — branch-protection setting on
  `main` (repo settings) so no merge can silently diverge from Spark. Then keep growing
  workloads toward the full function surface.
- [ ] **Full CI lane green end-to-end** `P0` — all jobs (clippy ✅, fmt, test, build-linux,
  distributed-scorecard, k8s/macos-scorecard, differential-spark ✅).
- [ ] **Delta byte-size snapshot refresh** `P1` — ~10 `@zelox-only` operation_metrics/merge
  snapshots fail only on physical Parquet byte sizes (all semantic counters already match
  Spark). Regenerate from Zelox's deterministic output, or match upstream parquet encoding.
- [ ] **Delta EXPLAIN plan-shape + MERGE-source nullability** `P2` — remaining ~few Delta
  feature failures (plan-string snapshots; VALUES/temp-view source nullability in the plan dump).
- [ ] **Official Apache Spark test-suite breadth** `P1` — extend beyond the 105/105 scorecard
  for a defensible parity number (see 5.1).

### Remaining — the moat (Pillar 2, highest leverage)
The single biggest credibility gap. We claim 5–10x but lack a published real-scale result.

- [x] **Benchmark harness repaired + validated** (commit `8a7a1af7`) — fixed two bugs
  (Arrow ChunkedArray conversion in `zelox bench`; f-string SyntaxError in
  `scripts/tpch_distributed.py`). Canonical path is client→server:
  `SPARK_REMOTE=sc://localhost:50051 TPCH_SF=1 python scripts/tpch_distributed.py`
  → **22/22 TPC-H pass** (debug build, cold, single-node).
- [x] **Release + warm + vs-Spark single-node number** — published head-to-head in
  [docs/benchmarks/TPCH_SF1.md](docs/benchmarks/TPCH_SF1.md): TPC-H SF-1, 22/22 both engines,
  identical Parquet + queries, same machine, both warm, `local[4]`. **Zelox 1.780s vs
  Apache Spark 3.5.3 63.463s → ~36×.** Reproducible via `scripts/tpch_distributed.py`
  (now dual-engine: remote=Zelox, `local[*]`=reference Spark).
- [x] **Distributed at 100M-row scale on real AWS EKS** `P0` — full **ClickBench (100M rows,
  13.7 GB)** run distributed on a 3-node Graviton **spot** EKS cluster (`ZELOX_MODE=
  kubernetes-cluster`, driver spawned worker pods, data from **S3**): **43/43 passed,
  377.9s.** Proves distributed + S3 object store + real K8s at scale. Whole run cost **~$1**,
  torn down to **$0**. Toolkit: `k8s/eks/`, `scripts/aws_eks_teardown.sh`,
  [docs/SCALE_TESTING.md](docs/SCALE_TESTING.md).
- [x] **TPC-H SF-100 (100 GB) time + memory head-to-head vs Spark** — same 128 GB node,
  same data ([docs/benchmarks/TPCH_SF100.md](docs/benchmarks/TPCH_SF100.md)): **Zelox
  346.97s / 51.7 GiB vs Apache Spark 3.5.3 1099.27s / 115 GiB → ~3.2× faster, ~2.2× less
  memory**, 22/22 each. Memory claim now MEASURED. Honest scaling: ~36× at SF-1 (warm)
  narrows to ~3.2× at SF-100 — publish per-scale, not a flat "30–40×". ~$1.5 run, torn to $0.
- [x] **ClickBench 43-query run vs Spark** — published in
  [docs/benchmarks/CLICKBENCH.md](docs/benchmarks/CLICKBENCH.md): single-node smoke (~1M
  rows), identical data + SQL, same machine. **Zelox 3.872s (43/43) vs Apache Spark 3.5.3
  48.072s (42/43) → ≈12.4×** (+ full 100M distributed on EKS above). Zelox also passes Q40
  where Spark 3.5.3 errors on a CASE coercion (matches Spark 4.x). `scripts/clickbench.py`
  is dual-engine and S3-aware.
- [ ] **Refactor `zelox bench` self-test** `P2` — spawn the server out-of-process so the
  embedded interpreter doesn't share the GIL with gRPC + DuckDB (current fatal-GIL crash).

---

## Sprint 5 — Scale + Officiality (2026-06-07 → 2026-06-21)

### 5.1 Official Apache Spark Test Suite  `[x]` P0 · ~5 days

Run the official Spark SQL test suite against Zelox. Target ≥ 95% pass rate.

**Steps:**
1. Clone `apache/spark`, extract `sql/core/src/test/resources/sql-tests/inputs/`
2. Write `scripts/spark_sql_tests.py` — run each `.sql` file, compare to `.sql.out` golden output
3. Fix top failures (likely: type coercion edge cases, datetime formatting, specific functions)
4. Document failures that are intentional deviations

---

### 5.2 TPC-H SF-100 Distributed  `[ ]` P0 · ~2 days

Validate 22/22 TPC-H queries at SF-100 on a 10-node K8s cluster.

**Steps:**
1. Generate SF-100 TPC-H data (TPCH-Kit, ~150 GB Parquet)
2. Upload to S3 or local PV
3. Run `scripts/tpch_distributed.py --sf 100 --mode kubernetes-cluster`
4. Target: < 60s total, all 22 pass

---

### 5.3 Kafka → Delta 24h Endurance Test  `[ ]` P1 · ~1 day

A production-fidelity durability test:
- Kafka source emitting 10k events/sec
- Spark Structured Streaming aggregation
- Delta Lake sink writing micro-batches
- Run 24 hours without OOM, restart, or data loss

Write `scripts/test_endurance.py`.

---

### 5.4 HMS Thrift Client  `[x]` P1 · ~3 days

Hive Metastore Thrift client for reading catalog tables from existing Hive/Glue deployments.

**Files:** `crates/zelox-catalog/src/hms/` (new)

---

### 5.5 Provider-Agnostic Catalog Caching  `[x]` P2 · ~2 days

Cache table metadata (schema, stats) in memory to avoid repeated remote catalog calls. Required for < 100ms query latency on catalog-heavy workloads.

---

## Sprint 6 — Streaming Completion + Advanced (2026-06-21 → 2026-07-05)

### 6.1 Streaming Event-Time Window Execution  `[x]` P0 · ~3 days

`withWatermark` + `window()` → tumbling/sliding windows computed per micro-batch.

Planner already accepts it (Sprint 3). Need to wire the executor:
- Accumulate state keyed by (groupKey, windowStart)
- Emit rows when watermark advances past windowEnd
- State store: RocksDB or in-memory HashMap with eviction

**Files:**
- `crates/zelox-spark-connect/src/streaming/` — state store + window accumulator
- `crates/zelox-plan/src/resolver/query/misc.rs` — emit proper Window LogicalPlan nodes

---

### 6.2 Streaming Stateful Deduplication  `[x]` P1 · ~2 days

`df.dropDuplicates(["id"])` across micro-batches (needs state to track seen keys).

Currently only stateless per-batch distinct is supported.

---

### 6.3 Theta Sketch Aggregates  `[x]` P2 · ~2 days

`approx_count_distinct` via Apache DataSketches theta sketch. More accurate than HLL at high cardinalities.

---

### 6.4 Vortex Data Source  `[x]` P2 · ~3 days

Vortex columnar format with aggressive encoding (often 10× smaller than Parquet). LakeSail shipped in v0.6.0.

---

## Apple Container + K8s Deployment Matrix

Every sprint must stay green on all three modes:

| Mode | Command | CI job |
|---|---|---|
| `local` | `zelox server` | `macos-scorecard` |
| `local-cluster` | `zelox cluster start` | `distributed-scorecard` |
| `kubernetes-cluster` | Helm + kind | `k8s-scorecard` |
| Apple Container cluster | `make container-run-cluster` | manual / future CI |

**Apple Container specifics:**
- `docker/apple/Dockerfile` — linux/arm64 optimised, tarball cache workaround
- SIGTERM handler, HEALTHCHECK TCP probe
- `make container-build` / `make container-run` / `make container-run-cluster`
- Test: `SPARK_REMOTE=sc://localhost:50051 python3 scripts/spark_compat_score.py` inside container

---

## Sprint Checklist Summary

| Sprint | Theme | Target date | Key deliverable |
|---|---|---|---|
| **4** | Spark 4.x parity | 2026-06-07 | VARIANT, time travel, GroupedMap, V2 checkpoint |
| **4.2** | Trust + perf proof | in progress | Clippy green ✅, Delta nullability ✅, full CI green, **real-scale perf** |
| **5** | Scale + officiality | 2026-06-21 | Official Spark tests ≥95%, TPC-H SF-100, HMS |
| **6** | Streaming completion | 2026-07-05 | Event-time windows, stateful dedup, endurance test |

**True Spark Replacement milestone:** Sprint 5 complete date (2026-06-21).

---

## Known Permanent Limitations

| Limitation | Notes |
|---|---|
| Python UDFs require PYTHONPATH on server | Inherent to PyO3 — not fixable without bundling PySpark |
| mimalloc disabled | Must stay off when Python UDFs active — allocator re-entrancy crash with PyO3 on Tokio |
| Streaming event-time windows | Planner ✅, executor Sprint 6 |
| HMS Thrift client | Sprint 5 — stubs only today |
