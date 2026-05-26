# Vajra — Production Spark Replacement Roadmap

> Last updated: 2026-05-26
> Branch: `phase2/distributed`
> Goal: Full Apache Spark replacement for **batch + streaming** that outperforms both Apache Spark and LakeSail across correctness, performance, and operational maturity.

---

## Competitive Intelligence (2026-05-26)

### LakeSail v0.6.3 (released 2026-05-21, 2,732 stars)

LakeSail is the primary open-source benchmark. They are shipping daily. Key things they just landed:

| Feature | Their version | Our status |
|---|---|---|
| VARIANT type (Spark 4.x) | v0.6.3 | ❌ Sprint 4 |
| GroupedMap/CoGroupedMap UDFs (Spark 4.1) | v0.6.3 | ❌ Sprint 4 |
| variant_explode / variant_explode_outer | v0.6.3 | ❌ Sprint 4 |
| bitmap_and_agg | v0.6.2 | ❌ Sprint 4 |
| Delta time travel (AT VERSION/TIMESTAMP) | v0.6.0 | ❌ Sprint 4 |
| Delta V2 checkpointing + log compaction | v0.6.0 | ❌ Sprint 4 |
| Delta type widening | v0.6.3 | ❌ Sprint 4 |
| Iceberg V3 spec support | v0.6.3 | ❌ Sprint 4 |
| to_variant_object / schema_of_variant_agg | v0.6.1 | ❌ Sprint 4 |
| Provider-agnostic catalog caching | v0.6.3 | ❌ Sprint 5 |
| HMS table metadata parity | v0.6.3 | ❌ Sprint 5 |
| dbt integration guide | v0.6.3 | ❌ Sprint 4 |
| ClickBench benchmark | v0.6.3 | ❌ Sprint 4 |
| Vortex Python data source | v0.6.0 | ❌ Sprint 5 |
| theta sketch aggregates | PR open | ❌ Sprint 5 |

**Their open gaps (we have, they don't):**

| Feature | Vajra | LakeSail |
|---|---|---|
| Kafka source (readStream.format("kafka")) | ✅ rdkafka | ❌ not shipped |
| foreachBatch | ✅ | ❌ |
| memory sink | ✅ | ❌ |
| Streaming checkpoint + recovery | ✅ | ❌ (issue #1969 open) |
| JWT bearer auth | ✅ | ❌ |
| mTLS | ✅ | ❌ |
| Apple Container (macOS 26) | ✅ only one | ❌ |
| K8s Helm chart | ✅ | ❌ |
| Scheduler HA (K8s Lease) | ✅ | ❌ |
| Web UI on :4040 | ✅ | ❌ |
| vajra-pyspark PyPI | ✅ | pysail |
| 40× TPC-H speedup (measured) | ✅ 1.515s | claimed ~4× |

### Other Competitors

| Project | Stars | Model | Gap |
|---|---|---|---|
| Databricks Photon | n/a (closed) | C++ accelerator, JVM still required | Closed source, vendor lock-in |
| Apache Comet | ~2k | Rust native execution plugin, JVM still required | Not standalone — still needs Spark JVM |
| Gluten / Velox | ~3k | C++ vectorized execution, JVM wrapper | Not standalone — complex deploy |
| Blaze | ~1.8k | Rust accelerator, JVM still required | Not standalone |
| Spark SQL WASM | experimental | WebAssembly target | Not production |

**Vajra is the only fully standalone, JVM-free Spark replacement with production operational features.**

---

## Current Baseline (2026-05-26)

| Metric | Value |
|---|---|
| SQL compat scorecard | **105/105 (100%)** — all 3 deployment modes |
| TPC-H SF-1 (22 queries) | **22/22 PASS, 1.515s total (40× faster than Spark warm)** |
| K8s modes validated | local / local-cluster / kubernetes-cluster (kind) |
| Phase 1 (SQL parity) | ✅ Complete |
| Phase 2 (streaming + auth + HA) | ✅ Complete |
| Phase 3 (Spark 4.x + competitive parity) | 🔄 In progress |

---

## Sprint 4 — Spark 4.x Features + Competitive Parity (2026-05-26 to 2026-06-07)

Priority: close the feature gap with LakeSail v0.6.3.

### 4.1 VARIANT Type (Spark 4.x)  `[ ]` P0 · ~3 days

**What:** `VARIANT` is a new semi-structured type in Spark 4.0. Used for schemaless JSON blobs.

**Key functions:** `parse_json`, `try_parse_json`, `is_variant_null`, `variant_get`, `variant_explode`, `variant_explode_outer`, `to_variant_object`, `schema_of_variant_agg`.

**Files:**
- `crates/sail-plan/src/resolver/data_type.rs` — add `DataType::Variant` mapping
- `crates/sail-plan/src/function/scalar/variant.rs` (new) — implement Spark VARIANT functions
- `crates/sail-sql-parser/src/ast/data_type.rs` — parse `VARIANT` keyword

**Test:**
```python
spark.sql("SELECT parse_json('{\"k\":1}')").printSchema()
# root: variant (nullable = true)
spark.sql("SELECT variant_get(parse_json('{\"a\":42}'), '$.a', 'INT')").collect()
# [Row(42)]
```

---

### 4.2 Delta Lake Time Travel  `[ ]` P0 · ~2 days

**What:** `SELECT * FROM t TIMESTAMP AS OF '2024-01-01'` / `VERSION AS OF 5`.

**Files:**
- `crates/sail-plan/src/resolver/table.rs` — detect `AT TIMESTAMP`/`AT VERSION` modifiers
- Pass `version`/`timestamp` to `delta-rs` `DeltaTable::load_with_datetime`/`load_version`

**Test:**
```python
spark.sql("INSERT INTO t VALUES (1)")
spark.sql("INSERT INTO t VALUES (2)")
result = spark.sql("SELECT * FROM t VERSION AS OF 1").collect()
assert len(result) == 1
```

---

### 4.3 GroupedMap / CoGroupedMap Iterator UDFs (Spark 4.1)  `[ ]` P0 · ~3 days

**What:** `df.groupBy("key").applyInPandas(fn, schema)` — grouped iterator UDFs. Sail landed this in v0.6.3.

**Files:**
- `crates/sail-spark-connect/src/proto/plan.rs` — handle `ApplyInPandas` / `CoGroupMap` plan nodes
- `crates/sail-plan/src/resolver/command/udf.rs` — wire PyO3 grouped iterator callback

**Test:**
```python
def normalize(key, pdf):
    pdf["v"] = pdf["v"] / pdf["v"].mean()
    return pdf

df = spark.createDataFrame([(1, 2.0), (1, 3.0)], ["k", "v"])
df.groupBy("k").applyInPandas(normalize, schema="k long, v double").show()
```

---

### 4.4 Delta V2 Checkpointing + Log Compaction  `[ ]` P1 · ~2 days

**What:** Delta V2 checkpoint with sidecar files (smaller, incremental). Log compaction to avoid reading thousands of JSON log files.

**Files:**
- `crates/sail-data-source/src/delta/checkpoint.rs` — write V2 checkpoint format
- `crates/sail-data-source/src/delta/log.rs` — compact when log file count exceeds threshold

**Why:** Production Delta tables accumulate thousands of JSON log files without compaction — read latency degrades significantly. Sail landed this in v0.6.0.

---

### 4.5 Delta Type Widening  `[ ]` P1 · ~1 day

**What:** When evolving a Delta table schema, allow widening casts (e.g., INT → BIGINT) without rewrite.

**Files:** `crates/sail-data-source/src/delta/schema_evolution.rs`

**Why:** Sail landed this in v0.6.3. Required for production schema evolution workflows.

---

### 4.6 Iceberg V3 + REST Catalog Improvements  `[ ]` P1 · ~2 days

**What:** Iceberg spec V3 introduces new delete file formats, improved partitioning. REST catalog needs sort transform parsing (Sail fixed this in v0.6.3).

**Files:** `crates/sail-data-source/src/iceberg/`

---

### 4.7 ClickBench Benchmark  `[ ]` P1 · ~1 day

**What:** ClickBench is 43 analytical queries on a 100M-row web analytics dataset — a more realistic OLAP workload than TPC-H. Sail added ClickBench snapshots in v0.6.3.

**Steps:**
1. Download ClickBench dataset (Parquet from ClickHouse S3)
2. Write `scripts/clickbench.py` — run all 43 queries, measure time
3. Add results to `BENCHMARKS.md`
4. Compare to Sail's numbers and Spark's numbers

**Target:** Run all 43 queries correctly; beat LakeSail total time.

---

### 4.8 dbt Integration Guide  `[ ]` P1 · ~4 hours

**What:** dbt is the dominant SQL transformation tool in the modern data stack. Sail published a dbt integration guide in v0.6.3 — we need one too.

**Steps:**
1. Test `dbt-spark` (SparkSession connector) with Vajra as the backend
2. Verify `dbt run`, `dbt test`, `dbt docs generate` work
3. Write `docs/guide/integrations/dbt.md`
4. Note any incompatibilities

**Why:** dbt has millions of users. A working dbt integration is a strong acquisition channel.

---

### 4.9 variant_explode / bitmap_and_agg / theta_sketch  `[ ]` P1 · ~2 days

**What:**
- `variant_explode(v)` / `variant_explode_outer(v)` — lateral view of VARIANT array
- `bitmap_and_agg(col)` / `bitmap_or_agg(col)` — Roaring Bitmap aggregates
- `approx_count_distinct` via theta sketch (HyperLogLog) — Sail has PR open

**Files:** `crates/sail-plan/src/function/scalar/` and `aggregate/`

---

## Sprint 5 — Catalog, HMS, Performance (2026-06-07 to 2026-06-21)

### 5.1 Provider-Agnostic Catalog Caching  `[ ]` P0 · ~3 days

**What:** Delta/Iceberg catalog listings are expensive (S3 LIST calls). Cache listing results in memory with TTL. Sail landed this in v0.6.3.

**Files:** `crates/sail-plan/src/catalog/cache.rs` (new)

**Impact:** 10–100× faster repeated table lookups on large catalogs.

---

### 5.2 HMS (Hive Metastore) Thrift Client  `[ ]` P1 · ~1 week

**What:** Generate a proper Thrift client for Hive Metastore from the official `.thrift` IDL (at build time). Sail has this as their highest-open-priority item.

**Files:**
- `crates/sail-catalog/src/hms/thrift/` (new) — Thrift client codegen via `thrift` crate
- `crates/sail-catalog/src/hms/client.rs` — HMS connection + table/partition operations

**Why:** Most enterprise Hadoop/Spark deployments use HMS as the catalog. Without a proper Thrift client, HMS-backed tables fail or return wrong metadata.

---

### 5.3 TPC-H SF-10 and SF-100 Distributed  `[ ]` P0 · ~3 days

**What:** Validate correctness AND performance at scale.

**Steps:**
1. Generate SF-10 Parquet (6 GB) via DuckDB
2. Run `vajra bench --scale-factor 10 --mode local-cluster --workers 4`
3. Generate SF-100 Parquet (60 GB) on kind cluster
4. Run `vajra bench --scale-factor 100 --mode kubernetes-cluster --workers 8`
5. Publish to `BENCHMARKS.md`

**Target:** SF-100 in < 30s on 8 workers × 4 vCPU. Publish vs LakeSail and Spark.

---

### 5.4 Official Apache Spark Test Suite  `[ ]` P1 · ~1 week

**What:** Run the patched Spark Python test suite (`scripts/spark-tests/`) against Vajra. Measure pass rate vs LakeSail's reported figure.

**Steps:**
1. Apply `spark-4.1.1.patch` to Spark test suite
2. `scripts/spark-tests/run-tests.sh` against running Vajra server
3. Capture fail list; add known gaps to `COMPAT.md`
4. Fix the top-20 most common failures

**Target:** 95%+ pass rate (LakeSail claims ~95%, we should match or beat it).

---

### 5.5 Profile-Guided Optimization (PGO)  `[ ]` P2 · ~2 days

**What:** Run the TPC-H benchmark as a profiling workload, then compile with PGO enabled. Expected 5–15% additional speedup on hot paths.

**Files:** `.cargo/config.toml` — add `profile.release.pgo` configuration.

**Reference:** LakeSail has this as an open enhancement request (issue #193).

---

### 5.6 Vortex Data Source  `[ ]` P2 · ~1 week

**What:** [Vortex](https://github.com/spiraldb/vortex) is a new high-performance columnar format with superior compression and vectorized execution. Sail added Python Vortex data source in v0.6.0.

**Files:** `crates/sail-data-source/src/formats/vortex/` (new)

**Why:** Vortex benchmarks show 2–5× faster reads than Parquet on compressible data. Early adoption = differentiation.

---

## Sprint 6 — Streaming GA + Scale (2026-06-21 to 2026-07-05)

### 6.1 Streaming Event-Time Window Execution  `[ ]` P0 · ~1 week

**What:** Wire `F.window("timestamp", "1 minute")` tumbling/sliding windows into the physical execution layer. The planner already accepts them; the executor drops the window grouping.

**Files:**
- `crates/sail-plan/src/streaming/rewriter.rs` — detect `F.window()` grouping key, emit `StreamWindowNode`
- `crates/sail-execution/src/streaming/window.rs` (new) — per-micro-batch tumbling window aggregation

**Test:**
```python
sdf = spark.readStream.format("rate").load() \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(F.window("timestamp", "1 minute")).count()
q = sdf.writeStream.outputMode("append").format("memory").queryName("w").start()
time.sleep(65); q.stop()
assert spark.sql("SELECT * FROM w").count() > 0
```

---

### 6.2 Stream × Stream Join  `[ ]` P1 · ~1 week

**What:** Join two streaming DataFrames with watermark-based state management. Uses `stateful` operator with per-batch join and state expiry on watermark advance.

**Files:** `crates/sail-plan/src/streaming/rewriter.rs`, `crates/sail-execution/src/streaming/join.rs` (new)

---

### 6.3 Delta Streaming Sink  `[ ]` P0 · ~3 days

**What:** `writeStream.format("delta").option("checkpointLocation", ...)` with exactly-once semantics via Delta transaction log.

**Files:** `crates/sail-data-source/src/delta/streaming_sink.rs` (new)

**Why:** The most common production streaming sink. Kafka → Delta is the canonical pipeline.

---

### 6.4 mapGroupsWithState / flatMapGroupsWithState  `[ ]` P2 · ~3 weeks

**What:** Arbitrary stateful processing per group key. Requires a persistent state store.

**Design:**
- State store: RocksDB (via `rocksdb-rs` crate) for low-latency per-key access
- `GroupStateImpl` serialises/deserialises user-defined state objects via `cloudpickle`
- Timeout: `NoTimeout` / `ProcessingTimeTimeout` / `EventTimeTimeout`

**Note:** This is the most complex streaming feature in Spark. Phase 4 target.

---

## Track: Infrastructure & Operations (ongoing)

### I.1 Worker Image Pull Secrets  `[ ]` P1 · ~2h

**What:** Worker pods spawned by the driver need `imagePullSecrets` when using a private registry (ECR, GCR, ACR).

**File:** `helm/vajra/templates/server-deployment.yaml` — pass `imagePullSecrets` in `SAIL_KUBERNETES__WORKER_POD_TEMPLATE` when `image.pullSecrets` is set.

---

### I.2 Resource Quotas & Multi-Tenant Isolation  `[ ]` P1 · ~3 days

**What:** Multiple teams sharing one K8s cluster need namespace-level resource quotas.

**Files:**
- `helm/vajra/templates/resourcequota.yaml` (new)
- Add `quota.enabled`, `quota.maxCPU`, `quota.maxMemory` to Helm values

---

### I.3 Graceful Query Cancellation on Pod Eviction  `[ ]` P1 · ~3 days

**What:** When K8s evicts a worker pod (OOM, node pressure), in-flight query partitions should be retried on another worker rather than failing the whole job.

**Files:**
- Worker: handle SIGTERM → complete current task or report failure to scheduler
- Driver scheduler: treat worker disconnect as retriable error

---

### I.4 OAuth2 OIDC / API Key Auth  `[ ]` P1 · ~1 week

**What:** For multi-tenant SaaS use case: validate tokens against JWKS endpoint, support API keys.

**Files:**
- `crates/sail-spark-connect/src/auth.rs` — add `JwksClient` alongside existing `BearerTokenInterceptor`
- Add `auth.jwksUri` / `auth.apiKeys` to Helm values

---

### I.5 Session Isolation & Per-User Quotas  `[ ]` P1 · ~3 days

**What:** Multiple users share one Vajra server. Ensure temp views, session configs, and UDFs are isolated. Rate-limit per user.

---

## Track: Function Coverage Gaps

These functions appear frequently in production Spark code.

### F.1 `timestampdiff` (Spark 4.x)  `[ ]` P1 · ~2h

**What:** `timestampdiff(unit, ts1, ts2)` — difference between two timestamps in specified units. Sail registered this in a recent commit (2026-05-26).

**File:** `crates/sail-plan/src/function/scalar/datetime.rs`

---

### F.2 `to_csv` Function  `[ ]` P1 · ~2h

**What:** `to_csv(struct_col, options)` — serialize a struct column to CSV string. Sail added this 2026-05-26.

**File:** `crates/sail-plan/src/function/scalar/csv.rs`

---

### F.3 `coalesce` / `repartition` Spark Semantics  `[ ]` P1 · ~1 day

**What:** `df.coalesce(n)` should reduce partitions without shuffle; `df.repartition(n)` should shuffle. Sail has a follow-up issue (#1988) open on this.

**File:** `crates/sail-plan/src/resolver/relation/repartition.rs`

---

### F.4 Full Java DateTime Format  `[ ]` P2 · ~1 week

**What:** `to_date(col, "dd/MM/yyyy HH:mm:ss")` — Spark accepts Java SimpleDateFormat patterns. We map to Rust's `chrono` formats but many patterns differ. Sail has this as an open issue (#1972).

---

## Definition of Done: "Full Spark Replacement + LakeSail Outperformer"

Vajra can be called production-complete when:

- [ ] **SQL**: 105/105 scorecard + 95%+ official Spark test suite + all VARIANT/GroupedMap UDF tests green
- [ ] **Batch**: TPC-H SF-100 distributed (8 workers, kind cluster) published + ClickBench 43/43 correct
- [ ] **Streaming**: Kafka → Delta pipeline with event-time windows runs 24h without error
- [ ] **Storage**: Delta time travel + V2 checkpoint + Iceberg V3 all validated
- [ ] **K8s**: HA scheduler, HPA, mTLS, resource quotas, image pull secrets
- [ ] **Ops**: Web UI :4040, OTLP traces, Prometheus metrics, Grafana dashboard
- [ ] **Install**: `pip install vajra-pyspark` + `curl | sh` + `helm install` all work
- [ ] **Docs**: dbt integration guide, migration guide (Spark 3.5 → Vajra), ClickBench comparison
- [ ] **Performance**: TPC-H SF-1 remains < 2s; SF-100 < 30s on 8 workers

**Estimated completion: Sprint 6 end (2026-07-05)**

---

## Sprint Progress Tracker

### Sprint 1 (2026-05-24) — Complete ✅
- SQL compat 105/105, all 3 modes
- TPC-H SF-1 22/22 @ 1.515s

### Sprint 2 (2026-05-24) — Complete ✅
- Streaming aggregates, Kafka source, foreachBatch, memory sink
- Scheduler HA, bearer auth, K8s CI, macOS CI

### Sprint 3 (2026-05-25) — Complete ✅
- F.window(), withWatermark planner, checkpoint + recovery
- TPC-DS script, TPC-H distributed script, vajra-pyspark, Web UI :4040
- mTLS auth, stream×static join, streaming analytic windows (per-batch)
- DESCRIBE QUERY, approxQuantile, freqItems, concurrency test

### Sprint 4 (2026-05-26 → 2026-06-07) — In Progress 🔄

| # | Item | Status |
|---|---|---|
| 4.1 | VARIANT type (Spark 4.x) | `[ ]` |
| 4.2 | Delta time travel (AT VERSION/TIMESTAMP) | `[ ]` |
| 4.3 | GroupedMap/CoGroupedMap UDFs (Spark 4.1) | `[ ]` |
| 4.4 | Delta V2 checkpointing + log compaction | `[ ]` |
| 4.5 | Delta type widening | `[ ]` |
| 4.6 | Iceberg V3 + REST catalog sort transform fix | `[ ]` |
| 4.7 | ClickBench 43-query benchmark | `[ ]` |
| 4.8 | dbt integration guide | `[ ]` |
| 4.9 | variant_explode, bitmap_and_agg | `[ ]` |
| 4.10 | timestampdiff, to_csv (Spark 4.x) | `[ ]` |

### Sprint 5 (2026-06-07 → 2026-06-21) — Planned

| # | Item | Status |
|---|---|---|
| 5.1 | Provider-agnostic catalog caching | `[ ]` |
| 5.2 | HMS Thrift client (proper codegen) | `[ ]` |
| 5.3 | TPC-H SF-10 + SF-100 distributed | `[ ]` |
| 5.4 | Official Spark test suite (95%+ target) | `[ ]` |
| 5.5 | PGO (Profile-Guided Optimization) | `[ ]` |
| 5.6 | Vortex data source | `[ ]` |
| 5.7 | Full Java datetime format | `[ ]` |
| 5.8 | OAuth2 OIDC / API key auth | `[ ]` |

### Sprint 6 (2026-06-21 → 2026-07-05) — Planned

| # | Item | Status |
|---|---|---|
| 6.1 | Streaming event-time window execution | `[ ]` |
| 6.2 | Stream × stream join | `[ ]` |
| 6.3 | Delta streaming sink (exactly-once) | `[ ]` |
| 6.4 | Resource quotas + session isolation | `[ ]` |
| 6.5 | Worker eviction + task retry | `[ ]` |

---

## Quick Reference: Key Files

| What | File |
|---|---|
| Streaming rewriter | `crates/sail-plan/src/streaming/rewriter.rs` |
| Command dispatcher (DDL) | `crates/sail-plan/src/resolver/command/mod.rs` |
| Scalar function registry | `crates/sail-plan/src/function/scalar/` |
| Data source formats | `crates/sail-data-source/src/formats/` |
| Delta integration | `crates/sail-data-source/src/delta/` |
| Iceberg integration | `crates/sail-data-source/src/iceberg/` |
| Catalog implementations | `crates/sail-catalog/src/` |
| Auth middleware | `crates/sail-spark-connect/src/auth.rs` |
| Web UI | `crates/sail-spark-connect/src/web/` |
| Helm chart | `helm/vajra/` |
| CI pipeline | `.github/workflows/ignite-ci.yml` |
| TPC-H benchmark | `scripts/tpch_bench.py` |
| TPC-DS benchmark | `scripts/tpcds_score.py` |
| ClickBench (planned) | `scripts/clickbench.py` |
| Spark compat scorecard | `scripts/spark_compat_score.py` |
