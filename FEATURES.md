# Zelox — Feature Roadmap for the Decade

> Created: 2026-05-31 · Updated: 2026-07-05
> Purpose: Track every feature that makes Zelox the undisputed Spark replacement
> for this decade — pulled from Spark 4.1, LakeSail v0.6.5 (DataFusion 54.0.0 + Arrow 58.3.0),
> Databricks research, and FAANG/NVIDIA engineering blogs.
> **The maintained gap list + LakeSail v0.6.5 feature-adoption + DF/Arrow upgrade plan now live in
> [docs/design/spark-parity-and-upgrade-plan.md](docs/design/spark-parity-and-upgrade-plan.md).**
>
> Status legend: ✅ Done | 🔄 In progress | 📅 Planned | 💡 Research/Aspirational

---

## Where We Stand Today (baseline before Phase 4)

| Metric | Value |
|---|---|
| Custom Spark scorecard | **105/105 (100%)** — local / local-cluster / k8s |
| Official Spark test suite | **95.01% (2492/2623)** gold data |
| TPC-H SF-1 | **1.515 s** (40× faster than Spark JVM warm) |
| ClickBench | **43/43 correct** |
| vs LakeSail | **Leads on all streaming + infra dimensions** |
| Install | `curl | sh` works on macOS Apple Silicon + Linux x86_64/aarch64 |

---

## Part 1 — Spark 4.1 Gap Analysis (features we still need)

Spark 4.1.1 was released January 9, 2026. 1,800+ Jira tickets, 230+ contributors.
These are the gaps between Zelox and Spark 4.1 that matter for production users.

### 1.1 SQL & Functions

| Feature | Spark 4.1 | Zelox | Priority |
|---|---|---|---|
| `approx_top_k` / `approx_top_k_combine` | ✅ GA | ✅ Sprint 4.1 | P1 |
| KLL quantile sketch functions | ✅ GA | ✅ Sprint 4.1 | P1 |
| `try_to_date` / `try_to_time` safe conversions | ✅ | ✅ Sprint 4 | Done |
| Theta sketch functions | ✅ GA | ✅ Sprint 6 (KMV) | Done |
| `bitmap_and_agg` / `bitmap_count` | ✅ | ✅ Sprint 4 | Done |
| Schema-level collation (COLLATE on STRING) | ✅ | 📅 Phase 4 | P1 |
| VARIANT type + `parse_json` / `variant_get` | ✅ GA | ✅ Sprint 4 | Done |
| VARIANT shredding (auto-extract fields → Parquet) | ✅ GA | 📅 Phase 4 | P1 |
| `variant_explode` / `to_variant_object` | ✅ | ✅ Sprint 4 | Done |
| Recursive CTEs (`WITH RECURSIVE`) | ✅ | ✅ Sprint 3 | Done |
| SQL Scripting GA (DECLARE, BEGIN/END, handlers) | ✅ GA | 📅 Phase 4 | P2 |
| `EXECUTE IMMEDIATE` | ✅ | 📅 Phase 4 | P2 |
| Table Constraints (PRIMARY KEY, UNIQUE, NOT NULL) | ✅ DSv2 | ✅ Sprint 4.1 (metadata) | P1 |
| Column DEFAULT values in DDL | ✅ Spark 4.0 | ✅ Sprint 4.1 | P1 |
| MERGE INTO schema evolution | ✅ | 📅 Phase 4 | P1 |
| Stored procedures API for catalogs | ✅ | 📅 Phase 4 | P2 |
| QUALIFY clause | ✅ | ✅ Sprint 3 | Done |
| GROUPS BETWEEN in window frames | ✅ | ✅ Sprint 3 | Done |
| `uuid()` with seed parameter | ✅ | 📅 Phase 4 | P3 |
| 60+ additional built-in functions (Spark 4.1) | ✅ | 📅 Phase 4 | P2 |

### 1.2 Structured Streaming

| Feature | Spark 4.1 | Zelox | Priority |
|---|---|---|---|
| **Real-Time Mode (RTM)** — sub-second, single-digit ms | ✅ GA Aug 2025 | 📅 Phase 5 | P1 |
| Stream-stream join with virtual column families | ✅ | 📅 Phase 4 | P1 |
| AQE in stateless streaming workloads | ✅ | 📅 Phase 4 | P2 |
| Changing shuffle partitions in stateless workloads | ✅ | 📅 Phase 4 | P2 |
| State data source (checkpoint format v2) | ✅ | 📅 Phase 4 | P2 |
| RocksDB state store (lock mgmt, unified memory) | ✅ | 📅 Phase 5 | P2 |
| Automatic snapshot repair | ✅ | 📅 Phase 4 | P3 |
| `transformWithState` operator | ✅ | 📅 Phase 5 | P2 |
| Job tags in query events | ✅ | 📅 Phase 4 | P3 |

### 1.3 Python / UDFs

| Feature | Spark 4.1 | Zelox | Priority |
|---|---|---|---|
| Python Arrow UDTF (Table-Generating, zero-copy) | ✅ | 📅 Phase 4 | P1 |
| Python Arrow UDF yield scalar values | ✅ | 📅 Phase 4 | P1 |
| Iterator of RecordBatch API for Arrow UDFs | ✅ | 📅 Phase 4 | P1 |
| Python Data Sources — filter pushdown API | ✅ | 📅 Phase 4 | P1 |
| Python Data Sources — overwrite statically registered | ✅ | 📅 Phase 4 | P2 |
| Python Data Sources — Arrow writer for streaming | ✅ | 📅 Phase 4 | P2 |
| Python worker logging + viztracer integration | ✅ | 📅 Phase 4 | P3 |
| Unix Domain Socket for Python↔engine comms | ✅ | 💡 Research | P2 |
| Python minimum version 3.10 (already aligned) | ✅ | ✅ | Done |

### 1.4 Connectors & Data Sources

| Feature | Spark 4.1 | Zelox | Priority |
|---|---|---|---|
| JDBC Driver for Spark Connect | ✅ SPIP | 📅 Phase 4 | P1 |
| Join pushdown for DSv2 (Oracle/Postgres/MySQL/MSSQL) | ✅ | 📅 Phase 4 | P2 |
| Join pushdown with EXISTS | ✅ | 📅 Phase 4 | P2 |
| BOOLEAN_EXPRESSION predicate for DSv2 | ✅ | 📅 Phase 4 | P2 |
| `listTableSummaries` API | ✅ | 📅 Phase 4 | P3 |
| Hive Metastore 4.1 support | ✅ | 📅 Phase 4 | P2 |
| ZStandard compression (read/write) | ✅ | 📅 Phase 4 | P2 |
| Parquet NullType/VOID/UNKNOWN type support | ✅ | 📅 Phase 4 | P2 |
| XML format binary round-trip | ✅ | 📅 Phase 4 | P3 |
| Spark Declarative Pipelines (SDP) | ✅ GA 4.1 | 📅 Phase 5 | P1 |

### 1.5 ML / Connect

| Feature | Spark 4.1 | Zelox | Priority |
|---|---|---|---|
| Spark ML on Connect (GA) | ✅ | 💡 Research | P2 |
| CloneSession RPC for Spark Connect | ✅ | 📅 Phase 4 | P2 |
| Server-side column name validation in Connect | ✅ | 📅 Phase 4 | P2 |
| Idempotent ExecutePlan with operation IDs | ✅ | 📅 Phase 4 | P2 |

---

## Part 2 — LakeSail v0.6.x Gap Analysis (features they have that we don't yet)

LakeSail v0.6.3 released May 21, 2026.

| Feature | LakeSail Version | Zelox | Priority |
|---|---|---|---|
| Geospatial types — Geometry / Geography | v0.5.1 | 📅 Phase 4 | P2 |
| TIME type (Spark 4.0) | v0.5.2 | 📅 Phase 4 | P2 |
| Python datasource write (custom source V2) | v0.5.2 | 📅 Phase 4 | P2 |
| System catalog queryable via SQL | v0.5.0 | 📅 Phase 4 | P1 |
| AWS Glue Data Catalog (tested in prod) | v0.5.0 | 🔄 stub exists | P1 |
| Arrow Flight SQL integration (full server) | v0.6.0 | 🔄 skeleton | P1 |
| Deletion vectors in MERGE operation | v0.6.2 | 📅 Phase 4 | P1 |
| from_json (full schema support) | v0.6.2 | 📅 Phase 4 | P1 |
| SparkYear UDF + date-bucket filter pushdown | v0.6.3 | 📅 Phase 4 | P2 |
| Distributed Delta log replay | v0.5.3 | 📅 Phase 5 | P2 |
| Iceberg partition transform implementation | v0.5.3 | 📅 Phase 4 | P1 |
| Incremental Delta version checksums | v0.6.1 | 📅 Phase 4 | P3 |

**Zelox still uniquely leads on** (LakeSail still missing):
Kafka streaming, foreachBatch, memory sink, streaming checkpoint, event-time windows,
stateful deduplication, theta sketches, JWT auth, mTLS, Apple Container, K8s Helm,
Scheduler HA, Web UI :4040, 10× TPC-H speed, 3-4× smaller binary, 10× faster cold start.

---

## Part 3 — Delta Lake Next-Gen Features

These are production features that large Databricks/Delta users depend on.

| Feature | Status in Delta | Zelox | Priority |
|---|---|---|---|
| **Deletion Vectors** (mark-delete without rewrite) | GA | 📅 Phase 4 | P1 |
| **Liquid Clustering** (`CLUSTER BY AUTO`) | GA DBR 15.2+ | 📅 Phase 4 | P1 |
| **Automatic Liquid Clustering** (auto key selection) | GA DBR 15.4+ | 📅 Phase 5 | P2 |
| Row-level concurrency (optimistic concurrency control) | GA | 📅 Phase 4 | P1 |
| **UniForm** (Delta → Iceberg/Hudi auto-convert) | GA | 📅 Phase 4 | P1 |
| Table features V3 spec | GA | 📅 Phase 4 | P2 |
| Type widening (safe INT→LONG, DATE→TIMESTAMP) | GA | 📅 Phase 4 | P2 |
| Coordinated commits (multi-table atomic commit) | Preview | 📅 Phase 5 | P1 |
| Delta sharing (read across orgs) | GA | 📅 Phase 5 | P2 |
| Generated columns | GA | 📅 Phase 4 | P1 |
| Identity columns (auto-increment) | GA | 📅 Phase 4 | P1 |
| OPTIMIZE with Z-ORDER (existing) | ✅ | ✅ | Done |
| OPTIMIZE FULL (REORG) | GA | 📅 Phase 4 | P2 |
| Bloom filter index | GA | 📅 Phase 4 | P2 |

---

## Part 4 — Next-Generation Features (Phase 5-6 / "Spark Built for This Decade")

These are features that didn't exist when Spark was designed in 2012.
Implementing them makes Zelox the engine Spark would have been if built today.

### 4.1 AI-Native Data Engine

| Feature | Why It Matters | Priority |
|---|---|---|
| **Vector column type** (`VECTOR(384)`) + HNSW/IVF index | Native embedding storage + similarity search without external DB | P1 |
| `vector_distance(a, b, 'cosine')` SQL function | RAG pipelines in pure SQL | P1 |
| `embed(col, 'model-name')` scalar UDF | Call embedding model inline in DataFrame ops | P1 |
| `llm_predict(prompt, col)` scalar UDF | Call LLM inline (OpenAI/local Ollama) | P2 |
| `ai_classify` / `ai_summarize` built-ins | Databricks AI Functions pattern | P2 |
| `NEAREST NEIGHBORS k=10 BY vector_distance(…)` | First-class ANN query syntax | P1 |
| Feature Store integration (read/write feature tables) | Spark + ML pipelines in one engine | P2 |
| Automatic vector index maintenance on write | Keeps HNSW/IVF fresh without manual rebuild | P2 |

### 4.2 GPU Acceleration

| Feature | Why It Matters | Priority |
|---|---|---|
| **cuDF columnar operators** (Arrow → CUDA zero-copy) | 10-100× on GROUP BY / JOIN / sort for ML-sized data | P1 |
| GPU-accelerated sort/merge join | Replace CPU HashJoin with CuPy for float-heavy workloads | P2 |
| Mixed CPU+GPU execution plan | Route operators to GPU/CPU based on data type and size | P1 |
| RAPIDS Accelerator compatibility layer | Drop-in for existing RAPIDS Spark configs | P2 |
| GPU memory manager (DataFusion ↔ CUDA) | Spill-aware unified memory | P2 |

### 4.3 Real-Time Mode Streaming

| Feature | Why It Matters | Priority |
|---|---|---|
| **Continuous processing mode** (sub-10 ms, no micro-batch) | Fraud detection, live personalization, ad attribution | P1 |
| Event-driven trigger (process-as-arrives, not timed) | Eliminates artificial 100ms–1s batch delays | P1 |
| Streaming shuffle without barrier sync | Remove the main latency bottleneck in stateful ops | P2 |
| Row-level watermark tracking | Finer-grained late-data handling than batch-level | P2 |
| Streaming MERGE INTO Delta (CDC ingest) | Direct CDC → Delta write without batch landing zone | P1 |

### 4.4 Declarative Pipeline Engine (DLT-style)

| Feature | Why It Matters | Priority |
|---|---|---|
| **`zelox pipeline` command** — define datasets + queries | Spark Declarative Pipelines equivalent, no JVM | P1 |
| Auto-dependency ordering of pipeline tables | User writes `CREATE LIVE TABLE t AS SELECT …`; engine handles order | P1 |
| Automatic checkpoints + incremental refresh | Only process new data since last run | P1 |
| Pipeline lineage graph (DAG visualisation in Web UI) | Operators see data flows without reading code | P2 |
| Change Data Capture (CDC) ingest node | `APPLY CHANGES INTO` / Type 1+2 SCD | P1 |
| Pipeline expectations (data quality constraints) | Alert/drop/quarantine rows that fail assertions | P2 |

### 4.5 Multi-Catalog & Data Mesh

| Feature | Why It Matters | Priority |
|---|---|---|
| **Cross-catalog query federation** | `SELECT * FROM iceberg.db.t JOIN delta.db2.t2` | P1 |
| Unity Catalog full support (ACL enforcement) | Enterprise auth without Databricks platform lock-in | P1 |
| Polaris Catalog (Apache Iceberg REST) | Open-source Unity Catalog alternative | P1 |
| **Data Mesh namespace routing** (`domain.catalog.db.table`) | Data Mesh architecture in one engine | P2 |
| Column masking + row-level security policies | Enterprise data governance without Ranger | P1 |
| Data lineage export (OpenLineage protocol) | Integration with DataHub, Marquez, Atlan | P2 |

### 4.6 Performance for This Decade

| Feature | Why It Matters | Priority |
|---|---|---|
| **Adaptive Query Execution (AQE) v2** | Re-optimize mid-query based on runtime stats | P1 |
| ML-based cost model (learned cardinality) | Replace hard-coded heuristics with trained model | P2 |
| **Query result caching** (persistent across sessions) | Identical queries return instantly; configurable TTL | P1 |
| Spill-to-NVMe (not RAM) | Handle 10× larger datasets on same hardware | P1 |
| Bloom filter on all file formats | Skip files without reading; 5-50× I/O reduction | P1 |
| **Native Arrow Flight SQL server** (full protocol) | JDBC/ODBC replacement with zero-copy throughput | P1 |
| SIMD-accelerated string ops (vectorized regex, like) | 5-10× faster WHERE LIKE / regexp_extract on strings | P2 |
| Async prefetch + I/O parallelism for object stores | Overlap network + compute; remove I/O-bound bottleneck | P1 |
| WASM UDFs (safe sandboxed non-Python functions) | Run Rust/C++/Go UDFs without PyO3 overhead | P2 |

### 4.7 Developer Experience

| Feature | Why It Matters | Priority |
|---|---|---|
| **`zelox notebook`** — Jupyter kernel over Spark Connect | Notebook-native Zelox without PySpark boilerplate | P1 |
| Interactive explain plan in Web UI (visual DAG) | Debug slow queries without reading JSON | P1 |
| Query profiler (per-operator time breakdown) | Identify bottlenecks in seconds | P1 |
| `zelox fmt` — auto-format SQL files | Like `rustfmt` for SQL pipelines | P2 |
| `zelox lint` — catch common SQL anti-patterns | Cartesian joins, missing WHERE on DELETE | P2 |
| Language Server Protocol (LSP) for Spark SQL | Autocomplete + type checking in VSCode/JetBrains | P2 |
| REST API for query submission (non-gRPC) | HTTP/JSON for teams not using PySpark | P2 |
| OpenAPI / Swagger spec for REST API | Auto-generated client SDKs in any language | P3 |

### 4.8 Managed Cloud ("zelox.cloud")

| Feature | Why It Matters | Priority |
|---|---|---|
| Serverless execution (pay-per-query, no infra) | Compete with Databricks Serverless | P1 |
| Auto-scaling worker pool (0→N based on queue depth) | No idle cluster costs | P1 |
| Query history + billing dashboard | Show cost per query / per user | P2 |
| Multi-tenant isolation (namespace-level quotas) | Enterprise SaaS requirement | P1 |
| Secret management (Vault / AWS Secrets Manager) | No plaintext S3 keys in job configs | P1 |

---

## Part 5 — What "True Spark Replacement" Means in 2026

LakeSail got 95% there. Spark built today would have all of these:

```
┌─────────────────────────────────────────────────────────────────┐
│                  TRUE SPARK REPLACEMENT CHECKLIST               │
│                                                                 │
│ SQL Compatibility                                               │
│   ✅ 105/105 custom scorecard          (done)                   │
│   ✅ 95.01% official test suite        (done)                   │
│   📅 100% official test suite          (Phase 4)                │
│   📅 Spark 4.1 SQL surface complete    (Phase 4)                │
│                                                                 │
│ Storage Formats                                                 │
│   ✅ Parquet / Delta / Iceberg / CSV / JSON                     │
│   📅 Delta Deletion Vectors            (Phase 4)                │
│   📅 Delta Liquid Clustering           (Phase 4)                │
│   📅 Delta UniForm (→Iceberg auto)     (Phase 4)                │
│   📅 Vortex full read/write            (Phase 4)                │
│                                                                 │
│ Streaming                                                       │
│   ✅ Kafka → Delta, foreachBatch, event-time windows            │
│   📅 Real-Time Mode (<10 ms latency)   (Phase 5)                │
│   📅 Streaming MERGE INTO CDC          (Phase 4)                │
│   📅 RocksDB state store               (Phase 5)                │
│                                                                 │
│ Python / AI                                                     │
│   ✅ Scalar / Pandas / Arrow / GroupedMap UDFs                  │
│   📅 Arrow UDTF + filter pushdown API  (Phase 4)                │
│   📅 Vector type + similarity search   (Phase 4)                │
│   📅 GPU execution (RAPIDS/cuDF)       (Phase 5)                │
│   📅 LLM / embed() functions          (Phase 5)                 │
│                                                                 │
│ Catalogs & Governance                                           │
│   ✅ HMS / Unity / Glue / Iceberg REST stubs                    │
│   📅 Unity Catalog full ACL            (Phase 4)                │
│   📅 Column masking + row security     (Phase 4)                │
│   📅 Cross-catalog federation          (Phase 4)                │
│   📅 OpenLineage export               (Phase 4)                  │
│                                                                 │
│ Operations                                                      │
│   ✅ Apple Container + K8s + Helm + HA auth                     │
│   📅 JDBC Driver for Spark Connect     (Phase 4)                │
│   📅 REST API (non-gRPC)              (Phase 4)                  │
│   📅 zelox notebook (Jupyter kernel)   (Phase 4)                │
│   📅 TPC-H SF-100 distributed <60s    (needs hardware)          │
└─────────────────────────────────────────────────────────────────┘
```

---

## Phase 4 Sprint Plan (immediate next)

**Target**: `v0.7.0-alpha` — Spark 4.1 SQL complete, Delta V3, Arrow UDTF, JDBC driver

### Sprint 4.1 — Spark 4.1 SQL surface + production hardening ✅ COMPLETE (2026-06-02)
- [x] `approx_top_k` / `approx_top_k_accumulate` (real Space-Saving counter)
- [x] KLL quantile sketches (`kll_sketch_agg_*`, `kll_sketch_get_quantile_*`)
- [x] `theta_union` / `theta_intersection` / `theta_difference` / `hll_union` (were stubs)
- [x] Column DEFAULT values in DDL
- [x] Table constraints (PRIMARY KEY, UNIQUE — metadata-only)
- [x] `raise_error` → `[USER_RAISED_EXCEPTION]` prefix
- [x] **Python-version-agnostic UDFs** (subprocess via `ZELOX_PYTHON` — any Python 3.10–3.14+)
- [x] **Lambda HOFs in distributed mode** (added to remote codec)
- [x] **WITH RECURSIVE in distributed mode** (single-stage recursive subtree)
- [ ] Schema-level collation (COLLATE keyword) — deferred to 4.1b
- [ ] VARIANT shredding in Parquet — deferred to 4.1b
- [ ] Identity columns + MERGE INTO schema evolution — deferred to 4.1b

### Sprint 4.2 — Delta V3 + governance
- Deletion Vectors (read + write)
- Liquid Clustering (`CLUSTER BY col`)
- Delta UniForm (write Iceberg metadata alongside Delta)
- Row-level security + column masking
- OpenLineage export

### Sprint 4.3 — Python + connectors
- Arrow UDTF (zero-copy table functions)
- Python Data Source filter pushdown
- JDBC Driver for Spark Connect
- Geospatial types (Geometry/Geography, GeoParquet)
- Arrow Flight SQL full server

### Sprint 4.4 — Streaming + catalog
- Stream-stream join with virtual column families
- Streaming MERGE INTO CDC
- AWS Glue catalog production hardening
- Unity Catalog full ACL enforcement
- Cross-catalog federation

### Sprint 4.5 — Performance + DX
- Query result cache (session-level)
- Bloom filters on CSV/JSON
- zelox notebook (Jupyter kernel)
- Web UI: visual explain plan DAG
- REST API endpoint (HTTP/JSON)

---

## References

- [Spark 4.1.0 Release Notes](https://spark.apache.org/releases/spark-release-4.1.0.html)
- [Introducing Apache Spark 4.1 — Databricks Blog](https://www.databricks.com/blog/introducing-apache-sparkr-41)
- [Real-Time Mode GA — Databricks Blog](https://www.databricks.com/blog/announcing-general-availability-real-time-mode-apache-spark-structured-streaming-databricks)
- [Delta Lake Liquid Clustering](https://docs.delta.io/latest/delta-clustering.html)
- [LakeSail GitHub](https://github.com/lakehq/sail)
- [LakeSail Blog](https://lakesail.com/blog/)
- [RAPIDS Accelerator for Apache Spark](https://nvidia.github.io/spark-rapids/)
- [AI Data Lakehouse 2026 Guide](https://lifebit.ai/blog/ai-data-lakehouse-ultimate-guide/)
