# Zelox — Master Build Plan

> Last updated: 2026-05-20  
> Branch: `phase1/production-hardening`  
> Status: **Day 10 complete — C5 JSON permissive mode fully implemented + PySpark smoke tests green; Apple Container optimised (layer caching, SIGTERM, HEALTHCHECK); next: C3 UDF type casting, C5 no-schema _corrupt_record**

---

## Table of Contents

1. [Project Vision](#1-project-vision)
2. [High-Level Architecture (HLD)](#2-high-level-architecture)
3. [Low-Level Design (LLD)](#3-low-level-design)
   - 3.1 Spark Connect Server
   - 3.2 SQL Parser & Analyzer
   - 3.3 Query Planner & Optimizer
   - 3.4 Execution Engine
   - 3.5 Storage Layer
   - 3.6 Python UDF Bridge
   - 3.7 Distributed Scheduler (Phase 2)
   - 3.8 Arrow Flight Shuffle (Phase 2)
   - 3.9 Structured Streaming (Phase 2)
4. [Crate Dependency Graph](#4-crate-dependency-graph)
5. [Phase 1 Execution Plan](#5-phase-1-execution-plan-months-16)
6. [Phase 2 Execution Plan](#6-phase-2-execution-plan-months-712)
7. [Phase 3 Execution Plan](#7-phase-3-execution-plan-months-1324)
8. [Day-by-Day Tracker](#8-day-by-day-tracker)
9. [Key Technical Decisions](#9-key-technical-decisions)
10. [Risk Register](#10-risk-register)
11. [Definition of Done](#11-definition-of-done)

---

## 1. Project Vision

Zelox is a **Rust-native, single-binary Spark engine**. It implements the
Spark Connect gRPC protocol so existing PySpark code runs unchanged, but
replaces the JVM + Python ser/de loop with a columnar, vectorized Rust
execution engine built on Apache DataFusion and Apache Arrow.

**Core goals:**
- 4–8× faster than vanilla Spark on TPC-H workloads
- < 500 ms cold start (vs 30–120 s for Spark)
- Single statically-linked binary, zero runtime dependencies
- Drop-in PySpark compatibility (Spark Connect protocol)
- `curl | sh` install on any Linux/macOS machine

**Non-goals (Phase 1):**
- Stateful streaming (Session windows, complex state stores)
- Full 100% Spark feature parity (target top-80% by usage)
- Native Scala/Java client SDK

---

## 2. High-Level Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                         CLIENT LAYER                                   │
│   PySpark (pip install pyspark)  │  SQL CLI  │  Any Spark Connect SDK  │
└────────────────────┬───────────────────────────────────────────────────┘
                     │  Spark Connect gRPC (protobuf over HTTP/2)
                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│                      ZELOX SERVER  (zelox-spark-connect)               │
│  tonic gRPC server  •  Spark Connect proto deserialiser                │
│  Session management (zelox-session)  •  Auth middleware                 │
└────────────────────┬───────────────────────────────────────────────────┘
                     │  Unresolved Relation/Plan
                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│                      QUERY PIPELINE                                    │
│  Parser (zelox-sql-parser)  →  Analyzer (zelox-sql-analyzer)             │
│  →  Planner (zelox-plan)  →  Logical Optimizer (zelox-logical-optimizer) │
│  →  Physical Planner (zelox-physical-plan)                              │
│  →  Physical Optimizer (zelox-physical-optimizer)                       │
└────────────────────┬───────────────────────────────────────────────────┘
                     │  Optimised Physical Plan
                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│                      EXECUTION ENGINE  (zelox-execution)                │
│  DataFusion StreamingExec  •  Arrow RecordBatch  •  SIMD via AVX-512   │
│  8 192 rows/batch  •  columnar operators  •  vectorised aggregations   │
└──────┬─────────────────────────────────────┬───────────────────────────┘
       │  [local]                             │  [distributed, Phase 2]
       │                                      ▼
       │                         ┌────────────────────────────┐
       │                         │  SCHEDULER  (zelox-cli)      │
       │                         │  DAG partitioning           │
       │                         │  Task dispatch to workers   │
       │                         └────────────┬───────────────┘
       │                                      │  Arrow Flight RPC
       │                                      ▼
       │                         ┌────────────────────────────┐
       │                         │  WORKERS  (zelox-cli worker) │
       │                         │  Stateless  •  same binary  │
       │                         │  zelox-flight shuffle        │
       │                         └────────────────────────────┘
       ▼
┌────────────────────────────────────────────────────────────────────────┐
│                      STORAGE LAYER  (zelox-object-store, zelox-data-source)│
│  AWS S3  •  GCS  •  Azure ADLS  •  Local FS  •  HDFS                  │
│  Parquet  •  Delta Lake  •  Apache Iceberg  •  ORC  •  CSV  •  JSON    │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Low-Level Design

### 3.1 Spark Connect Server (`zelox-spark-connect`)

**Protocol:** Spark Connect uses gRPC over HTTP/2. The proto definitions are
from `apache/spark` — `spark/connect/proto`. Messages:

| Message | Direction | Purpose |
|---|---|---|
| `ExecutePlanRequest` | Client → Server | Execute a plan (query/command) |
| `ExecutePlanResponse` | Server → Client | Streaming Arrow batches |
| `AnalyzePlanRequest` | Client → Server | Explain/analyze without executing |
| `FetchErrorDetailsRequest` | Client → Server | Retrieve error details |

**Implementation flow:**

```
tonic::Server::builder()
  .add_service(SparkConnectServiceServer::new(handler))
  .serve(addr)
  .await

handler.execute_plan(request)
  → extract Relation proto
  → call QueryPipeline::execute(relation, session)
  → stream RecordBatch chunks back as Arrow IPC
```

**Session state** (`zelox-session`):
- `SessionConfig`: Spark conf key→value map
- `CatalogState`: current database, temp view registry
- `UdfRegistry`: registered Python/Rust UDFs
- Sessions keyed by `session_id` UUID, stored in a `DashMap`

**Key files:**
- `crates/zelox-spark-connect/src/service.rs` — gRPC handler impl
- `crates/zelox-spark-connect/src/proto/` — generated protobuf types
- `crates/zelox-session/src/session.rs` — session state

---

### 3.2 SQL Parser & Analyzer (`zelox-sql-parser`, `zelox-sql-analyzer`)

**Parser design:** Uses `chumsky` (a parser combinator library) with a Pratt
expression parser for operator precedence. Targets Spark SQL dialect.

**AST node hierarchy:**
```
Statement
├── Query
│   ├── Select { projections, from, where, group_by, having, order_by, limit }
│   ├── SetOperation { op: Union|Intersect|Except, left, right }
│   └── Values { rows }
├── CreateTable { name, schema, using, partitioned_by, location }
├── Insert { target, source, overwrite }
├── Explain { analyzed, extended, plan }
└── ... (DDL commands)

Expression
├── Literal { value: ScalarValue }
├── Column { qualifier, name }
├── BinaryOp { op, left, right }
├── UnaryOp { op, expr }
├── Function { name, args, distinct, filter, over }
├── Case { operand, branches, else_expr }
├── Cast { expr, data_type }
├── Subquery { plan }
└── ...
```

**Analyzer passes** (in order):
1. `ResolveTables` — look up table names in catalog
2. `ResolveColumns` — bind `col("x")` to a schema field
3. `ResolveDataTypes` — infer/coerce expression types
4. `ResolveSubqueries` — unnest correlated subqueries
5. `ResolveFunctions` — look up UDF registry
6. `ResolveAliases` — expand `SELECT *`, resolve `col AS alias`
7. `ValidateConstraints` — check GROUP BY completeness, etc.

---

### 3.3 Query Planner & Optimizer (`zelox-plan`, `zelox-logical-optimizer`, `zelox-physical-plan`)

**Logical plan** = DataFusion `LogicalPlan` extended with Spark-specific nodes:
- `RepartitionByExpr` — DISTRIBUTE BY
- `HintNode` — query hints (BROADCAST, MERGE, SHUFFLE_HASH)
- `LateralView` — LATERAL VIEW EXPLODE
- `Pivot` / `Unpivot`

**Logical optimizer rules** (subset, 40+ total):

| Rule | What it does |
|---|---|
| `PushDownFilter` | Move WHERE clauses below joins and scans |
| `PushDownProjection` | Eliminate unused columns early |
| `EliminateSubquery` | Rewrite EXISTS/IN subqueries as joins |
| `ReorderJoins` | Cost-based join reordering (CBO) |
| `ConstantFolding` | Evaluate `1 + 1` → `2` at plan time |
| `BooleanSimplification` | `x AND TRUE` → `x` |
| `DecimalOptimizer` | Avoid DECIMAL → FLOAT precision loss |
| `EliminateLimits` | Remove redundant LIMIT nodes |
| `PropagateEmptyRelation` | Short-circuit on empty inputs |

**Physical planner — operator selection:**

```
LogicalJoin
├── small build side (< broadcast_threshold) → BroadcastHashJoin
├── sort inputs available                    → SortMergeJoin
└── default                                  → HashJoin (partitioned)

LogicalAggregate
├── no grouping keys                         → SingleAggregate
├── grouping keys fit in memory              → HashAggregate
└── streaming / sorted input                 → SortAggregate

LogicalSort
├── result fits in memory                    → InMemorySort
└── large data                               → ExternalSort (spill-aware)
```

**Partition pruning:**
Parquet statistics (min/max per row group) are read via the `parquet` crate.
The physical optimizer injects `ParquetFilter` nodes that skip row groups
whose statistics prove they cannot satisfy the predicate.

---

### 3.4 Execution Engine (`zelox-execution`)

**DataFusion integration:**
Zelox registers custom physical operators into DataFusion's `ExecutionContext`.
DataFusion drives execution via its `ExecutionPlan::execute()` → `SendableRecordBatchStream`.

**Batch processing:**
```
RecordBatch (8192 rows default)
├── columnar arrays (Arrow ArrayRef)
│   ├── Int64Array  →  SIMD add/compare via arrow-rs kernels
│   ├── StringArray →  vectorised utf8 ops
│   └── ...
└── schema (field names + types)
```

**Aggregation pipeline:**
```
Input stream of RecordBatch
  → GroupBy hash table (AHashMap<GroupKeys, AccumulatorState>)
  → Partial aggregate per batch  (in-place SIMD reduce)
  → Merge partial aggregates
  → Final aggregate → output RecordBatch
```

**Memory management:**
- `MemoryPool` tracks RSS per query. Operators reserve before allocating.
- When RSS > `memory_limit` (default: 80% of system RAM), operators spill
  intermediate state to temp files via `object_store` local backend.
- Spill format: Arrow IPC (fast re-read, no re-parse).

**Vectorized UDFs:**
Python Pandas UDFs receive a `RecordBatch` as a `pyarrow.RecordBatch` via
`arrow-pyarrow` zero-copy. The UDF returns a `pyarrow.RecordBatch`.
No serialisation — the same memory is reused.

---

### 3.5 Storage Layer (`zelox-object-store`, `zelox-data-source`)

**Abstraction:** The `object_store::ObjectStore` trait provides:
```rust
async fn get(&self, location: &Path) -> Result<GetResult>
async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult>
async fn list(&self, prefix: Option<&Path>) -> BoxStream<Result<ObjectMeta>>
async fn delete(&self, location: &Path) -> Result<()>
```

**Backend implementations:**

| Backend | Crate | Credentials |
|---|---|---|
| AWS S3 | `object_store::aws` | Instance profile, env vars, explicit keys |
| GCS | `object_store::gcp` | Application Default Credentials |
| Azure ADLS | `object_store::azure` | Service Principal, SAS token |
| Local FS | `object_store::local` | None |
| HDFS | `hdfs-native-object-store` | Kerberos / simple auth |

**Parquet reading:**
```
object_store.get_range(path, byte_range)   // footer first (statistics)
  → parquet::ArrowReader
  → row group pruning via statistics
  → page-level filter pushdown
  → async column chunk fetch (parallel I/O)
  → Arrow RecordBatch stream
```

**Delta Lake** (`zelox-delta-lake` → `delta-rs`):
- Read: resolve latest snapshot, construct add-file list, filter by partition
- Write: write Parquet files, then write `_delta_log/N.json` action file
- CDF (Change Data Feed): read `_change_data/` files for incremental ETL

**Iceberg** (`zelox-catalog-iceberg` → `iceberg-rust`):
- REST catalog: `GET /v1/namespaces/{ns}/tables/{tbl}` → table metadata
- Snapshot resolution: walk snapshot chain to find current manifest list
- Manifest files: per-partition file lists with column statistics
- Time travel: `AS OF TIMESTAMP` → find snapshot where `snapshot.timestamp ≤ T`

---

### 3.6 Python UDF Bridge (`zelox-python-udf`, `zelox-python`)

**Execution model:**
```
Python UDF registered in session
  → stored as: cloudpickle bytes + input/output schema

At execution time:
  input RecordBatch
    → arrow_pyarrow::to_pyarrow(batch)  // zero-copy FFI
    → Python GIL acquired
    → call deserialized_fn(pyarrow_batch)
    → Python fn returns pyarrow.RecordBatch
    → arrow_pyarrow::from_pyarrow(result)  // zero-copy FFI back
    → output RecordBatch
```

**UDF types supported:**
| Type | Input | Output | Notes |
|---|---|---|---|
| Scalar UDF | Row values | Scalar value | Vectorised over batch |
| Pandas UDF (SCALAR) | `pd.Series` per column | `pd.Series` | Arrow zero-copy |
| Pandas UDF (GROUP_MAP) | `pd.DataFrame` per group | `pd.DataFrame` | Used for grouped ops |
| Pandas UDF (MAP_ITER) | Iterator of `pd.DataFrame` | Iterator of `pd.DataFrame` | Streaming |
| UDAF | `pd.Series` per group | Scalar per group | Aggregate |
| UDTF | `pd.DataFrame` | `pd.DataFrame` | Table-returning |

**Python subprocess isolation:**
Zelox embeds the Python interpreter via PyO3. Child processes (e.g., from
`multiprocessing`) see the same binary as `sys.executable` and fork correctly
because of the `ZELOX_RUN_PYTHON` env var gate in `main.rs`.

---

### 3.7 Distributed Scheduler (Phase 2)

**Design:** Lightweight tokio task, not a separate process (initially).

```
PhysicalPlan (DAG)
  → StageBuilder: split at shuffle boundaries → Stage[]
     Stage {
       id: StageId,
       plan_fragment: PhysicalPlan,   // sub-plan for this stage
       input_partitions: Vec<PartitionSpec>,
       shuffle_type: Repartition | BroadcastExchange | None,
     }
  → TaskQueue: priority queue sorted by stage dependency order
  → WorkerPool: consistent-hash ring of worker addresses

For each ready Stage:
  for each partition:
    task = Task { stage_id, partition_id, input_locations }
    worker = ring.pick(task.input_locations)   // data locality
    worker.execute(task) via gRPC

Result aggregation:
  collect output Arrow IPC from all partition workers
  merge via MergeSortExec
  return to Spark Connect server
```

**Fault tolerance:**
- Task failure → retry on different worker (max 3 retries)
- Worker heartbeat: 5 s interval, 15 s timeout → mark dead, requeue tasks
- Scheduler itself: single point of failure in Phase 2, HA in Phase 3

---

### 3.8 Arrow Flight Shuffle (Phase 2, `zelox-flight`)

**Why Flight not disk:**
Spark writes >110 GB to disk during a 100 GB TPC-H run (shuffle amplification).
Arrow Flight keeps shuffle in-memory when workers have headroom, serialising
only to object store under memory pressure.

```
Map-side (producing worker):
  ExchangeExec output partitions
    → sort by partition key (for sort-merge join compat)
    → hold in memory as Vec<RecordBatch> per output partition
    → expose via FlightService::do_get()

Reduce-side (consuming worker):
  FlightClient::do_get(ticket) → RecordBatch stream
    → merge with local sort
    → feed into next stage

Spill path (when RSS > 80%):
  Vec<RecordBatch> → Arrow IPC → object_store temp path
  FlightService streams from object store instead of memory
```

**Ticket format:**
```
ShuffleTicket {
  job_id: Uuid,
  stage_id: u32,
  map_partition_id: u32,
  reduce_partition_id: u32,
}
```

---

### 3.9 Structured Streaming (Phase 2)

**Microbatch model:**
```
StreamingQuery {
  source: KafkaSource | FileSource,
  trigger: ProcessingTime(Duration),
  sink: DeltaSink | ParquetSink | KafkaSink,
  checkpoint_location: ObjectStorePath,
}

Loop:
  1. Read new data since last offset  (source.get_batch(start_offset, end_offset))
  2. Execute micro-batch as a regular batch query
  3. Write results to sink
  4. Commit offsets to checkpoint (atomic write to object store)
  5. Sleep until next trigger interval
```

**Kafka source** (`rdkafka`):
```
KafkaSource {
  brokers: Vec<String>,
  topic: String,
  starting_offsets: Earliest | Latest | Specific(HashMap<Partition, Offset>),
  consumer_group: String,
}
impl Source {
  fn get_batch(&self, start: Offsets, end: Offsets) -> RecordBatch
    // poll rdkafka consumer, deserialise value bytes (JSON/Avro/binary)
    // return as Arrow RecordBatch
}
```

**Checkpoint format** (written to `object_store`):
```
checkpoint/
  offsets/
    0.json    { "batchId": 0, "offsets": { "topic-0": 42, "topic-1": 17 } }
    1.json
  commits/
    0.json    { "batchId": 0, "numOutputRows": 1234 }
```

---

## 4. Crate Dependency Graph

```
zelox (binary)
└── zelox-cli
    ├── zelox-spark-connect          ← Spark Connect gRPC server
    │   ├── zelox-session            ← session state
    │   ├── zelox-plan               ← AST → LogicalPlan
    │   │   ├── zelox-sql-parser     ← SQL → AST
    │   │   ├── zelox-sql-analyzer   ← name resolution
    │   │   ├── zelox-logical-plan   ← LogicalPlan nodes
    │   │   └── zelox-catalog        ← catalog trait
    │   │       ├── zelox-catalog-memory
    │   │       ├── zelox-catalog-iceberg
    │   │       ├── zelox-catalog-hms
    │   │       ├── zelox-catalog-unity
    │   │       ├── zelox-catalog-glue
    │   │       └── zelox-catalog-onelake
    │   ├── zelox-execution          ← DataFusion execution
    │   │   ├── zelox-logical-optimizer
    │   │   ├── zelox-physical-plan
    │   │   ├── zelox-physical-optimizer
    │   │   ├── zelox-python-udf     ← Python UDF bridge
    │   │   │   └── zelox-python     ← PyO3 interpreter
    │   │   ├── zelox-data-source    ← Parquet/ORC/CSV readers
    │   │   ├── zelox-delta-lake     ← Delta Lake
    │   │   └── zelox-iceberg        ← Iceberg format
    │   └── zelox-object-store       ← S3/GCS/Azure/local
    ├── zelox-flight                 ← Arrow Flight SQL + shuffle
    └── zelox-telemetry              ← OpenTelemetry tracing
```

---

## 5. Phase 1 Execution Plan (Months 1–6)

### Milestone: v0.1.0 — "Single-node GA"

**Success criteria:**
- Pass 95%+ of TPC-H 22-query suite (SF-10)
- 4× faster than Spark 3.5 on equivalent hardware on TPC-H SF-100
- Install with `curl | sh` in < 60 s on Linux/macOS
- Publicly published benchmark results

---

### Week-by-Week Breakdown

| Week | Theme | Key deliverables |
|---|---|---|
| **W1** | Foundation | Fork zelox → zelox, CI pipeline, binary rename, PLAN.md |
| **W2** | Cross-compile | `cargo-zigbuild`, musl + universal macOS builds, install.sh, binary size < 80 MB |
| **W3** | Spark compat audit | Full Spark 4.0 proto surface audit, compatibility test harness setup |
| **W4** | Compat gaps batch 1 | Fix top-10 SQL compatibility failures (from W3 triage) |
| **W5** | Python UDF | Pandas UDF / Arrow UDF roundtrip tests, cloudpickle support |
| **W6** | Delta Lake write | `delta-rs` write path, ACID commit, schema enforcement |
| **W7** | Compat gaps batch 2 | Fix next-10 SQL compat failures |
| **W8** | PyPI package | `zelox-pyspark` thin client, `pip install zelox-pyspark` |
| **W9** | Iceberg read | `iceberg-rust` REST catalog, snapshot time travel |
| **W10** | TPC-H harness | SF-1 → SF-100 benchmark infra, first public numbers |
| **W11** | Storage backends | GCS, Azure ADLS, HDFS auth, S3 multipart upload |
| **W12** | Performance pass | Profile TPC-H bottlenecks, SIMD kernel tuning |
| **W13** | Docs site | mdBook docs, API reference, `zelox explain` command |
| **W14** | Hardening | Error messages, config validation, graceful shutdown |
| **W15** | Beta | Private beta with 3–5 design partners |
| **W16–20** | Beta feedback | Bug fixes from beta, Spark 4.1 compat, ORC reader |
| **W21–24** | v0.1.0 release | Blog post, HN post, benchmark comparison public |

---

### Phase 1 Critical Path

```
W1: fork + CI
  → W2: cross-compile (enables release artifacts)
    → W8: PyPI package (needs working binary)
      → W15: beta (needs PyPI + working SQL compat)
        → W24: v0.1.0 release

W3: compat audit
  → W4/W7: compat fixes
    → W10: TPC-H harness (needs correct query results)
      → W12: performance tuning
        → W24: v0.1.0 release
```

---

## 6. Phase 2 Execution Plan (Months 7–12)

### Milestone: v0.3.0 — "Distributed GA"

**Success criteria:**
- Run 1 TB TPC-H distributed across 10 workers on K8s
- Kafka Structured Streaming with at-least-once delivery
- Unity Catalog + Iceberg REST catalog support

| Month | Theme | Key deliverables |
|---|---|---|
| **M7** | Scheduler core | Tokio-based scheduler, stage DAG builder, task queue |
| **M8** | Worker protocol | Worker gRPC API, heartbeat, task lifecycle |
| **M8** | K8s Helm chart | `helm install zelox zelox/zelox`, scheduler + worker pods |
| **M9** | Flight shuffle | Arrow Flight map/reduce shuffle, memory-first with spill |
| **M9** | Fault tolerance | Task retry, worker failure detection, dead letter queue |
| **M10** | Kafka source | `rdkafka` consumer, JSON/Avro/binary deserialisation |
| **M10** | Microbatch engine | Trigger loop, checkpoint write, watermark tracking |
| **M11** | Delta streaming sink | Transactional Delta write per microbatch |
| **M11** | HMS + Iceberg catalog | Thrift HMS client, Iceberg REST spec full impl |
| **M12** | Unity Catalog | UC REST client, external table + Delta table support |
| **M12** | 1 TB benchmark | Distributed TPC-H SF-1000 on 10 workers, publish results |

---

## 7. Phase 3 Execution Plan (Months 13–24)

### Milestone: v1.0.0 — "Cloud GA"

| Month | Theme | Key deliverables |
|---|---|---|
| **M13–14** | Control plane | REST API, web UI skeleton, auth (OAuth2/OIDC) |
| **M15–16** | Data plane | BYOC worker deploy (VPC injection), customer isolation |
| **M17** | Auto-scaling | Scale-to-zero workers, cold start < 500 ms validated |
| **M18** | Per-job billing | Usage metering, cost dashboard, invoice generation |
| **M19–20** | MLflow compat | MLflow REST API server backed by object store |
| **M21** | GPU workers | DataFusion GPU execution path (CUDA/ROCm) |
| **M22** | ONNX inference | Run ONNX models inside execution pipeline |
| **M23** | Notebooks | Jupyter kernel backed by Zelox, web notebook UI |
| **M24** | v1.0.0 launch | Public cloud GA, pricing announcement |

---

## 8. Day-by-Day Tracker

### Week 1 (Complete ✅)

| Day | Done |
|---|---|
| Day 1 | Fork zelox → vikashgargg/zelox; Rust 1.95 installed; `cargo check` passing; binary renamed `zelox`; CLI restructured; ARCHITECTURE.md; GitHub Actions CI (zelox-ci.yml); install.sh; README updated; committed + pushed `phase1/foundation` |

### Week 2 (Current)

| Day | Status | Goal |
|---|---|---|
| **Day 2** | ✅ Done | `cargo-zigbuild` + musl targets; cross-compile Linux x86_64 + aarch64; macOS universal binary; binary size report |
| **Day 3** | ✅ Done | Gold test suite: all passing. Compat audit: 94 skip/xfail annotations triaged into 10 categories → `COMPAT.md` |
| **Day 4** | ✅ Done | Spark compat fixes: DELETE without WHERE (C1), monotonically_increasing_id in aggregates (C2/C10), UPDATE SET CoW (C1), FILTER in aggregates (C4 — stale skip removed); workspace clean |
| **Day 5** | ✅ Done | `zelox bench` implemented (DuckDB-driven, all 22 queries, timing table); C6 INSERT OVERWRITE stale skip removed; Makefile bench targets; end-goal memory + perf targets set |
| **Day 6** | ✅ Done | C8 managed tables fixed; C3 UDF skip removed (awaiting CI); README compat + memory target section added |
| **Day 7** | ✅ Done | C5 JSON permissive mode (schema case): `PermissiveJsonDecoder` + `PermissiveJsonFormat` + `PermissiveJsonSource`; skip markers removed from `test_json_schema_show` / `test_json_schema_collect`; no-schema `_corrupt_record` remains open |
| **Day 8** | ✅ Done | Apple Container local dev: fixed DNS (#656) + context bug (#425); thin LTO fix (OOM); `zelox:latest` built; PySpark smoke test ✅ `SELECT 1+1=2` |
| **Day 9** | ✅ Done | C5 full impl: `PermissiveJsonDecoder` streaming pipeline + 7 Rust unit tests (incl. `test_streaming_pipeline_permissive`); `scripts/smoke_json_permissive.py` — 5 PySpark end-to-end tests: PERMISSIVE, DROPMALFORMED, FAILFAST, columnNameOfCorruptRecord ✅ all green; merged via PR #1 squash into `phase1/foundation` |
| **Day 10** | ✅ Done | Production hardening: Apple Container layer-cache split (`manifests.tar.gz` → `cargo fetch` + `crates.tar.gz` → build); SIGTERM handler in `zelox-cli/src/spark/server.rs`; `Zelox ready on …` readiness log; HEALTHCHECK TCP probe; `container-build` / `container-build-clean` Makefile targets; smoke test updated for new readiness string |

### Day 2 Delivery Notes

**Cross-compilation toolchain verified:**
- `cargo-zigbuild` + zig 0.14.0 installed locally
- Rust targets installed: `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`, `x86_64-apple-darwin`, `aarch64-apple-darwin`
- Local native build (`aarch64-apple-darwin`): **105 MB** release binary (macOS dynamically links Python3.framework via PyO3 — expected)
- Linux musl sizes (statically linked, truly portable): measured in CI via `zelox-ci.yml`

**Binary size note:**
The < 80 MB target applies to the Linux musl binary (static, no dylib deps). The macOS binary is larger because PyO3 links against Python3.framework dynamically. Linux musl CI builds will produce the stripped static binary meeting the target.

**PyO3 macOS linking fix:**
System CommandLineTools Python 3.9 lacks `python3-config`, causing PyO3's build script to resolve the wrong `LIBDIR` (Xcode path vs CLT path). Fixed via:
- `PYO3_PYTHON=/usr/bin/python3`
- `RUSTFLAGS="-L $(python3 -c 'import sys; print(sys.prefix)')/lib"`
- Makefile `build-macos` target detects and sets this automatically

**CI additions (this session):**
- `zelox-ci.yml`: `build-binary` → matrix `build-linux` (x86_64 + aarch64 musl) + new `build-macos-universal` job
- `release-binary.yml`: new workflow — publishes `zelox-x86_64-unknown-linux-musl`, `zelox-aarch64-unknown-linux-musl`, `zelox-universal2-apple-darwin` as GitHub Release assets on `v*` tags (required by `install.sh`)

### Day 4 Delivery Notes

**Fix C1a — DELETE without WHERE clause** (`crates/zelox-delta-lake/src/table_format.rs`):
- Both MoR and CoW DELETE branches no longer error when `condition = None`
- Use `lit(true)` as the predicate — negated to `NOT true = false` by delta-rs, retaining no rows

**Fix C2/C10 — `monotonically_increasing_id()` in aggregate context** (`crates/zelox-plan/src/resolver/query/aggregate.rs`):
- Exclude `SparkMonotonicallyIncreasingId` from the volatile-in-aggregate check (it has special pre-projection handling)
- Pre-project `monotonically_increasing_id()` calls out of aggregate function arguments into deterministic column refs before DataFusion builds the Aggregate node
- Removed 3 skips from `test_monotonic_id.py` (2 explode-in-aggregate skips intentionally kept — separate issue)

**Fix C1b — UPDATE SET as Copy-on-Write** (`crates/zelox-plan/src/resolver/command/update.rs` + `mod.rs`):
- New file: `update.rs` implements `resolve_command_update` as: scan table → project each column through `CASE WHEN condition THEN new_val ELSE original_col END` → overwrite with `WriteMode::Truncate`
- Wired into command dispatcher in `mod.rs`
- Removed 6 UPDATE skips and 2 DELETE skips from `test_dml.py`; removed module-level Zelox skip (cleanup fixed via `try/finally` on `meow` table)

**Fix C4 — FILTER in aggregate functions** (`test_group_by.py`):
- Confirmed `filter` is already lowered in `zelox-plan/src/resolver/expression/function.rs:147`
- Removed stale `@pytest.mark.skip` from `test_aggregation_filter`

**Workspace status:** `cargo check --workspace` passes clean.

---

## 9. Key Technical Decisions

### D1 — Fork zelox rather than build from scratch
**Decision:** Fork `lakehq/sail` as the foundation.  
**Reason:** Zelox already has Spark Connect proto impl, SQL parser, DataFusion integration, PyO3 UDF bridge, Delta Lake, Iceberg, catalog integrations — roughly 12–18 months of work already done.  
**Trade-off:** Inherits zelox's naming conventions and some zelox-specific abstractions. We rename only the binary/CLI; internal crates keep `zelox-` prefix to enable upstreaming patches.

### D2 — Keep `zelox-` prefix on internal crates
**Decision:** Internal crates stay `zelox-*`; only the binary is named `zelox`.  
**Reason:** Makes it easy to contribute fixes back to `lakehq/sail` without large rename diffs. Users never see crate names.  
**Trade-off:** Slight naming confusion internally. Acceptable.

### D3 — Static binary via musl libc
**Decision:** Ship a `x86_64-unknown-linux-musl` binary with no dynamic deps.  
**Reason:** Works on any Linux distro with kernel ≥ 3.2. No `glibc` version mismatch.  
**Trade-off:** Musl's `malloc` is slower than `glibc` for multi-threaded workloads. Mitigated by `mimalloc` (already in place in `zelox-cli`).

### D4 — `cargo-zigbuild` for macOS universal binary
**Decision:** Use `cargo-zigbuild` with zig as cross-linker for macOS universal2 target.  
**Reason:** Allows cross-compiling `x86_64-apple-darwin` + `aarch64-apple-darwin` → merged into one universal binary using `lipo`. No macOS SDK required on Linux CI.  
**Trade-off:** Zig toolchain adds ~50 MB to CI image. Worth it for macOS support.

### D5 — Microbatch streaming, not continuous
**Decision:** Phase 2 streaming targets `Trigger.ProcessingTime` (microbatch), not continuous processing.  
**Reason:** Covers ~70% of real Structured Streaming usage; stateful streaming (session windows, mapGroupsWithState) is 10× harder and used by < 10% of teams.  
**Trade-off:** Higher latency floor (~1 s minimum) vs continuous mode (< 100 ms). Acceptable for ETL and analytics use cases.

### D6 — Arrow Flight for shuffle, not disk
**Decision:** Use Arrow Flight RPC for shuffle data between workers; spill to object store under memory pressure.  
**Reason:** Spark's disk shuffle is its biggest I/O bottleneck — >110 GB disk write on a 100 GB TPC-H run. Keeping shuffle in-memory eliminates this.  
**Trade-off:** Workers must have enough RAM headroom. Configuration doc will note recommended RAM-to-data ratio.

---

## 10. Risk Register

| Risk | Probability | Impact | Mitigation |
|---|---|---|---|
| SQL compatibility gaps in edge cases | High | Medium | Zelox gold-test suite + official PySpark test suite in CI |
| Python UDF ecosystem (complex cloudpickle) | Medium | High | Test against top-50 PyPI ML libraries; fail gracefully with clear error |
| iceberg-rust API instability | Medium | Medium | Pin version; contribute upstream; fallback to REST-only if needed |
| musl + PyO3 linking issues | Low | High | Test musl build in CI from Day 2; catch early |
| Databricks IP/trademark | Low | High | Implement protocol (Apache-licensed), not code; no Databricks logos/assets |
| Binary size bloat (> 200 MB) | Medium | Medium | Profile with `cargo bloat`; exclude unused DataFusion features; strip symbols |
| Arrow Flight memory pressure in shuffle | Medium | High | Implement spill path in Phase 2; set conservative default threshold |
| Solo/small team bandwidth | High | Medium | AI-assisted coding; ruthless prioritisation; monthly milestone reviews |

---

## 11. Definition of Done

A feature is **done** when:
1. Unit tests pass (`cargo test`)
2. SQL compatibility test suite passes for affected queries
3. No `clippy` warnings (`cargo clippy -- -D warnings`)
4. Documented in ARCHITECTURE.md (if it changes the architecture)
5. This PLAN.md Day tracker is updated

A **phase milestone** is done when:
1. All phase success criteria are met (see §5/§6/§7)
2. TPC-H benchmark runs clean at target scale factor
3. `cargo test --workspace` green
4. Release binary built and tagged on GitHub Releases
5. Changelog updated and blog post drafted
