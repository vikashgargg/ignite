# Ignite — Architecture

Ignite is a Rust-native, single-binary Spark engine. It is a fork of
[lakehq/sail](https://github.com/lakehq/sail), rebranded and extended with a
streamlined deployment story, a richer CLI, and a roadmap toward a managed
cloud offering.

## Design Principles

- **Single binary, zero runtime deps.** One statically-linked binary covers
  every mode: local server, distributed scheduler, distributed worker, one-shot
  SQL, and benchmark.
- **Drop-in PySpark compatibility.** Implements the Spark Connect gRPC protocol
  so existing PySpark code works without modification.
- **Columnar + vectorized execution.** Apache Arrow in-memory format +
  DataFusion's SIMD-accelerated operators replace Spark's row-based JVM model.
- **No GC pauses.** Rust ownership eliminates stop-the-world garbage collection.

---

## Query Execution Pipeline

```
PySpark / SQL client
       │
       │ Spark Connect (gRPC / protobuf)
       ▼
┌─────────────────────────────┐
│  sail-spark-connect         │  Deserialise Relation/Plan proto
│  (gRPC server — tonic)      │
└────────────┬────────────────┘
             │ Unresolved logical plan
             ▼
┌─────────────────────────────┐
│  sail-sql-parser            │  nom/chumsky SQL → AST
│  sail-sql-analyzer          │  Name resolution, type checking
└────────────┬────────────────┘
             │ Resolved AST
             ▼
┌─────────────────────────────┐
│  sail-planner               │  AST → DataFusion LogicalPlan
│  sail-logical-optimizer     │  40+ optimisation rules
│                             │  (predicate pushdown, projection
│                             │   pruning, join reorder, const fold)
└────────────┬────────────────┘
             │ Optimised logical plan
             ▼
┌─────────────────────────────┐
│  sail-physical-plan         │  Selects operators:
│  sail-physical-optimizer    │  HashJoin vs SortMergeJoin,
│                             │  local vs distributed aggregation
└────────────┬────────────────┘
             │ Physical plan (DAG of stages)
             ▼
┌─────────────────────────────┐
│  sail-execution             │  DataFusion RecordBatch streaming
│  (DataFusion engine)        │  8192 rows/batch, SIMD via Arrow
└────────────┬────────────────┘
             │  [if distributed]
             ▼
┌─────────────────────────────┐
│  sail-flight                │  Arrow Flight RPC shuffle between
│  (shuffle transport)        │  workers; spills to object store
└────────────┬────────────────┘
             │ Arrow IPC result batches
             ▼
       gRPC response → client
```

---

## Crate Map

| Crate | Role |
|---|---|
| `sail-cli` | Single binary entrypoint (`ignite`). Clap CLI with `server`, `sql`, `run`, `shell`, `bench`, `cluster`, `flight` subcommands. |
| `sail-spark-connect` | Spark Connect gRPC server (tonic). Deserialises Spark Connect proto messages. |
| `sail-sql-parser` | SQL parser (chumsky + custom grammar). Produces an AST from SQL strings. |
| `sail-sql-analyzer` | Name resolution, type inference, semantic analysis. |
| `sail-plan` | Converts analysed AST → DataFusion `LogicalPlan`. |
| `sail-plan-lakehouse` | Lakehouse-specific planning (Delta Lake, Iceberg table routing). |
| `sail-logical-optimizer` | Rule-based logical optimiser passes. |
| `sail-physical-plan` | Physical plan selection (join strategy, aggregation mode). |
| `sail-physical-optimizer` | Physical optimiser rules (e.g., partition pruning). |
| `sail-execution` | DataFusion execution engine integration. Streams `RecordBatch` chunks. |
| `sail-flight` | Arrow Flight RPC server + client. Used for shuffle and Flight SQL. |
| `sail-session` | Session state management (config, temp views, UDF registry). |
| `sail-catalog` | Catalog abstraction trait. |
| `sail-catalog-memory` | In-memory catalog (dev/test). |
| `sail-catalog-system` | Built-in system catalog (`spark_catalog`). |
| `sail-catalog-iceberg` | Apache Iceberg REST catalog client. |
| `sail-catalog-hms` | Hive Metastore Thrift client. |
| `sail-catalog-unity` | Databricks Unity Catalog REST client. |
| `sail-catalog-glue` | AWS Glue Data Catalog client. |
| `sail-catalog-onelake` | Microsoft OneLake / Fabric catalog client. |
| `sail-delta-lake` | Delta Lake read/write via delta-rs. |
| `sail-iceberg` | Apache Iceberg table format operations. |
| `sail-object-store` | Unified storage layer (S3, GCS, Azure, HDFS, local) via `object_store`. |
| `sail-data-source` | Parquet, ORC, CSV, JSON, Arrow IPC readers/writers. |
| `sail-python` | Python interpreter embedding (PyO3). |
| `sail-python-udf` | Python UDF / UDAF / UDTF bridge. Zero-copy Arrow batch passing. |
| `sail-function` | Built-in Spark SQL functions (matches Spark's function surface). |
| `sail-common` | Shared types, config, error types. |
| `sail-common-datafusion` | DataFusion extension helpers shared across crates. |
| `sail-telemetry` | OpenTelemetry tracing + metrics. |
| `sail-gold-test` | Gold test harness for SQL compatibility tests. |
| `sail-sql-macro` | Proc macros for SQL test generation. |
| `sail-build-scripts` | Shared build.rs utilities (protobuf codegen). |
| `sail-cache` | Shared caching utilities (moka). |

---

## Binary Modes

```
ignite server                          # Spark Connect server (local dev)
ignite sql "SELECT 1 + 1"             # One-shot SQL
ignite run -f job.py                  # Run PySpark script
ignite shell                          # Interactive PySpark shell
ignite bench --scale-factor 10        # TPC-H benchmark
ignite cluster --role=scheduler       # Distributed scheduler (Phase 2)
ignite cluster --role=worker \        # Distributed worker (Phase 2)
  --scheduler scheduler:7070
ignite flight server                  # Arrow Flight SQL server
ignite mcp-server                     # Spark MCP server
```

---

## Key Dependencies

| Dependency | Version | Purpose |
|---|---|---|
| `datafusion` | 53.1 | Query planning + vectorized execution |
| `arrow` / `arrow-flight` | 58.1 | Columnar in-memory format + shuffle RPC |
| `object_store` | 0.13 | Unified S3/GCS/Azure/local storage |
| `tonic` | 0.14 | gRPC server (Spark Connect protocol) |
| `pyo3` | 0.28 | Python UDF bridge |
| `delta-rs` | via sail-delta-lake | Delta Lake table format |
| `iceberg-rust` | via sail-catalog-iceberg | Apache Iceberg |
| `tokio` | 1.52 | Async runtime |
| `clap` | 4.6 | CLI argument parsing |

---

## Phase Roadmap

| Phase | Scope | Target |
|---|---|---|
| **Phase 1** (Months 1–6) | Single-node, local mode, SQL compatibility, TPC-H benchmark, PyPI package | v0.1.0 |
| **Phase 2** (Months 7–12) | Distributed scheduler + workers, Arrow Flight shuffle, Structured Streaming, catalog integrations | v0.3.0 |
| **Phase 3** (Months 13–24) | Managed cloud (ignite.cloud), auto-scaling, GPU execution, MLflow compatibility | v1.0.0 |

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). The existing sail gold-test suite is the
primary correctness signal — run it with `cargo test -p sail-gold-test`.
