# Zelox — Architecture

Zelox is a Rust-native, single-binary Spark engine. It is a fork of
[lakehq/sail](https://github.com/lakehq/sail), extended with a streamlined
deployment story, a richer CLI, full Spark compatibility fixes, and a roadmap
toward a managed cloud offering.

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
│  zelox-spark-connect         │  Deserialise Relation/Plan proto
│  (gRPC server — tonic)      │
└────────────┬────────────────┘
             │ Unresolved logical plan
             ▼
┌─────────────────────────────┐
│  zelox-sql-parser            │  nom/chumsky SQL → AST
│  zelox-sql-analyzer          │  Name resolution, type checking
└────────────┬────────────────┘
             │ Resolved AST
             ▼
┌─────────────────────────────┐
│  zelox-planner               │  AST → DataFusion LogicalPlan
│  zelox-logical-optimizer     │  40+ optimisation rules
│                             │  (predicate pushdown, projection
│                             │   pruning, join reorder, const fold)
└────────────┬────────────────┘
             │ Optimised logical plan
             ▼
┌─────────────────────────────┐
│  zelox-physical-plan         │  Selects operators:
│  zelox-physical-optimizer    │  HashJoin vs SortMergeJoin,
│                             │  local vs distributed aggregation
└────────────┬────────────────┘
             │ Physical plan (DAG of stages)
             ▼
┌─────────────────────────────┐
│  zelox-execution             │  DataFusion RecordBatch streaming
│  (DataFusion engine)        │  8192 rows/batch, SIMD via Arrow
└────────────┬────────────────┘
             │  [if distributed]
             ▼
┌─────────────────────────────┐
│  zelox-flight                │  Arrow Flight RPC shuffle between
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
| `zelox-cli` | Single binary entrypoint (`zelox`). Clap CLI with `server`, `sql`, `run`, `shell`, `bench`, `cluster`, `flight` subcommands. |
| `zelox-spark-connect` | Spark Connect gRPC server (tonic). Deserialises Spark Connect proto messages. |
| `zelox-sql-parser` | SQL parser (chumsky + custom grammar). Produces an AST from SQL strings. |
| `zelox-sql-analyzer` | Name resolution, type inference, semantic analysis. |
| `zelox-plan` | Converts analysed AST → DataFusion `LogicalPlan`. |
| `zelox-plan-lakehouse` | Lakehouse-specific planning (Delta Lake, Iceberg table routing). |
| `zelox-logical-optimizer` | Rule-based logical optimiser passes. |
| `zelox-physical-plan` | Physical plan selection (join strategy, aggregation mode). |
| `zelox-physical-optimizer` | Physical optimiser rules (e.g., partition pruning). |
| `zelox-execution` | DataFusion execution engine integration. Streams `RecordBatch` chunks. |
| `zelox-flight` | Arrow Flight RPC server + client. Used for shuffle and Flight SQL. |
| `zelox-session` | Session state management (config, temp views, UDF registry). |
| `zelox-catalog` | Catalog abstraction trait. |
| `zelox-catalog-memory` | In-memory catalog (dev/test). |
| `zelox-catalog-system` | Built-in system catalog (`spark_catalog`). |
| `zelox-catalog-iceberg` | Apache Iceberg REST catalog client. |
| `zelox-catalog-hms` | Hive Metastore Thrift client. |
| `zelox-catalog-unity` | Databricks Unity Catalog REST client. |
| `zelox-catalog-glue` | AWS Glue Data Catalog client. |
| `zelox-catalog-onelake` | Microsoft OneLake / Fabric catalog client. |
| `zelox-delta-lake` | Delta Lake read/write via delta-rs. Includes V2 checkpointing, time travel, type widening. |
| `zelox-iceberg` | Apache Iceberg table format operations. Snapshot producer with dynamic partition overwrite (`Operation::OverwritePartitions`), manifest writer with `add_existing()` for retained entries. |
| `zelox-vortex` | Vortex columnar format skeleton. `VortexTableFormat` registered in `TableFormatRegistry`; read/write stubs pending `vortex-datafusion` DataFusion 53.x compat. |
| `zelox-object-store` | Unified storage layer (S3, GCS, Azure, HDFS, local) via `object_store`. |
| `zelox-data-source` | Parquet, ORC, CSV, JSON, Arrow IPC readers/writers. |
| `zelox-python` | Python interpreter embedding (PyO3). |
| `zelox-python-udf` | Python UDF / UDAF / UDTF bridge. Zero-copy Arrow batch passing. |
| `zelox-function` | Built-in Spark SQL functions (matches Spark's function surface). |
| `zelox-common` | Shared types, config, error types. |
| `zelox-common-datafusion` | DataFusion extension helpers shared across crates. |
| `zelox-telemetry` | OpenTelemetry tracing + metrics. |
| `zelox-gold-test` | Gold test harness for SQL compatibility tests. |
| `zelox-sql-macro` | Proc macros for SQL test generation. |
| `zelox-build-scripts` | Shared build.rs utilities (protobuf codegen). |
| `zelox-cache` | Shared caching utilities (moka). |

---

## Streaming Architecture (Sprint 6)

Zelox's structured streaming runs as a micro-batch loop on a background Tokio task. The new stateful components added in Sprint 6:

```
Kafka / Rate source
       │  RecordBatch per micro-batch
       ▼
StreamAggregateExec      ← per-batch aggregate (COUNT/SUM)
       │
WatermarkNode            ← tracks max event-time; advances watermark
       │
WindowAccumNode          ← groups rows into (key, window_start, window_end) tuples
       │
WindowAccumExec          ← stateful; HashMap<(GroupKey, WindowInterval), AccumState>
       │                   emits complete windows when watermark > window_end
StreamDeduplicateExec    ← stateful; HashSet<Vec<ScalarValue>> seen-keys across batches
       │
Memory / Delta / Kafka sink
```

Key design points:
- **State store**: in-memory `HashMap` / `HashSet` keyed by `Vec<ScalarValue>` — no external state backend needed for single-node mode
- **Watermark**: `WatermarkNode` tracks `max(event_time) - allowed_lateness`; `WindowAccumExec` only emits when watermark advances past window end
- **Deduplication**: `StreamDeduplicateExec` holds `HashSet<Vec<ScalarValue>>` across micro-batches; idempotent for at-least-once Kafka delivery

---

## Binary Modes

```
zelox server                          # Spark Connect server (local dev)
zelox sql "SELECT 1 + 1"             # One-shot SQL
zelox run -f job.py                  # Run PySpark script
zelox shell                          # Interactive PySpark shell
zelox bench --scale-factor 10        # TPC-H benchmark
zelox cluster --role=scheduler       # Distributed scheduler (Phase 2)
zelox cluster --role=worker \        # Distributed worker (Phase 2)
  --scheduler scheduler:7070
zelox flight server                  # Arrow Flight SQL server
zelox mcp-server                     # Spark MCP server
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
| `delta-rs` | via zelox-delta-lake | Delta Lake table format |
| `iceberg-rust` | via zelox-catalog-iceberg | Apache Iceberg |
| `tokio` | 1.52 | Async runtime |
| `clap` | 4.6 | CLI argument parsing |

---

## Phase Roadmap

| Phase | Scope | Status |
|---|---|---|
| **Phase 1** ✅ | Single-node, 105/105 SQL compat, 22/22 TPC-H, K8s + Apple Container | v0.3.0-alpha |
| **Phase 2** ✅ | Structured Streaming (Kafka + foreachBatch + checkpoint), JWT/mTLS auth, Helm chart, K8s HA | v0.4.0-alpha |
| **Phase 3** ✅ (2026-05-30) | Sprint 4–6: VARIANT type, GroupedMap, Delta time travel, V2 checkpoint, Iceberg OverwritePartitions, event-time windows, stateful dedup, theta sketch, Vortex skeleton, 95% Spark test suite | v0.5.0-alpha |
| **Phase 4** 📅 (Q3 2026) | TPC-H SF-100 distributed, GPU workers, sub-interpreter UDFs, SaaS | v1.0.0 |

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). The existing sail gold-test suite is the
primary correctness signal — run it with `cargo test -p zelox-gold-test`.
