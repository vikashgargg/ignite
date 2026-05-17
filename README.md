# Ignite

**A Rust-native, single-binary Spark engine.**

Run your existing PySpark code 4–8× faster. No JVM. No cluster setup. One binary.

[![CI](https://github.com/vikashgargg/ignite/actions/workflows/ignite-ci.yml/badge.svg)](https://github.com/vikashgargg/ignite/actions/workflows/ignite-ci.yml)

```sh
curl https://raw.githubusercontent.com/vikashgargg/ignite/main/install.sh | sh
```

---

## Why Ignite?

| | Apache Spark | Ignite |
|---|---|---|
| Runtime | JVM + Python | Single Rust binary |
| Cold start | 30–120 seconds | < 500 ms |
| Min memory | 2–4 GB | < 10 MB idle |
| Install | JDK + Hadoop + PySpark | `curl \| sh` |
| GC pauses | Yes | No (Rust ownership) |
| Execution | Row-based (Volcano) | Columnar + SIMD (Arrow) |
| PySpark compat | ✅ | ✅ (Spark Connect protocol) |

---

## Quick Start

### Install

```sh
# Linux / macOS (x86_64 or arm64)
curl https://raw.githubusercontent.com/vikashgargg/ignite/main/install.sh | sh

# macOS via Homebrew (coming soon)
brew install ignite
```

### Start the server

```sh
ignite server
# Spark Connect server running at localhost:50051
```

### Connect with PySpark (unchanged code)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .remote("sc://localhost:50051") \
    .getOrCreate()

df = spark.read.parquet("s3://my-bucket/data/")
df.groupBy("region").agg({"revenue": "sum"}).show()
```

### One-shot SQL

```sh
ignite sql "SELECT 1 + 1 AS result"
```

### Run a PySpark script

```sh
ignite run -f my_job.py
```

### Run TPC-H benchmark

```sh
ignite bench --scale-factor 10
```

---

## CLI Reference

```
ignite server                          Start local Spark Connect server
ignite sql "<query>"                   Execute SQL and print results
ignite run -f <script.py>             Run a PySpark script
ignite shell                           Interactive PySpark shell
ignite bench [--scale-factor N]       TPC-H benchmark self-test
ignite cluster --role=scheduler       Distributed scheduler (Phase 2)
ignite cluster --role=worker \        Distributed worker (Phase 2)
  --scheduler <host:port>
ignite flight server                   Arrow Flight SQL server
ignite mcp-server                      Spark MCP server
```

---

## Spark Compatibility Status

Phase 1 compat work tracked in [COMPAT.md](COMPAT.md).

| Feature | Status |
|---|---|
| `SELECT`, `JOIN`, `GROUP BY`, `ORDER BY`, window functions | ✅ |
| `DELETE FROM` / `UPDATE SET` (Delta Lake) | ✅ |
| `INSERT INTO` / `INSERT OVERWRITE` | ✅ |
| `CREATE TABLE` / `DROP TABLE` / `ALTER TABLE` | ✅ |
| Persistent tables — MANAGED vs EXTERNAL | ✅ |
| `monotonically_increasing_id()` in aggregates | ✅ |
| `FILTER (WHERE ...)` in aggregate functions | ✅ |
| Python UDFs (Arrow + non-Arrow) | ✅ |
| Pandas UDFs / Arrow batch UDFs | ✅ |
| Delta Lake DML (merge, vacuum, history) | ✅ |
| JSON `_corrupt_record` (PERMISSIVE mode) | 🔄 Phase 1 |
| Structured Streaming (`readStream`) | 📅 Phase 2 |

---

## Memory Target

Ignite is designed to run a full PySpark workload in **≤ 1 GB RAM**:

| Component | Configuration |
|---|---|
| DataFusion sort spill threshold | 256 MB |
| Arrow batch size | 8 192 rows |
| Execution partition count | `2 × CPU cores` |
| JVM overhead | **0** (no JVM) |

Set `IGNITE_MEMORY_LIMIT=1g` to enforce the limit at the process level.

---

## Storage Support

- AWS S3 (instance profile + explicit credentials)
- Cloudflare R2 (S3-compatible)
- Google Cloud Storage
- Azure Blob Storage / ADLS Gen2
- Local filesystem
- Apache Hadoop HDFS

## Table Formats

- **Delta Lake** — full read + write
- **Apache Iceberg** — read + REST catalog
- **Parquet, ORC, CSV, JSON, Arrow IPC**

## Catalog Support

- Hive Metastore (Thrift)
- Apache Iceberg REST (Glue, Nessie, Polaris)
- Databricks Unity Catalog
- AWS Glue Data Catalog
- Microsoft OneLake / Fabric

---

## Build from Source

```sh
# Prerequisites: Rust 1.95+, protoc, Python 3.8+
git clone https://github.com/vikashgargg/ignite
cd ignite

# Development build
cargo build -p sail-cli

# Production static binary (Linux)
cargo build --release -p sail-cli --target x86_64-unknown-linux-musl

./target/x86_64-unknown-linux-musl/release/ignite --version
```

---

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full query pipeline, crate map,
and phase roadmap.

**Stack:**
- [Apache DataFusion](https://github.com/apache/datafusion) — vectorized query engine
- [Apache Arrow](https://github.com/apache/arrow-rs) — columnar in-memory format
- [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) — shuffle transport
- [PyO3](https://github.com/PyO3/pyo3) — Python UDF bridge
- [tonic](https://github.com/hyperium/tonic) — gRPC (Spark Connect protocol)
- [delta-rs](https://github.com/delta-io/delta-rs) — Delta Lake

---

## Roadmap

| Phase | Timeline | Goal |
|---|---|---|
| Phase 1 | Months 1–6 | Single-node, SQL compatibility, TPC-H benchmark, v0.1.0 |
| Phase 2 | Months 7–12 | Distributed mode, Structured Streaming, v0.3.0 |
| Phase 3 | Months 13–24 | Managed cloud (ignite.cloud), GPU, v1.0.0 |

---

## License

Apache 2.0. Built on [lakehq/sail](https://github.com/lakehq/sail).
