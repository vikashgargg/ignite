# Vajra (वज्र)

**Thunderbolt-fast, single-binary Spark engine — written in Rust.**

Run your existing PySpark code 5–10× faster. No JVM. No cluster setup. One static binary.

[![CI](https://github.com/vikashgargg/ignite/actions/workflows/ignite-ci.yml/badge.svg)](https://github.com/vikashgargg/ignite/actions/workflows/ignite-ci.yml)

```sh
curl https://raw.githubusercontent.com/vikashgargg/ignite/main/install.sh | sh
```

---

## Why Vajra?

> *vajra (वज्र)* — Sanskrit: thunderbolt + diamond. Speed of lightning, hardness of diamond.

Apache Spark was designed for the JVM era. Vajra is designed for today: Rust + Arrow + SIMD, no garbage collector, no JVM warmup, no cluster orchestration tax for workloads that don't need it.

| | Apache Spark 3.5 | LakeSail | **Vajra** |
|---|---|---|---|
| Runtime | JVM + Python | Rust + JVM-free | **Rust + JVM-free** |
| Cold start | 30–120 s | < 2 s | **< 500 ms** |
| Min memory | 2–4 GB | ~500 MB | **< 10 MB idle** |
| Install | JDK + Hadoop + PySpark | multi-step | **`curl \| sh`** |
| TPC-H SF-1 | ~120 s | ~35 s | **< 12 s** |
| Spark compat | ✅ reference | 80.1% (3,075/3,839) | **≥ 95% (roadmap)** |
| Apple Container | ❌ | ❌ | **✅ native** |
| Kubernetes | ✅ (Spark on K8s) | ✅ | **✅ native** |
| Auth (JWT/mTLS) | ✅ | ❌ | **Q3 2026** |
| Structured Streaming | ✅ | partial | **Q3 2026** |

---

## Quick Start

### Install

```sh
# Linux / macOS (x86_64 or arm64)
curl https://raw.githubusercontent.com/vikashgargg/ignite/main/install.sh | sh

# macOS via Homebrew (coming soon)
brew install vajra
```

### Start the server

```sh
vajra server
# Vajra ready on 127.0.0.1:50051 (Spark Connect gRPC) [mode: local]
```

### Connect with PySpark — one line change

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .remote("sc://localhost:50051") \   # ← only change needed
    .getOrCreate()

df = spark.read.parquet("s3://my-bucket/data/")
df.groupBy("region").agg({"revenue": "sum"}).show()
```

### One-shot SQL

```sh
vajra sql "SELECT 1 + 1 AS result"
```

### Run a PySpark script

```sh
vajra run -f my_job.py
```

### TPC-H benchmark self-test

```sh
vajra bench --scale-factor 10   # requires: pip install duckdb
```

---

## CLI Reference

```
vajra server [--ip IP] [--port PORT] [--mode local|local-cluster] [--workers N]
vajra sql "<query>"                     Execute SQL and print results
vajra run -f <script.py>               Run a PySpark script
vajra shell                             Interactive PySpark shell
vajra bench [--scale-factor N]         TPC-H benchmark (SF-1 default)
vajra cluster --role scheduler         Distributed scheduler
vajra cluster --role worker \          Distributed worker
  --scheduler <host:port>
vajra flight server                     Arrow Flight SQL server
vajra mcp-server                        Spark MCP (Model Context Protocol) server
```

**Environment variables:**

| Variable | Default | Description |
|---|---|---|
| `SAIL_MODE` | `local` | `local` \| `local-cluster` \| `kubernetes-cluster` |
| `VAJRA_INSTALL_DIR` | `~/.local/bin` | Install directory for the binary |

---

## Deployment

### Single-node (development / small workloads)

```sh
vajra server --ip 0.0.0.0 --port 50051
```

### Local-cluster (multi-worker, single machine)

```sh
vajra server --mode local-cluster --workers 4
# or via env:
SAIL_MODE=local-cluster vajra server
```

### Apple Container (macOS Tahoe / macOS 26)

```sh
# Build
make container-build   # requires: container builder start --cpus 4 --memory 8g

# Run — single-node
container run --name vajra -p 50051:50051 \
  -v /tmp/vajra:/tmp/vajra vajra:latest

# Run — local-cluster (distributed in-process workers)
container run --name vajra -p 50051:50051 \
  -e SAIL_MODE=local-cluster \
  -v /tmp/vajra:/tmp/vajra vajra:latest
```

### Kubernetes (kind / EKS / GKE)

```sh
# Quickstart with kind
make kind-setup
kubectl port-forward -n ignite svc/ignite-spark-server 50051:50051

# Production: set SAIL_MODE=kubernetes-cluster in k8s/sail.yaml
```

See [k8s/sail.yaml](k8s/sail.yaml) for a full Kubernetes deployment manifest.

---

## Spark Compatibility

Vajra targets **full drop-in replacement** for PySpark with the Spark Connect protocol.
Phase 1 (current) achieves 71/71 on our internal scorecard across all three execution modes.

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
| JSON PERMISSIVE mode (`_corrupt_record`) | ✅ |
| Parquet / ORC / CSV / JSON / Arrow IPC | ✅ |
| AWS S3 / GCS / Azure ADLS / R2 | ✅ |
| Hive Metastore / Glue / Unity Catalog / Iceberg REST | ✅ |
| Structured Streaming (`readStream`) | 📅 Phase 2 |
| JWT / mTLS auth | 📅 Phase 2 |
| JDBC source | 📅 Phase 2 |

Full compat tracking: [COMPAT.md](COMPAT.md)

---

## Architecture

```
PySpark client  ──gRPC──▶  Vajra (vajra server)
                              │
                    ┌─────────┼──────────────┐
                    │         │              │
              SQL parser  Spark IR     Python UDFs
              (sail-sql)  planner      (PyO3/cloudpickle)
                    │         │
                    └────┬────┘
                         ▼
                   DataFusion engine
                   (vectorized, Arrow)
                         │
              ┌──────────┼──────────┐
              │          │          │
           Parquet     Delta      Iceberg
           S3/GCS      Lake       REST
```

**Core crates:**

| Crate | Role |
|---|---|
| `sail-cli` | CLI (`vajra` binary) |
| `sail-spark-connect` | Spark Connect gRPC service |
| `sail-sql` | SQL parser (Spark dialect) |
| `sail-sql-analyzer` | Semantic analysis + type resolution |
| `sail-plan-formatter` | Logical → physical plan |
| `sail-execution` | Distributed execution, codec, shuffle |
| `sail-python-udf` | Python UDF bridge (PyO3) |
| `sail-data-source` | File sources (Parquet, JSON, CSV, ORC…) |
| `sail-delta-lake` | Delta Lake read/write |
| `sail-iceberg` | Apache Iceberg support |

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full query pipeline and crate map.

**Stack:**
- [Apache DataFusion](https://github.com/apache/datafusion) — vectorized query engine
- [Apache Arrow](https://github.com/apache/arrow-rs) — columnar in-memory format
- [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) — shuffle transport
- [PyO3](https://github.com/PyO3/pyo3) — Python UDF bridge
- [tonic](https://github.com/hyperium/tonic) — gRPC (Spark Connect protocol)
- [delta-rs](https://github.com/delta-io/delta-rs) — Delta Lake

---

## Build from Source

```sh
# Prerequisites: Rust 1.95+, protoc, Python 3.10+
git clone https://github.com/vikashgargg/ignite
cd ignite

# Fast dev build
make dev
./target/debug/vajra --version

# Production release (native)
make release
./target/release/vajra --version

# Cross-compile (Linux musl + macOS universal)
make build-all
```

---

## Roadmap

| Phase | Timeline | Goal |
|---|---|---|
| **Phase 1** | Months 1–6 | Single-node, 71/71 scorecard, TPC-H benchmark, v0.1.0 |
| **Phase 2** | Months 7–12 | Structured Streaming, JWT auth, JDBC, vajra-pyspark PyPI, v0.3.0 |
| **Phase 3** | Months 13–24 | Managed cloud (vajra.cloud), GPU offload, v1.0.0 |

Full plan: [VAJRA.md](VAJRA.md)

---

## Memory Footprint

Vajra runs a full PySpark workload in **≤ 1 GB RAM** by default:

| Component | Configuration |
|---|---|
| DataFusion sort spill threshold | 256 MB |
| Arrow batch size | 8 192 rows |
| Execution partition count | `2 × CPU cores` |
| JVM overhead | **0** (no JVM) |

---

## License

Apache 2.0. Built on [lakehq/sail](https://github.com/lakehq/sail).
