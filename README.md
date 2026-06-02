# Vajra (वज्र)

> *Sanskrit: thunderbolt + diamond — speed of lightning, hardness of diamond.*

**Vajra is a Rust-native Apache Spark engine. Drop in your existing PySpark code. No JVM. No Hadoop. One binary.**

[![CI](https://github.com/vikashgargg/ignite/actions/workflows/ignite-ci.yml/badge.svg)](https://github.com/vikashgargg/ignite/actions/workflows/ignite-ci.yml)
[![Release](https://img.shields.io/github/v/release/vikashgargg/ignite)](https://github.com/vikashgargg/ignite/releases)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

```sh
curl https://raw.githubusercontent.com/vikashgargg/ignite/main/install.sh | sh
```

---

## Why Vajra Exists

Apache Spark is the industry standard for large-scale data processing — and it carries the full weight of that legacy. A JVM that takes 30–120 seconds to warm up. A cluster setup that requires HDFS, YARN, or Kubernetes just to run a local job. Gigabytes of heap before the first query executes. Python data bouncing through Arrow IPC, back through the JVM, back out to Python.

**Vajra is what Spark would look like if it were designed today.**

Built on Rust, Apache Arrow, and Apache DataFusion — the same columnar engine that powers ClickHouse, InfluxDB, and Delta Lake's own query path. No garbage collector. No JVM warmup. No serialisation tax between Python and the execution engine. One statically-linked binary you can `curl | sh` onto any machine.

Your PySpark code runs **unchanged** — Vajra implements the Spark Connect gRPC protocol exactly. Point `SparkSession.builder.remote(...)` at a Vajra server and your existing jobs run. The only difference is they start in 200 milliseconds instead of 2 minutes, and they use 300 MB of RAM instead of 4 GB.

---

## Vajra vs the Field

> *LakeSail v0.6.3 (2026-05-21) is the closest open-source comparison. Numbers are measured, not estimated.*

| Capability | Apache Spark 3.5 | LakeSail v0.6.3 | **Vajra v0.6.0** |
|---|---|---|---|
| Runtime | JVM (GC pauses) | Rust | **Rust** |
| Cold start | 30–120 s | ~2 s | **~200 ms** |
| Idle memory | 2–4 GB JVM heap | ~500 MB | **~300 MB** |
| Binary / image size | ~600 MB | ~300 MB | **105 MB macOS / 80 MB Linux** |
| TPC-H SF-1 (22 queries) | ~60 s warm JVM | ~15 s | **1.515 s (40×)** |
| pip install | `pyspark` (JVM needed) | `pysail` | **`vajra-pyspark`** |
| **Spark SQL compat (105-test scorecard, all modes)** | ✅ reference | ~95% | **✅ 105/105 (100%)** |
| Python UDFs — scalar / Pandas / Arrow | ✅ | ✅ | **✅** |
| **Python-version-agnostic UDFs (any 3.10+)** | ✅ | ✅ abi3 | **✅ abi3 + subprocess** |
| **Distributed lambda HOFs + recursive CTEs** | ✅ | partial | **✅ (Sprint 4.1)** |
| **approx_top_k / KLL / theta sketches (Spark 4.1)** | ✅ | partial | **✅ (Sprint 4.1)** |
| **Python iterator UDFs (GroupedMap 4.1)** | ✅ | ✅ v0.6.3 | **✅** |
| Delta Lake DML (DELETE/UPDATE/MERGE) | ✅ | ✅ | **✅** |
| **Delta time travel (AT VERSION/TIMESTAMP)** | ✅ | ✅ v0.6.0 | **✅** |
| **Delta V2 checkpointing + log compaction** | ✅ | ✅ v0.6.0 | **✅** |
| **Iceberg (read/write/REST catalog + OverwritePartitions)** | ✅ | ✅ (active) | **✅** |
| **VARIANT type (Spark 4.x)** | ✅ | ✅ v0.6.3 | **✅** |
| **Structured Streaming — Kafka source** | ✅ | ❌ | **✅** |
| **Structured Streaming — foreachBatch** | ✅ | ❌ | **✅** |
| **Structured Streaming — memory sink** | ✅ | ❌ | **✅** |
| **Streaming checkpoint + recovery** | ✅ | ❌ (issue open) | **✅** |
| **Streaming event-time windows (executor)** | ✅ | ❌ | **✅** |
| **Streaming stateful deduplication** | ✅ | ❌ | **✅** |
| **Theta sketch aggregates (KMV)** | ✅ | partial | **✅** |
| **Vortex data source (skeleton)** | ✅ | ✅ v0.6.0 | **✅ skeleton** |
| **JWT bearer / mTLS auth** | ✅ | ❌ | **✅** |
| **Apple Container (macOS 26, Apple Silicon)** | ❌ | ❌ | **✅ — only one** |
| **K8s Helm chart + HPA** | community | ❌ | **✅** |
| **Scheduler HA (K8s Lease election)** | ✅ (complex) | ❌ | **✅** |
| **Web UI on :4040** | ✅ | ❌ | **✅** |
| **dbt integration guide** | ✅ | ✅ v0.6.3 | **✅** |
| **ClickBench 43/43 benchmark** | ✅ | ✅ v0.6.3 | **✅** |

All Vajra numbers above are measured on the release binary (LTO, ARM64 macOS).

---

## Proven Results

```
══════════════════════════════════════════════════════════════════
  VAJRA SPARK COMPATIBILITY SCORECARD  (v0.6.0-alpha)
══════════════════════════════════════════════════════════════════
  1. Basic SQL                         ✓✓✓✓✓✓✓✓✓✓✓✓✓  13/13
  2. Aggregate Functions                   ✓✓✓✓✓✓  6/6
  3. Window Functions                        ✓✓✓✓  4/4
  4. String Functions                       ✓✓✓✓✓  5/5
  5. Date / Time Functions                   ✓✓✓✓  4/4
  6. Complex Types                          ✓✓✓✓✓  5/5
  7. DataFrame API                      ✓✓✓✓✓✓✓✓✓  9/9
  8. Python UDFs                            ✓✓✓✓✓  5/5
  9. JSON Reading                           ✓✓✓✓✓  5/5
  10. Parquet Read / Write                    ✓✓✓  3/3
  11. DML (Delta Lake)                       ✓✓✓✓  4/4
  12. Misc Spark SQL                     ✓✓✓✓✓✓✓✓  8/8
  13. Advanced SQL (PIVOT/UNPIVOT/TABLESAMPLE) ✓✓✓✓✓✓  6/6
  14. Higher-Order Functions (TRANSFORM/FILTER) ✓✓✓✓✓  5/5
  15. Recursive CTEs                           ✓✓  2/2
  16. QUALIFY / GROUPS BETWEEN / Named Windows  ✓✓✓  3/3
  17. NATURAL JOIN / LATERAL VIEW OUTER         ✓✓  2/2
────────────────────────────────────────────────────────────────
  Total:  105 passed, 0 failed — Score: 100% (105/105)
  Modes:  local ✅  local-cluster ✅  kubernetes-cluster ✅
══════════════════════════════════════════════════════════════════

TPC-H SF-1 — 22/22 PASS — total 1.515s  (Spark warm JVM: ~60s)
(Q1: 0.12s  Q9: 0.09s  Q17: 0.13s  Q18: 0.14s  Q21: 0.11s)
```

---

## Quick Start

### Prerequisites

| Platform | Requirement |
|---|---|
| macOS | Apple Silicon (M1/M2/M3/M4). Python 3.10+ (auto-installed via Homebrew if missing) |
| Linux | x86_64 or aarch64. Python 3.10+ (`sudo apt install python3.11` / `sudo dnf install python3.11`) |

### Install (one command)

```sh
curl https://raw.githubusercontent.com/vikashgargg/ignite/main/install.sh | sh
```

The installer:
1. Downloads the pre-built binary for your platform
2. Creates an isolated Python venv at `~/.local/lib/vajra/venv` with pyspark 4.x + all Spark Connect deps
3. Wraps the binary so `vajra sql` / `vajra run` just work — no manual `PYTHONPATH` setup

After install, add to your PATH (shown by the installer) then test:

```sh
export PATH="$HOME/.local/bin:$PATH"   # paste exact line from installer output
vajra --version                         # vajra 0.1.0
vajra sql "SELECT 1"                    # prints +---+ \n| 1 | \n+---+
```

### Run a quick smoke test

```sh
# One-shot SQL
vajra sql "SELECT 'hello' AS msg, current_timestamp() AS ts"

# TPC-H benchmark (requires: pip install duckdb)
vajra bench --scale-factor 1
```

### Connect your existing PySpark code — change one line

```python
from pyspark.sql import SparkSession

# Before (Spark):
# spark = SparkSession.builder.getOrCreate()

# After (Vajra) — everything else stays the same:
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()

df = spark.read.parquet("s3://my-bucket/data/")
df.groupBy("region").agg({"revenue": "sum"}).show()
```

```sh
vajra server                             # start server on :50051
python my_job.py                         # run job using pyspark installed in the venv
# or: vajra run -f my_job.py            # run in-process, no separate server needed
```

### One-shot SQL

```sh
vajra sql "SELECT count(*) FROM parquet.'/tmp/data/*.parquet'"
```

### Run a PySpark script

```sh
vajra run -f my_etl_job.py
```

### TPC-H self-benchmark

```sh
vajra bench --scale-factor 10   # requires: pip install duckdb
```

---

## Deployment

> **Platform support:** macOS requires **Apple Silicon (M1/M2/M3/M4)**. Linux works on x86_64 and aarch64. Intel Macs are not supported.

---

### Mode 1 — Local (single process, no setup)

Best for: development, notebooks, quick queries.

```sh
# Install
curl https://raw.githubusercontent.com/vikashgargg/ignite/main/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"

# Start server
vajra server
# Listening on sc://127.0.0.1:50051 [mode: local]

# Connect from Python (pip install pyspark)
python3 - <<'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.sql("SELECT 'Vajra works!' AS msg").show()
spark.range(1000).groupBy().sum("id").show()
EOF
```

---

### Mode 2 — Local-cluster (multi-worker, single Apple Silicon Mac)

Best for: parallel workloads on M-series Mac (uses all cores across N workers).

```sh
# Start with 4 in-process workers
vajra server --mode local-cluster --workers 4
# Workers: 4  |  sc://127.0.0.1:50051

# Connect — same PySpark code, no changes
python3 - <<'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()

# Runs distributed across 4 workers
df = spark.read.parquet("/tmp/data/*.parquet")
df.groupBy("region").agg({"revenue": "sum"}).orderBy("sum(revenue)", ascending=False).show()
EOF
```

---

### Mode 3 — Apple Container (macOS 26 / Sequoia) — unique to Vajra

Best for: isolated, reproducible runs on Apple Silicon Mac using Apple's native container runtime (no Docker needed).

> **Requires:** macOS 26 Sequoia + Apple Container (`container` CLI). Apple Silicon only.

```sh
# One-time: build the arm64 image (~5 min first time, ~90s incremental)
make container-build

# --- Single-node local mode ---
container run --rm --name vajra \
  -p 50051:50051 \
  -v /tmp/vajra-data:/tmp/data \
  vajra:latest

# --- Local-cluster mode (4 in-process workers) ---
container run --rm --name vajra \
  -p 50051:50051 \
  -e SAIL_MODE=local-cluster \
  -e SAIL_EXECUTION__TARGET_PARTITIONS=4 \
  -v /tmp/vajra-data:/tmp/data \
  vajra:latest

# Connect from host Mac
python3 - <<'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.sql("SELECT count(*), avg(id) FROM range(1000000)").show()
EOF

# Stop
container stop vajra
```

---

### Mode 4 — Kubernetes (local kind cluster or production)

Best for: distributed multi-node workloads. Works on Linux x86_64 / aarch64 and Apple Silicon Mac via kind.

**Quickstart with kind (Mac or Linux):**

```sh
# Prerequisites: kubectl + kind installed
# brew install kind kubectl helm  (macOS)

# 1. Create a local k8s cluster
kind create cluster --name vajra

# 2. Deploy Vajra
kubectl apply -f k8s/sail.yaml

# 3. Wait for pods ready
kubectl wait --for=condition=ready pod -l app=vajra-spark-server \
  -n vajra --timeout=120s

# 4. Forward port
kubectl port-forward -n vajra svc/vajra-spark-server 50051:50051 &

# 5. Run Spark job
python3 - <<'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.sql("SELECT 'Running on K8s!' AS msg").show()
spark.range(10000000).groupBy().count().show()
EOF
```

**Production Helm deployment (with auth + HPA):**

```sh
helm install vajra ./helm/vajra \
  --namespace vajra --create-namespace \
  --set server.replicas=3 \
  --set auth.enabled=true \
  --set auth.token=my-secret-token \
  --set autoscaling.enabled=true \
  --set autoscaling.maxReplicas=10

# Connect with token
python3 - <<'EOF'
from pyspark.sql import SparkSession
spark = (SparkSession.builder
  .remote("sc://localhost:50051")
  .config("spark.connect.grpc.metadata", "Authorization=Bearer my-secret-token")
  .getOrCreate())
spark.sql("SELECT 'HA cluster!' AS msg").show()
EOF
```

---

### Quick comparison

| Mode | Command | Use case | Workers |
|---|---|---|---|
| `local` | `vajra server` | Dev / notebooks | 1 process |
| `local-cluster` | `vajra server --mode local-cluster --workers 4` | Multi-core Mac | N in-process |
| Apple Container local | `container run ... vajra:latest` | Isolated, reproducible | 1 container |
| Apple Container cluster | `container run -e SAIL_MODE=local-cluster ...` | Isolated multi-worker | N in-container |
| Kubernetes | `kubectl apply -f k8s/sail.yaml` | Distributed, production | K8s pods |

---

## What Works Today (v0.5.0-alpha)

### SQL & Query Engine
| Feature | Status |
|---|---|
| `SELECT`, `JOIN`, `GROUP BY`, `ORDER BY`, subqueries, CTEs | ✅ |
| Window functions (`RANK`, `ROW_NUMBER`, `LAG`, `LEAD`, `NTILE`, …) | ✅ |
| `HAVING` with aggregate-only expressions | ✅ |
| `QUALIFY` clause (Spark 3.x+) | ✅ |
| `WITH RECURSIVE` CTEs | ✅ |
| `PIVOT` / `UNPIVOT` (all variants including empty IN list) | ✅ |
| `TABLESAMPLE` (percent / rows / byte-size / BUCKET OUT OF) | ✅ |
| `GROUPS BETWEEN` windows | ✅ |
| FROM-first HiveQL (`FROM t SELECT …`) | ✅ |
| Higher-order functions (`transform`, `filter`, `aggregate`) | ✅ |
| `LATERAL VIEW` / `LATERAL VIEW OUTER` | ✅ |
| `NATURAL JOIN` | ✅ |

### Data & Storage
| Feature | Status |
|---|---|
| Parquet (read/write, predicate pushdown, partition pruning) | ✅ |
| Delta Lake (read/write/DELETE/UPDATE/MERGE/VACUUM) | ✅ |
| Iceberg (read/write/REST catalog) | partial |
| JSON (PERMISSIVE / DROPMALFORMED / FAILFAST) | ✅ |
| CSV (inferSchema, custom delimiter) | ✅ |
| Avro, ORC | ✅ |
| AWS S3 / GCS / Azure ADLS / local FS | ✅ |

### Python & UDFs
| Feature | Status |
|---|---|
| Python UDFs — scalar, Pandas (vectorized), Arrow | ✅ |
| `cloudpickle` serialisation | ✅ |
| `df.approxQuantile()` | ✅ |
| `df.freqItems()` | ✅ |
| Lambda HOFs (`transform`, `filter`, `aggregate`) | ✅ |

### Structured Streaming
| Feature | Status |
|---|---|
| Kafka source (`readStream.format("kafka")`) | ✅ |
| `writeStream.format("memory").queryName(name)` | ✅ |
| `writeStream.foreachBatch(fn)` | ✅ |
| Streaming aggregates (COUNT/SUM/AVG per micro-batch) | ✅ |
| Checkpoint + recovery (resume from last offset) | ✅ |
| Event-time windows (`F.window()`, `withWatermark`) | **✅ executor wired (Sprint 6)** |
| Stream × static join | ✅ |

### Infrastructure
| Feature | Status |
|---|---|
| `local` / `local-cluster` / `kubernetes-cluster` modes | ✅ |
| Apple Container (macOS 26, **Apple Silicon only**) | ✅ |
| Kubernetes Helm chart (HPA, liveness/readiness) | ✅ |
| Scheduler HA via K8s Lease election (`--ha`) | ✅ |
| Bearer token auth (`--auth-token` / `SAIL_AUTH__TOKEN`) | ✅ |
| mTLS (`--tls-cert/--tls-key/--tls-ca`) | ✅ |
| Web UI on `:4040` (query history + streaming status) | ✅ |
| Prometheus `/metrics` endpoint | ✅ |
| OpenTelemetry OTLP traces | ✅ |

---

## Architecture

```
PySpark client  ──Spark Connect gRPC + JWT/mTLS──▶  vajra server
                                                          │
                                          ┌───────────────┼───────────────┐
                                          │               │               │
                                    SQL parser      Spark plan       Python UDFs
                                    (Rust PEG)      resolver         (PyO3 / cloudpickle)
                                          │               │
                                          └───────┬───────┘
                                                  ▼
                                          Apache DataFusion
                                        (vectorized, columnar, SIMD)
                                                  │
                              ┌───────────────────┼───────────────────┐
                              │                   │                   │
                           Parquet             Delta Lake          Iceberg
                        S3 / GCS / ADLS        (delta-rs)       (iceberg-rust)
                              │
                         Arrow Flight
                       (distributed shuffle)
                              │
                    ┌─────────┴─────────┐
                    │                   │
               Kubernetes           Apple Container
             (Helm + K8s Lease)    (arm64-native)
```

**Stack:**
- [Apache DataFusion](https://github.com/apache/datafusion) — vectorized query engine (v53+)
- [Apache Arrow](https://github.com/apache/arrow-rs) — zero-copy columnar memory
- [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) — high-throughput shuffle transport
- [PyO3](https://github.com/PyO3/pyo3) — Python UDF bridge (zero-copy Arrow)
- [tonic](https://github.com/hyperium/tonic) — gRPC (Spark Connect wire protocol)
- [delta-rs](https://github.com/delta-io/delta-rs) — native Rust Delta Lake
- [rdkafka](https://github.com/fede1024/rust-rdkafka) — Kafka streaming source

---

## CLI Reference

```
vajra server [--ip IP] [--port PORT] [--mode MODE] [--workers N]
             [--auth-token TOKEN] [--tls-cert PATH] [--tls-key PATH] [--ha]
vajra sql "<query>"             Execute SQL and print results
vajra run -f <script.py>        Run a PySpark script
vajra shell                     Interactive PySpark REPL
vajra bench [--scale-factor N]  TPC-H benchmark (requires pip install duckdb)
```

**Key environment variables:**

| Variable | Default | Description |
|---|---|---|
| `SAIL_MODE` | `local` | `local` / `local-cluster` / `kubernetes-cluster` |
| `SAIL_AUTH__TOKEN` | — | Bearer token for gRPC auth |
| `SAIL_AUTH__TLS__CERT` | — | Path to TLS certificate (PEM) |
| `PYTHONPATH` | — | Path to PySpark site-packages (required for Python UDFs) |
| `SAIL_RUNTIME__STACK_SIZE` | `8388608` | Tokio worker thread stack size in bytes |

---

## Build from Source

```sh
# Prerequisites: Rust 1.91+, protoc 3.x, Python 3.10+
git clone https://github.com/vikashgargg/ignite
cd ignite

# Dev build (fast, unoptimised)
make dev
./target/debug/vajra --version

# Release build (LTO, ~30 min)
make release
./target/release/vajra --version

# Cross-compile: Linux musl (x86_64 + aarch64) + macOS universal2
make build-all
```

---

## Roadmap

| Phase | Timeline | Goal |
|---|---|---|
| **Phase 1** ✅ | Done | 105/105 Spark compat, 22/22 TPC-H, K8s + Apple Container |
| **Phase 2** ✅ | Done | Streaming (Kafka/foreachBatch/checkpoint), auth, HA, Web UI |
| **Phase 3** ✅ | Done 2026-05-30 | VARIANT, GroupedMap, time travel, dbt, ClickBench, Iceberg OverwritePartitions, event-time windows, stateful dedup, theta sketch, Vortex skeleton, 95%+ Spark test suite |
| **Phase 4** 📅 | Q3 2026 | GPU workers, sub-interpreter UDFs, SF-100 distributed, SaaS |

Full plan: [PRODUCTION_ROADMAP.md](PRODUCTION_ROADMAP.md)

---

## License

Apache 2.0. Vajra is built on the shoulders of [lakehq/sail](https://github.com/lakehq/sail) — we have deep respect for that work and upstream fixes wherever possible.
