# TPC-H SF-1 — Zelox vs Apache Spark (head-to-head)

Reproducible single-node benchmark: all 22 TPC-H queries, scale factor 1
(~1 GB), identical Parquet input and identical query SQL, run on the same
machine with both engines warm and 4-way parallelism.

## Result

| Engine | Build | Total (22q) | Avg/query | Passed | Speedup |
|---|---|---|---|---|---|
| **Zelox** | release (thin LTO) | **1.780 s** | 0.081 s | 22/22 | **~36×** |
| Apache Spark 3.5.3 | JVM (Java 8) | 63.463 s | 2.885 s | 22/22 | 1× |

## Per-query time (seconds)

| Q | Zelox | Spark | | Q | Zelox | Spark |
|---|---|---|---|---|---|---|
| 1 | 0.14 | 3.84 | | 12 | 0.09 | 1.55 |
| 2 | 0.03 | 2.28 | | 13 | 0.07 | 3.77 |
| 3 | 0.06 | 3.00 | | 14 | 0.05 | 0.74 |
| 4 | 0.04 | 2.86 | | 15 | 0.06 | 1.46 |
| 5 | 0.08 | 3.72 | | 16 | 0.07 | 1.08 |
| 6 | 0.04 | 0.64 | | 17 | 0.15 | 2.39 |
| 7 | 0.09 | 3.24 | | 18 | 0.17 | 7.65 |
| 8 | 0.08 | 2.40 | | 19 | 0.11 | 0.74 |
| 9 | 0.10 | 5.82 | | 20 | 0.06 | 1.02 |
| 10 | 0.10 | 4.93 | | 21 | 0.12 | 7.54 |
| 11 | 0.03 | 1.49 | | 22 | 0.02 | 1.33 |

## How to reproduce

```bash
# 1. Build release Zelox
CARGO_PROFILE_RELEASE_LTO=thin cargo build --release -p zelox-cli

# 2. Start the server (see scripts/spark-tests/run-server.sh for full env)
target/release/zelox server -C <pyspark_dir>

# 3. Zelox (warm), generating SF-1 data
SPARK_REMOTE=sc://localhost:50051 TPCH_SF=1 TPCH_DATA_DIR=/tmp/tpch_sf1 \
  TPCH_WARMUP=1 python scripts/tpch_distributed.py

# 4. Reference Apache Spark on the SAME data (classic JVM, local master)
SPARK_REMOTE=local[4] TPCH_SF=1 TPCH_DATA_DIR=/tmp/tpch_sf1 \
  TPCH_SKIP_GENERATE=1 TPCH_WARMUP=1 python scripts/tpch_distributed.py
```

## Caveats / next proof points
- SF-1 (~1 GB) is small; the moat must also be shown at **SF-100 distributed**
  (10-node K8s, see PRODUCTION_ROADMAP.md §5.2) and on **ClickBench**.
- Reference is Apache Spark **3.5.3** (the standard production line); a Spark 4.x
  comparison is a follow-up.
- Both engines read the same DuckDB-generated Parquet; both warm; `local[4]`.
