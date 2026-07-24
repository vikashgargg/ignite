# Zelox Benchmark Results

> Last updated: 2026-07-02
> Tag: **v0.6.0-alpha**
> Machine: macOS Apple Silicon (ARM64) for local; AWS Graviton EKS for head-to-heads
> Build: release (`lto = true, codegen-units = 1`) unless otherwise noted
> Mode: `local` (`SPARK_REMOTE=sc://localhost:50051`) unless otherwise noted

---

## Authoritative head-to-heads (measured, honest — 2026-07-02)

**Batch vs Spark 3.5.3 — Zelox wins across the board:**

| Benchmark | Zelox | Spark 3.5.3 | Verdict |
|---|---|---|---|
| TPC-H SF-1 (22q, warm) | 1.78 s | 63.46 s | ~36× (small/warm) |
| TPC-H SF-100 (22q, 100 GB, EKS) | 347 s / 51.7 GiB | 1099 s / 115 GiB | ~3.2× faster, ~2.2× less RAM |
| TPC-DS-99 (EKS, coverage) | 97/99, 0.32 GiB | 99/99, 2.5 GiB | ~8× less memory (Q5/Q9 compat gaps) |
| **P4 batch Parquet-on-S3 (200M rows, EKS)** | **5.92 s / 3.44 GiB** | 36.94 s / 8.1 GiB | **6.2× faster, 2.4× less mem, bit-identical output** |

**Streaming vs Flink 1.19 — competitive, NOT categorically-better** (rigorous tri-engine
Nexmark-methodology run, 2026-07-01; supersedes an earlier lighter ~1.5M-ev/s run that reported
Zelox faster — we claim only the measured head-to-head + flag path-dependence):

| Dimension | Flink 1.19 | Zelox | Verdict |
|---|---|---|---|
| Throughput | 5.78M ev/s | 5.28M ev/s | ~1.10× slower (competitive, after T1–T7a) |
| Memory (peak RSS) | 8.55 GiB | ~7.1 GiB | ~1.2× less (path-dependent; batch ~8× less) |
| Latency | ms (Kafka) | competitive, tail better (no GC) | tail win / median tie |
| Exactly-once (hard crash) | mature | EO ✓ incl. real S3 sink (dup=0) | correct / less hardened |

**Production workloads on real S3** (Uber/Netflix patterns, EKS 2026-07-02):
- **P1** Kafka → 10 s windowed-agg → Parquet-on-S3, **exactly-once incl. crash** (kill -9 →
  resume from S3 checkpoint): rows=9000, **dup=0**, sum=90M bit-identical; 4.67M ev/s, 7.25 GiB.
- **P4** (above): batch Parquet-on-S3 vs Spark, 6.2× faster / 2.4× less mem / identical output.

Full writeups: [docs/design/production-workload-benchmark.md](docs/design/production-workload-benchmark.md),
[docs/design/tri-engine-benchmark-matrix.md](docs/design/tri-engine-benchmark-matrix.md),
[docs/benchmarks/STREAMING_VS_FLINK_EKS.md](docs/benchmarks/STREAMING_VS_FLINK_EKS.md).

---

## ClickBench — 43/43 PASS

Official [ClickBench](https://benchmark.clickhouse.com/) 43 OLAP queries on the `hits` web-analytics
dataset. Smoke run: 1 of 100 Parquet shards (1 M rows, 116 MB), **dev build**.

```
Q01 ✓ 0.041s   Q02 ✓ 0.063s   Q03 ✓ 0.150s   Q04 ✓ 0.089s   Q05 ✓ 0.150s
Q06 ✓ 0.158s   Q07 ✓ 0.048s   Q08 ✓ 0.068s   Q09 ✓ 0.223s   Q10 ✓ 0.336s
Q11 ✓ 0.110s   Q12 ✓ 0.119s   Q13 ✓ 0.139s   Q14 ✓ 0.172s   Q15 ✓ 0.165s
Q16 ✓ 0.162s   Q17 ✓ 0.243s   Q18 ✓ 0.236s   Q19 ✓ 4.959s   Q20 ✓ 0.071s
Q21 ✓ 0.697s   Q22 ✓ 0.728s   Q23 ✓ 1.436s   Q24 ✓ 2.970s   Q25 ✓ 0.578s
Q26 ✓ 0.103s   Q27 ✓ 0.588s   Q28 ✓ 0.671s   Q29 ✓ 1.846s   Q30 ✓ 2.333s
Q31 ✓ 0.205s   Q32 ✓ 0.235s   Q33 ✓ 0.404s   Q34 ✓ 0.888s   Q35 ✓ 0.894s
Q36 ✓ 0.259s   Q37 ✓ 2.387s   Q38 ✓ 2.091s   Q39 ✓ 2.093s   Q40 ✓ 3.145s
Q41 ✓ 1.898s   Q42 ✓ 1.800s   Q43 ✓ 4.684s

Total: 40.6s (dev build, 1M rows — release build on full 100M rows pending)
```

Run: `SPARK_REMOTE=sc://localhost:50051 python scripts/clickbench.py`
Full 100M-row run: `CLICKBENCH_FULL=1 SPARK_REMOTE=sc://localhost:50051 python scripts/clickbench.py`

---

## TPC-H — Scale Factor 1 (6 M lineitem rows)

```
======================================================================
  Zelox TPC-H Benchmark  —  Scale Factor 1  (release + LTO)
======================================================================
  Q         Time      Rows  Status
  ----  --------  --------  ------
  Q01     0.120s         4  PASS
  Q02     0.032s       100  PASS
  Q03     0.047s        10  PASS
  Q04     0.039s         5  PASS
  Q05     0.079s         5  PASS
  Q06     0.032s         1  PASS
  Q07     0.090s         4  PASS
  Q08     0.069s         2  PASS
  Q09     0.089s       175  PASS
  Q10     0.097s        20  PASS
  Q11     0.019s       665  PASS
  Q12     0.071s         2  PASS
  Q13     0.053s        41  PASS
  Q14     0.039s         1  PASS
  Q15     0.053s         1  PASS
  Q16     0.042s     18267  PASS
  Q17     0.131s         1  PASS
  Q18     0.137s         9  PASS
  Q19     0.085s         1  PASS
  Q20     0.058s       162  PASS
  Q21     0.108s       100  PASS
  Q22     0.023s         7  PASS

======================================================================
  22/22 PASSED, 0 FAILED | total query time: 1.515s
======================================================================
```

### Summary

| Metric              | Value             |
|---------------------|-------------------|
| TPC-H SF-1 pass     | **22 / 22** (100%) |
| Total query time    | **1.515 s**        |
| Median query time   | **0.064 s**        |
| Fastest query (Q11) | **0.019 s**        |
| Slowest query (Q18) | **0.137 s**        |

### Notes

- Times include gRPC round-trip from Python client but **not** Parquet I/O startup cost (tables are registered as lazy views; I/O happens inside query execution).
- No JVM, no Hadoop, no HDFS — pure Rust + DataFusion.

---

## Spark Compatibility — 105/105 (100%)

> Tested against the Spark compat scorecard (`scripts/spark_compat_score.py`).
> Binary: `./target/debug/zelox server`  
> Client: PySpark 4.0.2 on Python 3.9  
> Platform: macOS 26 ARM64  

```
═══════════════════════════════════════════════════════
  ZELOX SPARK COMPATIBILITY SCORECARD
═══════════════════════════════════════════════════════
  1. Basic SQL                           ✓✓✓✓✓✓✓✓✓✓✓✓✓  13/13
  2. Aggregate Functions                     ✓✓✓✓✓✓  6/6
  3. Window Functions                          ✓✓✓✓  4/4
  4. String Functions                         ✓✓✓✓✓  5/5
  5. Date / Time Functions                     ✓✓✓✓  4/4
  6. Complex Types                            ✓✓✓✓✓  5/5
  7. DataFrame API                        ✓✓✓✓✓✓✓✓✓  9/9
  8. Python UDFs                              ✓✓✓✓✓  5/5
  9. JSON Reading                             ✓✓✓✓✓  5/5
  10. Parquet Read / Write                      ✓✓✓  3/3
  11. DML (Delta Lake)                         ✓✓✓✓  4/4
  12. Misc Spark SQL                       ✓✓✓✓✓✓✓✓  8/8
  13. Lambda / Higher-Order Functions     ✓✓✓✓✓✓✓✓✓  9/9
  14. PIVOT                                      ✓✓  2/2
  15. Named Windows                              ✓✓  2/2
  16. Cache / Catalog                            ✓✓  2/2
  17. _metadata Column                           ✓✓  2/2
  18. Advanced SQL                         ✓✓✓✓✓✓✓✓  8/8
  19. Window Frames & Joins                   ✓✓✓✓✓  5/5
  20. QUALIFY & Recursive CTEs                 ✓✓✓✓  4/4
───────────────────────────────────────────────────────
  Total:  105 passed, 0 failed, 0 skipped
  Score:  100% (105/105 executed)
═══════════════════════════════════════════════════════
```

### Compatibility Summary

| Metric                    | Zelox v0.4.0 | LakeSail v0.6.3 |
|---------------------------|--------------|-----------------|
| Spark compat score        | **100% (105/105)** | ~95% |
| UDF support               | ✓ (5/5)      | ✓ partial       |
| DML (DELETE/UPDATE)       | ✓ (4/4)      | ✓               |
| JSON PERMISSIVE           | ✓            | ✓               |
| VARIANT type (Spark 4.x)  | ✓            | ✓               |
| Delta time travel         | ✓            | ✓               |
| ClickBench 43/43          | ✓            | partial         |

### Notes

- UDFs require `PYTHONPATH` pointing to a PySpark installation on both the server and client.
- The binary must be built WITHOUT mimalloc (`default = []` in `zelox-cli/Cargo.toml`); mimalloc
  causes re-entrant allocator recursion when Python UDFs run on Tokio worker threads.

---

## Reproducing

```bash
# 1. Generate TPC-H SF-1 Parquet files
python -c "
import duckdb, os
conn = duckdb.connect()
conn.sql('INSTALL tpch; LOAD tpch; CALL dbgen(sf=1)')
os.makedirs('/tmp/tpch_sf1', exist_ok=True)
for tbl in ['customer','lineitem','nation','orders','part','partsupp','region','supplier']:
    conn.sql(f\"COPY {tbl} TO '/tmp/tpch_sf1/{tbl}.parquet' (FORMAT PARQUET)\")
print('Done')
"

# 2. Start Zelox server
ZELOX_RUNTIME__STACK_SIZE=67108864 ./target/release/zelox server --port 50055 &

# 3. Run benchmark via PySpark client
SPARK_REMOTE=sc://localhost:50055 python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote('sc://localhost:50055').getOrCreate()
for tbl in ['customer','lineitem','nation','orders','part','partsupp','region','supplier']:
    spark.read.parquet(f'/tmp/tpch_sf1/{tbl}.parquet').createOrReplaceTempView(tbl)
# ... run queries
"
```
