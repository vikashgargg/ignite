# Vajra (वज्र) Benchmark Results

> Measured 2026-05-23 on Apple M-series (macOS 26, ARM64)  
> Release build with LTO (`lto = true, codegen-units = 1`)  
> Runtime: Vajra server (`./target/release/vajra server --port 50055`)  
> Data: TPC-H tables as Parquet files, read lazily via Spark Connect gRPC  

---

## TPC-H — Scale Factor 1 (6 M lineitem rows)

```
======================================================================
  Vajra (वज्र) TPC-H Benchmark  —  Scale Factor 1  (release + LTO)
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

## Spark Compatibility

> Tested against the Spark compat scorecard (`scripts/spark_compat_score.py`),  
> which covers 71 key Spark features: SQL, DataFrames, UDFs, DML,  
> JSON/Parquet, complex types, aggregation, window functions.
>
> Binary: `./target/release/vajra server --port 50055`  
> Client: PySpark 4.0.0 on Python 3.12  
> Platform: macOS 26 ARM64  

```
═══════════════════════════════════════════════════════
  VAJRA SPARK COMPATIBILITY SCORECARD
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
───────────────────────────────────────────────────────
  Total:  71 passed, 0 failed, 0 skipped
  Score:  100% (71/71 executed)
═══════════════════════════════════════════════════════
```

### Compatibility Summary

| Metric                    | Vajra      | LakeSail (baseline) |
|---------------------------|------------|---------------------|
| Spark compat score        | **100%**   | 80.1%               |
| UDF support               | ✓ (5/5)    | ✓ partial           |
| DML (DELETE/UPDATE)       | ✓ (4/4)    | partial             |
| JSON PERMISSIVE           | ✓          | ✓                   |

### Notes

- UDFs require `PYTHONPATH` pointing to a PySpark installation on both the server and client.
- The binary must be built WITHOUT mimalloc (`default = []` in `sail-cli/Cargo.toml`); mimalloc
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

# 2. Start Vajra server
SAIL_RUNTIME__STACK_SIZE=67108864 ./target/release/vajra server --port 50055 &

# 3. Run benchmark via PySpark client
SPARK_REMOTE=sc://localhost:50055 python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote('sc://localhost:50055').getOrCreate()
for tbl in ['customer','lineitem','nation','orders','part','partsupp','region','supplier']:
    spark.read.parquet(f'/tmp/tpch_sf1/{tbl}.parquet').createOrReplaceTempView(tbl)
# ... run queries
"
```
