# ClickBench — Vajra vs Apache Spark (head-to-head)

The 43 standard [ClickBench](https://benchmark.clickhouse.com/) analytical
queries over the `hits` table, identical Parquet input and identical SQL on the
same machine. This run uses the **`hits_0` smoke subset (~1M rows, 122 MB)** —
tractable on an 8 GB host; the official 100M-row (~14 GB) set needs a larger box
(tracked alongside TPC-H SF-100 in PRODUCTION_ROADMAP.md).

## Result (smoke, ~1M rows, single pass, `local[4]`)

| Engine | Build | Total (43q) | Avg/query | Passed |
|---|---|---|---|---|
| **Vajra** | release (thin LTO) | **3.872 s** | 0.090 s | **43/43** |
| Apache Spark 3.5.3 | JVM (Java 8) | 48.072 s | 1.136 s | 42/43 |

**Vajra is ≈12.4× faster** end-to-end and passes **all 43** queries. Spark 3.5.3
fails Q40 with `DATATYPE_MISMATCH.DATA_DIFF_TYPES` on its `CASE WHEN … THEN Referer`
branch (a stricter 3.5 coercion rule); Vajra accepts it, matching Spark 4.x.

## How to reproduce
```bash
# Vajra (server running on :50051)
SPARK_REMOTE=sc://localhost:50051 python scripts/clickbench.py            # smoke
SPARK_REMOTE=sc://localhost:50051 CLICKBENCH_FULL=1 python scripts/clickbench.py  # full 14 GB

# Reference Apache Spark on the SAME cached data (classic JVM, local master)
SPARK_REMOTE=local[4] CLICKBENCH_DATA=~/.cache/clickbench python scripts/clickbench.py
```

## Caveats / next
- Smoke subset (~1M rows); the official 100M-row ClickBench is the scale proof
  point (needs a larger host — same constraint as TPC-H SF-100).
- Single pass (no warmup); both engines under identical conditions.
- Reference is Apache Spark 3.5.3 (the production line); a Spark 4.x reference is
  a follow-up.
