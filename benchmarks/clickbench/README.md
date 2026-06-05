# ClickBench — Vajra in LakeSail's exact harness

This reproduces the **ClickHouse/ClickBench `sail` harness**
(<https://github.com/ClickHouse/ClickBench/tree/main/sail>) for Vajra, so Vajra
and LakeSail numbers are **directly comparable**. Because Vajra is forked from
`lakehq/sail` and both speak Spark Connect, the only difference is the server the
client connects to — the query set, data, and run protocol are identical.

## Methodology (identical to LakeSail's published run)
- **Data:** official `hits.parquet`, 99.99M rows, 14,779,976,446 bytes, local disk.
- **Machine:** AWS `c6a.4xlarge` (16 vCPU, 32 GB) — LakeSail's exact instance.
- **Run:** each of 43 queries via `spark.sql(q).toPandas()`, **3 runs, best-of-3**.
- **Reported metric:** hot (best-of-3) total. Load time ≈ 0 (reads Parquet directly).

## Files
| File | Purpose |
|---|---|
| `queries.sql` | The 43 ClickBench queries (Spark-Connect dialect; identical to sail). |
| `run.py` | Runner — Spark Connect → Vajra, best-of-3, emits ClickBench JSON. |
| `compare.py` | Per-query Vajra-vs-LakeSail diff + verdict. |
| `results/lakesail_c6a.4xlarge.json` | LakeSail's **published** numbers (verbatim), the reference. |
| `results/vajra_c6a.4xlarge.json` | Vajra's run (produced by `run.py`). |

## Run it
```bash
# 1. Get the data once (on the c6a.4xlarge):
curl -sSL https://datasets.clickhouse.com/hits_compatible/hits.parquet -o /data/hits.parquet

# 2. Vajra (server on :50051):
SPARK_REMOTE=sc://localhost:50051 CLICKBENCH_HITS=/data/hits.parquet \
  python benchmarks/clickbench/run.py > benchmarks/clickbench/results/vajra_c6a.4xlarge.json

# 3. Compare against LakeSail's published numbers:
python benchmarks/clickbench/compare.py \
  benchmarks/clickbench/results/vajra_c6a.4xlarge.json \
  benchmarks/clickbench/results/lakesail_c6a.4xlarge.json
```

## What "matching" means
LakeSail's published hot best-of-3 total is **65.50 s** (cold 197.04 s), median
per-query **1.52 s**. Given the shared DataFusion core, Vajra on this identical
harness should land **within noise** (total within ~±15%, most queries ±25%). A
large *systematic* gap would flag a fork regression worth investigating — that is
exactly the correctness check this directory exists to perform.

See [../../docs/benchmarks/CLICKBENCH_VS_LAKESAIL.md](../../docs/benchmarks/CLICKBENCH_VS_LAKESAIL.md)
for the full analysis and LakeSail's per-query reference table.
