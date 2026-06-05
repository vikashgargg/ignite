"""
Vajra ClickBench runner — faithful to the ClickHouse/ClickBench `sail` harness.

This mirrors LakeSail's published methodology *exactly* so the results are
directly comparable (https://github.com/ClickHouse/ClickBench/tree/main/sail):

  * Reads the official `hits` Parquet (single file, 99.99M rows, ~14.78 GB).
  * Runs each of the 43 queries via Spark Connect: `spark.sql(q).toPandas()`.
  * 3 runs per query; emits ClickBench-format JSON  [[r1, r2, r3], ...].
  * The "hot" (best-of-3) total is what ClickBench reports.

The ONLY difference vs sail's harness is the server the Spark Connect client
points at (Vajra instead of LakeSail) — the query set and protocol are identical,
because both implement Spark Connect. Run both on the same c6a.4xlarge for a true
apples-to-apples comparison; the shared DataFusion core means Vajra should land
within noise of LakeSail's published numbers.

Usage:
    SPARK_REMOTE=sc://localhost:50051 \
    CLICKBENCH_HITS=/data/hits.parquet \
    python benchmarks/clickbench/run.py > results/vajra_c6a.4xlarge.json
"""
from __future__ import annotations

import json
import os
import re
import sys
import timeit
from pathlib import Path

from pyspark.sql import SparkSession

SPARK_REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:50051")
HITS = os.environ.get("CLICKBENCH_HITS", "/data/hits.parquet")
TRIES = int(os.environ.get("CLICKBENCH_TRIES", "3"))
QUERIES = Path(__file__).with_name("queries.sql")


def build_session() -> SparkSession:
    # SPARK_REMOTE=local[*] runs the JVM reference Spark for the baseline side.
    if SPARK_REMOTE.startswith("local"):
        os.environ.pop("SPARK_REMOTE", None)
        return SparkSession.builder.master(SPARK_REMOTE).getOrCreate()
    return SparkSession.builder.remote(SPARK_REMOTE).getOrCreate()


def main() -> int:
    spark = build_session()
    spark.read.parquet(HITS).createOrReplaceTempView("hits")

    queries = [q.strip() for q in QUERIES.read_text().split(";") if q.strip()]
    results: list[list[float]] = []

    for i, q in enumerate(queries, 1):
        # sail applies the same backreference fixup for REGEXP_REPLACE.
        q = re.sub(r"\\(\d)", r"$\1", q)
        runs: list[float] = []
        for _ in range(TRIES):
            start = timeit.default_timer()
            try:
                spark.sql(q).toPandas()
                runs.append(round(timeit.default_timer() - start, 3))
            except Exception as exc:  # noqa: BLE001 — match sail: record a null
                print(f"Q{i} FAILED: {exc}", file=sys.stderr)
                runs.append(None)  # type: ignore[arg-type]
        print(f"Q{i:>2}: {runs}", file=sys.stderr)
        results.append(runs)

    json.dump(results, sys.stdout, indent=0)
    print(file=sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
