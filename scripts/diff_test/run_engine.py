"""Run all differential-testing workloads on one engine, emit normalized JSON.

Usage:
  # Reference (real Apache Spark, local JVM):
  ENGINE=reference python run_engine.py out_reference.json
  # Candidate (Vajra via Spark Connect; server must be running on SPARK_REMOTE):
  ENGINE=candidate SPARK_REMOTE=sc://localhost:50051 python run_engine.py out_candidate.json

Normalization: rows are converted to plain Python, sorted deterministically, and
schema is captured as (name, simple_type). This makes the JSON directly diffable
between engines regardless of row order.
"""

import datetime
import decimal
import json
import os
import sys

from workloads import WORKLOADS


def _norm(v):
    """Normalize a cell value to a stable, JSON-serializable form."""
    if v is None:
        return None
    if isinstance(v, float):
        # Round to tame floating-point noise across engines.
        return round(v, 9)
    if isinstance(v, decimal.Decimal):
        return f"DEC:{v.normalize()}"
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return "BYTES:" + v.hex()
    if isinstance(v, list):
        return [_norm(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _norm(x) for k, x in sorted(v.items())}
    return v


def _row_key(row):
    return json.dumps(row, sort_keys=True, default=str)


def build_session():
    engine = os.environ["ENGINE"]
    from pyspark.sql import SparkSession

    if engine == "reference":
        return (
            SparkSession.builder.master("local[2]")
            .appName("diff-ref")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
    if engine == "candidate":
        return SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).getOrCreate()
    raise SystemExit(f"unknown ENGINE={engine}")


def run():
    spark = build_session()
    results = {}
    for name, setup, query in WORKLOADS:
        try:
            for s in setup:
                spark.sql(s)
            df = spark.sql(query)
            schema = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
            rows = [[_norm(c) for c in row] for row in df.collect()]
            rows_sorted = sorted(rows, key=_row_key)
            results[name] = {"schema": schema, "rows": rows_sorted, "n": len(rows_sorted)}
        except Exception as e:  # noqa: BLE001 — capture engine errors as data
            results[name] = {"error": f"{type(e).__name__}: {str(e)[:300]}"}
    spark.stop()
    with open(sys.argv[1], "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"[{os.environ['ENGINE']}] wrote {len(results)} workload results to {sys.argv[1]}")


if __name__ == "__main__":
    run()
