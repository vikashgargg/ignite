# zelox-pyspark

Drop-in PySpark client for [Zelox](https://github.com/vikashgargg/zelox) — a Rust-native Spark Connect server that is 40× faster than Apache Spark with 100% Spark SQL compatibility.

## Installation

```bash
pip install zelox-pyspark
```

## Quickstart

```python
from zelox_pyspark import ZeloxSession

# Connect to a running Zelox server
spark = ZeloxSession.connect("localhost:50051")

# Your existing PySpark code works unchanged
df = spark.read.parquet("s3://my-bucket/data/")
result = df.groupBy("region").agg({"revenue": "sum"}).orderBy("sum(revenue)", ascending=False)
result.show()
```

## Starting a local server

```python
from zelox_pyspark import ZeloxSession

# Auto-start a local Zelox server (requires zelox binary in PATH or ZELOX_BIN env var)
with ZeloxSession.local() as spark:
    spark.sql("SELECT 1 + 1").show()
```

Or from the command line:

```bash
# Start a server on port 50051
zelox-pyspark start --port 50051

# Run a quick smoke test
zelox-pyspark smoke --host localhost --port 50051
```

## Zero-change migration

If you already have PySpark code, just change the session builder:

```python
# Before
spark = SparkSession.builder.master("spark://...").getOrCreate()

# After — one line change
spark = SparkSession.builder.remote("sc://zelox-host:50051").getOrCreate()
```

See [MIGRATION.md](../../MIGRATION.md) for the full migration guide.

## Performance

| Workload | Apache Spark 3.5 | Zelox |
|---|---|---|
| Cold start | 30–120 s | ~200 ms |
| TPC-H SF-1 | ~60 s | 1.5 s |
| Idle memory | 2–4 GB | ~300 MB |
