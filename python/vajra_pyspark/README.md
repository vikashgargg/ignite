# vajra-pyspark

Drop-in PySpark client for [Vajra](https://github.com/vikashgargg/ignite) — a Rust-native Spark Connect server that is 40× faster than Apache Spark with 100% Spark SQL compatibility.

## Installation

```bash
pip install vajra-pyspark
```

## Quickstart

```python
from vajra_pyspark import VajraSession

# Connect to a running Vajra server
spark = VajraSession.connect("localhost:50051")

# Your existing PySpark code works unchanged
df = spark.read.parquet("s3://my-bucket/data/")
result = df.groupBy("region").agg({"revenue": "sum"}).orderBy("sum(revenue)", ascending=False)
result.show()
```

## Starting a local server

```python
from vajra_pyspark import VajraSession

# Auto-start a local Vajra server (requires vajra binary in PATH or VAJRA_BIN env var)
with VajraSession.local() as spark:
    spark.sql("SELECT 1 + 1").show()
```

Or from the command line:

```bash
# Start a server on port 50051
vajra-pyspark start --port 50051

# Run a quick smoke test
vajra-pyspark smoke --host localhost --port 50051
```

## Zero-change migration

If you already have PySpark code, just change the session builder:

```python
# Before
spark = SparkSession.builder.master("spark://...").getOrCreate()

# After — one line change
spark = SparkSession.builder.remote("sc://vajra-host:50051").getOrCreate()
```

See [MIGRATION.md](../../MIGRATION.md) for the full migration guide.

## Performance

| Workload | Apache Spark 3.5 | Vajra |
|---|---|---|
| Cold start | 30–120 s | ~200 ms |
| TPC-H SF-1 | ~60 s | 1.5 s |
| Idle memory | 2–4 GB | ~300 MB |
