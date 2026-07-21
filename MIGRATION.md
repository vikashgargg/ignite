# Migrating from Apache Spark to Zelox

> Zelox implements the Spark Connect gRPC protocol.  
> In most cases **zero code changes are required** — just point your session at a Zelox server.

---

## The One-Line Migration

```python
# Before (Spark)
spark = SparkSession.builder \
    .appName("my-job") \
    .master("spark://scheduler:7077") \
    .getOrCreate()

# After (Zelox) — change only this line
spark = SparkSession.builder.remote("sc://zelox-server:50051").getOrCreate()
```

That's it for most batch workloads. The rest of your PySpark code runs unchanged.

---

## What Works Today (No Changes Needed)

### Batch SQL & DataFrames ✅
```python
# All standard SQL
spark.sql("SELECT a, COUNT(*) FROM t GROUP BY a").show()
spark.sql("WITH RECURSIVE cte AS (...) SELECT * FROM cte")
spark.sql("SELECT * FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts) = 1")

# DataFrame API — complete
df.filter(...).groupBy(...).agg(...).orderBy(...).limit(100).collect()
df.join(other, on="id", how="left")
df.withColumn("x", F.col("a") + F.col("b"))

# Window functions
df.withColumn("rank", F.rank().over(Window.partitionBy("dept").orderBy("salary")))

# Higher-order functions
df.select(F.transform("arr", lambda x: x * 2))
df.select(F.filter("arr", lambda x: x > 0))
df.select(F.aggregate("arr", F.lit(0), lambda acc, x: acc + x))
```

### Python UDFs ✅
```python
# Scalar UDFs
@udf(returnType=IntegerType())
def double(x): return x * 2

# Pandas UDFs
@pandas_udf(DoubleType())
def zscore(s: pd.Series) -> pd.Series:
    return (s - s.mean()) / s.std()

# Arrow batch UDFs — same syntax, zero-copy
```

> **Note**: The Zelox server process must have `PYTHONPATH` pointing to your PySpark installation
> so embedded Python can find `pyspark` for UDF deserialization.
> In Docker: `pip install pyspark` in the image (already done in the published Dockerfile).

### DML — Delta Lake ✅
```python
spark.sql("DELETE FROM orders WHERE status = 'cancelled'")
spark.sql("UPDATE products SET price = price * 1.1 WHERE category = 'luxury'")
spark.sql("INSERT OVERWRITE TABLE staging SELECT * FROM raw")
```

### JSON & Parquet ✅
```python
# All read modes
df = spark.read.option("mode", "PERMISSIVE").json("s3://...")
df = spark.read.option("mode", "DROPMALFORMED").json("s3://...")
df = spark.read.parquet("s3://bucket/path/")

# Parquet write
df.write.parquet("/tmp/output", mode="overwrite")
```

### Structured Streaming (Micro-Batch) ✅
```python
# Rate source
df = spark.readStream.format("rate").option("rowsPerSecond", 100).load()

# Kafka source
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "events") \
    .load()

# Aggregations per micro-batch
counts = df.groupBy(F.window("timestamp", "1 minute")).count()

# Memory sink — queryable via spark.sql
query = counts.writeStream \
    .format("memory") \
    .queryName("counts") \
    .outputMode("complete") \
    .start()
time.sleep(5)
spark.sql("SELECT * FROM counts").show()

# foreachBatch — arbitrary Python logic per batch
def process_batch(batch_df, batch_id):
    batch_df.write.mode("append").parquet(f"/output/{batch_id}")

query = df.writeStream.foreachBatch(process_batch).start()
```

### Catalogs ✅
```python
# In-memory catalog (default)
spark.sql("CREATE DATABASE mydb")
spark.sql("CREATE TABLE mydb.t (id INT, val STRING) USING DELTA")

# Iceberg REST catalog (configure via ZELOX_CATALOG__*)
# Unity Catalog (stub — production-ready in Sprint 4)
```

---

## What Needs Attention

### 1. Event-Time Streaming Windows ⚠️ (Sprint 4, ~2026-06-14)

```python
# NOT YET SUPPORTED — planned Sprint 4
df.withWatermark("timestamp", "10 minutes") \
  .groupBy(F.window("timestamp", "1 hour")) \
  .count()
```

**Workaround**: Use `foreachBatch` to implement tumbling windows manually.

### 2. Streaming Checkpoint / Recovery ⚠️ (Sprint 4)

```python
# checkpointLocation option is accepted but NOT honoured yet
query = df.writeStream \
    .option("checkpointLocation", "/tmp/ckpt")  # ignored today
    .start()
```

**Workaround**: Stateless streaming pipelines (rate → filter → Kafka/Delta) work without checkpoints.

### 3. INSERT INTO ... PARTITION ⚠️ (Sprint 3)

```sql
-- NOT YET SUPPORTED
INSERT INTO t PARTITION (dt='2024-01-01') SELECT * FROM raw
```

**Workaround**: Use `df.write.partitionBy("dt").mode("append").saveAsTable("t")`.

### 4. ALTER VIEW ⚠️ (Sprint 3)

```sql
-- NOT YET SUPPORTED
ALTER VIEW v AS SELECT 2 AS b
```

**Workaround**: `CREATE OR REPLACE VIEW v AS SELECT 2 AS b`.

### 5. mapGroupsWithState / flatMapGroupsWithState ⚠️ (Sprint 5)

Advanced stateful streaming operators. No workaround — this is a Sprint 5 item.

### 6. Unity Catalog / HMS in Production ⚠️

Stubs exist but are not production-hardened for schema evolution or ACL enforcement.

---

## Deployment

### Single Binary (Development)
```sh
curl https://raw.githubusercontent.com/vikashgargg/ignite/main/install.sh | sh
zelox server --ip 0.0.0.0 --port 50051
```

### Docker (K8s / Apple Container)
```sh
docker build -t zelox:latest -f docker/Dockerfile .
docker run -p 50051:50051 zelox:latest
```

### Kubernetes
```sh
kubectl apply -f k8s/sail.yaml
kubectl port-forward -n zelox svc/zelox-spark-server 50051:50051
```

### High Availability (K8s Lease election)
```sh
# Set env var and enable HA flag — multiple pods can run; one holds the lease
zelox server --ip 0.0.0.0 --port 50051 --ha
# Or in K8s: ZELOX_MODE=kubernetes-cluster with --ha in args
```

### Bearer Token Auth
```sh
zelox server --ip 0.0.0.0 --port 50051 --auth-token my-secret-token
# Or via env var: ZELOX_AUTH__TOKEN=my-secret-token zelox server ...
```

Client side:
```python
spark = SparkSession.builder \
    .remote("sc://zelox:50051") \
    .config("spark.connect.grpc.binding.userAgent", "..") \
    .getOrCreate()
# PySpark Spark Connect passes Authorization header automatically when
# configured via the connection string token parameter (Spark 4.0+)
```

---

## Environment Variable Quick Reference

| Variable | Effect |
|---|---|
| `ZELOX_MODE` | `local` / `local-cluster` / `kubernetes-cluster` |
| `ZELOX_AUTH__TOKEN` | Require Bearer token on all gRPC calls |
| `ZELOX_CLUSTER__WORKER_INITIAL_COUNT` | Number of workers for local-cluster mode |
| `ZELOX_KUBERNETES__NAMESPACE` | K8s namespace for worker pods |
| `RUST_LOG` | Log level (`warn` / `info` / `debug`) |
| `PYTHONPATH` | Must include PySpark site-packages for UDFs |

---

## Performance Comparison

| Workload | Apache Spark 3.5 | Zelox | Speedup |
|---|---|---|---|
| Cold start | 30–120 s | **~200 ms** | **150–600x** |
| TPC-H SF-1 (22 queries) | ~60 s (warm JVM) | **1.515 s** | **40x** |
| Idle memory | 2–4 GB JVM | **~300 MB** | **7–13x** |
| Binary / image size | ~600 MB | **105 MB** | **6x** |

---

## FAQ

**Q: Do I need to change my `pyspark` version?**  
A: No. Zelox is compatible with `pyspark[connect]==4.0.0`. Client-side PySpark is unchanged.

**Q: Does it work with existing Delta Lake tables?**  
A: Yes. Zelox uses `delta-rs` to read/write Delta Lake tables in the same format as Spark.

**Q: Can I run both Spark and Zelox against the same Delta table?**  
A: Yes. The on-disk format is identical.

**Q: What about Spark 3.x vs 4.x APIs?**  
A: Zelox targets the Spark Connect protocol (Spark 4.0). DataFrame API compatibility covers both 3.x and 4.x usage patterns.

**Q: Does Zelox replace the Spark driver or the whole cluster?**  
A: Both. Zelox is a drop-in replacement for the entire Spark cluster — driver, executors, and scheduler. You don't need Hadoop, YARN, or a separate cluster manager.
