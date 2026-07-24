<div align="center">

# Zelox

**One engine for batch *and* streaming. Spark's API, Flink's latency, no JVM.**

Zelox runs your existing PySpark code unchanged — then runs it in 200 ms instead of 2 minutes,
and streams it at millisecond latency with exactly-once guarantees that survive `kill -9`.

[![CI](https://github.com/vikashgargg/zelox/actions/workflows/zelox-ci.yml/badge.svg)](https://github.com/vikashgargg/zelox/actions/workflows/zelox-ci.yml)
[![Release](https://img.shields.io/github/v/release/vikashgargg/zelox)](https://github.com/vikashgargg/zelox/releases)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Spark compat](https://img.shields.io/badge/Spark%20SQL%20compat-105%2F105-brightgreen)](COMPAT.md)

[Quick start](#quick-start) · [Realtime mode](#realtime-mode--millisecond-streaming-through-the-spark-api) · [Benchmarks](docs/benchmarks/README.md) · [Streaming](docs/STREAMING.md) · [Docs](https://docs.zelox.club/)

</div>

```sh
curl https://raw.githubusercontent.com/vikashgargg/zelox/main/install.sh | sh
```

---

## The name

**Zelox** — from Greek **ζῆλος** (*zêlos*: zeal, ardour, the drive to pursue something
relentlessly) + **-x**, for exactness.

**Zeal and exactness.** Those are the two halves of this engine, and they are usually
sold as a trade-off.

Every data system asks you to pick a side. Want speed? Take at-least-once delivery,
reconcile the duplicates downstream, and accept that your streaming numbers and your
batch numbers will quietly disagree. Want correctness? Take the checkpoint pause, the
JVM, the second engine, the 30-second cold start. The industry has spent a decade
treating that as a law of physics.

It isn't. It's an artifact of implementation — of garbage collectors that must stop the
world, of batch and streaming written as two separate engines with two separate notions
of what a row means.

Zelox is the argument that you can refuse the trade. Rust and Arrow remove the GC pause,
so the tail stays flat under load. Aligned checkpoint barriers commit state, offsets, and
sink visibility as one atomic act, so exactly-once survives `kill -9` at 16 partitions and
100M events. One execution core runs both batch and streaming, so the same query means
the same thing in both. Relentless *and* exact — the whole name is the whole claim.

*In Greek myth Zelos was one of the four winged enforcers who stood at Zeus's throne
alongside Nike (victory), Kratos (strength), and Bia (force). Zelos was the one who
never let up.*

The name is a promise; the [benchmarks](#proven-results) are whether we keep it. Every
number in this README is measured and reproducible — check them.

---

## Why Zelox exists

Apache Spark is the industry standard for large-scale data processing — and it carries the
full weight of that legacy. A JVM that takes 30–120 seconds to warm up. A cluster setup that
wants HDFS, YARN, or Kubernetes just to run a local job. Gigabytes of heap before the first
query executes. Python data bouncing through Arrow IPC, back through the JVM, back out to
Python.

And when you need low-latency streaming, Spark hands you off to a second system entirely.
So you run Spark for batch and Flink for streaming: two engines, two deployment stories, two
sets of semantics, two on-call runbooks, and a permanent correctness question about whether
the batch and streaming versions of the same logic actually agree.

**Zelox collapses that into one engine.**

Built on Rust, Apache Arrow, and Apache DataFusion — the same columnar foundation behind
ClickHouse, InfluxDB, and Delta Lake's query path. No garbage collector. No JVM warmup. No
serialisation tax between Python and the execution engine. One statically-linked binary you
can `curl | sh` onto any machine.

Your PySpark code runs **unchanged** — Zelox implements the Spark Connect gRPC protocol
exactly. Point `SparkSession.builder.remote(...)` at a Zelox server and your existing jobs
run. Batch and streaming share the same execution core, so the same query means the same
thing in both.

### What you get

|  | |
|---|---|
| **One engine, two workloads** | Spark-class batch **and** Flink-class streaming on the same binary, same SQL surface, same semantics |
| **Drop-in** | Spark Connect protocol; change one line (`.remote(...)`) and your PySpark job runs |
| **Fast cold** | ~200 ms to first query vs 30–120 s JVM warmup |
| **Small** | ~300 MB idle vs 2–4 GB JVM heap; 80 MB Linux binary |
| **Millisecond streaming** | `trigger(continuous=…)` → event-at-a-time pipeline, ~62 ms p50 end-to-end through Kafka |
| **Exactly-once that survives crashes** | `kill -9` → resume with `dup=0`, bit-identical output — verified on EKS at 16 partitions / 100M events |
| **Proven compatible** | 105/105 Spark SQL scorecard across all four modes; 124 workloads checked byte-exact against real Spark in CI |

---

## Lineage

Zelox began as a fork of [Sail](https://github.com/lakehq/sail) (LakeSail, Inc., Apache-2.0)
and has grown into its own product with its own goals — the same way
**MariaDB** grew out of MySQL, **OpenSearch** out of Elasticsearch, and
**Valkey** out of Redis.

We inherited an excellent Rust + DataFusion analytical core, and we say so
plainly: on raw batch query performance against Spark, Zelox and Sail sit in the
same ballpark, because that lineage is shared. **We do not claim to be faster
than Sail.**

Where Zelox diverges is scope. Sail is a Spark batch replacement. Zelox is a
**unified batch *and* streaming engine** — Flink-class Structured Streaming with
exactly-once semantics verified across hard crashes, event-time windows and
watermarks, stream-stream joins, and millisecond realtime mode — plus the
operational surface a production deployment actually needs: JWT/mTLS auth,
Kubernetes HA with lease election, a Helm chart, a web UI, and a CI-gating
differential trust harness that checks 124 workloads byte-exact against real
Spark.

Attribution for the inherited code is preserved in [`NOTICE`](NOTICE), and the
pre-fork release history is kept intact in the
[upstream changelog](docs/reference/changelog/index.md). Where a fix applies to
the shared core, we send it upstream.

---

## Zelox vs the Field

> *LakeSail v0.6.3 (2026-05-21) is the closest open-source comparison. Numbers are measured, not estimated.*

| Capability | Apache Spark 3.5 | LakeSail v0.6.3 | **Zelox v0.6.0** |
|---|---|---|---|
| Runtime | JVM (GC pauses) | Rust | **Rust** |
| Cold start | 30–120 s | ~2 s | **~200 ms** |
| Idle memory | 2–4 GB JVM heap | ~500 MB | **~300 MB** |
| Binary / image size | ~600 MB | ~300 MB | **105 MB macOS / 80 MB Linux** |
| TPC-H SF-1 (22q, warm) | 63.46 s | ~15 s | **1.78 s (~36×)** |
| TPC-H SF-100 (22q, 100 GB, same node) | 1099 s / 115 GiB | not run | **347 s / 51.7 GiB (~3.2× faster, ~2.2× less RAM)** |
| ClickBench 100M (distributed on EKS) | — | — | **377.9 s, 43/43** |
| pip install | `pyspark` (JVM needed) | `pysail` | **`zelox-pyspark`** |
| **Spark SQL compat (105-test scorecard, all modes)** | ✅ reference | ~95% | **✅ 105/105 (100%)** |
| Python UDFs — scalar / Pandas / Arrow | ✅ | ✅ | **✅** |
| **Python-version-agnostic UDFs (any 3.10+)** | ✅ | ✅ abi3 | **✅ abi3 + subprocess** |
| **Distributed lambda HOFs + recursive CTEs** | ✅ | partial | **✅ (Sprint 4.1)** |
| **approx_top_k / KLL / theta sketches (Spark 4.1)** | ✅ | partial | **✅ (Sprint 4.1)** |
| **Python iterator UDFs (GroupedMap 4.1)** | ✅ | ✅ v0.6.3 | **✅** |
| Delta Lake DML (DELETE/UPDATE/MERGE) | ✅ | ✅ | **✅** |
| **Delta time travel (AT VERSION/TIMESTAMP)** | ✅ | ✅ v0.6.0 | **✅** |
| **Delta V2 checkpointing + log compaction** | ✅ | ✅ v0.6.0 | **✅** |
| **Iceberg (read/write/REST catalog + OverwritePartitions)** | ✅ | ✅ (active) | **✅** |
| **VARIANT type (Spark 4.x)** | ✅ | ✅ v0.6.3 | **✅** |
| **Structured Streaming — Kafka source** | ✅ | ❌ | **✅** |
| **Structured Streaming — foreachBatch** | ✅ | ❌ | **✅** |
| **Structured Streaming — memory sink** | ✅ | ❌ | **✅** |
| **Streaming exactly-once (stateless + stateful), crash-verified** | ✅ | ❌ (issue open) | **✅** |
| **Streaming event-time windows + watermarks (keyed, parallel)** | ✅ | ❌ | **✅** |
| **Streaming stream-stream / interval joins** | ✅ | ❌ | **✅** |
| **Streaming stateful deduplication** | ✅ | ❌ | **✅** |
| **Theta sketch aggregates (KMV)** | ✅ | partial | **✅** |
| **Vortex data source (skeleton)** | ✅ | ✅ v0.6.0 | **✅ skeleton** |
| **JWT bearer / mTLS auth** | ✅ | ❌ | **✅** |
| **Apple Container (macOS 26, Apple Silicon)** | ❌ | ❌ | **✅ — only one** |
| **K8s Helm chart + HPA** | community | ❌ | **✅** |
| **Scheduler HA (K8s Lease election)** | ✅ (complex) | ❌ | **✅** |
| **Web UI on :4040** | ✅ | ❌ | **✅** |
| **dbt integration guide** | ✅ | ✅ v0.6.3 | **✅** |
| **ClickBench 43/43 benchmark** | ✅ | ✅ v0.6.3 | **✅** |

All Zelox numbers above are measured (LTO release binary; SF-100 + ClickBench-100M
on AWS EKS Graviton), not estimated. **The speedup is scale-dependent** — ~36× on
small/warm TPC-H SF-1, narrowing to a still-substantial **~3.2× faster + ~2.2× less
memory at 100 GB**. Quote the scale with the number; full conditions, per-query
tables, and the honest Zelox-vs-Spark-vs-LakeSail read are in
[docs/benchmarks/](docs/benchmarks/README.md) and
[docs/benchmarks/COMPETITIVE.md](docs/benchmarks/COMPETITIVE.md).

> **On LakeSail:** Zelox is forked from `lakehq/sail`, so the analytical core
> (Rust + DataFusion) is shared lineage — raw query perf vs Spark sits in the same
> ballpark for both. We do **not** claim "faster than LakeSail." Zelox's
> differentiation is operational features (streaming, auth, K8s HA, Apple
> Container, Web UI), a **CI-gating differential trust harness** (124 workloads
> byte-exact vs real Spark), **four-mode** 105/105 verification, and **transparent,
> per-scale benchmarks**. See [COMPETITIVE.md](docs/benchmarks/COMPETITIVE.md).

---

## Proven Results

> **Latest verified — renamed + PySpark 4.2 build (`zelox:rename42`, 2026-07-24).** Fresh EKS tri-engine
> head-to-head with the **actual S3 output files read back and verified**, both engines on identical hardware,
> 100M scale ([full run](docs/benchmarks/RENAME42_EKS_TRIENGINE.md)):
>
> | | Zelox | Spark 3.5.3 / Flink 1.19 | Zelox |
> |---|--:|--:|:--|
> | **Batch → S3** (100M, output byte-identical) | **4.08 s / 1.89 GiB** | Spark 32.5 s / 5.6 GiB | **8.0× faster, ~3× less mem** |
> | **Realtime latency** (`trigger(realTime)`, p50 / tail) | **88 / 131 ms** | Flink 162 / 302 ms | **~2×, tail 2.3× (no-GC)** |
> | **Realtime → S3 exactly-once** (kill-9 mid-run) | **dup=0, resume == clean** | (Flink mature) | **parity, S3-verified** |
> | **Streaming throughput** (100M windowed-agg) | 4.09M ev/s | Flink 3.64M ev/s | ~parity (completeness-caveated) |
>
> *Honest scope:* both engines ran on **equal 6-vCPU** nodes (c7g/m7g 4xlarge were capacity-unavailable in
> ap-south-1) — ratios are valid, absolutes are ~half the 16-vCPU baseline below. Streaming here is
> **single-node**; **distributed throughput at 16-vCPU is [Phase 2](docs/design/phase2-distributed-parity-plan.md)**
> (not yet confirmed — Flink still leads the distributed exchange at scale).

```
══════════════════════════════════════════════════════════════════
  ZELOX SPARK COMPATIBILITY SCORECARD  (v0.6.0-alpha)
══════════════════════════════════════════════════════════════════
  1. Basic SQL                         ✓✓✓✓✓✓✓✓✓✓✓✓✓  13/13
  2. Aggregate Functions                   ✓✓✓✓✓✓  6/6
  3. Window Functions                        ✓✓✓✓  4/4
  4. String Functions                       ✓✓✓✓✓  5/5
  5. Date / Time Functions                   ✓✓✓✓  4/4
  6. Complex Types                          ✓✓✓✓✓  5/5
  7. DataFrame API                      ✓✓✓✓✓✓✓✓✓  9/9
  8. Python UDFs                            ✓✓✓✓✓  5/5
  9. JSON Reading                           ✓✓✓✓✓  5/5
  10. Parquet Read / Write                    ✓✓✓  3/3
  11. DML (Delta Lake)                       ✓✓✓✓  4/4
  12. Misc Spark SQL                     ✓✓✓✓✓✓✓✓  8/8
  13. Advanced SQL (PIVOT/UNPIVOT/TABLESAMPLE) ✓✓✓✓✓✓  6/6
  14. Higher-Order Functions (TRANSFORM/FILTER) ✓✓✓✓✓  5/5
  15. Recursive CTEs                           ✓✓  2/2
  16. QUALIFY / GROUPS BETWEEN / Named Windows  ✓✓✓  3/3
  17. NATURAL JOIN / LATERAL VIEW OUTER         ✓✓  2/2
────────────────────────────────────────────────────────────────
  Total:  105 passed, 0 failed — Score: 100% (105/105)
  Modes:  local ✅  local-cluster ✅  kubernetes-cluster ✅
══════════════════════════════════════════════════════════════════

TPC-H SF-1  — 22/22 PASS — Zelox 1.78s  vs  Spark 63.46s   (~36× warm/small)
TPC-H SF-100 — 22/22 PASS — Zelox 347s / 51.7GiB  vs  Spark 1099s / 115GiB
                            (~3.2× faster, ~2.2× less RAM — measured on EKS)
ClickBench 100M (distributed, EKS) — 43/43 PASS — Zelox 377.9s
```

### Streaming — measured head-to-head vs Apache Flink 1.19 (honest)

Zelox is also a streaming engine. The authoritative head-to-head is a **rigorous
Nexmark-methodology tri-engine run** vs **official Apache Flink 1.19** on AWS Graviton EKS,
identical 10 s tumbling keyed-COUNT over a shared Kafka topic (2026-07-01,
[docs/benchmarks/STREAMING_VS_FLINK_EKS.md](docs/benchmarks/STREAMING_VS_FLINK_EKS.md),
[docs/design/tri-engine-benchmark-matrix.md](docs/design/tri-engine-benchmark-matrix.md)):

Latest fair EKS run (2026-07-04, 100M events, BOTH engines measured like-for-like):

| Dimension | Flink 1.19 | Zelox | Verdict |
|---|--:|--:|---|
| **Throughput** (windowed-agg) | 5.66M ev/s | 5.51M ev/s | 🟡 **~tied** (Flink ~1.5% faster) |
| **Memory** (peak RSS) | 8.58 GiB | 7.06 GiB | 🟢 **~18% less** (no-JVM Arrow; batch ~8× less) |
| **Windowed-agg completeness** | 10 windows / 100M | 10 windows / 100M (`ZELOX_COMPLETE_ON_END`) | 🟢 **parity** (Flink `scan.bounded.mode`) |
| **Crash exactly-once** (16-part continuous, `kill -9`) | mature | **dup=0, sum exact, clean==crash** (EKS-confirmed) | 🟢 correct |
| **Kafka→Kafka passthrough** | parallel | **100M/100M @ 1.67M msg/s** (parallel sink) | 🟢 fixed (was a 1/16-partition data-loss bug) |
| **Latency** (ms, realtime) | p50 ~98 ms | tail better (no GC); clean re-measure pending | 🟡 to re-measure |

**Honest summary:** Zelox is **competitive, not categorically-better** on streaming — throughput **~tied**,
**memory wins**, **exactly-once holds across a hard crash** (16-partition continuous, verified via
`_spark_metadata`), **completeness matches Flink**, and the Kafka sink now writes **every partition** at
~1.67M msg/s (a fixed 15/16 data-loss bug). Latency vs Flink is being re-measured cleanly (the earlier
passthrough number was skewed by that sink bug). The head-to-heads surfaced + fixed real bugs the disciplined
way — see the [3-tier SDLC](docs/design/three-tier-sdlc.md) (T1 local → T2 `kind` → T3 EKS) and the
[Spark-parity + upgrade plan](docs/design/spark-parity-and-upgrade-plan.md).

- **Exactly-once** (Spark `MicroBatchExecution` / object-store checkpoint model): stateless
  **and** stateful, verified under clean restart **and hard crash (SIGKILL)** — including a
  real **Parquet-on-S3** sink (P1: rows=9000, dup=0, bit-identical after crash-resume).
- **Operators:** event-time tumbling windows + watermarks, keyed windowed aggregation,
  stream-stream / interval joins, stateful deduplication, durable file/S3 sink, parallel Kafka source.

> **The road to a true Spark + Flink replacement** — what's measured, where the real gaps
> are (throughput parse-fusion, latency, large-state, mid-job failure recovery), and the
> grounded plan to close them — is in **[docs/PROD_GRADE_ROADMAP.md](docs/PROD_GRADE_ROADMAP.md)**.
> Zelox is a strong Spark **batch** replacement and a **competitive** Flink streaming
> replacement; streaming throughput + operational maturity are the honest remaining work.

### Production workloads on real object storage (EKS 2026-07-02)

Canonical Uber/Netflix streaming-data-lake + batch-ETL patterns on **real S3**
([docs/design/production-workload-benchmark.md](docs/design/production-workload-benchmark.md)):

| Workload | Result |
|---|---|
| **P1** Kafka → 10 s windowed-agg → **Parquet on S3**, exactly-once | clean + **EO-under-crash** (kill -9 → resume from S3 checkpoint): rows=9000, **dup=0**, sum=90M **bit-identical**; 4.67M ev/s, 7.25 GiB |
| **P4** batch 200M rows → write **Parquet on S3** → read+agg **vs Spark 3.5.3** | Zelox **5.92 s / 3.44 GiB** vs Spark **36.94 s / 8.1 GiB** — **6.2× faster, 2.4× less memory, bit-identical output** |

---

## Quick Start

### Run with Docker (30 seconds, no install)

The published multi-arch image (linux/amd64 + linux/arm64 — the **same arm64 image** that runs on
EKS and Apple `container`) is on GHCR, signed and SBOM-attested:

```sh
# Start a Zelox Spark Connect server on :50051 (bind 0.0.0.0 so it's reachable through -p)
docker run --rm -p 50051:50051 ghcr.io/vikashgargg/zelox:latest server --ip 0.0.0.0 --mode local
```

```python
# Point any PySpark job at it — unchanged
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.range(1_000_000).selectExpr("sum(id)").show()
```

Verify provenance: `cosign verify ghcr.io/vikashgargg/zelox:latest` (keyless, Sigstore).

### Prerequisites

| Platform | Requirement |
|---|---|
| macOS | Apple Silicon (M1/M2/M3/M4). Python 3.10+ (auto-installed via Homebrew if missing) |
| Linux | x86_64 or aarch64. Python 3.10+ (`sudo apt install python3.11` / `sudo dnf install python3.11`) |

### Install (one command)

```sh
curl https://raw.githubusercontent.com/vikashgargg/zelox/main/install.sh | sh
```

The installer:
1. Downloads the pre-built binary for your platform
2. Creates an isolated Python venv at `~/.local/lib/zelox/venv` with pyspark 4.x + all Spark Connect deps
3. Wraps the binary so `zelox sql` / `zelox run` just work — no manual `PYTHONPATH` setup

After install, add to your PATH (shown by the installer) then test:

```sh
export PATH="$HOME/.local/bin:$PATH"   # paste exact line from installer output
zelox --version                         # zelox 0.1.0
zelox sql "SELECT 1"                    # prints +---+ \n| 1 | \n+---+
```

### Run a quick smoke test

```sh
# One-shot SQL
zelox sql "SELECT 'hello' AS msg, current_timestamp() AS ts"

# TPC-H benchmark (requires: pip install duckdb)
zelox bench --scale-factor 1
```

### Connect your existing PySpark code — change one line

```python
from pyspark.sql import SparkSession

# Before (Spark):
# spark = SparkSession.builder.getOrCreate()

# After (Zelox) — everything else stays the same:
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()

df = spark.read.parquet("s3://my-bucket/data/")
df.groupBy("region").agg({"revenue": "sum"}).show()
```

```sh
zelox server                             # start server on :50051
python my_job.py                         # run job using pyspark installed in the venv
# or: zelox run -f my_job.py            # run in-process, no separate server needed
```

### One-shot SQL

```sh
zelox sql "SELECT count(*) FROM parquet.'/tmp/data/*.parquet'"
```

### Run a PySpark script

```sh
zelox run -f my_etl_job.py
```

### TPC-H self-benchmark

```sh
zelox bench --scale-factor 10   # requires: pip install duckdb
```

---

## Batch **and** streaming in one engine

The whole point of Zelox: **one binary, one API** does Spark's batch **and** Flink-class streaming.
No JVM, no separate cluster, no second framework — the same `SparkSession` you already know. Start a
server once (`zelox server`, or the Docker image above), then point PySpark at `sc://localhost:50051`.

### Batch (Spark-class) — read → transform → write

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()

# Read Parquet (local, S3, or a Delta/Iceberg table), aggregate, write back — your Spark code, unchanged.
orders = spark.read.parquet("s3://my-bucket/orders/")
daily = (orders
         .withColumn("day", F.to_date("ts"))
         .groupBy("day", "region")
         .agg(F.sum("amount").alias("revenue"), F.countDistinct("user_id").alias("buyers")))

daily.write.mode("overwrite").partitionBy("day").parquet("s3://my-bucket/daily_revenue/")

# SQL works too — full Spark SQL surface (window functions, CTEs, PIVOT, QUALIFY, …)
spark.sql("SELECT region, SUM(revenue) FROM parquet.`s3://my-bucket/daily_revenue/` GROUP BY region").show()
```

### Streaming (Flink-class) — Kafka → event-time window → sink, **exactly-once**

The *same* `SparkSession`. Structured Streaming API — event-time windows, watermarks, and
exactly-once checkpointing to an object store (proven across a hard crash: see the P1 result above).

```python
from pyspark.sql import functions as F

# Kafka source → parse JSON → 10s tumbling event-time window with watermark → count per key
events = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "events")
          .load()
          .select(F.from_json(F.col("value").cast("string"),
                              "user_id STRING, amount DOUBLE, ts TIMESTAMP").alias("e"))
          .select("e.*"))

windowed = (events
            .withWatermark("ts", "30 seconds")
            .groupBy(F.window("ts", "10 seconds"), "user_id")
            .agg(F.sum("amount").alias("revenue")))

# Exactly-once sink to Parquet on S3 — checkpoint makes it crash-safe (kill -9 → resume, no dup/loss)
query = (windowed.writeStream
         .format("parquet")
         .option("path", "s3://my-bucket/windowed_revenue/")
         .option("checkpointLocation", "s3://my-bucket/_ckpt/windowed_revenue/")
         .outputMode("append")
         .trigger(processingTime="5 seconds")   # or .trigger(availableNow=True) for backfill
         .start())

query.awaitTermination()
```

## Realtime mode — millisecond streaming through the Spark API

This is the part Spark can't do and Flink makes you switch engines for.

Spark's Structured Streaming is micro-batch: latency is bounded below by your batch interval.
Flink gives you true event-at-a-time processing, but it's a different engine, a different API,
and a different operational story. **Zelox gives you both latency classes on one engine, and you
select between them with a single argument** — `trigger()`, the standard Spark API. Nothing else
in the query changes.

Under `continuous`, Zelox stops batching entirely and runs the query as a **long-lived,
event-at-a-time pipeline**, with the commit/epoch cadence decoupled from data flow. The trigger
interval controls how often state and offsets are committed, *not* how often data moves — data
flows continuously between commits. Tighten the interval for tighter commit granularity.

**Why the tail is flat:** there is no JVM and no garbage collector, so nothing in the pipeline can
be interrupted by a stop-the-world pause. In-engine *processing* latency is sub-millisecond;
end-to-end latency through Kafka is millisecond-class and network-dominated — **~62 ms p50,
126 ms p99.9** at 20k events/s, measured against Flink 1.19 on identical hardware (table below).
Flink edges the median; **Zelox's extreme tail is slightly better** — that's the no-GC payoff.

**Exactly-once holds in realtime mode, across hard crashes, at scale.** Multi-partition continuous
stateful processing commits window state, source offsets, and sink visibility together under
**aligned checkpoint barriers** (the Flink ABS model). Verified on EKS at 16 Kafka partitions and
100M events: `kill -9` mid-stream, resume, and the output is `dup=0`, sum exact, **bit-identical to
the uncrashed run**.

**Where this sits against Spark's own Real-Time Mode.** Spark 4.1 introduced RTM in Scala and 4.2
brought it to PySpark — but **stateless-only**: no Python UDFs, and stateful RTM (aggregations,
stream-stream joins, dedup) is deferred to **Spark 4.3**. Zelox's realtime path is **stateful today** —
windowed aggregation, joins, and dedup all run under the continuous engine with per-epoch commits. That
is the one place Zelox is genuinely ahead of upstream Spark rather than at parity.

> **On the 4.2 trigger:** Spark 4.2's `.trigger(realTime="<interval>")` (`Trigger.RealTime`) is wired end to
> end — `real_time_batch_duration` decodes to `StreamTrigger::RealTime` and routes to the same
> `StreamDriver::Realtime` engine as the pre-4.2 `.trigger(continuous=…)`. Either trigger enters Zelox's
> Flink-parity realtime mode; the duration is the commit/checkpoint interval (min 5 s per 4.2), not a latency
> target — records flow continuously between commits.

### Pick your latency — from backfill to millisecond realtime

Same query. One line changes:

| Mode | `trigger(...)` | Latency class | When to use |
|---|---|---|---|
| **Backfill** | `availableNow=True` | batch (process all, then stop) | catch-up, reprocessing, scheduled ETL |
| **Micro-batch** | `processingTime="5 seconds"` | **seconds → sub-second** | standard streaming ETL (Spark-class) |
| **Realtime** | `continuous="1 second"` | **millisecond-class, event-at-a-time** | Flink-class low-latency, per-event pipelines |

```python
# Same `windowed` query as above — only the trigger changes:
q1 = windowed.writeStream.format("parquet").option("path", OUT) \
     .option("checkpointLocation", CK).trigger(availableNow=True).start()        # backfill

q2 = windowed.writeStream.format("parquet").option("path", OUT) \
     .option("checkpointLocation", CK).trigger(processingTime="5 seconds").start()  # micro-batch

q3 = windowed.writeStream.format("parquet").option("path", OUT) \
     .option("checkpointLocation", CK).trigger(continuous="1 second").start()     # realtime mode
```

Tune the commit interval independently of throughput:

```python
q = df.writeStream.format("kafka")....trigger(continuous="1 second").start()
q = df.writeStream.format("kafka")....trigger(continuous="200 milliseconds").start()  # tighter commits
```

**This is the exact query our latency harness runs** ([scripts/stream_latency_query.py](scripts/stream_latency_query.py)) —
copy-paste and try it, don't take our word for it:

```python
# Kafka -> (passthrough) -> Kafka, REALTIME mode. Measured end-to-end latency, not a claim.
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "lat_in").option("startingOffsets", "latest").load())
(raw.select("value").writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "lat_out")
    .option("checkpointLocation", "/tmp/lat_ck")
    .trigger(continuous="1 second")      # <-- realtime mode
    .start().awaitTermination())
```

Run the whole probe (producer + this query + a latency consumer that reports p50/p99/p99.9) with
one command: `BOOT=localhost:9092 DURATION_S=60 RATE=20000 scripts/stream_latency.sh`.

**Measured head-to-head vs Apache Flink 1.19** (EKS, 20k events/s, end-to-end produce→consume):

| | p50 | p99 | p99.9 | max |
|---|--:|--:|--:|--:|
| **Zelox** (realtime) | 62 ms | 119 ms | **126 ms** | **129 ms** |
| Flink 1.19 | 53 ms | 110 ms | 127 ms | 131 ms |

Both are **millisecond-class** and competitive; Flink edges the median, **Zelox's extreme tail
(p99.9/max) is slightly better** — the no-GC payoff. This is a real, reproducible number, not a claim.

> **Status (EKS-measured 2026-07-04):** micro-batch modes are production-proven (incl. EO across a hard
> crash on S3). **Realtime-mode (continuous) multi-partition STATEFUL processing is exactly-once, no-duplicate
> and COMPLETE at scale** — validated on EKS at **16 partitions**. Crash-EO at scale was an open P0 through
> 2026-07-03; it was closed by **aligned checkpoint barriers** (Flink ABS-style: barrier-aligned atomic commit
> of window state + source offsets + sink visibility across all N instances), then EKS-confirmed at 100M
> events — `dup=0`, sum exact, clean run bit-identical to the crash run.

Both jobs run on the **same server**, the **same 105/105 Spark-compatible engine**, with **no JVM**
and **no Flink** — batch and streaming share one execution core. See
[docs/STREAMING.md](docs/STREAMING.md) for the streaming feature matrix and
[COMPAT.md](COMPAT.md) for the batch SQL surface.

---

## Deployment

> **Platform support:** macOS requires **Apple Silicon (M1/M2/M3/M4)**. Linux works on x86_64 and aarch64. Intel Macs are not supported.

---

### Mode 1 — Local (single process, no setup)

Best for: development, notebooks, quick queries.

```sh
# Install
curl https://raw.githubusercontent.com/vikashgargg/zelox/main/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"

# Start server
zelox server
# Listening on sc://127.0.0.1:50051 [mode: local]

# Connect from Python (pip install pyspark)
python3 - <<'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.sql("SELECT 'Zelox works!' AS msg").show()
spark.range(1000).groupBy().sum("id").show()
EOF
```

---

### Mode 2 — Local-cluster (multi-worker, single Apple Silicon Mac)

Best for: parallel workloads on M-series Mac (uses all cores across N workers).

```sh
# Start with 4 in-process workers
zelox server --mode local-cluster --workers 4
# Workers: 4  |  sc://127.0.0.1:50051

# Connect — same PySpark code, no changes
python3 - <<'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()

# Runs distributed across 4 workers
df = spark.read.parquet("/tmp/data/*.parquet")
df.groupBy("region").agg({"revenue": "sum"}).orderBy("sum(revenue)", ascending=False).show()
EOF
```

---

### Mode 3 — Apple Container (macOS 26 / Sequoia) — unique to Zelox

Best for: isolated, reproducible runs on Apple Silicon Mac using Apple's native container runtime (no Docker needed).

> **Requires:** macOS 26 Sequoia + Apple Container (`container` CLI). Apple Silicon only.

```sh
# One-time: build the arm64 image (~5 min first time, ~90s incremental)
make container-build

# --- Single-node local mode ---
container run --rm --name zelox \
  -p 50051:50051 \
  -v /tmp/zelox-data:/tmp/data \
  zelox:latest

# --- Local-cluster mode (4 in-process workers) ---
container run --rm --name zelox \
  -p 50051:50051 \
  -e ZELOX_MODE=local-cluster \
  -e ZELOX_EXECUTION__TARGET_PARTITIONS=4 \
  -v /tmp/zelox-data:/tmp/data \
  zelox:latest

# Connect from host Mac
python3 - <<'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.sql("SELECT count(*), avg(id) FROM range(1000000)").show()
EOF

# Stop
container stop zelox
```

---

### Mode 4 — Kubernetes (local kind cluster or production)

Best for: distributed multi-node workloads. Works on Linux x86_64 / aarch64 and Apple Silicon Mac via kind.

**Quickstart with kind (Mac or Linux):**

```sh
# Prerequisites: kubectl + kind installed
# brew install kind kubectl helm  (macOS)

# 1. Create a local k8s cluster
kind create cluster --name zelox

# 2. Deploy Zelox
kubectl apply -f k8s/zelox.yaml

# 3. Wait for pods ready
kubectl wait --for=condition=ready pod -l app=zelox-spark-server \
  -n zelox --timeout=120s

# 4. Forward port
kubectl port-forward -n zelox svc/zelox-spark-server 50051:50051 &

# 5. Run Spark job
python3 - <<'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
spark.sql("SELECT 'Running on K8s!' AS msg").show()
spark.range(10000000).groupBy().count().show()
EOF
```

**Production Helm deployment (with auth + HPA):**

```sh
helm install zelox ./helm/zelox \
  --namespace zelox --create-namespace \
  --set server.replicas=3 \
  --set auth.enabled=true \
  --set auth.token=my-secret-token \
  --set autoscaling.enabled=true \
  --set autoscaling.maxReplicas=10

# Connect with token
python3 - <<'EOF'
from pyspark.sql import SparkSession
spark = (SparkSession.builder
  .remote("sc://localhost:50051")
  .config("spark.connect.grpc.metadata", "Authorization=Bearer my-secret-token")
  .getOrCreate())
spark.sql("SELECT 'HA cluster!' AS msg").show()
EOF
```

---

### Quick comparison

| Mode | Command | Use case | Workers |
|---|---|---|---|
| `local` | `zelox server` | Dev / notebooks | 1 process |
| `local-cluster` | `zelox server --mode local-cluster --workers 4` | Multi-core Mac | N in-process |
| Apple Container local | `container run ... zelox:latest` | Isolated, reproducible | 1 container |
| Apple Container cluster | `container run -e ZELOX_MODE=local-cluster ...` | Isolated multi-worker | N in-container |
| Kubernetes | `kubectl apply -f k8s/zelox.yaml` | Distributed, production | K8s pods |

---

## What Works Today (v0.6.0-alpha)

### SQL & Query Engine
| Feature | Status |
|---|---|
| `SELECT`, `JOIN`, `GROUP BY`, `ORDER BY`, subqueries, CTEs | ✅ |
| Window functions (`RANK`, `ROW_NUMBER`, `LAG`, `LEAD`, `NTILE`, …) | ✅ |
| `HAVING` with aggregate-only expressions | ✅ |
| `QUALIFY` clause (Spark 3.x+) | ✅ |
| `WITH RECURSIVE` CTEs | ✅ |
| `PIVOT` / `UNPIVOT` (all variants including empty IN list) | ✅ |
| `TABLESAMPLE` (percent / rows / byte-size / BUCKET OUT OF) | ✅ |
| `GROUPS BETWEEN` windows | ✅ |
| FROM-first HiveQL (`FROM t SELECT …`) | ✅ |
| Higher-order functions (`transform`, `filter`, `aggregate`) | ✅ |
| `LATERAL VIEW` / `LATERAL VIEW OUTER` | ✅ |
| `NATURAL JOIN` | ✅ |

### Data & Storage
| Feature | Status |
|---|---|
| Parquet (read/write, predicate pushdown, partition pruning) | ✅ |
| Delta Lake (read/write/DELETE/UPDATE/MERGE/VACUUM) | ✅ |
| Iceberg (read/write/REST catalog) | partial |
| JSON (PERMISSIVE / DROPMALFORMED / FAILFAST) | ✅ |
| CSV (inferSchema, custom delimiter) | ✅ |
| Avro, ORC | ✅ |
| AWS S3 / GCS / Azure ADLS / local FS | ✅ |

### Python & UDFs
| Feature | Status |
|---|---|
| Python UDFs — scalar, Pandas (vectorized), Arrow | ✅ |
| `cloudpickle` serialisation | ✅ |
| `df.approxQuantile()` | ✅ |
| `df.freqItems()` | ✅ |
| Lambda HOFs (`transform`, `filter`, `aggregate`) | ✅ |

### Structured Streaming (Flink-class)
| Feature | Status |
|---|---|
| Kafka source, **parallel** (per Spark `KafkaSourceRDD` / Flink FLIP-27) | ✅ |
| Sinks: Parquet/file (incl. **S3**), Kafka, `memory`, `foreachBatch` | ✅ |
| Triggers: `processingTime`, `availableNow`, continuous | ✅ |
| Event-time windows (`F.window()`) + watermarks, **keyed & parallel** | ✅ |
| **Per-partition watermark** (Flink `withIdleness`) — no premature window close | ✅ |
| Streaming aggregates (COUNT/SUM/AVG), append + **update/retraction** output | ✅ |
| Stream–stream / interval joins; stream × static join | ✅ |
| Stateful deduplication (`dropDuplicates`) | ✅ |
| **Exactly-once**, crash-verified (`kill -9` → resume): stateless **and** stateful, incl. **Parquet-on-S3** sink (dup=0, bit-identical) | ✅ |
| **Multi-partition *continuous* stateful** — no-dup + **complete** (validated to **16 partitions on EKS**, dup=0, sum exact) | ✅ |
| Multi-partition continuous **exactly-once across hard crash** — aligned checkpoint barriers (Flink ABS), EKS-confirmed at 100M | ✅ |
| Spillable large state (object-store) + incremental checkpoints | ✅ |
| Rescale from checkpoint (key-groups, Flink FLIP-8) | ✅ gated |
| Iceberg sink | 🚧 in progress |

### Infrastructure
| Feature | Status |
|---|---|
| `local` / `local-cluster` / `kubernetes-cluster` modes | ✅ |
| Apple Container (macOS 26, **Apple Silicon only**) | ✅ |
| Kubernetes Helm chart (HPA, liveness/readiness) | ✅ |
| Scheduler HA via K8s Lease election (`--ha`) | ✅ |
| Bearer token auth (`--auth-token` / `ZELOX_AUTH__TOKEN`) | ✅ |
| mTLS (`--tls-cert/--tls-key/--tls-ca`) | ✅ |
| Web UI on `:4040` (query history + streaming status) | ✅ |
| Prometheus `/metrics` endpoint | ✅ |
| OpenTelemetry OTLP traces | ✅ |

---

## Architecture

```
PySpark client  ──Spark Connect gRPC + JWT/mTLS──▶  zelox server
                                                          │
                                          ┌───────────────┼───────────────┐
                                          │               │               │
                                    SQL parser      Spark plan       Python UDFs
                                    (Rust PEG)      resolver         (PyO3 / cloudpickle)
                                          │               │
                                          └───────┬───────┘
                                                  ▼
                                          Apache DataFusion
                                        (vectorized, columnar, SIMD)
                                                  │
                              ┌───────────────────┼───────────────────┐
                              │                   │                   │
                           Parquet             Delta Lake          Iceberg
                        S3 / GCS / ADLS        (delta-rs)       (iceberg-rust)
                              │
                         Arrow Flight
                       (distributed shuffle)
                              │
                    ┌─────────┴─────────┐
                    │                   │
               Kubernetes           Apple Container
             (Helm + K8s Lease)    (arm64-native)
```

**Stack:**
- [Apache DataFusion](https://github.com/apache/datafusion) — vectorized query engine (v53+)
- [Apache Arrow](https://github.com/apache/arrow-rs) — zero-copy columnar memory
- [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) — high-throughput shuffle transport
- [PyO3](https://github.com/PyO3/pyo3) — Python UDF bridge (zero-copy Arrow)
- [tonic](https://github.com/hyperium/tonic) — gRPC (Spark Connect wire protocol)
- [delta-rs](https://github.com/delta-io/delta-rs) — native Rust Delta Lake
- [rdkafka](https://github.com/fede1024/rust-rdkafka) — Kafka streaming source

---

## CLI Reference

```
zelox server [--ip IP] [--port PORT] [--mode MODE] [--workers N]
             [--auth-token TOKEN] [--tls-cert PATH] [--tls-key PATH] [--ha]
zelox sql "<query>"             Execute SQL and print results
zelox run -f <script.py>        Run a PySpark script
zelox shell                     Interactive PySpark REPL
zelox bench [--scale-factor N]  TPC-H benchmark (requires pip install duckdb)
```

**Key environment variables:**

| Variable | Default | Description |
|---|---|---|
| `ZELOX_MODE` | `local` | `local` / `local-cluster` / `kubernetes-cluster` |
| `ZELOX_AUTH__TOKEN` | — | Bearer token for gRPC auth |
| `ZELOX_AUTH__TLS__CERT` | — | Path to TLS certificate (PEM) |
| `PYTHONPATH` | — | Path to PySpark site-packages (required for Python UDFs) |
| `ZELOX_RUNTIME__STACK_SIZE` | `8388608` | Tokio worker thread stack size in bytes |

---

## Build from Source

```sh
# Prerequisites: Rust 1.91+, protoc 3.x, Python 3.10+
git clone https://github.com/vikashgargg/zelox
cd zelox

# Dev build (fast, unoptimised)
make dev
./target/debug/zelox --version

# Release build (LTO, ~30 min)
make release
./target/release/zelox --version

# Cross-compile: Linux musl (x86_64 + aarch64) + macOS universal2
make build-all
```

---

## Roadmap

| Phase | Timeline | Goal |
|---|---|---|
| **Phase 1** ✅ | Done | 105/105 Spark compat, 22/22 TPC-H, K8s + Apple Container |
| **Phase 2** ✅ | Done | Streaming (Kafka/foreachBatch/checkpoint), auth, HA, Web UI |
| **Phase 3** ✅ | Done 2026-05-30 | VARIANT, GroupedMap, time travel, dbt, ClickBench, Iceberg OverwritePartitions, event-time windows, stateful dedup, theta sketch, Vortex skeleton, 95%+ Spark test suite |
| **Phase 4** ✅ | Done 2026-07-02 | Flink 1.19 streaming head-to-head on EKS; exactly-once across hard crash incl. **Parquet-on-S3** sink; prod-workload benchmarks (P1 streaming EO, P4 batch 6.2× vs Spark); spillable/incremental state; per-partition watermark; TPC-DS-99 coverage |
| **Phase 5** 🔜 | In progress | **Public GA prod-grade**: pullable GHCR image (signed + SBOM), Helm publish, streaming Iceberg sink, streaming latency, large-state backend, observability metrics |

Full plans: distribution/repo prod-grade **[GA readiness board](docs/design/public-ga-readiness-board.md)**;
engine gaps **[PROD_GRADE_ROADMAP.md](docs/PROD_GRADE_ROADMAP.md)**; and the 1.0 GA acceptance
checklist **[docs/PRODUCTION_READINESS.md](docs/PRODUCTION_READINESS.md)**.

---

## License

Apache 2.0.

Zelox is a fork of [Sail](https://github.com/lakehq/sail), developed by LakeSail, Inc.
and the Sail contributors, and licensed under the Apache License 2.0. Portions of this
software are derived from Sail and retain that license and the copyright of their
original authors — see [`NOTICE`](NOTICE) for full attribution.

We have deep respect for that work and contribute fixes upstream wherever possible.
Zelox is an independent project and is not affiliated with or endorsed by LakeSail, Inc.
