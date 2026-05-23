# Vajra — Production Spark Replacement Roadmap

> Last updated: 2026-05-24  
> Branch: `phase1/spark-100`  
> Goal: Full Apache Spark replacement for **batch + streaming** on Apple Container cluster and Kubernetes

---

## Current Baseline

| Metric | Value |
|---|---|
| SQL compat scorecard | **105/105 (100%)** |
| TPC-H SF-1 (22 queries) | **22/22 PASS, 1.515s total** |
| K8s modes validated | local / local-cluster / kubernetes-cluster (kind) |
| Upstream lakehq/sail comparison | +20 SQL features ahead |
| Streaming | Stateless filter/project only |

---

## How to Read This Doc

Each item has:
- **Status**: `[ ]` not started · `[~]` in progress · `[x]` done
- **Effort**: rough estimate for a single focused session
- **Priority**: P0 (blocks production) / P1 (needed for GA) / P2 (nice to have)
- **Test**: what proves it's done

---

## Track 1 — DDL / Command Gaps

These gap out in real ETL workloads even when the query engine is fine.

### 1.1 ALTER VIEW  `[ ]` P1 · ~2h

**What:** `ALTER VIEW v AS SELECT ...` — replace view definition in catalog.

**File:** `crates/sail-plan/src/resolver/command/mod.rs:302`

**Test:**
```python
spark.sql("CREATE OR REPLACE TEMP VIEW v AS SELECT 1 AS a")
spark.sql("ALTER VIEW v AS SELECT 2 AS b")
assert spark.sql("SELECT * FROM v").collect()[0].b == 2
```

**Done when:** test passes, `PlanError::todo("CommandNode::AlterView")` removed.

---

### 1.2 INSERT INTO ... PARTITION  `[ ]` P1 · ~4h

**What:** `INSERT INTO t PARTITION (dt='2024-01-01') SELECT ...` — explicit partition clause on writes.

**File:** `crates/sail-plan/src/resolver/command/insert.rs:76`

**Test:**
```python
spark.sql("CREATE TABLE t (v INT, dt STRING) USING DELTA PARTITIONED BY (dt)")
spark.sql("INSERT INTO t PARTITION (dt='2024-01-01') VALUES (42)")
assert spark.sql("SELECT v FROM t WHERE dt='2024-01-01'").collect()[0].v == 42
```

**Done when:** partitioned INSERT works for Delta Lake tables.

---

### 1.3 DESCRIBE FUNCTION / CATALOG  `[ ]` P2 · ~2h

**What:** `DESCRIBE FUNCTION upper` / `DESCRIBE CATALOG my_catalog`.

**File:** `crates/sail-plan/src/resolver/command/mod.rs:310-315`

**Test:** `spark.sql("DESCRIBE FUNCTION upper").show()` returns at least one row.

---

### 1.4 COMMENT ON TABLE / COLUMN  `[ ]` P2 · ~2h

**What:** `COMMENT ON TABLE t IS 'my table'` — write table/column metadata.

**File:** `crates/sail-plan/src/resolver/command/mod.rs:341-350`

**Done when:** no error, comment persisted in catalog.

---

### 1.5 CLUSTER BY for write  `[ ]` P2 · ~3h

**What:** `CREATE TABLE t CLUSTER BY (col)` / `INSERT INTO t ... CLUSTER BY`.

**File:** `crates/sail-plan/src/resolver/command/write.rs:213`

**Note:** Maps to Delta Lake Liquid Clustering (`delta-rs` `ClusterBySpec`).

---

### 1.6 CREATE CATALOG  `[ ]` P2 · ~4h

**What:** `CREATE CATALOG my_cat USING iceberg OPTIONS (uri=...)`.

**File:** `crates/sail-plan/src/resolver/command/mod.rs:199`

---

## Track 2 — Structured Streaming

Phase 2 goal: `readStream → transform → writeStream` end-to-end for Kafka → Delta.

### 2.1 Streaming Aggregates (non-stateful)  `[ ]` P0 · ~2 weeks

**What:** `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` over each micro-batch window. No state carried between batches — append-mode aggregation.

**File:** `crates/sail-plan/src/streaming/rewriter.rs:89`

**Design:**
```
StreamingRewriter::f_up(LogicalPlan::Aggregate)
  → wrap in StreamAggregateNode (new logical node)
  → physical: AggregateExec per micro-batch, reset state per batch
  → output schema: original aggregate schema + marker/retracted fields
```

**New files needed:**
- `crates/sail-logical-plan/src/streaming/aggregate.rs` — `StreamAggregateNode`
- `crates/sail-execution/src/streaming/aggregate.rs` — physical exec

**Test:**
```python
sdf = spark.readStream.format("rate").option("rowsPerSecond", 10).load()
result = sdf.groupBy().count()
q = result.writeStream.format("memory").queryName("counts").start()
time.sleep(3); q.stop()
assert spark.sql("SELECT * FROM counts").count() > 0
```

**Done when:** append-mode `count()` over `rate` source works end-to-end.

---

### 2.2 Kafka Source  `[ ]` P0 · ~3 days

**What:** `spark.readStream.format("kafka").option("kafka.bootstrap.servers", ...).option("subscribe", "topic")`.

**Crate:** add `rdkafka = "0.36"` to `sail-data-source/Cargo.toml`

**Files:**
- `crates/sail-data-source/src/formats/kafka/mod.rs` — `KafkaSource` impl
- `crates/sail-data-source/src/formats/kafka/consumer.rs` — offset management
- Wire into `crates/sail-plan/src/resolver/command/write_stream.rs`

**Schema returned:**
```
key: Binary, value: Binary, topic: String,
partition: Int32, offset: Int64, timestamp: Timestamp,
timestampType: Int32
```

**Offset checkpoint:** write to `checkpoint_location/_kafka_offsets/batch_N.json`.

**Test:**
```python
# Requires local Kafka (docker compose or testcontainers)
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest").load()
q = df.writeStream.format("memory").queryName("kafka_test").start()
# ... publish messages ... 
assert spark.sql("SELECT count(*) FROM kafka_test").collect()[0][0] > 0
```

---

### 2.3 foreachBatch Sink  `[ ]` P0 · ~3 days

**What:** `writeStream.foreachBatch(fn)` — call a Python function with each micro-batch as a DataFrame.

**File:** `crates/sail-plan/src/resolver/command/write_stream.rs:31`

**Design:**
```
ForeachBatchSink {
  fn: Arc<dyn Fn(DataFrame, i64) -> Result<()>>,
  batch_id: AtomicI64,
}
impl StreamSink for ForeachBatchSink {
  fn write_batch(&self, batch: RecordBatch) → call Python fn via PyO3
}
```

**Test:**
```python
results = []
def process(df, batch_id):
    results.append(df.count())

sdf = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
q = sdf.writeStream.foreachBatch(process).start()
time.sleep(3); q.stop()
assert len(results) > 0
```

---

### 2.4 Streaming Window Functions (Event-Time)  `[ ]` P1 · ~1 week

**What:** `window("timestamp", "1 minute")` tumbling/sliding windows with watermark.

**File:** `crates/sail-plan/src/streaming/rewriter.rs:86`

**Prerequisite:** 2.1 (streaming aggregates) must be done first.

**Test:**
```python
sdf = spark.readStream.format("rate").load() \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(window("timestamp", "1 minute")).count()
q = sdf.writeStream.outputMode("append").format("memory").queryName("w").start()
time.sleep(65); q.stop()
assert spark.sql("SELECT * FROM w").count() > 0
```

---

### 2.5 Streaming Join (Stream × Static)  `[ ]` P1 · ~1 week

**What:** Join a streaming DataFrame with a static/batch DataFrame (broadcast join pattern).

**File:** `crates/sail-plan/src/streaming/rewriter.rs:95`

**Note:** Stream × Stream joins (with watermarks) are separate and more complex — P2.

---

### 2.6 Streaming Repartition  `[ ]` P1 · ~3 days

**What:** `sdf.repartition(n)` in a streaming query. Map to no-op or round-robin within micro-batch.

**File:** `crates/sail-plan/src/streaming/rewriter.rs:98`

---

### 2.7 Checkpoint & Recovery  `[ ]` P1 · ~1 week

**What:** On restart with same `checkpointLocation`, resume from last committed offset. Exactly-once semantics for Delta sink.

**Design:**
```
checkpoint/
  offsets/    ← what was read (per-source offsets)
    0.json, 1.json ...
  commits/    ← what was written (per-batch commit)  
    0.json, 1.json ...
  metadata    ← query ID, run ID
```

**Test:** Kill and restart streaming query; verify no duplicate rows in Delta sink.

---

### 2.8 mapGroupsWithState / flatMapGroupsWithState  `[ ]` P2 · ~3 weeks

**What:** Arbitrary stateful processing per group key. Most complex streaming feature.

**Note:** Requires a persistent state store (RocksDB or Delta-backed). Phase 3 target.

---

## Track 3 — Kubernetes Production Hardening

### 3.1 CI: K8s Validation in GitHub Actions  `[ ]` P0 · ~4h

**What:** Run `scripts/run_validation_only.sh` (kind cluster + 3 modes) on every PR.

**File:** `.github/workflows/ignite-ci.yml` — add `validate-k8s` job.

**Design:**
```yaml
validate-k8s:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - name: Install kind + kubectl
      run: |
        curl -Lo kind https://kind.sigs.k8s.io/dl/v0.23.0/kind-linux-amd64
        chmod +x kind && mv kind /usr/local/bin/
    - name: Build Docker image
      run: docker build -t vajra:latest -f docker/Dockerfile .
    - name: Run validation
      run: bash scripts/run_validation_only.sh
```

**Done when:** green badge on main branch, `105/105` in CI log for kubernetes-cluster mode.

---

### 3.2 Scheduler High Availability (eliminate SPOF)  `[ ]` P0 · ~2 weeks

**What:** The Spark Connect server / driver pod is a single point of failure. If it crashes, all in-flight queries are lost.

**Design options:**
1. **Leader election via Kubernetes lease** — multiple driver replicas, one holds the lease, others are standby. On leader death, etcd/lease election picks a new leader. Simplest to implement.
2. **Job journal to object store** — driver writes task state to S3/GCS before every task dispatch; new leader reads journal and re-dispatches. True HA.

**Recommended:** Start with option 1 (K8s Lease API). Option 2 is Phase 3.

**Files:**
- `crates/sail-cli/src/spark/leader_election.rs` (new)
- `helm/vajra/templates/server-deployment.yaml` — bump `replicas` to 2
- Add `LeaderElection` helm values

---

### 3.3 OAuth2 / mTLS Auth Middleware  `[ ]` P1 · ~1 week

**What:** Production multi-tenant deployment needs auth. Currently the gRPC server accepts all connections.

**Design:**
- mTLS (mutual TLS) for service-to-service (driver ↔ worker)
- Bearer token (JWT) via gRPC metadata for client → server

**Files:**
- `crates/sail-spark-connect/src/auth.rs` (new)
- tonic `Layer` that validates `Authorization: Bearer <jwt>` header
- Helm: `auth.enabled`, `auth.jwksUri` values

---

### 3.4 Worker Image Pull Secrets  `[ ]` P1 · ~2h

**What:** When deploying from a private registry (ECR, GCR, ACR), worker pods spawned by the driver need `imagePullSecrets`.

**File:** `helm/vajra/templates/server-deployment.yaml`

**Change:** Pass `imagePullSecrets` in `SAIL_KUBERNETES__WORKER_POD_TEMPLATE` env var when `image.pullSecrets` is set.

**Done when:** `helm install --set image.pullSecrets[0].name=my-secret` works.

---

### 3.5 Resource Quotas & Multi-Tenant Isolation  `[ ]` P1 · ~3 days

**What:** Multiple teams sharing one K8s cluster need namespace-level resource quotas.

**Files:**
- `helm/vajra/templates/resourcequota.yaml` (new)
- `helm/vajra/values.yaml` — add `quota.enabled`, `quota.maxCPU`, `quota.maxMemory`

---

### 3.6 Graceful Query Cancellation on Pod Eviction  `[ ]` P1 · ~3 days

**What:** When K8s evicts a worker pod (OOM, node pressure), in-flight query partitions should be retried on another worker rather than failing the whole job.

**Files:**
- `crates/sail-cli/src/cluster/worker.rs` — handle SIGTERM by completing current task or reporting failure
- Driver scheduler: treat worker disconnect during task as retriable

---

### 3.7 Web UI (Status Dashboard)  `[ ]` P2 · ~2 weeks

**What:** Simple HTTP endpoint showing: active queries, completed queries, worker health, memory usage.

**Design:** Axum HTTP server on port 4040 (same as Spark UI port) serving JSON + minimal HTML.

**Endpoint plan:**
```
GET /api/v1/queries          → list active/recent queries
GET /api/v1/queries/{id}     → query stages, tasks, metrics
GET /api/v1/workers          → worker pool status
GET /api/v1/metrics          → Prometheus-format metrics
GET /                        → minimal HTML dashboard
```

---

### 3.8 TPC-H SF-10 / SF-100 Distributed Benchmark  `[ ]` P1 · ~3 days

**What:** Validate correctness AND performance at scale in distributed mode. SF-1 single-node works; SF-100 distributed is unvalidated.

**Steps:**
1. Generate SF-100 Parquet files via DuckDB (needs ~120 GB disk)
2. Upload to S3 (or local PV in kind cluster)
3. Run `ignite bench --scale-factor 100 --mode kubernetes-cluster`
4. Publish results to `BENCHMARKS.md`

**Target:** SF-100 in < 60s on 5 workers × 4 vCPU.

---

## Track 4 — Testing Infrastructure

### 4.1 TPC-DS Query Suite  `[ ]` P1 · ~1 week

**What:** TPC-DS is a better benchmark for analytics than TPC-H (99 queries, complex correlated subqueries, window functions).

**Steps:**
1. Generate TPC-DS SF-1 data via DuckDB
2. Write `scripts/tpcds_bench.py` — same structure as current TPC-H bench
3. Run against Vajra and document pass rate
4. Fix failing queries

**Target:** 90%+ TPC-DS pass rate.

---

### 4.2 Official Apache Spark Test Suite Integration  `[ ]` P1 · ~1 week

**What:** `scripts/spark-tests/` has patches for Spark 3.5.7 and 4.1.1. Wire these into CI to run a subset of official Spark Python tests against Vajra.

**Steps:**
1. Apply `spark-4.1.1.patch` to Spark test suite
2. Run `scripts/spark-tests/run-tests.sh` targeting Vajra
3. Capture fail list → add to known gaps
4. Fix top-10 failures

---

### 4.3 Concurrency / Multi-Session Tests  `[ ]` P1 · ~3 days

**What:** Multiple simultaneous Spark Connect sessions executing queries in parallel. Tests for session isolation, no shared state corruption.

**File:** `scripts/test_concurrency.py` (new)

```python
import concurrent.futures
from pyspark.sql import SparkSession

def run_session(i):
    spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
    result = spark.sql(f"SELECT {i} * {i} AS sq").collect()[0].sq
    assert result == i * i, f"session {i}: expected {i*i}, got {result}"

with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
    list(ex.map(run_session, range(20)))
print("All 20 concurrent sessions OK")
```

---

### 4.4 Memory Pressure / Spill Tests  `[ ]` P2 · ~3 days

**What:** Verify that queries exceeding `memory_limit` correctly spill to disk and still produce correct results.

**File:** `scripts/test_spill.py` (new)

---

### 4.5 Chaos Tests (Worker Kill)  `[ ]` P2 · ~3 days

**What:** Kill a worker pod mid-query; verify the job retries and completes (or fails with a clear error, not a hang).

**Tool:** `kubectl delete pod` during benchmark run.

---

## Track 5 — Function Coverage

Currently 9 stub functions in `sail-plan/src/function/`. Most are obscure but some appear in production Spark code.

### 5.1 Lambda Functions (transform / filter / aggregate)  `[ ]` P1 · ~1 week

**What:** `transform(array, x -> x + 1)`, `filter(array, x -> x > 0)`, `aggregate(array, 0, (acc, x) -> acc + x)`.

**File:** `crates/sail-plan/src/function/scalar/lambda.rs:36`

**Test:**
```python
spark.sql("SELECT transform(array(1,2,3), x -> x * 2)").collect()
# expected: [Row([2, 4, 6])]
```

---

### 5.2 `date_diff` Extended Units  `[ ]` P2 · ~2h

**What:** `datediff(unit, start, end)` — currently unsupported for units other than `DAY`.

**File:** `crates/sail-plan/src/function/scalar/datetime.rs:227`

---

## Implementation Progress Tracker

### Sprint 1 (Current — Week of 2026-05-24)

| # | Item | Owner | Status | Notes |
|---|---|---|---|---|
| S1.1 | Write PRODUCTION_ROADMAP.md | Claude | `[x]` | This file |
| S1.2 | Update STATUS.md to 105/105 | Claude | `[ ]` | |
| S1.3 | Commit SafeOptimizeProjections fix | Claude | `[ ]` | Branch: phase1/spark-100 |
| S1.4 | Implement ALTER VIEW | Claude | `[ ]` | |
| S1.5 | Implement INSERT PARTITION | Claude | `[ ]` | |
| S1.6 | Wire K8s CI validation | Claude | `[ ]` | |

### Sprint 2 (Streaming Foundations — Week of 2026-05-31)

| # | Item | Status | Notes |
|---|---|---|---|
| S2.1 | Streaming aggregates (non-stateful) | `[ ]` | Biggest lift |
| S2.2 | Kafka source | `[ ]` | rdkafka integration |
| S2.3 | foreachBatch sink | `[ ]` | PyO3 callback |
| S2.4 | Lambda functions (transform/filter/aggregate) | `[ ]` | |

### Sprint 3 (Production Hardening — Week of 2026-06-07)

| # | Item | Status | Notes |
|---|---|---|---|
| S3.1 | Scheduler HA (K8s Lease election) | `[ ]` | Eliminates SPOF |
| S3.2 | mTLS auth middleware | `[ ]` | Multi-tenant |
| S3.3 | TPC-H SF-100 distributed benchmark | `[ ]` | Performance validation |
| S3.4 | TPC-DS test suite | `[ ]` | Wider SQL coverage |

### Sprint 4 (GA — Week of 2026-06-14)

| # | Item | Status | Notes |
|---|---|---|---|
| S4.1 | Streaming window functions (event-time) | `[ ]` | Tumbling/sliding |
| S4.2 | Streaming join (stream × static) | `[ ]` | Broadcast join |
| S4.3 | Official Spark test suite integration | `[ ]` | CI green |
| S4.4 | Web UI (status dashboard) | `[ ]` | Port 4040 |
| S4.5 | vajra-pyspark PyPI package | `[ ]` | pip install |

---

## Definition of Done: "Full Spark Replacement"

Vajra can be called a full production Spark replacement when:

- [ ] **SQL**: 105/105 scorecard + 95%+ TPC-DS + official Spark test suite green
- [ ] **Batch**: TPC-H SF-100 distributed (5 workers) in < 60s
- [ ] **Streaming**: Kafka → aggregate → Delta pipeline runs 24h without error
- [ ] **K8s**: Scheduler HA (no SPOF), HPA autoscaling, mTLS auth
- [ ] **Apple Container**: All 3 modes (local / local-cluster / cluster) validated in CI
- [ ] **Ops**: Web UI on :4040, OTLP metrics/traces, Grafana dashboard
- [ ] **Install**: `pip install vajra-pyspark` + `curl | sh` both work
- [ ] **Docs**: Migration guide from Spark 3.5 → Vajra

**Estimated completion: Sprint 4 end (2026-06-21)**

---

## Quick Reference: Key Files

| What | File |
|---|---|
| Streaming rewriter (all gaps) | `crates/sail-plan/src/streaming/rewriter.rs` |
| Command dispatcher (DDL gaps) | `crates/sail-plan/src/resolver/command/mod.rs` |
| Insert resolver | `crates/sail-plan/src/resolver/command/insert.rs` |
| Optimizer fix (RecursiveQuery) | `crates/sail-logical-optimizer/src/lib.rs` |
| Recursive CTE resolver | `crates/sail-plan/src/resolver/query/recursion.rs` |
| Helm chart | `helm/vajra/` |
| CI pipeline | `.github/workflows/ignite-ci.yml` |
| Scorecard | `scripts/spark_compat_score.py` |
| Validation runner | `scripts/run_validation_only.sh` |
| Benchmarks | `BENCHMARKS.md` |
