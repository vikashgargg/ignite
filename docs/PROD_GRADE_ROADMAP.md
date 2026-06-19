# Vajra → a true production-grade Spark **and** Flink replacement

> The honest, grounded roadmap. What Vajra is, what's **measured** today, where it
> genuinely stands against the systems it intends to replace, and exactly what's left —
> with each gap tied to how the proven systems (Apache Spark, Apache Flink, Apache
> Arrow, Apache DataFusion, Arrow Flight) actually solve it.
>
> Last updated: 2026-06-19 (branch `phase5/real-world-head-to-head`).

---

## 0. Thesis

One Rust-native, no-JVM engine, **one Spark API** (Spark Connect gRPC), spanning **batch
+ micro-batch + real-time streaming**, on Apache Arrow + DataFusion, with object-store
-centric state. The bet: the *architecture* (no GC, columnar/Arrow, cloud-native state)
lets a single engine match Spark on batch and approach Flink on streaming while using a
fraction of the memory — and unify what today needs two systems.

This document is deliberately honest: it records measured **wins** and measured
**losses**, and treats "we haven't measured it" as "we don't claim it."

---

## 1. Where Vajra stands today — measured, not asserted

### Batch / SQL (the Spark replacement) — strong
| Workload | Result | Source |
|---|---|---|
| TPC-H SF-1 (warm) | **~36× faster** than Spark 3.5.3 | `docs/benchmarks/TPCH_SF1.md` |
| ClickBench 1M (same box) | **~12×** faster than Spark | `docs/benchmarks/CLICKBENCH.md` |
| **ClickBench 100M distributed (EKS, S3, Graviton)** | 43/43, 377.9 s | `docs/SCALE_TESTING.md` |
| **TPC-H SF-100 (100 GB, EKS) vs Spark** | **3.2× faster, 2.2× less RAM**, 22/22 | `docs/benchmarks/TPCH_SF100.md` |
| Spark-compat differential trust | 124/124 vs real Spark | scorecard |

Scaling is honest: the speedup *shrinks with scale* (engine/JVM overhead dominates small
data; at 100 GB both are I/O/compute-bound → still ~3×). The defensible claim is
**"~3× faster and ~2× less memory at scale; up to ~36× on small/warm workloads."**

### Streaming (the Flink replacement) — mixed, just measured head-to-head
On one `c7g.4xlarge` (Graviton), official Flink 1.19, identical 100M-event 10 s tumbling
keyed-COUNT, shared Kafka topic (`docs/benchmarks/STREAMING_VS_FLINK_EKS.md`):

| Dimension | Flink 1.19 | Vajra | Verdict |
|---|---|---|---|
| **Throughput** | 1.157M ev/s | **1.543M ev/s** | 🟢 Vajra **1.33× faster** |
| **Memory** (peak RSS) | 8.24 GiB | **1.29 GiB** | 🟢 Vajra **~6.4× less** |
| **Exactly-once** | mature | EO across hard kill ✓ (100000/100000) | 🟢 correct / 🟡 less hardened |
| **Latency** | ms (Kafka) / ~ckpt (file) | p50 ~30 s (realtime probe) | 🔴 **Flink wins clearly** |

The throughput win came from fixing a real bug the head-to-head surfaced (single-threaded
Kafka source → parallelized per Spark `KafkaSourceRDD` / Flink FLIP-27). Latency is the
clear, honest gap (root cause: no record-level low-latency sink + immature realtime mode).

---

## 2. What "replacing Spark AND Flink" actually requires

Spark = batch + SQL + Structured Streaming (micro-batch, now also **Real-Time Mode**).
Flink = low-latency, stateful, exactly-once event streaming. A single engine that
replaces both must be **excellent on all of**: throughput, memory, latency, large state,
exactly-once under failure, elastic rescaling, and operational maturity. Vajra wins the
first two, holds correctness, and has real gaps on the rest. The sections below are the
gap analysis, each grounded in how the incumbents do it.

---

## 3. Gap analysis by subsystem (grounded → Vajra status → what's needed)

### 3.1 Streaming latency — **P0, the #1 gap**
- **Flink:** record-at-a-time pipelined operators; Kafka sink emits per record → **ms**
  end-to-end. Unaligned checkpoints keep latency low under backpressure.
- **Spark Real-Time Mode (2025):** abandons the micro-batch latency floor with
  **long-running stages** that process records on arrival → sub-second/ms p99, *keeping*
  exactly-once + the DataFrame API. (https://www.databricks.com/blog/introducing-real-time-mode-apache-sparktm-structured-streaming)
- **Vajra now:** **Kafka sink landed** (commit `74b167bc`) — `writeStream.format("kafka")`
  produces records **on arrival** (record-paced) via librdkafka, flushing per epoch. Measured
  Kafka→Vajra→Kafka: **p50 51 ms / p99 202 ms at a 250 ms epoch** (p99 ≈ epoch interval),
  vs the old file-sink ~30 s — **~600× better, Flink-class** (`STREAMING_VS_FLINK_EKS.md`).
  Delivery = at-least-once.
- **Remaining (build on Spark Real-Time Mode + Flink):**
  1. ✅ **Record-level low-latency sink** — Kafka sink done. (Socket sink next for non-Kafka.)
  2. **Exactly-once to Kafka** — transactions (`begin/commit_transaction`) tied to the
     per-epoch offset commit (Flink `KafkaSink` EXACTLY_ONCE / 2PC). Currently at-least-once.
  3. **Sub-interval p99** — decouple the produce/flush cadence from the offset-commit epoch
     so p99 < 100 ms without paying a commit per 100 ms (Spark Real-Time Mode long-running
     stages); honor `ProcessingTime` trigger intervals.
  - *Acceptance:* sustained 100k ev/s, **p99 < 100 ms** record-to-emit (close: 202 ms at
    250 ms epoch); exactly-once Kafka→Kafka across a kill.

### 3.2 State management & large state — **P0**
- **Flink:** pluggable keyed state; **RocksDB** backend for state >> memory; **incremental
  + changelog checkpoints** (only deltas uploaded); state TTL; **rescalable** state
  (key-group reassignment). **Flink 2.0 disaggregated state (ForSt):** state lives on
  DFS/object store, separating compute from state for cloud elasticity.
- **Vajra now:** windowed-agg keeps partial state **in memory**, snapshotted to the
  object-store checkpoint on EndOfData/epoch. Good for O(open windows); not for very large
  keyed state, and snapshots are full (not incremental).
- **Needed:**
  1. **Spillable / embedded-KV state backend** (RocksDB-class or an Arrow-native on-disk
     KV) so state exceeds RAM.
  2. **Incremental checkpoints** (upload deltas, not full state) — Vajra's object-store-first
     design is *already aligned* with Flink 2.0 ForSt; lean into it: disaggregated state on
     S3 as the native model, not a bolt-on.
  3. **State TTL** + **rescaling** (redistribute state when parallelism changes).
  - *Acceptance:* a keyed aggregation with **100 GB+ state** runs stable; checkpoint upload
    scales with the delta, not total state; parallelism change recovers correctly.

### 3.3 Checkpointing & exactly-once under failure — **P1**
- **Flink:** Chandy-Lamport **aligned** barriers + **unaligned** checkpoints (don't block on
  backpressure) + **2PC** sinks (`TwoPhaseCommitSinkFunction`) for EO to Kafka/files.
- **Vajra now:** `StreamBarrierAlignExec` (aligned Chandy-Lamport), object-store single-blob
  atomic commit, per-instance offset staging; **EO validated across hard kill** (this work)
  and across container kill. *Not yet:* unaligned checkpoints, 2PC for arbitrary sinks,
  mid-job single-worker failure recovery (vs full restart).
- **Needed:** unaligned checkpoints for backpressure tolerance; generalize 2PC sink commit;
  partial-failure recovery (restart only affected tasks, not the job).
  - *Acceptance:* EO holds under continuous backpressure + a worker killed mid-epoch, with
    only the failed task's work replayed.

### 3.4 Distributed source/shuffle parallelism — **P1**
- **Spark/Flink:** source parallelism = #partitions across the *cluster*; shuffle is a
  full N→M exchange with backpressure + spill.
- **Vajra now:** parallel Kafka source (this work) reports `UnknownPartitioning(N)`,
  per-instance partition assignment + EO offsets — but composed via an N→1 align before the
  single watermark, so the source parallelism is currently **intra-node**; the Arrow-Flight
  shuffle (`StreamExchangeExec`) is 1→N (broadcast markers + hash-route). ClickBench-100M
  proved the *batch* Flight shuffle at scale.
- **Needed:** true **N→M streaming exchange** across nodes (Arrow Flight `DoExchange`), so
  source + parse + pre-agg parallelize across the whole cluster; backpressure-aware channels;
  spill on the shuffle read.
  - *Acceptance:* streaming throughput scales ~linearly across nodes (not just cores).

### 3.5 Watermarks & event-time correctness at scale — **P1**
- **Flink:** per-split watermarks merged by MIN; **FLIP-182 watermark alignment** bounds
  skew across sources (a fast partition can't run far ahead).
- **Vajra now:** avoids the per-partition hazard by merging N→1 *before* a single
  `WatermarkExec` (correct, but serializes the merge). No idle-source detection / alignment.
- **Needed:** per-partition watermark generation + MIN-merge in the distributed exchange;
  idle-partition handling; allowed-lateness + side outputs.

### 3.6 Adaptive execution & query optimization (batch) — **P1**
- **Spark AQE:** runtime re-optimization (coalesce shuffle partitions, skew-join handling,
  switch join strategy from runtime stats). **DataFusion:** dynamic filters, bounded batches.
- **Vajra now:** DataFusion optimizer (strong static plans); no runtime AQE-style
  re-planning or skew handling.
- **Needed:** runtime statistics → adaptive shuffle-partition coalescing + skew-join split;
  spill everywhere (sort/agg/join) so large queries never OOM.

### 3.7 Fault tolerance, HA & elasticity (operational) — **P0 for GA**
- **Spark/Flink:** task retry + speculative execution; **JobManager/driver HA**; external
  shuffle service (decouple shuffle from executor lifetime); **autoscaling/reactive
  rescaling**.
- **Vajra now:** EO across full restart proven; **no** mid-job worker-failure recovery,
  driver HA, or autoscaling yet.
- **Needed:** worker-failure task replay without job restart; driver/scheduler HA;
  reactive rescaling (add/remove workers live) — the cloud-native elasticity story, made
  easier by object-store state (§3.2).

### 3.8 Connectors & ecosystem — **P1**
- **Have:** Kafka **source**, file formats, Iceberg/Delta (partial), Spark Connect API.
- **Missing / partial (blocking real use):** **Kafka sink** (also the latency blocker),
  CDC sources, Iceberg/Delta **streaming** sinks with EO, JDBC, Pulsar/Kinesis. Flink's
  connector breadth + Spark's DataSource v2 ecosystem are the bar.
  - *Acceptance:* Kafka→Vajra→Kafka and Kafka→Vajra→Iceberg both exactly-once.

### 3.9 Memory, spill & robustness — **partly proven**
- **Win:** measured **6.4× less RAM** than Flink (no JVM, Arrow). The Arrow i32 2 GiB
  offset limit was hit + fixed (byte-bounded batches; `LargeUtf8`/`StringView` are the
  Arrow-documented escalation for genuinely huge values).
- **Needed:** universal spill-to-disk (sort/join/agg/shuffle/state) so no operator OOMs;
  back-pressured memory accounting (DataFusion `MemoryPool`) across the whole pipeline.

### 3.10 Observability, security, release hygiene — **P1 for GA**
- Metrics (Prometheus), a real web UI (job/stage/operator + watermark lag + backpressure),
  structured logging, tracing. Security: CVE gate live, threat model + SECURITY.md exist;
  pen-test + fuzzing still open (`docs/PRODUCTION_READINESS.md`).

### 3.11 The unified / AI-native north star — **P2 (the "new standard")**
Beyond parity: one engine for **batch + streaming + interactive + AI** on Arrow. Python/
Arrow UDFs (have), vector/embedding types, model-inference operators, feature pipelines
that are the *same* code batch and streaming. This is where "not merely compete, but set a
new standard" is won — but only after the P0/P1 reliability/latency gaps are closed.

---

## 4. Prioritized roadmap (with acceptance criteria)

**P0 — close the gaps that block "replaces Flink" and "production-trustworthy":**
1. **Low-latency path:** Kafka sink + record-paced realtime emission (Spark Real-Time Mode
   model) → p99 < 100 ms. (§3.1)
2. **Scalable state:** spillable/embedded-KV state backend + incremental, disaggregated
   (object-store) checkpoints à la Flink 2.0 ForSt. (§3.2)
3. **Operational fault tolerance:** mid-job worker-failure recovery + driver HA. (§3.7)
4. **Universal spill** so no operator OOMs at scale. (§3.9)

**P1 — robustness & scale parity:**
5. True N→M cross-node streaming Flight shuffle + backpressure. (§3.4)
6. Unaligned checkpoints + generalized 2PC sinks. (§3.3)
7. Per-partition watermarks + alignment (FLIP-182). (§3.5)
8. Batch AQE (skew/coalesce) + spill-everywhere. (§3.6)
9. Connector breadth (Kafka sink, CDC, Iceberg/Delta streaming EO). (§3.8)
10. Observability (metrics, web UI, tracing) + security hardening (pen-test, fuzzing). (§3.10)

**P2 — set the new standard:** AI-native unified pipelines; reactive autoscaling; SQL
coverage to 100%.

---

## 5. The honest one-paragraph summary

Vajra is **already a credible Spark batch replacement** (measured ~3× faster, ~2× less
RAM at 100 GB scale, distributed on EKS) and now **wins streaming throughput (1.33×) and
memory (6.4×) vs Flink with exactly-once across a hard crash**. It is **not yet** a Flink
latency replacement (p99 is seconds, not ms — no low-latency sink, immature realtime mode)
nor as operationally hardened (no mid-job failure recovery / HA / large-state backend /
unaligned checkpoints). The path is concrete and grounded in exactly how Spark Real-Time
Mode, Flink 1.19/2.0, DataFusion, and Arrow solve each problem — and Vajra's object-store
-centric, no-JVM, Arrow-columnar architecture is genuinely well-positioned to leapfrog
(esp. on memory, cost, and disaggregated cloud-native state). Closing the P0 set is what
turns "wins two of four axes" into "replaces both Spark and Flink."
