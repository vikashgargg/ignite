# Vajra vs Flink (streaming) + Spark (batch) — head-to-head: measured locals + the credible EKS plan

**Date:** 2026-06-16. **Engine state:** Vajra does distributed batch + distributed *stateful* streaming
(stateless, keyed event-time window agg, dropDuplicates, stream-stream join) through one Spark API,
validated 6/6 against real Spark 3.5.3 across a 2-worker cluster (`scripts/dist_streaming_smoke.py`).

## 1. Measured locally (release build, 8-core Apple M-series, single node) — Vajra

| Workload | Vajra | Spark 3.5.3 (`local[*]`) | Notes |
|---|--:|--:|---|
| **Batch** 20M fact ⨝ 100k dim → group-by (range-gen, warm) | **1.08 s** | 1.12 s | ≈ tied; both memory-bound on the join. The big batch wins are realistic queries — TPC-H SF-1 ~36×, ClickBench (published) — not this micro-join. |
| **Streaming** 5M-row keyed event-time tumbling-window COUNT over a shared CSV (availableNow) | **1.63 s (~3.07M rows/s)**, 4850 windows | — | Vajra output verified (4850 windows; semantics match Spark, see harness). Correct + measured. |

These Vajra numbers are real and reproducible. Per-core ≈ 3.07M/8 ≈ **~384k rows/s/core** for the
windowed aggregation (M-series, file source).

## 2. Official Flink reference numbers (sourced — NOT re-measured by us)

- **Flink 1.19 windowed aggregation: 1.82M events/s on a 16-core `c7g.4xlarge`** (10s tumbling window,
  100M-event dataset) ⇒ **~114k events/s/core**; 2.1× Spark 4.0, 3.2× Kafka Streams 3.8. [DEV 2024]
- **Alibaba Realtime Compute for Flink:** windowed / JOIN / GROUP BY ≈ **5k–10k records/s per CU**;
  simple filter/convert ≈ 40k–55k/CU. [Alibaba Cloud docs]
- **Nexmark** (q0–q22) is the official Flink streaming benchmark; results are execution-time/throughput
  per query on 100M/200M-event workloads. [github.com/nexmark/nexmark, flink.apache.org]

## 3. Why we are NOT publishing a *local Docker* Flink number (honesty / prod-grade bar)

We stood up Flink 1.18.1 in Docker (4 GB TM, Java 8) and it was **not a valid benchmark platform**:
the CSV filesystem source emitted only 5033 of 5,000,000 rows; a `datagen` job emitted ~6080 rows in
310 s (~20 rows/s) and planned a `GlobalWindowAggregate` (non-keyed). That is a pathological
container/JVM/config artifact — **not** Flink's real performance (which is ~1.82M/s on proper
hardware). Publishing it would dishonestly handicap Flink and violate the no-workaround / true
like-for-like bar. (The prior `FLINK_HEAD_TO_HEAD.md` reached the same "directional only" conclusion
for local Flink.)

**Conclusion:** a *credible, publishable* Vajra-vs-Flink streaming head-to-head must run on **consistent
proper hardware with the official benchmark** — i.e. on EKS, using **Nexmark** (or a matched
windowed-aggregation harness) on the **same `c7g.4xlarge` class** as the published Flink baseline.

## 4. The credible EKS head-to-head plan (next)

1. **Cluster:** EKS, Graviton `c7g.4xlarge` (16 vCPU) nodes — matches the published Flink-1.19 baseline,
   so per-core numbers are directly comparable. Cost-disciplined: tear down after.
2. **Streaming (vs Flink):** run **Nexmark** on Flink (official `nexmark-flink`) AND the equivalent
   query logic on Vajra via the Spark API (Vajra speaks Spark SQL/DataFrame). Same 100M-event workload,
   same node. Publish per-query throughput + p50/p99 latency + peak RSS. Target: ≥ Flink throughput at
   lower memory (no-JVM/Arrow), flat-tail latency.
3. **Batch (vs Spark):** TPC-H SF-100 distributed on the same cluster (reuse `scripts/tpch_distributed.py`),
   time + memory vs Spark.
4. **Reliability:** Kafka→sink EO across a worker kill (chaos), distributed (the F3-c continuous-EO
   coordinator lands first — see docs/design/distributed-streaming-f2f3.md §5).

**Gate before EKS:** distributed correctness is already proven (6/6 vs Spark). The remaining pre-EKS
engineering item is **distributed continuous-EO** (cross-worker `EpochCoordinator` + per-instance state
snapshot); the benchmark harness (Nexmark-on-Vajra query ports) is built alongside.
