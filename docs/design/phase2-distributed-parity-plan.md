# Phase 2 — Distributed parity: confirm throughput + run EVERY test distributed vs Spark & Flink

> **Charter framing ([AIM.md](../AIM.md)):** Zelox must OBJECTIVELY BEAT Spark (batch) + Flink (streaming) on
> throughput at hyperscale, distributed, Kubernetes-native. Phase 1 (this branch) proved the renamed + PySpark
> 4.2 engine is **correct and wins single-node** (batch 8× vs Spark, realtime latency ~2× vs Flink, EO
> S3-verified — [RENAME42_EKS_TRIENGINE](../benchmarks/RENAME42_EKS_TRIENGINE.md)). Phase 1 did **NOT** confirm
> **distributed throughput at scale** — those runs were single-node 6-vCPU (4xlarge capacity blocked 16-vCPU).
> **Phase 2 = close that gap:** every test distributed (multi-node, kubernetes-cluster mode), throughput
> confirmed vs both engines at 16-vCPU scale. No claim of distributed-throughput parity until measured here.

## Why this is the open axis (grounded, from [BOARD.md](../BOARD.md) §1)

- **Streaming throughput vs Flink = `<` (behind), UNMEASURED at scale on the current build.** Root cause is
  the **Kafka source CONSUME rate** (Zelox `StreamConsumer` ~4M/s vs Flink `KafkaSource` ~10M/s at 16-vCPU);
  transport/shuffle/serde/JVM ruled out. Zelox is **SOURCE_READ bound** (source_read ≈ wall ≫ from_json ≫
  exchange_cpu≈0; the window starves behind the source).
- **Two fixes already measured but NOT yet confirmed distributed-at-scale on this build:**
  1. **FLIP-27 batch-queue consume** (`rd_kafka_consume_batch_queue`) — local 10M A/B measured **2.8×**
     (1.38→3.89 M/s), kind bounded EXACT + 2.33× wall. Gated `ZELOX_KAFKA_BATCH_QUEUE`. Branch
     `throughput/kafka-batch-queue-flip27` — **needs merge + EKS-at-scale confirmation**.
  2. **Shuffle coalescer** (`coalesce_flow_events` + periodic watermarks) — T1/T2 validated **2.14× fewer
     Flight messages, counts EXACT**. T3 throughput number pending.

## Goal & done-criteria (a claim is DONE only when measured distributed, both engines, T1→T2→T3)

| # | Workstream | Distributed target | Measured vs |
|---|---|---|---|
| P2-1 | **Streaming throughput at scale** | Distributed windowed-agg (N workers, Flight shuffle) with FLIP-27 batch-queue ON; **close/beat** Flink's consume rate at 16-vCPU | Flink 1.19 (ev/s, per-pod RSS, CPU/stage) |
| P2-2 | **Batch distributed** | Distributed TPC-H / TPC-DS + 200M ETL→S3 across N workers | Spark 3.5.3 (wall, RSS) |
| P2-3 | **Structured-streaming distributed** | availableNow windowed-agg → S3, N workers, completeness EXACT | Flink (completeness, throughput) |
| P2-4 | **Realtime distributed** | `trigger(realTime)` continuous → S3, N workers, EO across kill-9 at 16 partitions | Flink (latency p50/p99/p999, EO dup=0) |
| P2-5 | **Elasticity / rescale** | Rescale from checkpoint under load (key-group, FLIP-8) | Flink (correctness + recovery time) |

Every workstream reports the **same metric set**: throughput (ev/s), latency (p50/p99/p999), memory (peak
RSS/pod), CPU per-stage, shuffle Flight-message count — **distributed, at 16-vCPU scale, vs both engines.**

## Architecture levers (architect-first — attack the source-read bound, then re-measure)

1. **Kafka source parallelism.** Distribute the source across workers (one reader per partition group), FLIP-27
   batch-queue per reader. The single-node source is the bottleneck; distributing it is the primary lever.
2. **Arrow decode off the source thread.** `from_json` / Arrow decode is #2 after source_read — pipeline it so
   decode overlaps consume (no starve).
3. **Flight shuffle at scale.** The coalescer (2.14× fewer msgs, exchange_cpu≈0) is proven free at T1/T2;
   confirm it holds the throughput at 16-vCPU across real pod-to-pod Flight.
4. **No-JVM columnar advantage as the structural win.** Zelox already uses less memory/pod (3.70 vs Flink
   9.27 GiB at scale) — memory headroom is the lever to run more source parallelism per node than Flink.

## SDLC (STANDING — [three-tier-sdlc.md](three-tier-sdlc.md), never skip a tier)

- **T1 (free):** `local-cluster --workers N` routes shuffle over Flight in-process → distributed exchange +
  coalescer + FLIP-27 batch-queue A/B, counts EXACT. `scripts/local_dist_coalesce_check.sh`.
- **T2 (free):** kind, real pods = real cross-pod Flight + manifests. `scripts/kind_shuffle_coalesce_ab.sh`.
- **T3 (number only):** EKS 16-vCPU, ONE A/B for the throughput number, then $0. **Blocked-on: 4xlarge
  Graviton capacity** (Phase 1 hit ROLLBACK on c7g/m7g 4xlarge in ap-south-1) → per
  [eks-benchmark-infra-runbook](eks-benchmark-infra-runbook.md): flexible instance types
  (`m7g,c6g,m6g,c7g .4xlarge`), multi-AZ nodegroup, or an on-demand Capacity Reservation before the run.

## Definition of Phase-2-complete

BOARD row **Streaming throughput** moves off `<` to a **measured** `=`/`>` vs Flink at 16-vCPU, AND P2-2…P2-5
each have a distributed head-to-head number vs the engine they replace, T1→T2→T3 green, evidence linked. Until
then: **distributed throughput remains unconfirmed** — Phase 1's single-node wins do not transfer.
