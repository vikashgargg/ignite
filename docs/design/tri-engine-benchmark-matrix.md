# Tri-engine benchmark matrix — Vajra vs Spark (batch) vs Flink (streaming), every dimension

**Goal (2026-07-01):** ONE comprehensive, fair, like-for-like baseline of Vajra against the two engines
it replaces — **Spark (batch)** and **Flink (streaming)** — across *every* dimension that defines a
prod-grade engine. Capture Spark+Flink numbers ONCE, record in KB, then use as the standing reference
to drive prod-grade fixes (grounded REFERENCES §2/§3/§3c/§3d: Flink 2.0/2.3, Spark 4.1 RT-mode; +
RisingWave/StreamNative as architecture refs). Claim "replaces both" ONLY on measured per-dimension wins.

## Honesty: ✅ measured-better · ⚠️ competitive · ❌ worse · ❓ unmeasured. No number, no claim.

## A. STREAMING dimensions — baseline = Flink 1.19/2.x (EKS c7g.4xlarge, fair side-by-side)
| # | Dimension | Metric | Vajra (have) | Flink (have) | Status / need |
|---|---|---|---|---|---|
| S1 | Throughput | ev/s @100M | 5.37M | 5.74M | ⚠️ 1.068× (EKS measured) |
| S2 | Peak memory | GiB @100M | 9.61 | 8.57 | ❌ bounded-path 1.12× EKS; ✅ **continuous-path BOUNDED local soak (~125MB steady, late/early=1.01, NO leak)** |
| S3 | **Latency** | p50/p99/p999 e2e | — | — | ❓ NEED both (add Flink Kafka→Kafka passthrough baseline) |
| S4 | **Recovery time** | kill→caught-up s | — | — | ❓ NEED (Flink 2.0 ForSt 49× ref) |
| S5 | Checkpoint | dur ms + size | O(delta) proven | — | ❓ NEED timed (Flink 2.0 −94% ref) |
| S6 | State @ scale | 1M+ keys, bounded | 64k-cap FIXED | RocksDB/ForSt | ✅ **1M keys all emitted, no cap** (local, wall 5.2s) |
| S7 | Correctness/EO | dup/loss=0 ±crash | gate green | EO | ✅ local gate (C6/C7 xfail) |
| S8 | Rescale | exact across parallelism | mechanism done | FLIP-8 | ✅ local (bit-exact gated by EO residual) |
| S9 | Backpressure | bounded in-flight, slow sink | — | credit-flow FLIP-2 | ❓ NEED (local) |
| S10 | Cold start | launch→first-output ms | — | — | ❓ NEED (no-JVM should win big) |

## B. BATCH dimensions — baseline = Spark 3.5/4.1 (fair, same box)
| # | Dimension | Metric | Vajra (have) | Spark (have) | Status / need |
|---|---|---|---|---|---|
| B1 | TPC-H | query wall, SF | SF-1 1.78s (local) | 63.46s (3.5.3) | ✅ ~36× local — re-confirm @ bigger SF on EKS, fair |
| B2 | TPC-DS | query coverage + wall | partial | — | ❓ NEED full run |
| B3 | ClickBench | 43-query wall | vs LakeSail 60.11s | (LakeSail 65.5s) | ⚠️ have vs LakeSail, not Spark — add Spark |
| B4 | Batch memory | peak GiB | — | — | ❓ NEED comparative |
| B5 | Cold start (batch) | launch→result ms | — | — | ❓ NEED (no-JVM) |

## What we ALREADY have (don't redo)
S1 (throughput EKS), S2 (peak mem EKS), S7/S8 (correctness/rescale local gates), B1 (TPC-H SF-1 ~36×
local), B3 (ClickBench vs LakeSail matching). Reuse: eks_stream_headtohead.sh, correctness_gate.sh,
rescale_gate.sh, inc_ckpt_gate, state_scale_stress.py, tpch_distributed.py, clickbench.py,
stream_latency.sh, stream_soak_chaos.sh.

## Execution sequence (cost-smart: free local soundness → fair EKS comparatives)
**Phase 1 — LOCAL engine-soundness (free, no baseline needed; is Vajra itself prod-grade?):**
S6 state@1M · S7 EO · S8 rescale · S9 backpressure · S2-bounded (mem leak?) · S5/S4 recovery timing ·
S10/B5 cold-start · (debug binary OK for behavioral checks). Find bugs cheap BEFORE EKS spend.
**Phase 2 — FAIR EKS tri-engine session (one cluster, release binary):**
- Streaming vs Flink: S1, S2, S3 (add Flink passthrough latency job), S4 recovery, S5 ckpt.
- Batch vs Spark: B1 (bigger SF), B2 TPC-DS, B3 + Spark, B4 mem, B5 cold-start.
- Capture Spark+Flink numbers ONCE → record in this doc + REFERENCES → standing reference.
**Phase 3 — prod fixes** per worst dimensions, grounded in the recorded baselines + REFERENCES.

## Build inventory (2026-07-01) — what's reusable vs gaps
**REUSABLE NOW (zero new code, fair on EKS):**
- B1+B4 TPC-H SF-100 vs Spark: `k8s/eks/spark-bench-job.yaml` runs the SAME `tpch_distributed.py` on
  Spark 3.5.3 local[16] (after scaling Vajra→0 = same node, fair) + cgroup `memory.peak`. Vajra side =
  `tpch_distributed.py` vs Vajra server.
- S1+S2 streaming throughput/memory vs Flink: `eks_stream_headtohead.sh` (+ `flink-sql.sql`).
**BUILT this session:** S3 Flink latency passthrough `k8s/stream/flink-sql-latency.sql` (raw passthrough,
mirrors `stream_latency_query.py`); Vajra latency = `stream_latency.sh`.
**REMAINING gaps to build:** S3 latency *runner+orchestrator* (start continuous Flink job async, run
shared producer+latency-consumer for DURATION, cancel) · B2 TPC-DS · B3 ClickBench Spark-baseline (mirror
spark-bench-job.yaml pattern) · S4 recovery-timing (extend soak gate w/ kill→caught-up timer) · S10/B5
cold-start (launch→first-output timer).
**Phase-2 execution:** ONE EKS cluster → run reusable (B1/B4/S1/S2) for immediate fair numbers + the
built/remaining gaps → capture Spark+Flink baselines once → teardown $0 (clean, NO interrupt).

## Deliverable
`scripts/tri_engine_scorecard.sh` orchestrating Phase-2 on EKS (Vajra+Flink+Spark), emitting the full
table. Phase-1 reuses existing local gates. This matrix = the "stands against Spark+Flink in every
dimension" evidence; update cells with each measured number.
