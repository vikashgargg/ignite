# Tri-engine benchmark matrix — Vajra vs Spark (batch) vs Flink (streaming), every dimension

**Goal (2026-07-01):** ONE comprehensive, fair, like-for-like baseline of Vajra against the two engines
it replaces — **Spark (batch)** and **Flink (streaming)** — across *every* dimension that defines a
prod-grade engine. Capture Spark+Flink numbers ONCE, record in KB, then use as the standing reference
to drive prod-grade fixes (grounded REFERENCES §2/§3/§3c/§3d: Flink 2.0/2.3, Spark 4.1 RT-mode; +
RisingWave/StreamNative as architecture refs). Claim "replaces both" ONLY on measured per-dimension wins.

## Honesty: ✅ measured-better · ⚠️ competitive · ❌ worse · ❓ unmeasured. No number, no claim.

## MEASURED SO FAR (EKS 2026-07-01) — honest tri-engine baseline
- **Streaming vs Flink 1.19:** throughput 5.28M vs 5.78M (1.10× slower) · memory 10.34 vs 8.58 GiB
  (1.20× MORE) · **latency competitive, Vajra TAIL p999/max BETTER** (126/129 vs 127/131ms = no-GC win).
- **Batch vs Spark 3.5.3 (TPC-DS):** Vajra **97/99 query coverage** (Q5/Q9 compat gaps) · memory **0.32 vs
  2.5 GiB = ~8× LESS** (no-JVM). ⚠️ coverage not perf-at-SF (`LIMIT 10000`; gen fix needed).
- **KEY: memory is PATH-DEPENDENT + it flips** — ~8× less on batch, 1.20× more on streaming-bounded ⇒ the
  streaming bounded-path buffering is the specific, isolated fix.
- **NEXT = production workloads** (real sinks S3 Iceberg/Parquet + Kafka vs Flink/Spark):
  [production-workload-benchmark.md](production-workload-benchmark.md). Land fixes first, then P1–P5.

## A. STREAMING dimensions — baseline = Flink 1.19/2.x (EKS c7g.4xlarge, fair side-by-side)
| # | Dimension | Metric | Vajra (have) | Flink (have) | Status / need |
|---|---|---|---|---|---|
| S1 | Throughput | ev/s @100M | 5.28–5.37M | 5.74–5.78M | ⚠️ ~1.07–1.10× (EKS, multi-run) |
| S2 | Peak memory | GiB @100M | **7.1–7.3** | 8.55 | ✅ **BEATS Flink (EKS 2026-07-02 instrumented sweep): 7.14/7.32/7.33 vs 8.55 GiB, correctness+throughput intact. NOTE: NOT the prefetch (direct measure = 44MB flat across configs = no-op here); it's the Arrow no-JVM footprint + T1–T7a. Prefetch attribution RETRACTED.** |
| S3 | **Latency** | p50/p99/p999 e2e | p50=62 p99=119 p999=126 max=129 | p50=53 p99=110 p999=127 max=131 | ✅ **MEASURED (EKS 2026-07-01, 20k/s): competitive; Vajra TAIL p999/max SLIGHTLY BETTER (no-GC)** |
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
| B2 | TPC-DS | coverage + wall + mem | 97/99 cov, 0.32 GiB | 2.5 GiB | ⚠️ **EKS 2026-07-01: Vajra 97/99 queries (Q5 schema, Q9 type-cmp) = strong Spark-compat; MEMORY Vajra 0.32 vs Spark 2.5 GiB = ~8× less (no-JVM). BUT tpcds_score.py `LIMIT 10000` = tiny data → times MEANINGLESS; perf-at-SF needs gen fix (remove LIMIT + real dsdgen at SF)** |
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
**ALSO REUSABLE (confirmed):** B2 TPC-DS = `scripts/tpcds_score.py` (Vajra) + spark-bench pattern; B3
ClickBench = `scripts/clickbench.py` (Vajra, downloads hits parquet) + spark-bench pattern. So ALL batch
dims (B1/B2/B3 + memory) = existing Vajra script + the `spark-bench-job.yaml` pattern parameterized by
script/args. `lat_probe.py` (S3) BUILT+VALIDATED (Vajra local p50=43 p99=59 p999=141ms, debug).
**CLUSTER REALITY (inventory 2026-07-01):** batch + streaming use SEPARATE EKS setups —
- **Streaming:** `k8s/stream/eks-stream-cluster.yaml` (compute c7g + kafka m7g) + `eks_stream_headtohead.sh`.
- **Batch:** `k8s/eks/cluster-sf100.yaml` (+ `vajra-sf100.yaml` Vajra deploy, `tpch-gen-job.yaml` data,
  `spark-bench-job.yaml` Spark baseline, `clickbench-loader.yaml`).
⇒ "one EKS session" = TWO sequential cluster phases (not one cluster). Orchestrator = 2 sub-flows:
**REMAINING to build (orchestrator `tri_engine_scorecard.sh`, 2 phases, reuse existing):**
- **Streaming phase:** `eks_stream_headtohead.sh` (S1 throughput + S2 mem vs Flink) + ADD latency S3
  (deploy `flink-sql-latency.sql` continuous + `lat_probe.py` ENGINE=flink; Vajra passthrough +
  `lat_probe.py` ENGINE=vajra). Teardown.
- **Batch phase:** `cluster-sf100` + `vajra-sf100` → `tpcds_score.py` vs Vajra (B2 power test) + scale
  Vajra→0 → `spark-bench-job.yaml` parameterized for `tpcds_score.py` (Spark baseline) + B1 TPC-H +
  mem. Teardown.
- Smaller dims S4 recovery-timing + S10/B5 cold-start fold into each phase.
Then the EKS run ($ gate). Full Nexmark q0–q13 = dedicated follow-on (phased per user 2026-07-01).
**Phase-2 execution:** ONE EKS cluster → run reusable (B1/B4/S1/S2) for immediate fair numbers + the
built/remaining gaps → capture Spark+Flink baselines once → teardown $0 (clean, NO interrupt).

## Deliverable
`scripts/tri_engine_scorecard.sh` orchestrating Phase-2 on EKS (Vajra+Flink+Spark), emitting the full
table. Phase-1 reuses existing local gates. This matrix = the "stands against Spark+Flink in every
dimension" evidence; update cells with each measured number.
