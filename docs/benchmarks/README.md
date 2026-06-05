# Vajra benchmarks — published results

All numbers are reproducible, same-machine/same-cluster, identical data + SQL,
vs **real Apache Spark** (not estimates). Scripts are dual-engine
(`scripts/tpch_distributed.py`, `scripts/clickbench.py`).

## Summary

| Benchmark | Scale | Setup | Vajra | Apache Spark 3.5.3 | Speedup |
|---|---|---|---|---|---|
| **TPC-H** | SF-1 (~1 GB) | single node, warm, `local[4]` | **1.78 s** (22/22) | 63.46 s (22/22) | **≈36×** |
| **ClickBench** | ~1M rows | single node, `local[4]` | **3.87 s** (43/43) | 48.07 s (42/43) | **≈12.4×** |
| **ClickBench** | **100M rows (13.7 GB)** | **distributed, AWS EKS** (3× Graviton spot, S3) | **377.9 s** (43/43) | — (not run on same cluster) | — |

Details: [TPCH_SF1.md](TPCH_SF1.md), [CLICKBENCH.md](CLICKBENCH.md). EKS scale run:
[../SCALE_TESTING.md](../SCALE_TESTING.md).

## How to read the speedup — be precise
- The multiplier is **workload-dependent**, not a single number:
  - **TPC-H (join-heavy): ~36×** vs Spark 3.5.3 (single node).
  - **ClickBench (scan/aggregation on one wide table): ~12×.**
- So an honest public claim is **"up to ~36× on TPC-H; ~10–15× on ClickBench"**,
  not a flat "30–40× on everything." Quote the benchmark with the number.
- Vajra also passed queries Spark 3.5.3 rejects (ClickBench Q40 CASE coercion),
  i.e. broader Spark-4.x-compatible semantics.

## Memory claim — NOT YET MEASURED (do not publish as fact)
We have **no head-to-head memory number** yet. "No JVM → less memory" is
architecturally plausible but unproven here. To substantiate it, measure **peak
RSS of Vajra vs Spark on the same workload/cluster** — planned in the TPC-H
SF-100 EKS run (deploy both engines, capture container/pod memory). Until that
exists, claim only what is measured.

## Correctness (the trust pillar, not speed)
- **124/124** differential workloads byte-for-byte vs Apache Spark (CI-gating).
- **105/105** Spark-compat scorecard across **four** deployment modes: `local`,
  `local-cluster`, **Apple Container**, **Kubernetes** (kubernetes-cluster mode).

## Pending scale items
- [ ] **TPC-H SF-100** distributed on EKS, with a **same-cluster Spark reference**
  → first true large-scale head-to-head **and** the memory measurement.
- [ ] Spark 4.x reference (current reference is the production-line 3.5.3).
