# Vajra benchmarks — published results

All numbers are reproducible, same-machine/same-cluster, identical data + SQL,
vs **real Apache Spark** (not estimates). Scripts are dual-engine
(`scripts/tpch_distributed.py`, `scripts/clickbench.py`).

## Summary

| Benchmark | Scale | Setup | Vajra | Apache Spark 3.5.3 | Speedup |
|---|---|---|---|---|---|
| **TPC-H** | SF-1 (~1 GB) | single node, warm, `local[4]` | **1.78 s** (22/22) | 63.46 s (22/22) | **≈36×** |
| **TPC-H** | **SF-100 (~100 GB)** | **single node**, AWS EKS r7gd.4xlarge (128 GB) | **346.97 s** (22/22) | 1099.27 s (22/22) | **≈3.2× + ≈2.2× less RAM** |
| **ClickBench** | ~1M rows | single node, `local[4]` | **3.87 s** (43/43) | 48.07 s (42/43) | **≈12.4×** |
| **ClickBench** | **100M rows (13.7 GB)** | **distributed, AWS EKS** (3× Graviton spot, S3) | **377.9 s** (43/43) | — (not run on same cluster) | — |

Details: [TPCH_SF1.md](TPCH_SF1.md), [TPCH_SF100.md](TPCH_SF100.md),
[CLICKBENCH.md](CLICKBENCH.md). EKS scale runs: [../SCALE_TESTING.md](../SCALE_TESTING.md).

**Vajra vs LakeSail (fork-parity check) ✅:** [CLICKBENCH_VS_LAKESAIL.md](CLICKBENCH_VS_LAKESAIL.md)
— measured on the **identical** ClickBench harness (same c6a.4xlarge class, local
`hits.parquet`, best-of-3): **Vajra 60.11 s vs LakeSail 65.50 s = 0.92× — MATCHING**,
43/43, Vajra marginally faster overall. Confirms the shared DataFusion core is
correctly implemented in the fork. Runner + raw results:
[`benchmarks/clickbench/`](../../benchmarks/clickbench/README.md). (Vajra's 1M-smoke
and 100M-distributed-on-S3 numbers are a *different* setup, not comparable to this.)

## How to read the speedup — be precise (it depends on scale + workload)
- **Small/warm data → huge multiplier** (engine + JVM-startup overhead dominates):
  TPC-H **SF-1 ~36×**, ClickBench 1M ~12×.
- **Large data → smaller but still-substantial multiplier** (both engines become
  genuinely I/O/compute-bound): TPC-H **SF-100 ~3.2×**.
- Honest public claim: **"~3× faster and ~2× less memory at 100 GB scale; up to
  ~36× on small/warm workloads"** — *not* a flat "30–40× on everything." Always
  quote the benchmark + scale with the number.
- Vajra also passed queries Spark 3.5.3 rejects (ClickBench Q40 CASE coercion),
  i.e. broader Spark-4.x-compatible semantics.

## Memory — MEASURED at SF-100 ✅
Head-to-head peak memory on the same 128 GB node, same SF-100 data:
**Vajra 51.7 GiB vs Spark 115 GiB** (Spark saturated its cap) → **Vajra used
≥2.2× less memory** and never pinned RAM. See [TPCH_SF100.md](TPCH_SF100.md).
The "no JVM → less memory" claim is now backed by a measured number at scale.

## Correctness (the trust pillar, not speed)
- **124/124** differential workloads byte-for-byte vs Apache Spark (CI-gating).
- **105/105** Spark-compat scorecard across **four** deployment modes: `local`,
  `local-cluster`, **Apple Container**, **Kubernetes** (kubernetes-cluster mode).

## Pending scale items
- [ ] **TPC-H SF-100** distributed on EKS, with a **same-cluster Spark reference**
  → first true large-scale head-to-head **and** the memory measurement.
- [ ] Spark 4.x reference (current reference is the production-line 3.5.3).
