# TPC-H SF-100 — Zelox vs Apache Spark (time AND memory, same node)

The 22 TPC-H queries at **scale factor 100 (~100 GB raw; 38 GB Parquet)**, run on
the **same single node**, **same node-local data**, identical SQL — measuring
**wall-time and peak memory** for each engine. This is the controlled
"faster *and* less memory" head-to-head at real scale.

- Node: AWS EKS `r7gd.4xlarge` (16 vCPU Graviton3, **128 GB**, local NVMe), spot.
- Zelox: `local-cluster` (4 workers). Apache Spark **3.5.3**: `local[16]`, 95 GB driver.
- Data generated once with DuckDB to node-local disk; both engines read it.
- Single pass (no warmup), 2026-06-06, ap-south-1.

## Result

| Metric | **Zelox** | Apache Spark 3.5.3 | Zelox advantage |
|---|---|---|---|
| Queries passed | **22/22** | 22/22 | tie |
| **Total time (22q)** | **346.97 s** | 1099.27 s | **3.2× faster** |
| Avg / query | 15.77 s | 49.97 s | 3.2× |
| **Peak memory** | **51.7 GiB** | 115 GiB (saturated its cap) | **≥2.2× less** |

**At SF-100, Zelox is ~3.2× faster and used less than half the memory** — and
completed every query without saturating RAM, while Spark pinned its 115 GiB
ceiling.

## Per-query wall time (seconds)

| Q | Zelox | Spark | Q | Zelox | Spark |
|---|---|---|---|---|---|
| 1 | 4.74 | 69.41 | 12 | 4.37 | 21.42 |
| 2 | 5.72 | 13.13 | 13 | 4.52 | 37.67 |
| 3 | 14.20 | 21.64 | 14 | 1.64 | 7.89 |
| 4 | 4.12 | 19.84 | 15 | 3.13 | 36.24 |
| 5 | 30.89 | 120.07 | 16 | 2.13 | 8.18 |
| 6 | 1.32 | 4.23 | 17 | 45.38 | 136.18 |
| 7 | 32.14 | 38.26 | 18 | 32.93 | 62.65 |
| 8 | 31.84 | 130.75 | 19 | 3.38 | 9.81 |
| 9 | 68.37 | 200.61 | 20 | 5.17 | 11.90 |
| 10 | 7.28 | 53.46 | 21 | 38.11 | 63.42 |
| 11 | 3.96 | 22.34 | 22 | 1.63 | 10.18 |

## The honest scaling story (important)
The speedup **shrinks with scale**, and that is expected:
- **TPC-H SF-1 (1 GB, warm): ~36×** — small/warm data is dominated by engine
  overhead + JVM startup, where Zelox's Rust/DataFusion core wins big.
- **TPC-H SF-100 (100 GB): ~3.2× + ~2.2× less memory** — at scale both engines
  are genuinely I/O- and compute-bound; the constant-factor overhead matters
  less, so the gap narrows to a *still-substantial* ~3× faster while using half
  the RAM.

So the defensible public claim is **"~3× faster and ~2× less memory at 100 GB
scale; up to ~36× on small/warm workloads"** — *not* a flat "30–40× on
everything." Quote the scale with the number.

## Cost
Whole run (cluster → 100 GB gen → both benchmarks → full teardown) ≈ **$1.5**,
verified back to **$0** afterward. Toolkit: `k8s/eks/`, `scripts/aws_eks_teardown.sh`,
[../SCALE_TESTING.md](../SCALE_TESTING.md).
