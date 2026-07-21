# Zelox vs Apache Spark vs LakeSail — where we stand

Honest competitive read, backed by our **measured, reproducible** numbers and
**LakeSail's published claims**. Updated 2026-06-06.

## Important context
Zelox is **forked from `lakehq/sail`**. The analytical engine core (Rust +
Apache DataFusion) is therefore **shared lineage** with LakeSail — so raw
query-perf is in the **same ballpark** for both. We now have a **head-to-head on
the identical ClickBench harness**: Zelox **60.11 s vs LakeSail's published 65.50 s
(0.92×)** on the same c6a.4xlarge class — i.e. **~parity, Zelox marginally faster**
(details below). We do **not** spin this into "Zelox beats LakeSail" — a ~8% edge on
one run with a shared core is parity, not a moat. The real differentiation is
**operational features, demonstrable trust, and transparent multi-scale benchmarks.**

## Measured: Zelox vs Apache Spark (same machine/cluster, identical data + SQL)
| Benchmark | Scale | Zelox | Spark 3.5.3 | Gain |
|---|---|---|---|---|
| TPC-H | SF-1 (warm) | 1.78 s | 63.46 s | **~36× faster** |
| ClickBench | ~1M rows | 3.87 s | 48.07 s | **~12× faster** |
| **TPC-H** | **SF-100 (100 GB)** | **346.97 s / 51.7 GiB** | **1099.27 s / 115 GiB** | **~3.2× faster, ~2.2× less memory** |
| **ClickBench** | **100M rows (distributed, EKS)** | **377.9 s (43/43)** | — | distributed scale proof |

**The gain is scale-dependent: ~36× small/warm → ~3.2× at 100 GB, with ~2× less
memory at scale.** All reproducible (`scripts/`, `k8s/eks/`, `docs/SCALE_TESTING.md`).

## LakeSail's published claims (lakesail.com, v0.6.3)
- **"8× faster than Spark"** (headline).
- **"94% lower infrastructure cost vs Spark"** — from TPC-H, vs JVM Spark on
  `c6a.4xlarge`; scale factor / single-vs-distributed / warm-vs-cold not disclosed.

## How the numbers relate (honest)
- LakeSail's **8×** is an undisclosed-scale TPC-H figure. Our measured TPC-H gain
  brackets it: **~36× at SF-1** (warm/small) and **~3.2× at SF-100** — i.e. "8×" is
  plausibly a mid-scale TPC-H average, consistent with a shared DataFusion core.
- LakeSail's **94% cost** ≈ "no JVM + fewer/smaller nodes." Our **measured ~2.2×
  less memory at SF-100** is the concrete, reproducible basis for that kind of
  cost story (smaller instances, fewer nodes).
- We publish **per-scale numbers + conditions + a memory measurement**; LakeSail
  publishes a single "8×/94%" without disclosed conditions. Our benchmarking is
  the more transparent of the two.

## Where Zelox leads / is at parity / trails

| Dimension | Zelox | LakeSail v0.6.3 |
|---|---|---|
| Analytical query perf vs Spark | ~3–36× (measured, multi-scale) | "8×" (claimed) — shared core ⇒ ~parity |
| Memory at scale | **measured ~2.2× less than Spark** | "94% cost" (claimed) |
| **Differential trust gate** (byte-exact vs real Spark, in CI) | ✅ **124 workloads** | not published |
| **Multi-mode verification** (local, local-cluster, Apple Container, K8s) 105/105 | ✅ | not published |
| Streaming (Kafka, foreachBatch, memory sink, checkpoint) | ✅ | ❌ |
| Security (JWT, mTLS), K8s HA, Apple Container, Web UI | ✅ | ❌ |
| Distributed-at-scale proof on real EKS | ✅ ClickBench 100M | publishes single-node |
| Lakehouse (Delta/Iceberg/VARIANT) | parity | parity |
| Release maturity / track record | younger | more releases |

## Verdict
- **Vs Spark:** clearly faster (3–36×, scale-dependent) **and** lighter (~2× memory
  at scale) — a credible production replacement, now proven small → 100 GB.
- **Vs LakeSail:** ~perf-parity (shared engine), **ahead on operational features,
  demonstrable trust (CI-gating differential), multi-mode verification, and
  transparent benchmarking.**

## Direct Zelox-vs-LakeSail check (ClickBench, shared core) — ✅ MATCHING
Because Zelox is forked from sail, the honest correctness check is: does Zelox
match LakeSail on the **identical** ClickBench harness? **Measured 2026-06-06 on a
real c6a.4xlarge** (same instance class, local `hits.parquet`, single-node, best-of-3):

| | Zelox | LakeSail (published) | ratio |
|---|---|---|---|
| Hot total (43q) | **60.11 s** | 65.50 s | **0.92× — Zelox ~8% faster** |
| Passed | 43/43 | 43/43 | tie |

**Verdict: MATCHING — shared DataFusion core confirmed correctly implemented.**
Zelox is faster on 37/43 queries (notably Q7 min/max from Parquet stats, and the
Q37–43 filtered/OFFSET queries), LakeSail faster on 4 (Q21–24; Q24 wide top-N is
Zelox's weak spot). Runner + raw JSON: [`benchmarks/clickbench/`](../../benchmarks/clickbench/README.md);
full analysis: [CLICKBENCH_VS_LAKESAIL.md](CLICKBENCH_VS_LAKESAIL.md). (Zelox's
1M-smoke / 100M-distributed-on-S3 numbers are a different setup, not comparable.)

## What's next (to close the remaining gaps)
- **Same-cluster Spark reference for ClickBench 100M** (we have Zelox's number;
  add Spark for a full distributed head-to-head).
- **TPC-DS** (broader query surface than TPC-H).
- **Spark 4.x reference** (current reference is the production-line 3.5.3).
- **Endurance + concurrency** (multi-user, long-running streaming).
- Keep growing the **differential trust harness** and broaden **official Spark
  test-suite** coverage.
- (Optional, careful) a **direct Zelox-vs-LakeSail** run on identical hardware —
  only if framed honestly given the shared core.
