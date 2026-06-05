# ClickBench — Vajra vs Apache Spark (head-to-head)

The 43 standard [ClickBench](https://benchmark.clickhouse.com/) analytical
queries over the `hits` table, identical Parquet input and identical SQL on the
same machine. This run uses the **`hits_0` smoke subset (~1M rows, 122 MB)** —
tractable on an 8 GB host; the official 100M-row (~14 GB) set needs a larger box
(tracked alongside TPC-H SF-100 in PRODUCTION_ROADMAP.md).

## Result (smoke, ~1M rows, single pass, `local[4]`)

| Engine | Build | Total (43q) | Avg/query | Passed |
|---|---|---|---|---|
| **Vajra** | release (thin LTO) | **3.872 s** | 0.090 s | **43/43** |
| Apache Spark 3.5.3 | JVM (Java 8) | 48.072 s | 1.136 s | 42/43 |

**Vajra is ≈12.4× faster** end-to-end and passes **all 43** queries. Spark 3.5.3
fails Q40 with `DATATYPE_MISMATCH.DATA_DIFF_TYPES` on its `CASE WHEN … THEN Referer`
branch (a stricter 3.5 coercion rule); Vajra accepts it, matching Spark 4.x.

## How to reproduce
```bash
# Vajra (server running on :50051)
SPARK_REMOTE=sc://localhost:50051 python scripts/clickbench.py            # smoke
SPARK_REMOTE=sc://localhost:50051 CLICKBENCH_FULL=1 python scripts/clickbench.py  # full 14 GB

# Reference Apache Spark on the SAME cached data (classic JVM, local master)
SPARK_REMOTE=local[4] CLICKBENCH_DATA=~/.cache/clickbench python scripts/clickbench.py
```

## Full scale — 100M rows, distributed on AWS EKS ✅

The **complete official ClickBench** (100M rows, 13.7 GB Parquet) run **distributed
on a real EKS cluster** (2026-06-05, ap-south-1):
- 3× Graviton (arm64) **spot** nodes, NAT disabled (`k8s/eks/cluster.yaml`).
- Vajra in `SAIL_MODE=kubernetes-cluster` — the driver pod **dynamically spawned
  worker pods** across the nodes (true distributed execution).
- Data read from **S3** (`s3://…/clickbench`) via the node IAM role.

**Result: 43/43 queries passed, 377.9 s total, 8.79 s avg** at 100M-row scale on
production K8s. (Heaviest: Q24 56 s, Q19 41 s; several sub-second.) Whole run
(provision → 13.7 GB load → benchmark → full teardown) cost **~$1**, then verified
back to **$0** — see [docs/SCALE_TESTING.md](../SCALE_TESTING.md).

This proves the previously-missing pillar: **distributed execution + S3 object
store + real Kubernetes at 100M-row scale** — not just single-node.

## Caveats / next
- Single-node smoke (above) is the apples-to-apples vs-Spark ratio; the EKS run is
  the distributed scale proof. A same-cluster Spark 100M reference is a follow-up
  (would ~double cost — deferred to keep spend at ~$1).
- Single pass (no warmup); identical conditions per engine.
- Reference is Apache Spark 3.5.3 (production line); a Spark 4.x reference is a follow-up.
