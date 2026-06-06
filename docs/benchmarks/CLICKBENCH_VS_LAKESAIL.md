# Vajra vs LakeSail — ClickBench (the fork-parity check)

Vajra is forked from `lakehq/sail`, so the analytical core (Rust + DataFusion) is
**shared lineage** with LakeSail. The end goal is for Vajra to be **at least as good
as LakeSail** and a true Spark replacement. ClickBench is the right correctness
check: if Vajra's per-query times track LakeSail's on the **identical harness**,
the fork is implemented correctly; a large systematic gap flags a regression.

## ✅ RESULT (2026-06-06): MATCHING — Vajra 60.11 s vs LakeSail 65.50 s (0.92×)
We ran Vajra through LakeSail's **identical** ClickBench harness — same c6a.4xlarge
instance class, full `hits.parquet` (99.99M rows, 14.78 GB) on local disk, default
single-node `local` mode, best-of-3 — and compared to LakeSail's published numbers.

| | **Vajra** | LakeSail (published) | ratio |
|---|---|---|---|
| Hot total (best-of-3, 43q) | **60.11 s** | 65.50 s | **0.92×** (Vajra ~8% faster) |
| Median per-query V/L ratio | — | — | **0.68×** (Vajra faster on most) |
| Queries passed | 43/43 | 43/43 | tie |

**Verdict: MATCHING — the shared DataFusion core is correctly implemented in the
fork.** Vajra is in the same ballpark and marginally faster overall, exactly as a
common-core relationship predicts. Vajra is faster on 37/43 queries; LakeSail is
faster on 4 (Q21–Q24). Notable points:
- **Q7** (`MIN/MAX(EventDate)`): Vajra 0.007 s vs LakeSail 2.40 s — Vajra answers
  from Parquet column statistics without a full scan.
- **Q37–Q43** (filtered page-view + `OFFSET`): Vajra 3–9× faster (0.04–0.18 s vs
  0.35–0.70 s) — stronger predicate pushdown / late materialization.
- **Q24** (`SELECT * ... ORDER BY EventTime LIMIT 10`): Vajra 10.36 s vs 4.37 s —
  the one clear loss; wide-projection top-N is Vajra's weakest spot here.

Raw data: [`benchmarks/clickbench/results/vajra_c6a.4xlarge.json`](../../benchmarks/clickbench/results/vajra_c6a.4xlarge.json)
vs [`lakesail_c6a.4xlarge.json`](../../benchmarks/clickbench/results/lakesail_c6a.4xlarge.json).
Reproduce: [`benchmarks/clickbench/`](../../benchmarks/clickbench/README.md). This run
used Vajra's **published `v0.6.0-alpha` x86_64 release binary** (the one `install.sh`
ships) — fully reproducible by anyone.

> Note: Vajra's *other* ClickBench numbers (1M smoke `local[4]`; 100M **distributed**
> on EKS reading from **S3**, single-pass = 377.9 s) are a **different setup** and are
> *not* comparable to this single-node/local/best-of-3 run — they measure distributed
> scale, not the per-query core. This 60.11 s figure is the apples-to-apples one vs
> LakeSail.

## LakeSail's published ClickBench reference (c6a.4xlarge, best-of-3)
Source: `ClickHouse/ClickBench/sail/results` (copied verbatim to
`benchmarks/clickbench/results/lakesail_c6a.4xlarge.json`). Hot = best of 3 runs.

| Q | hot (s) | Q | hot (s) | Q | hot (s) | Q | hot (s) |
|--:|--:|--:|--:|--:|--:|--:|--:|
| 1 | 0.009 | 12 | 0.358 | 23 | 2.206 | 34 | 5.151 |
| 2 | 0.115 | 13 | 1.062 | 24 | 4.366 | 35 | 5.124 |
| 3 | 0.125 | 14 | 1.797 | 25 | 0.694 | 36 | 1.081 |
| 4 | 0.116 | 15 | 1.130 | 26 | 0.582 | 37 | 0.519 |
| 5 | 0.861 | 16 | 0.977 | 27 | 0.755 | 38 | 0.404 |
| 6 | 0.836 | 17 | 1.866 | 28 | 2.772 | 39 | 0.383 |
| 7 | 2.395 | 18 | 1.829 | 29 | 10.304 | 40 | 0.698 |
| 8 | 0.128 | 19 | 3.813 | 30 | 0.801 | 41 | 0.399 |
| 9 | 0.969 | 20 | 0.146 | 31 | 0.984 | 42 | 0.350 |
| 10 | 1.062 | 21 | 1.412 | 32 | 1.070 | 43 | 0.439 |
| 11 | 0.326 | 22 | 1.267 | 33 | 3.824 | | |

**LakeSail hot total: 65.50 s** · cold (first-run) total: 197.04 s · median/query: 1.52 s.
Vs Apache Spark on the same box, LakeSail reports **8.4× median** per-query, best Q7
216.7×, worst Q35 2.6×, 43/43.

## How it was run
On a real `c6a.4xlarge` (Ubuntu 24.04, 16 vCPU / 30 GB, local gp3), 2026-06-06,
ap-south-1: `curl` the published Vajra x86_64 release binary, download the 14.78 GB
`hits.parquet` to local disk, `vajra server` (default `local` mode), then
`benchmarks/clickbench/run.py` best-of-3 over Spark Connect. Whole run including the
box ≈ **$0.30**, torn down to **$0** afterward (instance terminated, EBS/SG/keypair
deleted, access key revoked — verified).

## Status
- [x] LakeSail reference captured + embedded for direct comparison.
- [x] Identical-harness runner + auto-comparator published (`benchmarks/clickbench/`).
- [x] **Vajra run on c6a.4xlarge, local `hits.parquet`, best-of-3 → 60.11 s vs
  LakeSail 65.50 s = MATCHING (0.92×).** `results/vajra_c6a.4xlarge.json` filled.
- [ ] Re-run with a build of the current `phase4` branch (this used the
  `v0.6.0-alpha` release binary) to confirm no regression since the release.
- [ ] Same-box Apache Spark reference (we compare to LakeSail's *published* Spark
  numbers; running Spark on the same box would close the last loop).
