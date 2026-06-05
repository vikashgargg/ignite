# Vajra vs LakeSail — ClickBench (the fork-parity check)

Vajra is forked from `lakehq/sail`, so the analytical core (Rust + DataFusion) is
**shared lineage** with LakeSail. The end goal is for Vajra to be **at least as good
as LakeSail** and a true Spark replacement. ClickBench is the right correctness
check: if Vajra's per-query times track LakeSail's on the **identical harness**,
the fork is implemented correctly; a large systematic gap flags a regression.

## ⚠️ Are the current numbers "matching"? Not directly comparable yet.
We have **not** yet run Vajra on LakeSail's exact setup, so we cannot claim a match.
The setups differ on every axis that matters:

| Axis | LakeSail published | Vajra published (today) |
|---|---|---|
| Dataset | full `hits.parquet`, 99.99M rows, 14.78 GB | 1M smoke **or** 100M |
| Topology | **single node** c6a.4xlarge (16 vCPU/32 GB) | **distributed** EKS (3× Graviton spot) |
| Storage | local disk | **S3** (object store) |
| Run protocol | **3 runs, best-of-3** (hot) | single pass (cold) |
| Total (43q) | **65.50 s hot** / 197.04 s cold | 377.9 s (distributed, S3, cold) |

So Vajra's 377.9 s (distributed/S3/cold) vs LakeSail's 65.50 s (single-node/local/hot)
is **apples-to-oranges — not evidence of a regression.** To actually answer "do they
match," run Vajra through the identical harness in [`benchmarks/clickbench/`](../../benchmarks/clickbench/README.md).

## Expectation (why they *should* match)
Shared DataFusion core ⇒ on the same c6a.4xlarge, local `hits.parquet`, best-of-3,
Vajra should land **within noise** of LakeSail: total within ~±15%, most queries
within ~±25%. `benchmarks/clickbench/compare.py` prints the per-query ratio and a
pass/fail verdict automatically.

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

## How to get Vajra's column (the missing measurement)
The harness is in [`benchmarks/clickbench/`](../../benchmarks/clickbench/README.md).
It needs a `c6a.4xlarge` (or equivalent 16 vCPU/32 GB box) with the 14.78 GB
`hits.parquet` on local disk — too big for the 8 GB dev Mac. Estimated cost on AWS:
**~$0.61/hr on-demand (~$0.10–0.20/hr spot), one run ≈ 1–2 h ⇒ ~$1–2**, then full
teardown to $0 via `scripts/aws_eks_teardown.sh`.

## Status
- [x] LakeSail reference captured + embedded for direct comparison.
- [x] Identical-harness runner + auto-comparator published (`benchmarks/clickbench/`).
- [ ] **Run Vajra on c6a.4xlarge with local `hits.parquet`, best-of-3** → fill
  `results/vajra_c6a.4xlarge.json` and report the verdict. *(pending — needs the
  paid cloud box; awaiting go-ahead)*
