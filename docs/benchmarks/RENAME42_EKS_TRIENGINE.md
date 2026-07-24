# Tri-engine EKS scorecard — renamed + PySpark 4.2 build (`rename42`, 2026-07-24)

Fresh EKS head-to-head on the `chore/rename-zelox` build (sail/vajra→zelox rename + PySpark 4.2
canonical + 4.2 UDF/`trigger(realTime)` wiring). Purpose: confirm the renamed/4.2 engine's standing
vs the engines it replaces — Spark 3.5.3 (batch) and Flink 1.19 (streaming/realtime) — with the actual
S3 output files verified, not just counts.

**Setup (fair, same cluster):** EKS `zelox-stream-dist`, ap-south-1, image `zelox:rename42` (arm64).
Both engines pinned to **identical 6 vCPU / 10 GiB** per pod. c7g.4xlarge AND m7g.4xlarge were
capacity-unavailable for new nodegroups (both CFN ROLLBACK) → ran on the existing c7g.2xlarge nodes,
both engines equal → **ratios are valid; absolutes are ~half the 16-vCPU published baseline.** Torn to $0.

## Batch → S3 vs Spark 3.5.3 (100M rows; output verified identical: sum + 1000 distinct keys match)

| Metric | Zelox | Spark 3.5.3 | Zelox advantage |
|---|--:|--:|---|
| Total (write+read+agg) | **4.08 s** | 32.52 s | **8.0× faster** |
| Write 100M → S3 parquet | **2.46 s** | 29.12 s | **11.8×** |
| Read + aggregate | **1.61 s** | 3.40 s | **2.1×** |
| Peak RSS | **1.89 GiB** | 5.6 GiB | **~3× less** |

Zelox parquet files physically confirmed on S3 (`zelox/*.zst.parquet`, ~63 MB/part). `scripts/eks_batch_s3.sh 100000000`.

## Streaming + realtime vs Flink 1.19 (100M, `trigger(realTime)`)

| Metric | Zelox | Flink 1.19 | Verdict |
|---|--:|--:|---|
| Throughput (consume 100M, bounded windowed-agg) | 4.09M ev/s (24.5s) | 3.64M ev/s (27.5s) | Zelox 1.12× ⚠️ |
| Peak memory | **7.53 GiB** | 8.41 GiB | Zelox ~10% less |
| Realtime latency p50 | **88 ms** | 162 ms | Zelox **1.8×** |
| Realtime p99 / p999 / max | **125 / 128 / 131 ms** | 254 / 285 / 302 ms | Zelox **~2×, tail 2.3×** |

⚠️ throughput caveat: Zelox emitted 9/10 windows (no `COMPLETE_ON_END` in that phase); the realtime-EO
run below proves completeness-correct output separately. `scripts/tri_engine_scorecard.sh streaming`.

## Realtime → S3 exactly-once (`trigger(realTime)`, kill-9 crash) — PASS

```
CLEAN : P1_VERIFY rows=1000 distinct_window_key=1000 sum_count=10000000 dup=0
CRASH : P1_VERIFY rows=1000 distinct_window_key=1000 sum_count=10000000 dup=0   (kill-9 mid-run, resume from S3 ckpt)
CONTINUOUS_CRASH_EO PASS (dup=0 AND crash sum == clean sum)
```
Realtime mode writes correct parquet to S3; exactly-once holds across a hard crash — the resumed output
is **byte-identical** to the clean run. Flink-parity realtime EO, verified on the actual S3 files.
`scripts/eks_continuous_eo.sh 20000000 rename42`.

## Standing vs AIM.md (measured, path-flagged)

- **vs Spark (batch): objectively beats** — 8× faster, ~3× less memory, byte-identical output. Columnar
  Arrow advantage vivid (100M→S3 in 2.46 s / 1.89 GiB).
- **vs Flink (realtime, the specified mode): beats on latency** (every percentile, ~2×, tail 2.3× — no-GC),
  **wins memory**, **EO parity verified on S3**; throughput ~parity (slight edge, completeness-caveated).
- **Unified engine:** batch + structured-streaming + realtime all on the SAME renamed/4.2 binary.
- **Open (unchanged):** distributed-exchange throughput at 16-vCPU scale (VAJ-BF2) — not exercised here
  (single-node); 4xlarge capacity blocked the 16-vCPU absolute-number confirmation.
