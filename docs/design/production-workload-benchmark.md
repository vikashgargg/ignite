# Production-workload benchmark — real sinks (S3 Parquet/Iceberg, Kafka) vs Flink + Spark

**Why (2026-07-01):** the blackhole/coverage benchmarks measure engine compute, but the workloads that
Uber/Netflix/Apple/LinkedIn actually run are **Kafka → transform → Iceberg/Parquet on S3 (exactly-once)**
(streaming) and **read/write Parquet/Iceberg on S3** (batch). To claim Zelox "replaces both, prod-grade,"
we must measure THOSE — real sinks, real object store, real EO — vs Flink + Spark. This is the
robustness/credibility layer on top of the tri-engine matrix.

## Grounding (canonical company patterns; REFERENCES §2/§3d)
- **Netflix Keystone / Mantis, Uber:** Kafka → Flink → **Iceberg/Hudi on S3** is the streaming data-lake
  standard; EO via checkpoint + transactional sink (Flink `RecoverableWriter`, FLINK-38592 native-S3).
- **Apple / Spark shops:** batch read **Parquet/Iceberg on S3** → transform/agg → write back; the TPC-DS/
  ETL workhorse. Metric = wall + memory + S3 write efficiency.
- Zelox already has the building blocks: realtime EO file sink (`RealtimeFileSinkExec`, per-epoch commit),
  Iceberg support (OverwritePartitions), object-store IO. This benchmark exercises them at prod shape.

## Workloads (each measured Zelox vs Flink and/or Spark on the SAME EKS node + S3 bucket)
| ID | Workload | Engines | Metrics |
|----|----------|---------|---------|
| **P1** | Kafka → 10s windowed-agg → **Iceberg table on S3** (append, EO) | Zelox vs Flink | throughput ev/s, e2e latency, peak mem, **EO row-exactness (no dup/loss across crash)**, S3 files/commits |
| **P2** | Kafka → JSON parse + project → **Parquet on S3** (EO) | Zelox vs Flink | throughput, mem, EO, output bytes/row (write amplification) |
| **P3** | Kafka → transform → **Kafka topic** (enrichment, EO) | Zelox vs Flink | throughput, p50/p99/p999 latency (extends lat_probe to a real transform) |
| **P4** | Batch: read **Parquet on S3** → agg/join → write **Parquet on S3** | Zelox vs Spark | wall, peak mem, output correctness, S3 read/write MB |
| **P5** | Batch: read **Iceberg on S3** → transform → write **Iceberg** (partition overwrite) | Zelox vs Spark | wall, mem, snapshot correctness |

## Metrics that matter for prod (beyond wall)
- **EO correctness under crash** (the prod differentiator): kill mid-run, assert output = exactly the
  input set (no dup/loss) — reuse the soak/chaos gate shape, but to S3/Iceberg sinks.
- **Memory** (path-dependent per our findings: Zelox 8× less batch, 1.20× more streaming-bounded).
- **Latency** p50/p99/**p999 tail** (no-GC edge — already competitive/better vs Flink).
- **S3 efficiency:** #files/commits, bytes written per row (small-file problem = a real Flink pain point).

## Harness plan (extend the existing, don't rebuild)
- **S3 bucket** (temp, per-run, deleted on teardown — $0 discipline). IRSA for pod S3 auth.
- **P1/P2/P3 streaming:** extend `tri_engine_scorecard.sh streaming` — Zelox `stream_windowed_agg.py`/
  realtime sink writing Iceberg/Parquet to `s3://…`; Flink SQL sink = `iceberg`/`filesystem` connector to
  the same bucket. Add an **EO-verify** step (read the S3/Iceberg output, assert row-exactness) + a crash
  variant.
- **P4/P5 batch:** extend `tri_engine_scorecard.sh batch` — read/write Parquet/Iceberg on S3 (a real ETL
  query), Zelox vs Spark same node/bucket. Also **fixes the TPC-DS `LIMIT 10000` gap** (real data at SF on
  S3 = a true power test).

## Pre-reqs / fixes to land first (from the tri-engine findings)
1. **TPC-DS gen fix** (remove `LIMIT 10000`, real `dsdgen` at SF to shared S3/parquet) → real batch perf.
2. **Q5/Q9 TPC-DS compat** (cr_return_amt schema, float-comparison) — close the 2 gaps.
3. **Streaming bounded-path memory 1.20×** (bounded buffers/backpressure/spill) — the one measured loss.
4. S3/IRSA wiring + a small-file/commit metric.

## ✅ P1 PASSED (EKS 2026-07-02) — Kafka→windowed-agg→Parquet-on-S3, exactly-once incl. crash
`scripts/eks_p1_s3_eo.sh`, 100M events, c7g.4xlarge, real S3 bucket:
- **P1a clean:** rows=9000 distinct_window_key=9000 **dup=0** sum_count=90,000,000 (9 windows × 1000 keys,
  exact) · throughput **4.67M ev/s** (to S3; ~17% under the 5.6M blackhole = the sink cost) · peak RSS
  **7.25 GiB** (still < Flink's 8.55 blackhole, WITH a real S3 sink).
- **P1b EO-under-crash:** `kill -9` the server mid-run → restart → **resume from the S3 checkpoint** →
  rows=9000 dup=0 sum_count=90,000,000 **bit-identical to clean**. Exactly-once across a hard crash,
  proven on a real object-store sink = the Flink EO guarantee, on Zelox.
⇒ **Zelox does the canonical Uber/Netflix streaming-data-lake pattern correctly, incl. EO recovery.** The
real-workload throughput bottleneck now INCLUDES the S3 sink (~17%), which reframes the T7 decision (parse-
fusion attacks from_json, but the sink is now a co-equal cost). Next: P4/P5 batch-on-S3 vs Spark; P1 Flink
side-by-side (needs Flink s3-fs plugin); Iceberg-table sink (P1 used Parquet — the EO substrate is the same).

### P4 PASSED — batch Parquet-on-S3 vs Spark 3.5.3 (EKS 2026-07-02, `scripts/batch_s3_bench.py`)
Canonical batch-ETL: generate 200M rows → **write Parquet to real S3** → read back + agg (count, sum(v),
distinct k). Like-for-like: SAME compute node, SAME S3 bucket, SAME script; Zelox scaled to 0 before Spark
ran (each gets the full node). **Correctness MATCHES exactly:** both rows=200,000,000, distinct_k=1000,
sum_v=39,999,999,800,000,000 (bit-identical).

| engine | write_s | read+agg_s | **total_s** | peak RSS |
|---|---|---|---|---|
| **Zelox** | 4.38 | 1.54 | **5.92** | **3.44 GiB** |
| Spark 3.5.3 | 33.43 | 3.52 | 36.94 | 8.1 GiB |
| Zelox advantage | 7.6× | 2.3× | **6.2× faster** | **2.4× less** |

⇒ **Zelox replaces Spark for batch Parquet-on-S3 ETL: ~6× faster wall + ~2.4× less memory, identical
output at 200M-row prod scale** (no-JVM Arrow footprint; write path dominates and is where Zelox wins most).
Spark-on-S3 harness gotchas (all fixed): py3.12 needs `setuptools<81` (distutils shim); the MAGIC
`SPARK_REMOTE` env forces Connect-mode (needs pandas) and ignores `.master()` — local baseline must use the
non-magic `BENCH_REMOTE` + classic `local[16]`; hadoop-aws:3.3.4 + aws-java-sdk-bundle:1.12.262 +
InstanceProfileCredentialsProvider + s3→s3a. Next: batch **Iceberg** table vs Spark (needs JdbcCatalog +
S3FileIO catalog svc); P1 Flink side-by-side; streaming Iceberg sink.

## Apple container compatibility (periodic gate — local + distributed)
Zelox must also run these workloads on **Apple `container`** (native macOS arm64 runtime) — SAME arm
image as EKS — to prove it runs distributed loads on Apple container, not just k8s. Validated once
2026-06-16 (Apple container 1.0.0, local-cluster 4 workers: functional 6/6 vs Spark + EO across a HARD
container kill; see [[project_apple_container]]). Standing plan:
- **Local mode:** Zelox `--mode local-cluster` in one Apple container — smoke P1/P2 (Kafka→Parquet/Iceberg)
  + EO across `container kill`.
- **Distributed/cluster mode:** multiple Apple containers (scheduler + N workers on the 192.168.64.x
  bridge, Kafka dual-listener) running P1–P5 as a distributed load — proves the SAME image scales out on
  Apple container the way it does on EKS.
- **Cadence:** run this gate **periodically (after major engine changes / before a release)**, NOT every
  EKS run — it's a compatibility+distributed-runtime check, cheap and local (no cloud $). Build-env
  gotchas in [[project_apple_container]] (builder VM 6cpu/4gb, opt-level=1 for AWS-SDK OOM, Kafka
  192.168.64.1:9093 dual-listener).

## Sequence
Commit+docs (this) → land fixes 1–3 → build P1–P5 harness on S3/Iceberg → ONE EKS session per phase
(streaming P1–P3, batch P4–P5) vs Flink/Spark, teardown $0 → **Apple-container gate (local + distributed,
same image)** periodically → record → iterate. Only claim "beats/replaces" per measured prod-workload win.
