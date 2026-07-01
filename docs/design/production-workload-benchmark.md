# Production-workload benchmark — real sinks (S3 Parquet/Iceberg, Kafka) vs Flink + Spark

**Why (2026-07-01):** the blackhole/coverage benchmarks measure engine compute, but the workloads that
Uber/Netflix/Apple/LinkedIn actually run are **Kafka → transform → Iceberg/Parquet on S3 (exactly-once)**
(streaming) and **read/write Parquet/Iceberg on S3** (batch). To claim Vajra "replaces both, prod-grade,"
we must measure THOSE — real sinks, real object store, real EO — vs Flink + Spark. This is the
robustness/credibility layer on top of the tri-engine matrix.

## Grounding (canonical company patterns; REFERENCES §2/§3d)
- **Netflix Keystone / Mantis, Uber:** Kafka → Flink → **Iceberg/Hudi on S3** is the streaming data-lake
  standard; EO via checkpoint + transactional sink (Flink `RecoverableWriter`, FLINK-38592 native-S3).
- **Apple / Spark shops:** batch read **Parquet/Iceberg on S3** → transform/agg → write back; the TPC-DS/
  ETL workhorse. Metric = wall + memory + S3 write efficiency.
- Vajra already has the building blocks: realtime EO file sink (`RealtimeFileSinkExec`, per-epoch commit),
  Iceberg support (OverwritePartitions), object-store IO. This benchmark exercises them at prod shape.

## Workloads (each measured Vajra vs Flink and/or Spark on the SAME EKS node + S3 bucket)
| ID | Workload | Engines | Metrics |
|----|----------|---------|---------|
| **P1** | Kafka → 10s windowed-agg → **Iceberg table on S3** (append, EO) | Vajra vs Flink | throughput ev/s, e2e latency, peak mem, **EO row-exactness (no dup/loss across crash)**, S3 files/commits |
| **P2** | Kafka → JSON parse + project → **Parquet on S3** (EO) | Vajra vs Flink | throughput, mem, EO, output bytes/row (write amplification) |
| **P3** | Kafka → transform → **Kafka topic** (enrichment, EO) | Vajra vs Flink | throughput, p50/p99/p999 latency (extends lat_probe to a real transform) |
| **P4** | Batch: read **Parquet on S3** → agg/join → write **Parquet on S3** | Vajra vs Spark | wall, peak mem, output correctness, S3 read/write MB |
| **P5** | Batch: read **Iceberg on S3** → transform → write **Iceberg** (partition overwrite) | Vajra vs Spark | wall, mem, snapshot correctness |

## Metrics that matter for prod (beyond wall)
- **EO correctness under crash** (the prod differentiator): kill mid-run, assert output = exactly the
  input set (no dup/loss) — reuse the soak/chaos gate shape, but to S3/Iceberg sinks.
- **Memory** (path-dependent per our findings: Vajra 8× less batch, 1.20× more streaming-bounded).
- **Latency** p50/p99/**p999 tail** (no-GC edge — already competitive/better vs Flink).
- **S3 efficiency:** #files/commits, bytes written per row (small-file problem = a real Flink pain point).

## Harness plan (extend the existing, don't rebuild)
- **S3 bucket** (temp, per-run, deleted on teardown — $0 discipline). IRSA for pod S3 auth.
- **P1/P2/P3 streaming:** extend `tri_engine_scorecard.sh streaming` — Vajra `stream_windowed_agg.py`/
  realtime sink writing Iceberg/Parquet to `s3://…`; Flink SQL sink = `iceberg`/`filesystem` connector to
  the same bucket. Add an **EO-verify** step (read the S3/Iceberg output, assert row-exactness) + a crash
  variant.
- **P4/P5 batch:** extend `tri_engine_scorecard.sh batch` — read/write Parquet/Iceberg on S3 (a real ETL
  query), Vajra vs Spark same node/bucket. Also **fixes the TPC-DS `LIMIT 10000` gap** (real data at SF on
  S3 = a true power test).

## Pre-reqs / fixes to land first (from the tri-engine findings)
1. **TPC-DS gen fix** (remove `LIMIT 10000`, real `dsdgen` at SF to shared S3/parquet) → real batch perf.
2. **Q5/Q9 TPC-DS compat** (cr_return_amt schema, float-comparison) — close the 2 gaps.
3. **Streaming bounded-path memory 1.20×** (bounded buffers/backpressure/spill) — the one measured loss.
4. S3/IRSA wiring + a small-file/commit metric.

## Apple container compatibility (periodic gate — local + distributed)
Vajra must also run these workloads on **Apple `container`** (native macOS arm64 runtime) — SAME arm
image as EKS — to prove it runs distributed loads on Apple container, not just k8s. Validated once
2026-06-16 (Apple container 1.0.0, local-cluster 4 workers: functional 6/6 vs Spark + EO across a HARD
container kill; see [[project_apple_container]]). Standing plan:
- **Local mode:** Vajra `--mode local-cluster` in one Apple container — smoke P1/P2 (Kafka→Parquet/Iceberg)
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
