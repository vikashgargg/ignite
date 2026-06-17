# Vajra vs Flink — streaming head-to-head on EKS (2026-06-17)

Real, like-for-like windowed-aggregation head-to-head on AWS EKS, Graviton
`c7g.4xlarge` (16 vCPU — the exact class of Flink's published 1.19 windowed-agg
baseline), official Apache Flink 1.19. **No workarounds: this records what actually
happened, including a genuine Vajra bug the test surfaced.**

## Methodology (true like-for-like)

- **Workload:** 100,000,000 keyed, event-timed records pre-loaded into one Kafka
  topic (`events`, 16 partitions, 1000 keys, event-time spanning 100 s). Both
  engines consume the **same topic** from earliest.
- **Query (identical on both):** 10-second event-time **tumbling window**, group by
  `(window, k)`, `COUNT(*)`. Bounded read of the whole backlog → wall time =
  catch-up throughput (directly comparable to Flink's published "events/s").
  - Flink: SQL `TUMBLE(...)`, Kafka connector, `scan.bounded.mode=latest-offset`,
    `table.dml-sync=true`, blackhole sink, parallelism 16. (`k8s/stream/flink-sql.sql`)
  - Vajra: Spark Structured Streaming `window(...,"10 seconds")`, `availableNow`.
    (`scripts/stream_windowed_agg.py`)
- **Hardware:** each engine ran ALONE on a dedicated `c7g.4xlarge` (16 vCPU / ~26 GiB
  for the engine), Kafka on a separate node, run sequentially — identical resources.
- **Cluster:** EKS 1.31, 3× `c7g.4xlarge`, arm64. Cost: ~a few USD, **torn down to $0**.

## Results

| Engine | Workload | Result | Peak RSS |
|---|---|---|---|
| **Apache Flink 1.19** | 100M-event 10s tumbling windowed COUNT | **8.8 s → 11.36M events/s** | **8.5 GiB** |
| **Vajra** | same | **FAILED — Arrow i32 offset overflow** (see below) | — |

Flink's number is clean and official-setup (the only config fix needed was exposing
the JobManager **blob port 6124** in the service, per Flink's standalone-Kubernetes
reference). It is higher than Flink's 1.82M/s headline because that headline is a
heavier Nexmark-class query; this simple 1000-key COUNT is lighter — but both engines
run the **identical** query here, so it's a fair comparison.

## The Vajra bug this surfaced (honest finding)

On the identical 100M-event workload, the Vajra worker **panicked**:

```
thread 'tokio-rt-worker' panicked at arrow-buffer-58.1.0/src/buffer/offset.rs:167:35:
offset overflow
```

An Arrow **i32 `OffsetBuffer` overflow** = a single `Utf8`/`Binary`/`List` array
exceeded **2 GiB** (i32 offset limit). The streaming windowed aggregation over the
100M-event backlog built/accumulated an oversized variable-length array.

### Diagnosis so far
- **Not the `value` column.** Reproduced locally with 1.5M × 2 KB messages (3 GB of
  `value` bytes) — it did **not** overflow, proving the raw Kafka `value` Binary is
  processed in bounded per-batch chunks and never concatenated across the stream.
  The logical plan confirms `value` is projected away right after `from_json`.
- **Distributed-path specific.** A single-host `local-cluster` debug build did not
  reproduce the overflow at 100M (sessions died at ~0.5 GiB for an unrelated reason);
  the overflow reproduces reliably only on **multi-node EKS**, pointing at the
  cross-node Arrow-Flight transport / state path at scale.
- The release image is `strip=true`, so the deployed binary yields no symbol frames;
  a debug-symbol build (`docker/Dockerfile.dbg`) on the distributed env is needed for
  the exact failing operator.

### Prod-grade fix direction (grounded in references; NOT yet applied)
1. **Bounded micro-batches (Spark model).** Vajra's Kafka source ignores
   `maxOffsetsPerTrigger`; `availableNow` reads the entire backlog as one micro-batch.
   Implement Spark's `KafkaMicroBatchStream` behavior: split the backlog into bounded
   micro-batches, each committing offsets + emitting + advancing the watermark. Bounds
   per-batch memory and prevents any single oversized array.
2. **Incremental watermark-driven window eviction (Flink/Spark model).** Ensure closed
   windows finalize+evict *during* the read, so state stays O(open windows), never
   O(backlog).
3. **Arrow large-offset / view layout.** Where large variable-length columns are
   unavoidable, use `LargeBinary`/`LargeUtf8` (i64 offsets) or DataFusion's
   `StringView`/`BinaryView` (the DataFusion 53+ default) — Arrow's documented remedy
   for the 2 GiB i32 limit.

## Reproduce
- Cluster: `k8s/eks-bench.yaml` (eksctl). Kafka + producer: `k8s/stream/kafka.yaml`,
  `k8s/stream/producer-job.yaml`. Flink: `k8s/stream/flink-session.yaml`,
  `flink-sql.sql`, `flink-runner-job.yaml`. Vajra: `k8s/stream/vajra-stream.yaml`,
  `scripts/stream_windowed_agg.py`. Local repro: `scripts/local_offset_overflow_repro.py`.

## Status
Batch (TPC-H SF-100) vs Spark is already published (`TPCH_SF100.md`: 3.2× faster,
2.2× less memory). Streaming: **Flink baseline captured; Vajra windowed-agg at 100M
events needs the offset-overflow fix above before a clean head-to-head number.**
