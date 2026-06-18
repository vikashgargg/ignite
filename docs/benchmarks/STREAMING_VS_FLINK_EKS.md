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
| **Vajra** (at time of run) | same | **FAILED — Arrow i32 offset overflow** (root-caused + FIXED, commit 6b812758; see below) | — |

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

### Root cause (CONFIRMED via on-instance debug-symbol backtrace)
The backtrace pinned it to `sail-function/src/scalar/json/from_json.rs:154` inside a
DataFusion `project_batch`. Chain:
`maxOffsetsPerTrigger=20000000` → (options.rs aliased it to `max_batch_size`) → the
Kafka source built a **single 20M-row Arrow RecordBatch** → `from_json` materialized a
`Utf8` column whose total bytes exceeded Arrow's **i32 `OffsetBuffer` limit (2 GiB)** →
`offset overflow`. Scale-dependent: 4M rows stayed under 2 GiB (worked), 20M did not.

The defect was **conflating three concerns the proven systems keep separate**:
- **Spark `KafkaMicroBatchStream`**: `maxOffsetsPerTrigger` is per-micro-batch
  *admission control*, NOT the columnar buffer size.
- **DataFusion**: streams **bounded** RecordBatches (`batch_size` default 8192) so
  arrays/memory stay bounded.
- **Arrow**: `Utf8`/`Binary` use i32 offsets (2 GiB/array) — bound batches by *bytes*,
  or use `LargeUtf8`/`BinaryView` (StringView).

### Fix (APPLIED — commit 6b812758)
1. **Decouple admission from buffering** (`options.rs`): `maxOffsetsPerTrigger` /
   `maxOffsetsPerMicroBatch` now parse into a separate `max_offsets_per_trigger`
   (admission); `maxbatchsize` sets the Arrow buffer (default **8192**, clamped to a
   2 GiB-safe ceiling). Admission is driver-side, so it is not serialized to workers.
2. **Byte-bounded batching** (`reader.rs`, all 3 collection loops incl. the realtime
   epoch loop): flush on **either** a row cap **or** a 128 MiB byte cap — the byte cap is
   the real guarantee since the overflow is byte-driven, keeping every Utf8/Binary
   column far under the i32 limit regardless of payload size.

**Validated** on the same c7g.4xlarge + 100M-event workload at `maxOffsetsPerTrigger=20M`
(the exact failing case): completes cleanly, **no overflow**.

### Still open for a clean head-to-head number
- A clean Vajra throughput figure needs the dedicated-node EKS topology (Kafka + engine
  on separate c7g.4xlarge), not the single-instance validation host. Pending a future
  EKS run with the fixed image (and an account vCPU-quota raise — current limit is 16).
- Windowed-agg under `availableNow`+checkpoint emits only watermark-*closed* windows
  (open windows are staged for the next run); for a like-for-like vs Flink's bounded
  read (which fires all windows), align emission semantics (flush-on-EndOfData for
  bounded/`Trigger.AvailableNow`).

## Reproduce
- Cluster: `k8s/eks-bench.yaml` (eksctl). Kafka + producer: `k8s/stream/kafka.yaml`,
  `k8s/stream/producer-job.yaml`. Flink: `k8s/stream/flink-session.yaml`,
  `flink-sql.sql`, `flink-runner-job.yaml`. Vajra: `k8s/stream/vajra-stream.yaml`,
  `scripts/stream_windowed_agg.py`. Local repro: `scripts/local_offset_overflow_repro.py`.

## Status
Batch (TPC-H SF-100) vs Spark is already published (`TPCH_SF100.md`: 3.2× faster,
2.2× less memory). Streaming: **Flink baseline captured (11.36M ev/s, 8.5 GiB); the
Vajra offset-overflow bug it surfaced is root-caused and FIXED (commit 6b812758),
validated overflow-free on the same hardware + 100M-event workload.** A clean Vajra
*throughput* head-to-head number remains to be measured on the dedicated-node EKS
topology with the fixed image (blocked only by an account vCPU-quota raise).

This is the intended value of a true, no-workaround head-to-head: it found a real,
scale-dependent engine bug, and the fix realigns Vajra with how Spark/DataFusion/Arrow
separate admission control, columnar buffering, and the 2 GiB array limit.
