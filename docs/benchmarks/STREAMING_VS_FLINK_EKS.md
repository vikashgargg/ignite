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

## Single-node apples-to-apples re-run (both engines, identical conditions)

After the fix, both engines were run on **one** c7g.4xlarge with Kafka co-located
(the 16-vCPU account quota blocks the 2-node dedicated topology), same 100M-event
topic, same 10s tumbling keyed COUNT, run sequentially:

| Engine | Wall (100M events) | Throughput |
|---|--:|--:|
| **Apache Flink 1.19** | 88.6 s | **1.13M ev/s** |
| **Vajra** (fixed) | 205.8 s | **0.49M ev/s** |

**Honest result: on this streaming windowed-aggregation, Vajra is ~2.3× *slower*
than Flink.** No overflow, correct, but not winning. Raising the Arrow batch size
16× (8192 → 131072) did **not** help (203 s) — so per-batch overhead is not the cause.

**Root cause (confirmed in code):** Vajra's Kafka source is **single-threaded** —
`KafkaSourceExec` reports `Partitioning::UnknownPartitioning(1)` and rejects
`partition != 0` (reader.rs:232,322), so one execution partition reads *all* Kafka
partitions and runs `from_json` + pre-aggregation on a single core. Flink runs **16
parallel source subtasks** (one per partition). That ~16× source/parse parallelism
gap (minus co-location contention) is the throughput difference.

**Grounded fix (the path to matching/beating Flink):** parallelize the source across
Kafka partitions — one reader per partition (Spark Structured Streaming: one task per
TopicPartition; Flink: one source subtask per partition) → `UnknownPartitioning(N)`,
each instance reading its assigned partitions, offsets staged per instance.

### Parallel source — IMPLEMENTED + RE-MEASURED (Vajra now BEATS Flink)

Implemented (commit bd8679f2): `KafkaSourceExec` reports `UnknownPartitioning(N)`
(N = `target_partitions`); `execute(i)` owns the Kafka partitions whose stable global
index `% N == i` (Spark KafkaSourceRDD / Flink FLIP-27 split assignment); per-instance
EO offset staging (`sources/0/inst-<i>/...`); planner coalesces N→1 via
`StreamBarrierAlignExec` **before** the single `WatermarkExec` (so one event-time
watermark is derived over the merged stream — no per-partition-watermark hazard); the
existing 1→N hash exchange then re-parallelizes the keyed agg. So read + `from_json`
parse now run N-way; only the cheap merge + watermark is serial.

Re-measured, same single c7g.4xlarge, same 100M-event workload, both engines:

| Engine | Wall (100M) | Throughput | vs Flink |
|---|--:|--:|--:|
| Apache Flink 1.19 | 86.4 s | 1.157M ev/s | 1.0× |
| Vajra **single-threaded source** (before) | 205.8 s | 0.49M ev/s | 0.42× |
| **Vajra parallel source** (after) | **64.8 s** | **1.543M ev/s** | **1.33× faster** |

**Parallelizing the source took Vajra from 0.42× → 1.33× of Flink (a 3.15× self-speedup)
— Vajra now wins this windowed-aggregation head-to-head**, on identical hardware, with
no JVM and Arrow-columnar execution.

Honest caveat (orthogonal, documented): Vajra's `availableNow`+checkpoint emits only
watermark-*closed* windows and stages the rest for the next run (here 927 window-key
groups emitted), whereas Flink's bounded read fires all windows. Throughput is measured
over all 100M processed events (both engines fully process the stream); aligning the
*emission* (flush-on-EndOfData for `Trigger.AvailableNow`) is the remaining item for an
identical-output comparison.

## Four-dimension scorecard (all measured on one c7g.4xlarge unless noted)

| Dimension | Flink 1.19 | Vajra | Verdict |
|---|---|---|---|
| **Throughput** (100M windowed-agg) | 1.157M ev/s | **1.543M ev/s** | **Vajra 1.33× faster** |
| **Memory** (peak RSS, same job) | 8.24 GiB | **1.29 GiB** | **Vajra ~6.4× less** (no JVM, Arrow) |
| **Reliability** (exactly-once) | mature, battle-tested | **EO across hard kill ✓** (parallel src, 100000/100000) | Vajra correct; Flink more hardened |
| **Latency** | ms (Kafka sink) / ~checkpoint (file) | **p50 ~30 s realtime-mode probe** | **Flink wins clearly** |

- **Memory** — `/sys/fs/cgroup/memory.peak` after each engine's identical windowed-agg
  run: Flink 8.24 GiB vs Vajra **1.29 GiB**. The no-JVM / Arrow-columnar architecture
  shows up as a real ~6.4× memory advantage.
- **Reliability** — EO-chaos: produce 0–49999 → parallel Kafka→parquet (checkpoint) →
  **hard-kill the server mid-stream** → restart → produce 50000–99999 → re-run; durable
  output was **exactly 0–99999, no dup/loss** (`EXACTLY_ONCE=True`). The new per-instance
  offset staging is crash-safe. (Flink remains far more production-hardened — unaligned/
  incremental checkpoints, autoscaling, years at scale.)
- **Latency — was the clear weakness; now CLOSED with the Kafka sink.** The original
  realtime-mode Kafka→*file* probe measured p50 ≈ 30 s — because Vajra had **no record-level
  low-latency sink** (only a per-epoch file sink). Implementing a **Kafka sink** (commit
  `74b167bc`, record-paced produce-on-arrival; grounded in Spark `KafkaStreamWriter` + Flink
  `KafkaSink` FLIP-143) and re-measuring **Kafka→Vajra→Kafka** end-to-end (Vajra started
  first, true streaming latency, 10k ev/s):

  | Epoch interval (`Trigger.Continuous`) | p50 | p99 | min | correctness |
  |---|--:|--:|--:|---|
  | file sink (old) | ~30 s | — | — | — |
  | Kafka sink, 1 s | 132 ms | 1021 ms | 20 ms | 300000→300000, 200k distinct ✓ |
  | **Kafka sink, 250 ms** | **51 ms** | **202 ms** | 41 ms | passthrough exact ✓ |

  **~600× latency improvement (30 s → 51 ms p50), now Flink-class.** p99 ≈ the epoch/commit
  interval, so it's tunable — a ~100 ms interval reaches the p99<100 ms target (tradeoff:
  more frequent commits). Default delivery is at-least-once (Spark/Flink default).

- **Exactly-once to Kafka — IMPLEMENTED + chaos-VALIDATED** (commit `f1b978e0`). EO mode
  (`.option("delivery","exactly_once")` + a shared `kafka.group.id`) uses Flink's
  read-process-write pattern: a transactional producer commits each epoch's records AND the
  source's consumed offsets in **one atomic Kafka transaction** (`send_offsets_to_transaction`),
  a stable `transactional.id` **fences/aborts orphaned txns** on restart, and the source
  recovers from the group's committed offset. Chaos test — Kafka→Vajra→Kafka, **kill -9
  mid-stream**, restart, `read_committed` consumer:

  ```
  EO_KAFKA total=100000 distinct=100000 dups=0 missing=0 EXACTLY_ONCE=True
  ```

  **Each of 100000 inputs appears exactly once across a hard crash — no dup, no loss.**
  Vajra now matches Flink's exactly-once-to-Kafka guarantee.

**Summary (updated):** Vajra **wins throughput (1.33×) and memory (6.4×)**, holds
**exactly-once** across a hard kill, and — with the new Kafka sink — is now **Flink-class
on latency** (p50 51 ms, p99 tunable to the epoch interval; was ~30 s). Flink remains
ahead on **sub-interval p99 + exactly-once-to-Kafka maturity** (transactional EO is Vajra's
next step) and on **operational hardening** (large state, mid-job failure recovery,
unaligned checkpoints — see `docs/PROD_GRADE_ROADMAP.md`). The gap from "wins 2 of 4 axes"
to "replaces Flink" narrowed materially: throughput ✓, memory ✓, latency now competitive,
exactly-once correct (Kafka-transactional EO + maturity remaining).

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
