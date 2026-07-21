# Zelox — one engine for batch + streaming + realtime (Spark API, Flink-class streaming)

> **⚠️ Measured update (2026-06-19):** the rigorous, like-for-like head-to-head vs **official
> Apache Flink 1.19** on EKS supersedes the older single-node "sub-ms / Flink 1.18"
> directional figures below. Measured: Zelox **wins throughput (1.33×) and memory (6.4×)**
> and holds **exactly-once across a hard kill**, but **Flink wins latency** (Zelox has no
> record-level low-latency sink yet). Authoritative results + the grounded gap analysis +
> roadmap: **[STREAMING_VS_FLINK_EKS.md](benchmarks/STREAMING_VS_FLINK_EKS.md)** and
> **[PROD_GRADE_ROADMAP.md](PROD_GRADE_ROADMAP.md)**. Treat the "realtime/latency" claims
> in this vision doc as *target*, not *measured*.

**Vision.** One product, one API. Write Spark DataFrame/SQL (the "Spark coding way") and run it three ways
on the same native engine:
- **batch** — Spark-class (already shipped; ~30× faster than Spark 3.5 on TPC-H SF-1, head-to-head).
- **streaming (micro-batch)** — Spark Structured Streaming-compatible (`readStream`/`writeStream`,
  triggers, watermarks, checkpoints).
- **realtime (Zelox realtime mode)** — Flink-class low-latency *continuous* execution of the same
  streaming query, for tens-of-ms tail latency and per-event processing.

This document is the honest gap analysis vs Apache Flink (the streaming gold standard) and the
prioritized, prod-grade roadmap to become a true Spark **and** Flink replacement. We do **not** claim
parity for anything not measured/implemented; gaps are named.

Sources: Flink stateful-stream-processing & fault-tolerance docs
(nightlies.apache.org/flink/flink-docs-release-1.18), Flink vs Spark Structured Streaming comparisons
(confluent.io, onehouse.ai, decodable.co), DataFusion/Arrow execution model.

---

## 0. Architecture thesis — why Zelox is fundamentally leaner (governs every feature)

**We do not port Flink's mechanisms into Zelox. We re-derive each capability on a leaner substrate,
then prove "better" with a fair, no-workaround measurement.** Three structural advantages Spark and
Flink cannot retrofit (they're baked into their foundations):

1. **No JVM (Rust, native).** This is the biggest lever, three ways: (a) **no GC pauses** → tail
   latency stays flat (**p99 ≈ p50**; Flink's p99 is dominated by stop-the-world GC) — this is *the*
   basis for "lower latency than Flink," not a faster loop but the *absence* of GC jitter; (b) **no
   JVM object overhead** → data lives in compact Arrow buffers, the structural reason for the measured
   7–16× memory wins; (c) no JIT warmup — a persistent native server.
2. **Arrow columnar + vectorized (DataFusion).** Flink processes one row/object at a time (good
   latency, costly per-record CPU+memory). Zelox processes Arrow batches vectorized (SIMD,
   cache-friendly). Realtime mode shrinks the flow-event batch toward 1 row for latency **while staying
   vectorized**, approaching Flink's latency without Flink's per-record CPU cost (measured: sub-ms
   latency *at* multi-M rows/s simultaneously — the micro-batch-vs-continuous tradeoff doesn't bind us
   the same way).
3. **One engine for batch + streaming.** Batch is a *bounded stream* in the flow-event model, so the
   optimizer/vectorization/perf work applies to both paths — no duplicated engine (Spark batch ≠
   Structured Streaming; Flink batch is bolted onto streaming).

**Re-derivation principle per gap** (take the reference *idea*, exploit the substrate):
- Checkpoint (F4): Arrow-IPC state as a **single atomic object** (no rename, fewer S3 ops, compact) —
  not Flink's serialized-Java multi-file coordinator.
- Realtime (F1): tiny **vectorized** flow-event batches at high tick — not per-record operator threads.
- Barriers (F2/F3): barriers = the **existing flow-event markers** on the existing Arrow-Flight shuffle
  — not a bespoke network barrier subsystem; one shuffle for batch+streaming.
- State backend (F5): **Arrow-columnar state** + Arrow spill to object store — not per-key RocksDB
  serialization.

**Discipline:** the substrate gives the headroom; a fair benchmark *earns* every claim (same input,
isolate the component, discard handicapped numbers — see `docs/benchmarks/ICEBERG_SINK.md`). Where we
don't yet beat Flink (e.g. distributed multi-node throughput today), we say so.

---

## 1. Flink feature matrix vs Zelox (honest status)

Legend: ✅ done & evidenced · 🟡 partial · ⬜ gap.

| # | Flink capability | Why it matters | Zelox status | Priority |
|---|---|---|---|---|
| F1 | **Continuous (event-at-a-time) pipelined execution**, tens-of-ms latency | Flink's defining property | ✅ realtime mode (`Trigger.Continuous`): long-lived flow-event pipeline; sub-ms processing latency measured (F1c); **continuous Kafka→durable file sink exactly-once across restart measured on real Kafka (F1b)** — marker-driven sink + atomic `realtime/committed`; beyond Spark Continuous (at-least-once, no file sink), Flink-EO without alignment. Gaps: multi-partition + stateful → F2/F3 | **P0 — DONE (stateless)** |
| F2 | **Distributed stateful streaming with operator parallelism** (sharded keyed state across the cluster) | Scale-out throughput | 🟡 single-partition per-core; keyed `StreamExchangeExec` + multi-partition `WindowAccumExec` exist locally; not distributed across nodes; streaming/Iceberg-commit not in distributed codec | **P0** |
| F3 | **Checkpointing via Chandy-Lamport aligned barriers** (global consistent snapshot) | Distributed exactly-once | 🟡 **two F3 cores built + unit-tested (2026-06-15):** (1) `StreamBarrierAlignExec` (N→1) aligns a broadcast `Checkpoint{epoch}` — blocks each input's post-barrier data until aligned, forwards one barrier (Flink "barriers never overtake records"); (2) `EpochCoordinator` (Flink JobManager / RisingWave meta equivalent): trigger / idempotent ack / **all-ack global completion → atomic commit** / abort / recover, monotonic + backpressured (7 tests). Design: [docs/design/distributed-streaming-f2f3.md](design/distributed-streaming-f2f3.md). (3) **marker-aware distributed shuffle (F3-b data plane, 2026-06-15):** `ShuffleWriteExec` over Arrow Flight now **broadcasts** flow-event marker/barrier batches to all downstream partitions (vs hash-routing) — the cross-node `StreamExchangeExec`, unit-tested (markers→all partitions, data hash-routed once). **F3-b control plane (concurrent producer+consumer scheduling) — found ALREADY DONE (2026-06-15, code-verified):** the distributed engine is fully *pipelined* (every stage `OutputMode::Pipelined`; `Blocking` never constructed), so the topology forms one co-scheduled pipelined region and all stages run concurrently with bounded-channel backpressure — Flink-streaming-style, not Ballista-blocking. **Codec serialization of all streaming operators ✅ DONE (2026-06-15)** — kafka-source / exchange / coalesce / barrier-align / watermark / dedup / window-agg (template AggregateExec) / stream-join / flow-decode / realtime-file-sink all round-trip through `RemoteExecutionCodec`; a full distributed streaming plan (incl. stateful) can ship to workers. 8 round-trip tests. *Remaining:* `StreamBarrierAlignExec` insertion at cross-node shuffle-receive, distributed `EpochCoordinator` + per-instance state snapshot (F3-c), multi-node gate vs Flink (F3-d). See [docs/design/distributed-streaming-f2f3.md §5](design/distributed-streaming-f2f3.md) | **P0 (with F2)** |
| F4 | **Durable / object-store checkpoints** (S3, HDFS) | Cloud-native HA, k8s pod restart | ✅ **done (2026-06-14):** all checkpoint state (offset markers, source offset record, operator-state blob) goes through `CheckpointStore` (object_store); commit = single atomic `put` (no rename). Verified on real S3 (stateless + stateful, EO across restart). | done |
| F5 | **Pluggable state backends incl. RocksDB** (state ≫ RAM, spill) | Large-state jobs | ⬜ in-memory `HashMap` + Arrow-IPC snapshot only | P1 |
| F6 | **Savepoints** (deliberate snapshot for upgrade/rescale/replay) | Operability | ⬜ none | P1 |
| F7 | **Event-time, watermarks, timers** | Correct out-of-order processing | ✅ `WatermarkExec` + event-time windows + watermark-bounded dedup; 🟡 user timers/`onTimer` | P1 (timers) |
| F8 | **Windowing: tumbling/sliding/session + incremental agg** | Core analytics | ✅ tumbling/sliding/session + two-phase incremental agg (275k→ throughput work) | done |
| F9 | **Stateful stream-stream joins (interval/windowed)** | Core analytics | ✅ inner equi + interval join with watermark-bounded eviction | 🟡 outer joins |
| F10 | **Transactional / exactly-once sinks (2PC)** | End-to-end EO | 🟡 Iceberg idempotent ✅, file `_spark_metadata` ✅; Delta ⬜, Kafka sink ⬜ | P1 |
| F11 | **Replayable sources w/ committed offsets (Kafka)** | End-to-end EO | ✅ **done (2026-06-15):** Kafka bounded reads + per-(topic,partition) offset commit/restore via CheckpointStore (Spark KafkaMicroBatchStream); gated on a real broker (EO across restart, incremental). File source ✅ too. | done |
| F12 | **CEP / `ProcessFunction` / `KeyedProcessFunction`** | Pattern detection, custom state | ⬜ none | P2 |
| F13 | **Backpressure + bounded buffers** | Stability under load | ✅ bounded mpsc exchange channels (memory-bounded) | done |
| F14 | **Unaligned checkpoints, reactive/elastic rescale** | Advanced ops | ⬜ | P2 |

**The features that make Flink "Flink" and that we must build to credibly claim parity: F1 (realtime
continuous mode), F2+F3 (distributed stateful streaming + barrier checkpoints), F4 (object-store
checkpoints), F11 (Kafka offset EO).** These are the P0s. Everything we ship is gated + measured before
any parity claim.

---

## 2. Zelox realtime mode (the F1 design)

Goal: run the *same* Spark-API streaming query with Flink-class latency, without forcing users to a new
API. A query opts in via a trigger, mirroring Spark's `Trigger.Continuous` but backed by Zelox's
flow-event engine:

```python
df.writeStream.format("iceberg").trigger(realtime=True)   # Zelox realtime mode
df.writeStream.format("iceberg").trigger(processingTime="1 second")  # micro-batch (today)
```

Design (builds on the existing flow-event `FlowEvent::{Data,Marker}` model):
- **Pipelined operators**: today operators emit per micro-batch; realtime mode keeps the same operator
  graph but drives it with a *continuous* source loop that emits small flow-event batches at a high tick
  (or per-record for low-rate sources), so records flow operator→operator without a batch barrier.
  Reuse `StreamExchangeExec` (already broadcasts markers) for keyed routing.
- **Latency markers**: `FlowMarker::LatencyTracker` already exists; realtime mode samples it to report
  `processedRowsPerSecond` + p50/p99 in `recentProgress`.
- **Commit cadence decoupled from latency**: data flows continuously; durable commits (Iceberg snapshot,
  file `_spark_metadata`) still happen on a periodic *commit interval* (Flink-style), so low latency does
  not mean a commit per record.
- **Scope discipline (no workarounds)**: realtime mode is correct-or-off — if an operator in the plan
  can't run continuously yet, the query rejects realtime mode with a named reason rather than silently
  falling back to micro-batch.

This is the unifying piece: **Spark API in, batch / micro-batch / realtime out — one engine.**

---

## 3. Spark & Flink compatibility (what "one-stop tool" means)

**Spark compatibility (the API surface, already strong):** Spark Connect gRPC server; DataFrame + SQL;
`spark.read`/`write` (parquet/csv/json/Delta/Iceberg); `readStream`/`writeStream`; triggers
(`availableNow`/`once`/`processingTime`); `withWatermark`, `window`/`session_window`;
`foreachBatch`; `StreamingQuery.recentProgress`/`lastProgress`. Differential-tested vs real Spark
(105/105 scorecard; TPC-H/TPC-DS). → see `docs/` scorecards.

**Flink compatibility (the *capabilities*, not the DataStream API):** Zelox does not expose Flink's
Java DataStream API; instead it delivers Flink's *semantics* under the Spark API + realtime mode —
event-time/watermarks (F7), stateful windows/joins (F8/F9), exactly-once (F3/F10), continuous latency
(F1). A `docs/FLINK_COMPATIBILITY.md` will track each Flink capability → Zelox mechanism → evidence.

---

## 4. Prioritized prod-grade roadmap

Each item: read the OSS reference design first, implement prod-grade (no workarounds), gate with
crash/correctness tests + a measured head-to-head, then claim.

1. ~~**P0 — Object-store checkpoint (F4).**~~ ✅ **DONE 2026-06-14.** `CheckpointStore` (object_store);
   all checkpoint state is single-object with an atomic-`put` commit (no rename); operator state is one
   Arrow-IPC blob; operators restore async on first poll. Verified on real S3 (stateless + stateful, EO
   across restart). Commits 9187582e → 37640fe3 → 47b9f10c.
2. **P0 — Kafka source offset EO (F11).** Persist/restore per-partition Kafka offsets in the atomic offset
   record (the file-source pattern) → end-to-end EO from the #1 production source.
3. **P0 — Zelox realtime mode (F1).** Continuous low-latency execution path + `trigger(realtime=True)` +
   latency metrics; reject plans that can't run continuously (named).
4. **P0 — Distributed stateful streaming + barrier checkpoints (F2+F3).** Thread streaming through the
   distributed codec (StageInput already carries `bounded`); a real `CheckpointCoordinator` with aligned
   barriers wired to the offset/state WAL; reuse the existing shuffle.
5. **P1 — Transactional sinks (F10): Delta (`Txn(appId,version)`), Kafka sink (2PC).**
6. **P1 — RocksDB-class spillable state backend (F5)**; **savepoints (F6)**; **user timers (F7)**.
7. **P2 — CEP/ProcessFunction (F12); unaligned checkpoints / reactive rescale (F14).**

After each P0: a measured, fair head-to-head vs Flink (same input, no workarounds — see
`docs/benchmarks/ICEBERG_SINK.md` for the standard) before any parity claim.

## 5. Dependency edge — DataFusion 54.0.0 upgrade (own sprint)

Zelox is on **DataFusion 54.0.0 / Arrow 58.3.0** (upgraded 2026-07-06). DataFusion **54.0.0** (released 2026-06-12) brings
engine-core wins that flow straight into Zelox's batch *and* streaming without us writing them — the
"leaner substrate" thesis paying off via the upstream:
- **Repartition coalesce → up to 50% faster on skew** — directly speeds Zelox's keyed
  `StreamExchangeExec` + the distributed shuffle (relevant to F2 parallel streaming).
- **Parquet morsel-driven parallelism → ~2× faster scans on skewed data** — speeds the streaming
  file source + batch reads.
- Sort-merge join (near-unique 20–50×), join-key `DynComparator` (~5% TPC-H), redundant ORDER BY
  pruning, sort/TopK pushdown via statistics, spilling nested-loop joins, `ahash`→`foldhash`.

**Decision:** schedule as a **dedicated upgrade sprint**, not folded into a feature P0. It's a major
version bump (breaking physical-plan/expr APIs + a likely Arrow bump) that ripples through the custom
crates (`zelox-physical-plan`, `zelox-execution` codec, `zelox-iceberg`, `zelox-delta`) and needs the full
regression (105/105 differential scorecard, TPC-H/DS, streaming all-in-one, the EO crash gates) before
shipping. Doing it mid-feature would risk destabilizing the verified baseline. Track breaking changes
from the DataFusion 54 upgrade guide. Sources: datafusion.apache.org/blog (54.0.0),
github.com/apache/datafusion.
