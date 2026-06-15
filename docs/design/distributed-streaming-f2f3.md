# F2/F3 — Distributed stateful streaming: one Vajra engine for batch *and* realtime, on power with Flink

**Goal.** Extend Vajra's existing *distributed batch* engine (driver/worker actors, Arrow-Flight
shuffle, staged job graph) into a **distributed stateful streaming** engine — so the **same** engine,
the **same** Spark API, and the **same** operators run **batch, micro-batch, and realtime
(continuous)** workloads, exactly-once, scaled across nodes. Not a second runtime bolted on: batch is
a bounded stream; realtime is the same dataflow with periodic epoch barriers. This is the unified
"Spark + Flink in one, no JVM" thesis, made distributed.

Earn every claim with a fair, measured head-to-head — re-derived on the Arrow/DataFusion/Rust
substrate, not ported.

## 0. Grounding (read first, per the prod-grade bar)

- **Flink** — *Asynchronous Barrier Snapshotting (Chandy-Lamport)*: the JobManager **checkpoint
  coordinator** periodically injects barriers at sources; barriers flow **in-band, never overtaking
  records**; **multi-input operators align** (wait for the barrier on every input) before snapshotting;
  **single-input ops need no alignment**; sinks **ack** the coordinator; once **all** tasks ack, the
  checkpoint is complete → **2PC commit** (pre-commit on barrier, commit on completion); recovery
  resets sources to the snapshotted offset + idempotent commit.
- **RisingWave** — the **meta service** injects barriers (= epochs) that flow through the actor graph;
  a **merger aligns barriers** at actors with multiple upstreams; cross-node movement goes through an
  **exchange service over RPC**; on checkpoint, each compute node flushes its dirty state to a
  **single SST object** in shared storage; "every state flushed is consistent with a barrier at the
  source."
- **Arroyo** — distributed streaming **on Arrow + DataFusion** (our substrate): streaming data is
  **Arrow batches**; DataFusion physical operators/expressions are **reused** for streaming;
  control/checkpoint messages travel alongside the Arrow data.
- **Arrow Flight `DoExchange`** — a **bidirectional** stream in one RPC (client and server stream
  simultaneously); `FlightData` carries `data_header` (Arrow IPC), `data_body` (batch), and
  `app_metadata` (control) — ideal for a **pipelined, marker-aware** cross-node shuffle.

**What Vajra already has that maps 1:1:**
- Flow-event model: `FlowEvent::Marker(Checkpoint{epoch}|Watermark|…)` flows in-band with
  `FlowEvent::Data` — *these markers ARE Chandy-Lamport barriers* (Flink/RisingWave invariant).
- `StreamExchangeExec` (1→N, **broadcasts markers**, hash-routes data) — the in-node exchange.
- `StreamBarrierAlignExec` (N→1, **aligns** a broadcast barrier, blocks post-barrier data per input
  until aligned) — the RisingWave "merger" / Flink alignment, **built + unit-tested (2026-06-15)**.
- `CheckpointStore` — object-store **single atomic blob** per snapshot (RisingWave's single-SST idea),
  S3-verified (F4).
- Distributed batch substrate: driver/worker actors, **Arrow-Flight** `ShuffleWriteExec`/
  `ShuffleReadExec`, staged `JobGraph` — to be given a streaming (pipelined) mode.
- Single-node realtime exactly-once (F1b): source emits epoch barriers + pre-commits offsets; sink
  commits files + a **single atomic** `realtime/committed={epoch,offsets}`. F2/F3 distributes this.

## 1. Architecture — five components

1. **Distributed barrier coordinator** (driver; = Flink JobManager / RisingWave meta). Owns the epoch
   clock. Triggers epoch *e* (sources begin emitting `Checkpoint{e}`), tracks **acks** from every leaf
   task, and when **all** ack, declares epoch *e* **globally complete** → writes ONE atomic global
   commit record (offsets + per-operator state pointers) and advances `last_committed`. Bounded
   in-flight epochs (backpressure); idempotent acks; abort+retry on task failure. **← increment 1.**
2. **Streaming Flight exchange** (pipelined cross-node shuffle). The streaming counterpart of the
   batch `ShuffleWrite/Read`: instead of materialize-then-read, a **continuous** `DoExchange` of
   flow-event batches — **data hash-routed** per partition, **markers broadcast** to all downstream
   partitions (the cross-node `StreamExchangeExec`). `app_metadata` carries epoch/seqno for ordering.
3. **Barrier alignment at receivers** — `StreamBarrierAlignExec` at every multi-upstream stateful
   operator instance: collect `Checkpoint{e}` from all N upstreams (block post-barrier data) → snapshot
   → forward one barrier. **DONE (primitive).**
4. **Distributed state snapshot** — on the aligned barrier, each operator **instance** writes its
   state as a **single atomic blob** keyed `state/<op>/<partition>/<epoch>` (F4 mechanism). The
   coordinator's global record references these (RisingWave single-SST-per-node generalized).
5. **Recovery** — coordinator finds the last **globally-complete** epoch; each instance restores its
   `(op,partition,epoch)` blob; sources seek to the committed offset. (Single-node version DONE in
   F1b/F4; distribute the bookkeeping.)

## 2. One engine: how batch / micro-batch / realtime share this

- **Batch** = a bounded stream. Sources emit `EndOfData` (a terminal barrier); no periodic epochs
  needed; the existing staged Flight shuffle already works. State snapshot at `EndOfData` = the single
  final checkpoint. **No new code on the batch path** — it is the e=∞ degenerate case.
- **Micro-batch** (`processingTime`) = bounded stream per trigger (already: each trigger drains to
  `EndOfData`, commits). Distributed via the same staged shuffle.
- **Realtime** (`Trigger.Continuous`) = the same dataflow, but the coordinator injects **periodic**
  `Checkpoint{e}` barriers and the shuffle is **pipelined** (component 2), aligned (component 3),
  snapshotted (component 4). Stateless single-input ⇒ no alignment (already EO single-node, F1b).
- **Same operators** (`WindowAccumExec`, `StreamJoinExec`, filter/project), **same** `CheckpointStore`,
  **same** Spark API. That is the "one engine, both capabilities" property — on power with Flink, but
  no JVM, Arrow-columnar, flat-tail latency.

## 3. Why this beats Flink (targets, each to be measured — no claim without a head-to-head)

- **Latency:** no JVM/GC ⇒ flat tail (p99≈p50, measured sub-ms single-node F1c); barriers are
  vectorized Arrow batches; **stateless aligns nothing** (no alignment tax).
- **Memory:** Arrow columnar + bounded exchange channels ⇒ measured 7–16× less RAM than Flink's JVMs.
- **Throughput:** vectorized DataFusion operators + Arrow-Flight zero-copy shuffle.
- **Reliability:** object-store single-atomic-commit (no torn checkpoints); replayable sources.
- **Ops:** one binary, one engine to secure/tune for both batch and streaming.

## 4. Build plan — gated increments (nothing claimed without a test)

- **F3-a (this increment): distributed barrier coordinator** — `EpochCoordinator` (trigger / ack /
  global-complete → atomic commit / abort / recover), unit-tested for all-ack completion, partial
  acks, idempotency, monotonic commit, abort. The control brain, testable without a worker cluster.
- **F3-b: streaming Flight exchange** — pipelined `DoExchange` flow-event shuffle (data hash-routed,
  markers broadcast); integration test across two in-process workers.
- **F3-c: distributed state snapshot + recovery** — per-instance `(op,partition,epoch)` blobs, global
  record, restore; SIGKILL gate across a 2-worker job.
- **F3-d: multi-node head-to-head vs Flink** — same Kafka→stateful→sink pipeline on K8s; latency,
  throughput, memory, EO-across-failure. Then — and only then — claim parity/superiority.

## 5. Honest gaps (today) — corrected 2026-06-15 after reading the scheduler

**Important correction (with code evidence).** The "concurrent producer+consumer stage scheduling"
that was assumed missing **already exists**. The distributed engine is **fully pipelined** (Flink
streaming-style), not Ballista blocking-style:
- `job_graph/planner.rs` creates **every** stage with `OutputMode::Pipelined`; `OutputMode::Blocking`
  is defined but **never constructed**.
- `job_scheduler/topology.rs::try_new` groups all `Pipelined`-connected stages into **one pipelined
  region** (connected-components over the pipelined adjacency); the cross-region `Succeeded`
  dependency gate in `schedule_task_regions` therefore **never blocks** (there are no blocking edges).
- So all stages are **co-scheduled and run concurrently**; the pipelined `ShuffleWriteExec`→Flight→
  `ShuffleReadExec` already streams per-batch with bounded-channel backpressure; a streaming region
  never reaches `Succeeded`, so the job runs until stopped (the `Draining`/cleanup paths simply never
  fire) — which is exactly the desired streaming lifecycle. **F3-b control plane = already done.**

**The actual blockers for distributed *stateful* streaming (re-scoped):**
1. **Codec serialization of streaming operators.** ✅ **DONE (2026-06-15), round-trip tested.** Every
   streaming operator now serializes through `RemoteExecutionCodec` (driver↔worker): `KafkaSourceExec`
   (incl. realtime EO config) · `StreamExchangeExec` · `StreamCoalesceExec` · `StreamBarrierAlignExec` ·
   `WatermarkExec` · `StreamDeduplicateExec` · `WindowAccumExec` (group-by + aggregates via a template
   `AggregateExec`, reusing DataFusion's UDAF serialization) · `StreamJoinExec` (equi-keys + join type +
   interval bounds + filter) · `FlowEventToDataExec` · `RealtimeFileSinkExec` — plus the pre-existing
   `filter`/`limit`/`collector`/`source-adapter`/`rate`/`socket`. **A full distributed streaming plan —
   stateless, event-time, *and* stateful (window-agg / stream-join) — can now be shipped to workers.**
   8 codec round-trip tests green. THE blocker is cleared.
2. **Insert `StreamBarrierAlignExec`** at distributed shuffle-receive points for stateful operators
   (the planner wires the in-node exchange today; the aligned merge needs wiring for the cross-node case).
3. **Distributed checkpoint commit:** wire `EpochCoordinator` into the driver + per-instance state
   snapshot `state/<op>/<partition>/<epoch>` (the single-node F1b commit generalized).

**Already done / pre-existing:** concurrent pipelined scheduling (pre-existing); marker-aware shuffle
broadcast (F3-b data plane, this session); `StreamBarrierAlignExec` (primitive + wired into the
parallel windowed-agg merge); `EpochCoordinator` (brain). Single-node realtime stateless EO is done +
measured (F1b). Multi-node continuous stateful is **not** end-to-end yet.

### Harness finding (2026-06-15) — the real gap is execution-model integration, not codec

Built the gate harness (`scripts/dist_streaming_smoke.py`): a Vajra server in **local-cluster mode**
(driver + 2 in-process workers, `--mode local-cluster --workers 2`) exercised through real Spark
Connect. Results:
- ✅ **Distributed batch is solid** — the full all-in-one batch suite (5/5) + a distributed
  read→compute→**parquet write** (1000 rows) run correctly across the 2-worker cluster (validates the
  `ClusterJobRunner` + Arrow-Flight shuffle + file write).
- ⚠️ **Distributed streaming produces no output** — both stateless (`rate→filter→parquet`,
  availableNow) and keyed-windowed streaming writes yield zero rows and the streaming query goes
  **inactive immediately**. (The `memory` sink additionally isn't codec-serializable — `MemoryBufferScan`
  — but that's a dev sink; the parquet path fails too.)

**✅ RESOLVED 2026-06-16 — distributed streaming works 4/4 (local-cluster, 2 workers).** It was a
**codec gap, not an execution-model gap**. A full codec-coverage audit of every custom Exec found the
missing nodes: the micro-batch sink wrappers (`StreamingSinkCommitExec`, `EmptySinkAdapterExec`,
`PartitionSelectExec`, `ParallelStreamSinkExec`) **and** the streaming **`FileSourceExec`** +
`ExplicitRepartitionExec`. `FileSourceExec` being unserializable is why distributed *file-source*
streaming silently produced 0 rows. All are now codec'd + round-trip tested. `scripts/dist_streaming_smoke.py`
passes **4/4** on both a 2-worker cluster and local single-node: `batch.write=1000`, `stream.rate=20000`,
`stream.file=1000`, **`stream.windowed_file=97` (keyed event-time window agg over a file source, exactly
matching real Spark 3.5.3)**. The earlier "windowed reads 0" was a *false alarm* — a single
availableNow batch + a 2s watermark closes no window, and real Spark produces 0 there too.

*(Historical note — the original failing symptom:)* a streaming write job failed at *submission* to the
cluster (`unsupported physical plan node: StreamingSinkCommitExec`) and the query went inactive. Fixed by codec'ing them + a
real `StreamBarrierAlignExec` bug (it rebuilt aligned marker batches with `new_null_array`, which
fails on non-nullable columns like a windowed `COUNT`; now it stashes + forwards the upstream marker
batch with the source encoder's placeholders). **Result: `dist_streaming_smoke.py` 1/3 → 2/3 —
distributed *stateless* streaming (`rate→filter→parquet`, 20000 rows) works end-to-end across 2
workers.**

**Remaining (narrowed): distributed windowed aggregation reads `numInputRows=0`.** Both keyed
(`groupBy(window,k)`) **and** non-keyed (`groupBy(window)`, parallelism=1, no exchange) windowed aggs
read zero input rows under `availableNow` in cluster mode and terminate in ~40 ms, while the
single-stage stateless path on the *same* rate source reads 20000. So it is **not** the parallel
exchange / multi-stage path — it is the **windowed-aggregation × availableNow × distributed
bounded-execution** interaction (the pipeline-breaking agg appears to hit the bounded-source
termination before the source produces). Next debugging target; gate with `dist_streaming_smoke.py`
`stream.windowed`.

**Then:** distributed `EpochCoordinator` wiring + per-instance state snapshot (F3-c) for cross-worker
continuous EO; `memory`-sink codec (dev convenience); multi-node Flink head-to-head (F3-d).
