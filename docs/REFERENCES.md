# Vajra REFERENCES — distilled external knowledge base

**Purpose.** Cite this instead of re-fetching/re-deriving. Each entry: source → key facts →
**implication for Vajra** (mapped to [docs/STREAMING_ARCHITECTURE.md](STREAMING_ARCHITECTURE.md)
cells/gaps). Vajra = no-JVM, Arrow + DataFusion engine replacing Spark(batch)+Flink(streaming),
built on the strongest ideas of these systems and fixing their known limits.

**MAINTENANCE RULE (standing):** whenever a doc/blog/paper is fetched and it yields a useful fact
**not already captured here** — and that could matter later — **append it to this file** (source +
key facts + Vajra implication) in the same turn. This file is the single growing knowledge base; do
not let learnings evaporate into one-off context. Refresh entries when a major release lands; note dates.

Last refreshed: 2026-06-21.

---

## 1. Spark 4.1 Real-Time Mode (Structured Streaming)
Source: https://www.databricks.com/blog/introducing-real-time-mode-apache-sparktm-structured-streaming
- **Architecture:** long-lived streaming jobs, **concurrent stage scheduling** (stages run in
  parallel, not sequential micro-batches), **in-memory streaming shuffle** (no disk, less
  coordination), **long-running tasks** (no per-batch start/stop). Removes batch boundaries.
- **Latency:** P99 single-digit ms → ~300ms by transform complexity. Real-world: Network
  International **15ms** P99 payment auth; a global bank <200ms.
- **Trigger:** `trigger(RealTimeTrigger.apply(checkpointInterval))`; default checkpoint interval
  **5 min** (decoupled from latency — commits are periodic, records flow continuously). Exactly-once.
- **Limits:** sources/sinks still expanding; slight overhead; for latency-critical pipelines only.
- **Implication for Vajra:** our realtime mode (F1b, `KafkaSourceExec` realtime path +
  `RealtimeFileSinkExec`) already matches this shape (long-lived pipeline, per-epoch commit decoupled
  from data flush, EO). **Edge to press:** no-JVM + Arrow-native ⇒ lower memory + GC-free tail
  latency; exposed under the same Spark API. Concurrent-stage scheduling + streaming (Flight) shuffle
  is the throughput lever we still owe (matrix: update/distributed, throughput P0).

## 2. Flink stateful stream processing (canonical architecture)
Source: https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/concepts/stateful-stream-processing/
- **State:** keyed state (partitioned by key, local K/V, no txn overhead) vs operator state.
  **Key Groups** = atomic unit of redistribution on rescale (= max parallelism).
- **Checkpoints:** Chandy-Lamport barriers injected at sources, flow with records (never overtake);
  multi-input operators **align** barriers before proceeding. Recovery = restore state + replay
  source from checkpoint offset.
- **Aligned vs unaligned:** aligned buffers until all barriers arrive (EO, adds latency); unaligned
  lets barriers overtake and stores in-flight data in the snapshot (low latency, more I/O).
- **EO end-to-end** = alignment + **replayable source** + **transactional/idempotent sink**.
  Embarrassingly-parallel dataflows are EO even in at-least-once (alignment only matters at joins/shuffles).
- **Savepoints** = manual, persistent checkpoints for upgrades/rescale.
- **Implication for Vajra:** our `StreamBarrierAlignExec` (N→1 Chandy-Lamport) + `state_io` +
  Kafka replay + EO sink mirror this. **Owe:** unaligned-checkpoint option (low-latency EO),
  Key-Group-style rescale for state, savepoints. `FlowEvent::Marker(Checkpoint{epoch})` is the barrier.

## 3. Flink 2.0 disaggregated state (ForSt) — current frontier
Sources: https://flink.apache.org/2025/03/24/apache-flink-2.0.0-a-new-era-of-real-time-data-processing/ ·
VLDB'25 https://www.vldb.org/pvldb/vol18/p4846-mei.pdf
- **State on remote DFS** (primary) + **local disk cache** (secondary); state streamed continuously
  to DFS. **ForSt** state store + unified file system ⇒ faster/lightweight checkpoint, recovery,
  rescale. **Async runtime execution model** hides remote I/O via parallel multi-I/O.
- **Numbers:** heavy-I/O queries +75–120% throughput vs local state (with 1GB cache); **−48%** with
  *no* cache (remote latency); beats Flink 1.20 on Nexmark + prod; lower resource use.
- **Implication for Vajra:** modern state architecture = **decouple state from compute, object-store
  backed + local cache + async access.** Informs F4 (object-store checkpoint/state). Lesson: **cache
  is mandatory** (no-cache remote = −48%). Design state object-store-native with tiered local cache +
  async access (Arrow/Parquet state format) from day one.

## 4. Arrow Flight + Flight Shuffle (DataFusion Ballista 53.0.0, 2026-05)
Sources: https://datafusion.apache.org/blog/output/2026/05/24/datafusion-ballista-53.0.0/ ·
https://datafusion.apache.org/ballista/contributors-guide/architecture.html · Flight bench arxiv 2204.03032
- Ballista: **remote shuffle reads use Arrow Flight directly** + executor-side client cache ⇒ better
  throughput for shuffle-heavy queries. **Sort-based shuffle is default.** `ShuffleWriterExec` /
  `ShuffleReaderExec` emit metrics. Arrow IPC on disk; **zero-copy** Arrow exchange (no ser/de).
  Scheduler/executor APIs on protobuf + gRPC + Arrow IPC + Flight SQL.
- **Implication for Vajra:** distributed/streaming shuffle should be **Arrow Flight + IPC, zero-copy,
  client-cached**, mirroring Ballista's `ShuffleWriter/ReaderExec` split. Foundation for streaming
  Flight shuffle (F2/F3) and for closing the throughput P0 (the exchange path). Reuse Ballista patterns.

## 5. DataFusion engine (foundation)
Source: https://datafusion.apache.org/
- Arrow-native columnar `ExecutionPlan`s; `AggregateMode::{Partial,Final}`; `RowConverter` for
  row-format keys; physical-plan codec for distributed serialization; vectorized operators.
- **Spill (for F5):** DataFusion's grouped-hash `AggregateExec` **spills its hash table to disk**
  under memory pressure via `RuntimeEnv` → `MemoryPool` (bounded, e.g. `FairSpillPool`) +
  `DiskManager` (temp spill files). Available in our pinned 53.1.0 (`RuntimeEnv` already used;
  `cluster.shuffle_spill_dir` config exists). So a streaming agg run under a **bounded memory pool**
  spills automatically — no hand-rolled spill for the final merge.
- **Implication for Vajra:** keep operators Arrow-vectorized, push work into DataFusion kernels.
  **F5 (spillable window state, docs/design/streaming-spillable-state-f5.md):** the operator's OWN
  `pending_rows: Vec<RecordBatch>` is the unbounded-RAM part (DataFusion's pool can't see it) — spill
  it via `state_io` Arrow-IPC ↔ `CheckpointStore` (object-store = ForSt §3) with a local memory cache;
  AND run the final merge under a bounded `MemoryPool` so DataFusion spills the hash table. Two
  complementary spills = state ≫ RAM, like Flink RocksDB/ForSt.
- **MEASURED 2026-06-23 (F5.1+F5.2 done):** both spills wired into `WindowAccumExec` (F5.1 accumulation
  spill, F5.2 lazy `SpillSourceExec` input + bounded-pool resumable+incremental finalize). Validated
  out==N EXACT at N=200k/500k/1M/5M, both 4 MiB (in-RAM) and 256 KB (spilling) budgets; spills scale
  23→56→120→602; 5M distinct keys under a 256 KB per-partition budget, no OOM/error. **LESSON for
  measuring "bounded state":** process RSS is a BAD proxy when (a) the sink is O(N) (parquet dump of N
  rows dominates), and (b) runs are sub-second so jemalloc retains freed pages (RSS = high-water, not
  working set). To prove bounded peak you need a **bounded/streaming sink + sustained stream + the
  operator's `MemoryPool` reservation/metrics**, not process RSS. (Informs the F5.4 gate design.)

---

## 6. Beating Flink on streaming throughput — the columnar edge (2026-06-22)
Sources: Flink FLIP-27 source (https://nightlies.apache.org/flink/flink-docs-master/docs/internals/sources/) ·
Arroyo "10x faster sliding windows / beats Flink" (https://www.arroyo.dev/) ·
Arroyo "Fast columnar JSON decoding with arrow-rs" (https://www.arroyo.dev/blog/fast-arrow-json-decoding/) ·
arrow-rs raw JSON reader PR #3479.
- **Flink's structural weakness:** Kafka SplitReader deserializes **per-record** into JVM objects
  (object churn + GC). Row-at-a-time. This is what a columnar engine beats.
- **Arroyo (Rust+Arrow, our exact stack) beats Flink 5×+** via columnar/vectorized execution +
  worker-memory state structures (not generic backends). **Proof our architecture CAN beat Flink —
  only if we stay columnar end-to-end and never fall back to row-at-a-time.**
- **arrow-json `Decoder`** (`decode(&[u8])` + `flush() -> Option<RecordBatch>`): simdjson-style
  two-pass (tape build → columnar construct), SIMD string-end/UTF-8, decodes straight into Arrow
  builders. **2.3× faster than Java per-record**; great on large/nested records; weaker on
  sparse-null/enum-like. Type coercion (string↔number), nulls via validity bitmap, `validate_row()`
  filters bad rows pre-construction.
- **Implication for Vajra:** source consume loop should be **alloc-free** (no per-msg String/Vec)
  to realize the no-GC edge.
- **MEASURED CORRECTION 2026-06-22 (don't repeat this mistake):** the arrow-json "2.3×" is
  arrow-json-**Rust vs Java/Jackson** (i.e. vs Flink) — NOT vs Rust `serde_json`. We tried an
  arrow-json columnar fast path in `from_json`: it was **~parity with our serde_json** (0.418 vs
  0.410 M rows/s nested; wash on tiny records), because our parse is already Rust (no JVM) and the
  NDJSON-rebuild offsets the decode gain. **Reverted** (no measured benefit over our path). The
  parse edge over Flink is simply **being Rust, not Java** — already realized. arrow-json would only
  help if we could feed it zero-copy (no NDJSON rebuild); not worth it now. Bottleneck for the
  streaming workload was the **read path** (fixed: builders + rdkafka tuning, 2.1×), not parse.

## Cross-cutting design principles for Vajra (synthesized)
1. **Unified API, two execution modes:** Spark API over batch + micro-batch + realtime
   (Spark-4.1-style long-lived/concurrent-stage). One engine, no JVM.
2. **Changelog-native:** `FlowEvent.retracted` = Flink retract stream ⇒ correctness by convergence
   (Materialize/RisingWave style), beating Spark-append/Flink-SQL drop on out-of-order.
3. **State = object-store-native + tiered local cache + async** (Flink 2.0 ForSt lesson; cache mandatory).
4. **Shuffle = Arrow Flight, zero-copy, client-cached** (Ballista lesson) — distributed batch + streaming.
5. **EO = barriers + replayable source + transactional sink**; offer aligned (EO) + unaligned
   (low-latency) checkpoints (Flink lesson).
6. **Prove it like-for-like** vs official Spark + Flink on the same hardware/data; report honestly.

---
## Backlog of sources to mine (when relevant; append findings above)
- Flink 2.0.0 release blog (full) · Nexmark streaming benchmark methodology
- Arrow Flight SQL spec · Arrow IPC format
- RisingWave / Materialize architecture (changelog, differential dataflow) · Arroyo
- DataFusion physical optimizer + codec internals · Comet Arrow Flight shuffle (issue #3596)
- FAANG streaming production blogs (Netflix Keystone, Uber, LinkedIn, Pinterest) on latency/state/EO
