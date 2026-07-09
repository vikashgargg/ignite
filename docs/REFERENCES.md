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
- **Per-partition watermark + `WatermarkStrategy.withIdleness(Duration)`:** each source split/partition
  tracks its own watermark; the operator watermark = MIN across splits (a window closes only when ALL
  active partitions pass it — prevents premature close on cross-partition event-time skew). **Idleness:**
  a partition with no events for the timeout is EXCLUDED from the MIN so the watermark never STALLS on
  an idle/absent partition (liveness vs completeness tradeoff). **Vajra impl (WatermarkExec):** per-
  partition (max_et,last_seen); watermark = MIN over partitions active within `idle_timeout`; startup
  grace withholds until all N seen OR grace elapses; a periodic tick excludes newly-idle partitions even
  with no input. This is the safety that makes per-partition non-blocking (a withhold-until-all-N
  version with NO idleness HUNG for 3h). See docs/design/streaming-per-partition-watermark.md.
- **§2b Rescalable keyed state — key-groups** (Stefan Richter, "A Deep Dive into Rescalable State in
  Apache Flink", 2017; FLIP-8): the key space is pre-partitioned into a fixed **G key-groups**
  (`maxParallelism`), `kg = hash(key) % G`. A subtask owns a **contiguous range** of key-groups; rescale
  to M′ just re-assigns ranges. State is written **in key-group order** with a kg→offset index, so a
  rescaled subtask reads the **byte-range** for its key-groups directly — no per-key deserialization.
  **Vajra impl (`state_io`, rescale steps 1–3a):** `key_group`/`instance_key_group_range`/
  `key_group_owner` (exchange routes by kg→owner, matching state ownership); manifest records each
  chunk's kg `[lo,hi)` coverage; `restore_keyed_range` gathers a new instance's rows by selecting chunks
  via manifest range (`chunks_for_range`, a lookup) + row-filter. **Beats Flink:** chunk selection is a
  manifest lookup over IMMUTABLE Arrow chunks (KG-aligned ⇒ zero rewrite) vs re-serializing RocksDB
  state. Proven: `rescale_redistributes_keyed_state_exactly`. See streaming-rescale-from-checkpoint.md.
- **§2d Parallel source + streaming shuffle (cross-system grounding for realtime multi-instance):**
  (1) **Flink FLIP-27** — one SourceReader per split (Kafka partition); SplitEnumerator assigns; each
  reader event-time-ordered → per-split watermark, op = MIN. (2) **Spark Structured Streaming / 4.1
  RT-mode** — `KafkaSourceRDD` one task per TopicPartition; RT-mode adds in-memory streaming shuffle +
  concurrent stage scheduling (decouple stages so they pipeline, not block). (3) **Arrow Flight shuffle /
  Ballista** — zero-copy columnar exchange between stages (DoGet/DoPut), the disaggregated-shuffle model
  for distributed (EKS). (4) **StreamNative/Pulsar + Kafka** — partitioned topics; consumers scale to
  partition count; ordering per partition. (5) **FAANG** (LinkedIn Samza/Brooklin, Uber, Netflix Mantis)
  — converge on per-partition parallel ingest + per-partition watermark/state. **Vajra synthesis (Phase
  B, docs/design/streaming-realtime-multi-instance.md):** realtime source → N readers (one per Kafka
  partition, reuse the bounded path) + per-instance epoch staging + atomic union commit (generalize the
  single-coordinator realtime EO); each reader single-partition-ordered ⇒ monotone watermark (removes the
  per-partition-WM workaround) + `StreamExchangeExec` keyed MIN-merge + `StreamBarrierAlignExec` N→1
  align; **EKS distributed shuffle = Arrow Flight** (zero-copy). This is the read+`from_json`
  parallelization the Phase A profile demands AND the watermark-correctness fix, in one change.
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
- **Recovery/cost numbers (fetched 2026-07-01):** up to **94% less checkpoint duration, 49× faster
  recovery after failure/rescale, 50% cost savings** vs Flink 1.x. **MEMORY INSIGHT:** Flink bounds RAM
  NOT via GC but by keeping **state off-heap on DFS/local-disk** (RAM = bounded cache, independent of
  state size) + **credit-based network backpressure** tightly bounding in-flight buffers. ⇒ a no-GC
  engine does NOT automatically win on RSS; bounded memory is an ARCHITECTURAL discipline (bounded
  in-flight buffers + spilled/tiered state + allocator), which Vajra must engineer explicitly — our
  no-JVM edge is for LATENCY/predictability, orthogonal to RSS. (Explains why T1–T7a CPU fixes left
  Vajra's bounded-path RSS at 1.12× Flink.)

### 3c. Spark 4.1 Structured Streaming Real-Time Mode — fetched 2026-07-01
Source: https://www.databricks.com/blog/introducing-real-time-mode-apache-sparktm-structured-streaming
- **Execution:** long-lived jobs that **schedule stages concurrently** + an **in-memory streaming
  shuffle** between tasks (no micro-batch coordination) — i.e. exactly Vajra's streaming-exchange model.
- **Latency:** p99 **a few ms → ~300ms** (transform-dependent); prod users report 15ms / <200ms e2e.
- **Tradeoff (KEY):** Databricks explicitly says RT-mode adds **"slight system overhead"** (higher
  memory) and recommend it only for latency-critical pipelines ⇒ even Spark **trades memory for latency**
  in this design. Validates Vajra's concurrent-stage + in-memory-shuffle direction; latency (p99) is the
  headline metric to measure, and memory overhead is expected — must be bounded, not eliminated.
- **Vajra implication:** measure **p99 latency** head-to-head (the RT-mode/Flink axis we have NOT
  measured), and treat memory as a bounded-buffer discipline, not a free win.

### 3d. Flink 2.3 release (current; fetched 2026-07-01 — cite this, don't re-fetch)
Source: https://nightlies.apache.org/flink/flink-docs-release-2.3/release-notes/flink-2.3/
- **Watermark alignment redesign (FLINK-37399):** prior watermark alignment *unintentionally throttled
  backlog processing*; 2.3 adds a configurable **alignment buffer (default 3 update-intervals)** to delay
  alignment ⇒ faster historical/backlog reprocessing, safety kept. → DIRECTLY relevant to our
  per-partition-watermark + idleness work: alignment must NOT throttle catch-up. Mirror this buffer idea.
- **Adaptive partition selection (FLINK-31655):** load-aware `StreamPartitioner` routes to **least-loaded
  channels** (vs round-robin) → **~3× throughput under skew** / slow downstream (Redis/HBase/LLM). →
  relevant to our exchange for NON-keyed/rebalance paths (keyed shuffle still must honor key→owner; but
  skew-aware buffering/backpressure is a lever).
- **Checkpoint during recovery (FLINK-35761):** can checkpoint *while recovering* from unaligned ckpt
  (was blocked until channel state drained) → preserves work across restart cascades. → recovery-time bar.
- **Mini-batch agg silent data-loss fix (FLINK-35661):** retraction-only bundles dropped remaining keys.
  → reminder: even Flink ships keyed-agg correctness bugs at scale; our correctness gate must stay adversarial.
- **Changelog PTFs `FROM_CHANGELOG`/`TO_CHANGELOG` (FLINK-39258)** + **`ON CONFLICT` upsert
  (DO NOTHING/ERROR/DEDUPLICATE) + watermark-based changelog compaction (FLINK-38926)** → maps to our
  FlowEvent retract/changelog + update-mode; adopt the ON CONFLICT vocabulary + wm-compaction for upserts.
- **Native S3 FS `flink-s3-fs-native` (FLINK-38592):** AWS SDK v2, **non-blocking I/O**, IRSA auth,
  exactly-once via `RecoverableWriter`. → blueprint for our object-store checkpoint/sink (F4): async SDK,
  recoverable writer for EO, no Hadoop dep.
- **Adaptive scheduler rescale history (FLINK-38333):** records rescale events (parallelism/slots/state)
  via REST. → our rescale work should expose similar observability.
- **Net for Vajra:** Flink's 2.x momentum is **disaggregated state + recovery speed + skew/backlog
  handling + changelog SQL + async object-store I/O + observability** — NOT raw single-node throughput.
  The replacement bar is these operational axes. Memory is bounded by off-heap state + credit backpressure.

### 3b. Incremental + async checkpointing (Flink large-state) — fetched 2026-06-24
Sources: https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/ops/state/large_state_tuning/ ·
.../docs/ops/state/checkpoints/
- **Incremental checkpoint = record only the CHANGES since the last completed checkpoint**, not a
  full self-contained backup. For RocksDB this = the **SST files** created since the last checkpoint
  are uploaded; **unchanged SST files are REFERENCED (shared), not re-uploaded**. Local hard-links ⇒
  no extra disk for active files (RocksDB + local-recovery dirs must be one physical device).
- **Checkpoint dir layout:** `shared/` (files referenced by ≥1 checkpoint — the SST blobs),
  `exclusive/chk-N/` (one checkpoint only), `taskowned/` (never dropped by the coordinator). A new
  checkpoint **reuses files from the previous** via the shared dir.
- **SharedStateRegistry + refcount:** a shared file is deleted only when **no retained checkpoint
  references it**; on subsumption (chk-N+1 done ⇒ chk-N cleaned) only files no longer referenced are
  removed. Prevents premature deletion.
- **Async:** the native checkpoint (hard-link of SSTs) is fast/local; the **upload to DFS is async**
  and does not block the dataflow — barriers keep flowing.
- **Implication for Vajra (THE unlock):** our F5 **spill chunks are already immutable, numbered
  Arrow-IPC blobs** = a perfect SST-analog. So incremental checkpointing is nearly free: a checkpoint
  = a **manifest** listing {referenced chunk-ids + a small in-RAM residual blob + meta}; spilled
  chunks (the bulk) are **referenced, not re-uploaded** (they were written out-of-band during spill,
  off the barrier critical path). Refcount chunks by referencing-epoch; GC a chunk only when no
  retained epoch references it (= SharedStateRegistry). Checkpoint cost = O(residual + new chunks),
  not O(total state). **Spill and incremental-checkpoint become ONE mechanism + ONE Arrow format —
  vs Flink bolting ForSt onto a RocksDB lineage.** Requires changing the F5 chunk lifecycle from
  consumed-on-finalize to **immutable + refcounted**. Design: docs/design/streaming-incremental-checkpoint.md.

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
- **`AggregateMode::PartialReduce` (53.1, used by F5.3 compaction):** input = intermediate accumulator
  state, output = intermediate accumulator state (the tree-reduce merge step: combine many partials
  into fewer partials WITHOUT finalizing; input/output schema both = the Partial-mode schema). This is
  the correct primitive to compact accumulated streaming partial state (collapse duplicate (window,key)
  partials to one accumulator/key) — no hand-rolled accumulator `merge`. (`Final` would finalize and
  lose mergeability for non-summable aggs like AVG.)
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
- **BOUNDED-PATH PROFILE 2026-06-30 (the EKS gap, re-localized):** the EKS throughput harness is
  `availableNow` (bounded, ALREADY 16-reader parallel). `VAJRA_WM_PROF` shows the window STILL STARVED
  (input_wait ~75%/instance, finalize ~20%) ⇒ with read + from_json already optimized (above), the
  remaining ~2.4× gap is the **exchange** (`StreamExchangeExec` per-batch Arrow-IPC re-encode + tokio
  channel copies) or the **`availableNow` micro-batch loop overhead** (re-plan + parquet-commit +
  checkpoint per `maxOffsetsPerTrigger` batch, ~25× at 100M — vs Flink's ONE continuous pipeline).
  NEXT = split exchange vs micro-batch (bigger maxOffsetsPerTrigger A/B + an exchange-side timer).
  Multi-instance/Flight = continuous/multi-node, not this gap. See throughput-robustness-review.md.
- **RisingWave 3.0 + Arroyo data-plane (2026-07-08, added on request).** Sources:
  risingwavelabs.github.io/risingwave/design/streaming-overview · docs.risingwave.com/get-started/architecture ·
  arroyo.dev + github.com/ArroyoSystems/arroyo · goldsky streamling.
  - **RisingWave** = actor-model streaming; data plane is the **Stream Chunk = columnar Data Chunk +
    visibility array + an ops column** (Insert/Delete/UpdateInsert/UpdateDelete — the built-in changelog,
    cf. our WindowOutputMode::Update retract). Local actor→actor via channels; cross-node via an
    **exchange service**. State = shared S3 object store; source connector **offsets persisted in
    checkpoints → exactly-once** (== our design). Vectorized batch-at-a-time, never per-row.
  - **Arroyo 0.10** rebuilt on **Arrow + DataFusion** (interpreted columnar exec, SIMD/cache) = +3×; its
    **Shuffle Edge** = key-hash partitioning with **connection pooling + BATCHING to amortize per-batch
    overhead** (directly relevant to our StreamExchange per-batch cost). Kafka msgs are batched into Arrow
    RecordBatches on the source side. Roar/Streamling: Kafka→RecordBatch→**Arrow Flight zero-copy**.
  - **CONCLUSION for Vajra (measured, don't re-chase parse):** our source consume loop is ALREADY
    alloc-free + columnar (kafka/reader.rs:877 appends borrowed bytes into Arrow builders, interned topic
    idx, batched to RecordBatch; read path already tuned 2.1×) and parse is already Rust-fast (tape, ~15%
    of pipeline). So the columnar-source box is CHECKED. The two prod-grade levers that remain — the ones a
    columnar streaming engine actually wins on — are: **(A) CONTINUOUS dataflow vs Spark `availableNow`
    micro-batch re-plan/commit/checkpoint-per-trigger tax (~25× at 100M — RisingWave/Flink/Arroyo run ONE
    long-lived pipeline; this is THE structural difference), and (B) exchange = BATCH + pool + zero-copy
    Arrow-Flight (Arroyo Shuffle-Edge / Ballista), no per-batch IPC re-encode.** Focus here, not parse.
- **VERSION-UPGRADE perf targets (separate upgrade repo; verify in release notes before hand-tuning):**
  (1) **arrow-rs `Utf8View`/`BinaryView` (StringView)** — fewer allocs/copies on string + JSON + shuffle
  paths (big for the value column + exchange re-encode). (2) **DataFusion grouped-`AggregateExec`** perf
  (hash, blocked emission, spill) — helps window finalize at scale. (3) **Arrow Flight** zero-copy /
  client-cache improvements — for the multi-node shuffle (§4). Bumping DataFusion/Arrow/Flight may close
  part of the gap "for free"; coordinate with the version-upgrade repo. **Add concrete release-note facts
  here as they're confirmed** (don't assert versions un-verified).

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
- Arrow Flight SQL spec · Arrow IPC format
- RisingWave / Materialize architecture (changelog, differential dataflow) · Arroyo
- DataFusion physical optimizer + codec internals · Comet Arrow Flight shuffle (issue #3596)
- FAANG streaming production blogs (Netflix Keystone, Uber, LinkedIn, Pinterest) on latency/state/EO

## 7. OFFICIAL benchmarks — the credible bar (fetched 2026-07-01; use these, don't invent)
Sources: Nexmark https://flock-lab.github.io/flock/flink_nexmark.html + github.com/nexmark/nexmark ·
TPC-DS https://www.databricks.com/blog/2021/11/02/databricks-sets-official-data-warehousing-performance-record.html · tpc.org TPC-DS spec · github.com/databricks/tpcds-kit

### Streaming = NEXMARK (Flink's official streaming benchmark)
- **Auction data model:** Person (bidders/sellers) · Auction (items) · Bid (bids). One generator stream
  with a fixed ratio (default ~ Bid-heavy). Workloads **100M / 200M events**.
- **Queries q0–q13 (+ variants to q22):** q0 passthrough (overhead) · q1 currency map · q2 selection/
  filter · q3 local-item (filter+join Person×Auction) · q4 avg-price-per-category (agg) · q5 hot-items
  (**sliding window**) · q6 avg-sell-by-seller (windowed) · q7 highest-bid (windowed) · q8 new-users
  (windowed join) · q9 winning-bids (**join**) · q10 log-to-fs (windowed sink) · q11 user-sessions
  (**session window**) · q12 **processing-time window** · q13 bounded-side-input join. ⇒ covers filter/
  agg/sliding+session+proc-time windows/joins = the full streaming surface (our windowed-COUNT ≈ q5/q6).
- **Metric = `Cores × Time(s)` per query + events/s** (NOT just wall). Full suite ~50 min. Baseline =
  the `nexmark-flink` package (official Flink runner) on a multi-node cluster.
- **Vajra implication:** for a credible "replaces Flink (streaming)" claim, run NEXMARK (Spark-API
  equivalents of q0–q13) vs `nexmark-flink`, report Cores×Time + ev/s. Our current windowed-COUNT is the
  q5/q6 slice only — full Nexmark is the gold standard to add.

### Batch = TPC-DS (Spark's official batch benchmark)
- **99 queries** (Databricks ran 104 = 99 + 4 approved variants + s_max full-scan), varying complexity
  (simple agg → pattern mining). Scale factors SF1(1GB)/SF1000(1TB)/SF100000(100TB).
- **Official metric `QphDS`** = combined: (1) data load, (2) **power test** (one sequential pass of all
  queries = single-user response time), (3) **throughput test** (concurrent query streams), (4) data
  maintenance (insert/delete). **Databricks SQL record: 32,941,245 QphDS @ 100TB** (2.2× prior Alibaba
  14.86M; 10% lower cost). Tooling: `databricks/tpcds-kit`.
- **Vajra implication:** for "replaces Spark (batch)", run the **TPC-DS 99 power test** (sequential
  per-query response time + total wall) at a fixed SF (SF100/SF1000) vs Spark same node/data, + peak mem.
  We have `scripts/tpcds_score.py`. TPC-H (SF-1/SF-100, `tpch_distributed.py`) = simpler secondary.

### Net: tri-engine matrix anchors → streaming on **Nexmark (Cores×Time + ev/s)**, batch on **TPC-DS-99
power test (response time + wall + mem)**. These are the one-time Spark/Flink reference numbers to beat.

## 8. RisingWave 3.0 + Polars — mine the best of BOTH (for a next-era engine)

Vajra must beat Spark (batch) AND Flink (streaming) on every axis. Two more engines carry ideas worth
stealing; the goal is the **union of the best**, built on our Arrow/DataFusion no-JVM core.

### RisingWave 3.0 (streaming database — the streaming SQL frontier)
- **Decoupled compute/storage state (Hummock):** an LSM state store on S3/object-store; compute nodes are
  near-stateless, state lives on cheap durable storage. ⇒ **fast elastic rescale** (state isn't pinned to
  a node) + cheap large state. This is the SAME direction as **Flink 2.0 ForSt (§3)** and as **Vajra's F5
  spillable Arrow state + inc-ckpt** (immutable Arrow chunks on object store, O(delta) checkpoints). Vajra
  already has the substrate; RisingWave validates the architecture (compute/state separation is the frontier).
- **Materialized views = incremental computation:** a streaming query is a MV kept fresh by *incremental*
  updates (differential-dataflow-style deltas), not re-execution. Maps to Vajra's **changelog/retraction
  output mode** (`WindowOutputMode::Update`, emit retract+insert) — extend it to full MV maintenance.
- **Arrangements (shared indexed state):** joins/aggs share indexed state (one arrangement, many readers) =
  less memory. Vajra analog: shared keyed state across operators on the same key-group (F5 chunks).
- **Barrier-based exactly-once** (Chandy-Lamport, like Flink §2) + **per-partition watermarks** — our
  T-EO-1/T-EO-3/T-EO-3.5 (per-instance FLIP-27 read + union commit + withIdleness merge) is exactly this
  model. **Honest note:** RW is Postgres-wire + MV-centric; Vajra is Spark-API + DataFrame/SQL — same
  streaming core ideas, different surface. (3.0 is the current line; cite durable architecture facts, not
  version-specific perf claims we haven't measured.)

### Polars (the fast single-node Arrow engine — batch/out-of-core)
- **New streaming engine (2024+ rewrite):** **morsel-driven** parallelism (small row batches flow through a
  work-stealing scheduler — Leis/Neumann morsel model, same as our exchange should approach) + **out-of-core
  spill** for larger-than-memory. **Honest scope:** Polars "streaming" = larger-than-memory **batch**, NOT
  infinite event streams (no watermarks/EO/state) — so it's a **batch/perf** reference, not a Flink rival.
- **Arrow-native, zero-copy, vectorized (SIMD)** columnar kernels + a **lazy query optimizer** (predicate/
  projection pushdown, common-subexpression elim, streaming physical plan). This is the **columnar edge (§6)**
  that lets Vajra beat Flink's per-record JVM cost — Polars proves how far a well-optimized Arrow engine goes
  on one node. Vajra's DataFusion core (§5) is the distributed equivalent; steal Polars' optimizer rigor +
  morsel scheduling for the single-node hot path.

### Apache Fluss (Incubating) — columnar streaming storage; validates the T7 pushdown thesis (2026-07-07)
Sources: https://fluss.apache.org/ · https://www.alibabacloud.com/blog/fluss-redefining-streaming-storage-for-real-time-data-analytics-and-ai_602412 · https://jack-vanlightly.com/blog/2025/9/2/understanding-apache-fluss · Flink Forward Asia 2025.
- **What it is:** next-gen **columnar log streaming storage built on Apache Arrow** (Arrow IPC as the native
  storage + wire format), sub-second streaming read/write. Disaggregated cluster (tablet servers +
  coordinators), **separate from the Flink compute cluster** (compute/state separation, like RisingWave 3.0).
  Union reads (streaming + batch), lakehouse tiering to Iceberg/Paimon.
- **The on-thesis fact:** because it is columnar, Fluss does **server-side projection + predicate pushdown +
  partition pruning on the Arrow stream** — "if a job reads 3 of 20 columns, Fluss sends only those 3,"
  claimed **up to 10× I/O/network/CPU** savings. This is the SAME principle as **VAJ-T7 source-fusion**
  (don't materialize/transmit what the query doesn't need — Vajra parses `value`→typed cols in-source so the
  raw value column is never materialized), and it is an **official Flink-ecosystem design** validating the
  approach. Fluss pushes at the *storage* layer; Vajra pushes at the *source-parse* layer — same columnar-
  end-to-end thesis (§6).
- **Where it informs the epic:** (a) **VAJ-T7** — projection/parse pushdown into the columnar source is the
  right axis (Fluss + Polars + DataFusion all agree). (b) **VAJ-BF2** — Arrow IPC as the zero-copy streaming
  wire (Fluss uses exactly this) reinforces Arrow Flight shuffle. (c) **VAJ-BF3 / roadmap** — disaggregated
  storage/compute (Fluss + RW Hummock) is the scale-out frontier. **Honest scope:** Fluss is a *storage
  system*, not an engine; Vajra doesn't adopt Fluss, it adopts the **columnar-pushdown + disaggregation
  principles** it validates. Not yet measured head-to-head (no claim).

### Synthesis — the "best of all" bar for Vajra (cite this in streaming/engine work)
| Concern | Best source | Vajra target (build to this) |
|---|---|---|
| Exactly-once | Flink barriers + RW barriers | Barrier-aligned union commit (T-EO-3), crash-proven ✅ |
| Columnar source pushdown | **Fluss** (Arrow server-side projection) + Polars | **VAJ-T7 parse-in-source** — raw `value` never materialized (T1 green, opt-in) 🟡 |
| Watermarks | Flink `withIdleness` + RW per-partition | Per-instance FLIP-27 + idleness merge (T-EO-1/3.5) ✅ |
| Large state / rescale | Flink 2.0 ForSt + RW Hummock (S3) | F5 immutable Arrow chunks + inc-ckpt on object store ✅ |
| Incremental results | RW materialized views | Changelog/retraction mode → full MV maintenance ⬜ |
| Vectorized hot path | Polars morsel-driven + DataFusion | Arrow/DataFusion vectorized, no-JVM (§6) ✅; add morsel scheduling ⬜ |
| Query optimization | Polars/DataFusion optimizer | DataFusion optimizer; mine Polars' pushdown/CSE rigor ⬜ |
| No-JVM footprint | (Vajra unique) | Arrow single-binary — the categorical memory/startup edge ✅ |

**Prod-grade bar (STANDING):** every one of these must be *measured* head-to-head (Nexmark/TPC-DS/prod
workloads) before claiming "better than Flink/Spark", path-dependence flagged. The win is the **union** —
Flink's correctness + RW's decoupled state/MV incrementalism + Polars' vectorized single-node speed +
Vajra's no-JVM Arrow core — that no single incumbent has all of.

---

## DataFusion 54 — morsel-driven file scan vs distributed execution (CRITICAL, 2026-07-06)

**Fact (from `datafusion-datasource-54.0.0` source):** DF54 rewrote file scans to be morsel-driven
with in-process sibling **work-stealing**. `DataSourceExec::execute(partition)` lazily builds ONE
`shared_state = data_source.create_sibling_state()` and passes it to every partition stream via
`OpenArgs::with_shared_state`. `FileScanConfig::create_sibling_state()` returns
`Some(SharedWorkSource::from_config(self))` — a pool of **all files across all file groups** — UNLESS
`self.preserve_order || self.partitioned_by_file_group`, in which case it returns `None` and each
partition uses `WorkSource::Local(file_groups[partition])`.

**Implication for a distributed engine (Vajra/Ballista-style):** when each output partition runs as an
ISOLATED task (separate process, no in-process siblings), each task builds its own pool of ALL files
and, with nobody to steal from, drains the whole pool → **every file is read once per partition → N×
row duplication** for N file groups. Single-process (`--mode local`) is correct because real siblings
steal; 1 file group is coincidentally correct. This silently breaks distributed correctness (wrong
counts / duplicated rows) with NO error, on ANY file format, and is invisible to single-node tests.

**Prod-grade fix:** in the per-task plan rewrite (worker/task runner), rebuild each `FileScanConfig`
scan with `FileScanConfigBuilder::from(cfg).with_partitioned_by_file_group(true)`. This is DF54's own
opt-out: it disables the shared work source so each partition reads only its assigned file group —
exactly the fixed one-group-per-task model a distributed scheduler already assigns. (Vajra:
`sail-execution/src/task_runner/core.rs::rewrite_parquet_adapters`.)

## 9. Task placement across workers — even-spread vs fill-first (fetched 2026-07-08; cite for scheduling)
The scheduler decision "which worker gets each of a stage's N task partitions" determines whether a
distributed stage actually parallelizes across nodes or collapses onto one. Two canonical policies:

- **Flink `cluster.evenly-spread-out-slots`** (SlotManager / ResourceManager): when `true`, each slot
  request is placed on the TaskManager with the **most free slots**, so a job's subtasks **fan out
  evenly** across all TMs (max parallelism, no hot TM). **Default is `false`** — Flink fills up
  already-used TMs first, which is *resource-efficient for reactive scale-down* (fewer TMs held) but
  co-locates subtasks. ⇒ for maximizing throughput/utilization on a **fixed** cluster you want
  even-spread; for elastic scale-in you want fill-first. (Flink docs: Deployment > Config >
  `cluster.evenly-spread-out-slots`; Flink slot-sharing groups place one subtask of each pipelined
  operator per slot.)
- **Spark `spark.deploy.spreadOut`** (Standalone, **default `true`**): spread an app's executors across
  as many worker nodes as possible (better data locality + parallelism) rather than consolidating on
  few nodes. Spark's task scheduler then round-robins tasks across executors within locality levels.
- **Synthesis for Vajra (VAJ-BF2 T-BF2.5):** the driver's `TaskSlotAssigner` was pure fill-first
  (`slots.iter_mut().find_map(pop)` drains worker[0] first) — measured cause of "all N window
  instances on one pod" (T2). Added an **even-spread policy** (pick the worker with the most free
  slots; Flink's algorithm) gated on distributed streaming (`VAJRA_DISTRIBUTED_STREAM`/`VAJRA_EVEN_SPREAD`),
  default fill-first so batch/scale-down behavior is unchanged. Cutting a stage boundary (T-BF2.2) is
  necessary but only *distributes* when paired with even-spread placement.
