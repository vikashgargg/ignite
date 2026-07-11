# Source-read throughput — the prod-grade fix to beat Flink (design, per [AIM](../AIM.md))

**Do NOT implement until the win is proven in isolation (confidence gate C1 below).** This is the one lever
that closes the measured Flink gap. Grounded in [REFERENCES §8](../REFERENCES.md) (FLIP-27 + rust-rdkafka).

## 1. Root cause (MEASURED, not assumed)
Fair EKS head-to-head (100M, both → S3 parquet): **Flink 5.07M vs Vajra 2.32M = 2.2×**. Per-pod WM_PROF:
`source_read` ≈ the wall. SOURCE_POLL/BUILD split (local 5M): **poll 58% + per-message bookkeeping 37% +
Arrow build 5%**. The columnar build is NOT the bottleneck (as it should be). Vajra is **per-message
CONSUME-bound**.

## 2. Why (official)
- **rust-rdkafka:** we use `StreamConsumer::stream()` + `.next().now_or_never()` — the async convenience
  layer adds **per-message channel/future overhead**; `BaseConsumer` is *considerably faster for small
  messages* (our JSON events). This IS the 58% poll.
- **per-message bookkeeping:** `next`/`ends` are `HashMap<u64,i64>` hashed+written **per message** (100M
  hashes). This is a chunk of the 37%.
- **Flink KafkaSource (FLIP-27):** `KafkaPartitionSplitReader` runs in a **dedicated fetcher thread** and
  `consumer.poll()` returns **many records/call** (batched) — 200k polls for 100M, not 100M.

## 3. The prod-grade design (synthesis — match Flink's batched poll AND keep our columnar/no-JVM edge)
Per source instance (already 1/partition, FLIP-27):
1. **Dedicated consume thread** (`std::thread`, off the tokio runtime — the Flink `SourceReaderBase`
   fetcher-thread model): runs a **`BaseConsumer`** in a tight `poll()` loop.
2. In the thread, build **columnar Arrow batches** (`KafkaArrowBuilders`, alloc-free — the edge Flink
   lacks: it deserializes per-record into JVM objects) and hand **full batches** to the async pipeline via
   a **bounded channel** (`tokio::sync::mpsc`, capacity = backpressure = Flink credit-flow analog).
3. **Per-partition `Vec<i64>` offsets** (dense index) — no per-message HashMap.
Net: Flink-batched consume + our columnar/no-JVM build ⇒ target **> Flink 5.07M**, and memory stays low
(Vajra 3.70 GiB/pod vs Flink 9.27 GiB).

## 4. Correctness — MUST preserve (the risk surface; each has a gate)
The consume loop (reader.rs:1245) is interwoven with:
- **Exactly-once offset commit** — bounded (`sources/<inst>/staged→committed`), realtime (`realtime/
  committed` per-epoch, per-instance staged union). The fetch thread must track + expose reached offsets so
  the epoch/commit machinery still stages the EXACT consistent-cut offset. *Gate: `inc_ckpt_gate.sh` +
  `f3c_stateful_crash` dup=0.*
- **Markers** — Checkpoint barriers, EndOfData (bounded end), Idle (`PartitionEOF`), watermark (downstream
  WatermarkExec). The thread→channel→async path must emit these at the SAME points. *Gate: `nm_dist_gate`
  counts-exact + crash-EO dup=0.*
- **3 paths** — bounded(availableNow) / realtime(continuous EO) / unbounded. Port carefully; do bounded
  FIRST (the throughput path), validate, then realtime.
- **Bounded memory** — the prefetch cap (`VAJRA_KAFKA_PREFETCH_*`) + the bounded channel keep RSS ≈
  prefetch×partitions. *Gate: F5/RSS unchanged.*

## 5. Measurement plan (measure, don't assume — the confidence gates)
- **C1 (isolate the win, FREE, DO THIS FIRST):** add a BaseConsumer-fetch-thread variant to the
  `kafka_read_bench` micro-bench (reader.rs:1502) — raw consume throughput, NO EO/marker complexity. A/B vs
  the current `StreamConsumer.now_or_never` read. **PROCEED ONLY IF BaseConsumer is materially faster** (else
  redesign — the win must be real before touching the production loop).
- **C2 (T1 full pipeline, local):** windowed-agg WM_PROF SOURCE_POLL/BUILD — source_read poll share DROPS,
  counts exact.
- **C3 (T2 kind):** real pods, counts exact + crash-EO dup=0.
- **C4 (T3 EKS):** ONE fair head-to-head vs Flink — Vajra throughput ≥ Flink, tear $0.

## 6. Confidence bar (do NOT implement the production rewrite until ALL true)
- [ ] C1 micro-bench shows BaseConsumer-fetch-thread materially faster than current.
- [ ] Design preserves EO + markers + idle + bounded memory (reviewed against reader.rs:1245 loop).
- [ ] Staged: bounded path first (validate) before realtime/unbounded.
Only then implement — incrementally, gate at each stage.

## 7. The COLUMNAR angle (KB-checked — where Arrow/DataFusion help, and where already tried)
Per AIM, leverage our columnar/Arrow edge. KB findings (REFERENCES §6/§8):
- **Already leveraged (why build is only 5%):** `KafkaArrowBuilders` build a columnar Arrow batch alloc-free;
  the vectorized window/agg + no-JVM edge are downstream. The columnar advantage is REAL and realized — it's
  why our memory wins (3.70 vs 9.27 GiB) and why build ≠ bottleneck.
- **Tried, MEASURED no help (don't repeat):** (a) **arrow-json columnar decode** in from_json = ~PARITY with
  our serde_json (we're already Rust; the win is vs Java/Jackson) — REFERENCES §6. (b) **VAJ-T7
  source-fusion** (parse pushdown into the columnar source) = MEASURED NO throughput beat (from_json already
  pre-exchange); kept opt-in `VAJRA_T7_FUSE`.
- **The columnar-OPTIMAL consume (this fix):** the CONSUME bottleneck (58% poll) is librdkafka per-message +
  StreamConsumer overhead — NOT a columnar gap, a CONSUME-MODEL gap. The columnar-right fix = **batch the
  consume (BaseConsumer / `rd_kafka_consume_batch`) and build ONE Arrow columnar batch from the message
  ARRAY per call** (Flink batches its poll; we then out-columnar it in the build). i.e. feed the columnar
  build efficiently instead of per-message. Highest-bar variant to A/B in C1: `rd_kafka_consume_batch` (FFI,
  true batch → columnar) vs safe `BaseConsumer::poll` loop.
- **Secondary columnar lever (separate, defer):** arrow-rs **`Utf8View`/`BinaryView`** on the `value` column
  → fewer copies on the value + from_json input + shuffle re-encode (REFERENCES §6 version-upgrade target).
  Marginal for source_read build (5%) but helps from_json (11%) + shuffle; track as a follow-up, not the
  source_read lever.
**Conclusion:** the columnar stack is already our edge; the source_read fix is a batch-CONSUME fix that
FEEDS the columnar build faster (not a new columnar decode). StringView is the next columnar lever after.

## 8. C1 measurement (2026-07-11, local 10M/16-part, release) — hypothesis REFUTED, refined
| variant | throughput |
|---|---|
| StreamConsumer (current, now_or_never + prefetch-tuned) | **2.357M rows/s** |
| BaseConsumer-thread (poll 100ms, under-tuned) | 1.913M rows/s (SLOWER) |
- **Per-message BaseConsumer is NOT the win** — the current StreamConsumer.now_or_never is already tuned.
  The confidence gate C1 correctly STOPPED the production rewrite (would not have helped).
- Raw read 2.357M ≈ pipeline 2.32M ⇒ pipeline IS source-read-bound; 2.357M is the current PER-MESSAGE
  consume ceiling.
- **Caveats making C1 not yet decisive:** (a) the variant used blocking `poll(100ms)` vs the baseline's
  non-blocking `now_or_never`, and lacked `apply_consumer_throughput_defaults` (unfair). (b) `BaseConsumer.
  poll()` is still PER-MESSAGE — it does NOT test Flink's BATCHED poll. The real lever = **`rd_kafka_consume
  _batch`** (librdkafka batch API, N messages/call, unsafe FFI) → build ONE Arrow batch from the message
  array. **NEXT C1b:** fair BaseConsumer (equal tuning + poll(0)) AND a `rd_kafka_consume_batch` variant;
  proceed to the production rewrite ONLY if batch-consume materially beats 2.357M. Else: the source_read gap
  vs Flink is the rust-rdkafka per-message API vs Java's batched KafkaConsumer.poll — flag honestly.

## 9. C1b FAIR measurement (2026-07-11, equal tuning + non-blocking poll) — CONSUME REWRITE REFUTED
| variant (fair: apply_consumer_throughput_defaults + poll(0)) | throughput |
|---|---|
| BaseConsumer-thread | 2.529M rows/s |
| StreamConsumer (current) | 2.518M rows/s |
**IDENTICAL (0.4% noise).** BaseConsumer gives NO win; and the variant OMITS the per-message HashMap
bookkeeping yet is the same speed ⇒ the `Vec`-offsets bookkeeping fix is ALSO not the lever. Both refuted.
The source read is at the **per-message poll ceiling ~2.5M rows/s** (16-part, this box). The gate C1
correctly PREVENTED the BaseConsumer-fetch-thread + Vec-offsets production rewrite (≈0% gain).
**Remaining consume lever:** `rd_kafka_consume_batch` (librdkafka batch API, N msgs/call, unsafe FFI) — the
only thing that reduces the per-CALL count (Flink's batched poll analog). Likely MODEST (~20-30%: saves the
poll call, not the per-msg BorrowedMessage+append+copy), NOT 2×. HONEST: the ~2× read gap vs Flink is
substantially the rust-rdkafka per-message consume vs Java KafkaConsumer.poll(500)-batched; closing it fully
may need the batch FFI + is not guaranteed. **Vajra's PROVEN wins stand: memory (3.70 vs 9.27 GiB), batch
6.2× vs Spark, latency (no-GC), unified engine, correctness/EO.** DECISION POINT: (a) build the batch-FFI
variant + measure (modest, some risk) OR (b) accept streaming-throughput as competitive-not-beating on this
per-message-consume-bound axis and invest in the proven-win axes.

## 10. C1c batch-FFI (2026-07-11, local 10M/16-part, release) — CONSUME THESIS DEFINITIVELY CLOSED
| variant | throughput |
|---|---|
| batch-FFI (rd_kafka_consume_batch_queue, N/call = Flink poll(500) analog) | 2.287M rows/s |
| BaseConsumer (per-message) | 2.279M rows/s |
| StreamConsumer (current) | 2.250M rows/s |
**ALL EQUAL (~1.6% noise).** Even librdkafka's TRUE batch API gives NO win ⇒ the poll model is NOT the
lever; ~2.28M is the librdkafka-delivery + per-message payload-copy ceiling, independent of the poll API.
Three gates (C1/C1b/C1c) prevented a big unsafe-FFI production rewrite for ~0%.
**REVISED root-cause hypothesis (the EKS 2.2× gap is NOT the raw consume):** the local bench is CORE-
CONTENDED (16 threads on ~10 laptop cores) so it under-measures the c7g read; and on EKS the SINGLE compute
node runs source+window+shuffle+Flight all on 16 vCPU (contended), while **Flink's mini-batch aggregation**
(`table.exec.mini-batch` 2s/50k) makes its WINDOW very cheap → more CPU left for the read. So the gap is
likely **CPU CONTENTION + Flink's cheaper aggregation / operator fusion**, NOT the Kafka consume. NEXT (per
option c): re-profile the FULL pipeline stage CPU on EKS (source vs window vs shuffle) with a MULTI-node
cluster (uncontend the read) + measure Vajra's window/agg CPU vs Flink's mini-batch. The `kafka_read_bench_*`
variants stay (#[ignore]) as the proof. Vajra's PROVEN wins stand (memory, batch 6.2× vs Spark, latency, EO).

## 11. REFRAME (2026-07-11) — the lever is FETCH/COMPUTE OVERLAP, not the consume model
**Stage-by-stage map vs Flink (grounded: FLIP-27 + Flink operator-chaining/mini-batch research + Vajra code):**
- Vajra source = `async_stream::stream!` (reader.rs:681/949) that `yield`s batches, **pulled inline by the
  downstream operator on the SAME tokio task**. No spawn/channel decouples fetch from compute ⇒ per source
  task, **poll Kafka → build Arrow → from_json → shuffle-encode → yield are SERIALIZED on one thread**.
- Flink FLIP-27: `KafkaPartitionSplitReader` runs in a **DEDICATED fetcher thread** feeding a handover queue;
  the pipeline thread consumes it ⇒ **fetch OVERLAPS compute**. Flink also chains operators (one thread, no
  handover) and mini-batches the agg — but the source/compute OVERLAP is the structural piece Vajra lacks.
- ⇒ Vajra wall ≈ fetch + compute (serial); Flink wall ≈ max(fetch, compute) (overlapped). If fetch≈compute
  that alone is ~2× — matching the measured 2.32M vs 5.07M. **Why the 3 consume gates showed nothing:** they
  measured fetch ALONE (no downstream) ⇒ blind to the overlap. The lever was never the poll model.
- Window mini-batch: Vajra ALREADY does per-batch Partial pre-agg (window_accum.rs:337) ✅ not the gap.
  Same-node shuffle: ALREADY mpsc RecordBatch zero-copy (stream_manager/local.rs) ✅; only CROSS-node
  serializes via Flight IPC (a separate, secondary lever — measure after overlap).
**Prod-grade design (FLIP-27 SourceReaderBase model): dedicated fetch thread (`std::thread`, off the tokio
runtime) runs the tight consume+build loop, hands FULL Arrow batches to the pipeline via a BOUNDED channel
(`tokio::sync::mpsc`, cap = backpressure = credit-flow) ⇒ Kafka fetch overlaps from_json+encode+shuffle+sink.**
**CONFIRM BEFORE IMPLEMENTING (gate C2, free/local):** micro-bench A/B — (a) inline source→from_json→encode
on one task (serialized, today) vs (b) fetch-thread + bounded channel + from_json+encode on the main task
(overlapped). Proceed to the production fetch-thread ONLY if (b) materially beats (a). This measures the RIGHT
lever (overlap) in isolation, unlike C1/C1b/C1c which measured the consume alone.

## 12. C2 OVERLAP + fetch-model, and the laptop-noise wall (2026-07-11) — HONEST STOP on free levers
Bench (bench_src 10M, release), spare-core counts:
| test (4 partitions, cores free) | throughput |
|---|---|
| pipe OVERLAP (fetch thread + bounded chan + parse) | 1.189M |
| pipe SERIAL (fetch+parse inline) | 1.149M |
| fetch-only batch-FFI | 1.585M → 2.330M (rerun, same config) |
| fetch-only BaseConsumer | 1.072M |
- **Overlap = +3.5% only** ⇒ from_json compute is SMALL vs fetch; little to hide → overlap is NOT a 2× lever.
  The source is **fetch/delivery-bound**, not compute-bound (consistent with the 3 consume gates).
- **Fetch-model swings 50% run-to-run (1.585→2.330M)** ⇒ this laptop (loopback Kafka + ~10 cores + bg load)
  is TOO NOISY to rank sub-2× levers or measure at-scale throughput. Matches the SDLC skill: *EKS = the
  throughput number; laptop = correctness/mechanism only.*
- Fetch config already aggressive (10 MiB/part fetch, 64 MiB prefetch, 16 MiB socket) — not a free under-fetch.
**CONCLUSION:** free source-side levers (consume model, fetch/compute overlap) are within noise / small; none
is the decisive 2×. The EKS 2.2× is real but NOT yet isolated to a stage with confidence (WM_PROF summed-CPU
can't cleanly attribute the serial-path time). **The one AIM-right decisive experiment = a PROGRESSIVE
per-stage throughput profile on real cores** (source→count, +from_json→count, +window→count, full→S3), Vajra
vs Flink per-operator (Flink web UI). Build+validate the harness FREE on kind (correctness), then ONE EKS run
pinpoints the exact 2× stage. Until then: do NOT implement a source rewrite (all free evidence says it won't
move the 2×). Vajra's PROVEN wins stand: batch 6.2× vs Spark, memory, latency (no-GC tail), unified, EO.

## 13. Per-stage profiler built + VALIDATED FREE locally (2026-07-11) — local is SOURCE-bound; the 2× is CROSS-NODE Flight IPC
Built `scripts/stream_stage_profile.py` + `scripts/stage_profile_local.sh` (progressive stages via the PROVEN
parquet-append sink; COMPLETE_ON_END flushes every window ⇒ equal counts = fair). Free-validation findings
(each a real harness lesson, caught before EKS): (a) Vajra streaming has NO memory/complete sink (rows=None) —
use parquet; (b) a giant window never closes in append mode (watermark) — use 10s windows that close;
(c) macOS bash 3.2 has no assoc arrays; (d) normalize throughput by ACTUAL emitted rows, not assumed N.
**Local result (bench_src 10M, equal counts):** nokey(from_json+window, 1 hot group) 2.251M/s ≈ source ceiling
~2.3M; full(+keyed, 1000 groups) 3.069M/s FASTER (parallel groups, not the exchange). ⇒ **locally parse+window
add ~nothing over the source; the whole single-node pipeline is SOURCE/FETCH-bound and the same-node shuffle is
mpsc (zero-copy, free).** The one thing local CANNOT exercise = the **CROSS-NODE Flight shuffle serialization**
(single-node stays mpsc). KB already root-caused exactly this ([[project_shuffle_coalesce_wip]]: distributed lag
= Flight small-batch IPC, Vajra 2.09M vs Flink 5.77M = 2.77×) — matches the user's "no-serde-but-slow" intuition:
same-node no serde, CROSS-node serializes via Flight IPC. **NEXT (free): run the SAME profiler on kind (T2,
multi-pod = REAL Flight shuffle) to measure the cross-pod shuffle delta before EKS.** The lever is the Flight
shuffle IPC (batch-size / zero-copy / coalescing), NOT the Kafka source. Don't rewrite the source.
