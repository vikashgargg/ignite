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
