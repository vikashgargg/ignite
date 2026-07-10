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
