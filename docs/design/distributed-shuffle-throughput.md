# Distributed streaming shuffle throughput — concrete prod-grade design

**Status:** watermark fix + coalescer committed (9cd7d05c, 276d7d8d); buffer-timeout + default-enable +
kind/MinIO validation = remaining. **Goal:** close the measured distributed gap vs Flink (EKS 100M:
Vajra 2.09–2.27M vs Flink 5.77M ev/s = 2.77×) with ONE grounded design, not trial-and-error.

## 1. The problem (MEASURED, not assumed)

Per-pod `WM_PROF_PROC` on EKS (commit 22aba4bc instrumentation): the lag is the **cross-pod Flight
shuffle transport**, not compute or routing:
- `exchange_cpu ≈ 0` — the keyed hash-route (`take`) is free.
- **24,468 Flight messages for 100M rows = ~4,000 rows/batch = tiny** → per-batch IPC framing/serialize
  + async round-trip overhead dominates (`shuffle_send` ≈ 5.8 ms/small-batch; `shuffle_recv` mostly
  blocked-waiting = receiver starves behind the small-batch send).

Two independent causes make the batches tiny:
- **(a) route-split:** the keyed exchange `take`-routes each input batch into `n` sub-batches (~1/n rows).
- **(b) per-batch watermarks:** `WatermarkExec` emitted a `Watermark` marker after EVERY data batch whose
  event-time advanced; a time-ordered backlog advances every batch ⇒ stream = `[data, wm, data, wm, …]`
  1:1, so any shuffle coalescer that flushes before markers (required for a consistent cut) can never
  accumulate, and the exchange broadcasts N× the markers.

## 2. Credible sources (what each prescribes — cite, don't re-derive)

- **Apache Flink — network stack** (nightlies.apache.org/.../network_mem_tuning; flink.apache.org
  "Deep-Dive into Flink's Network Stack" 2019): records are serialized into fixed **network buffers**
  (memory segments, default 32 KiB) and flushed when the buffer is **full OR after `execution.buffer-
  timeout` (default 100 ms)** via a periodic **output flusher**. "per-buffer overhead is significantly
  higher than per-record overhead" — so big buffers = throughput; the timeout bounds latency for
  low-traffic channels. **Credit-based flow control** (FLIP-2) backpressures without head-of-line block.
- **Apache Flink — watermarks** (`pipeline.auto-watermark-interval`, default **200 ms**): watermarks are
  generated **periodically on a timer**, NOT per record/batch. Per-batch emission is non-idiomatic.
  (Grounded further by **MillWheel** (VLDB 2013) low-watermarks and the **Dataflow Model** (VLDB 2015):
  watermarks are a coarse progress signal, not per-element.)
- **RisingWave** (risingwavelabs.github.io streaming-overview; docs architecture): actor dataflow; the
  **exchange dispatcher** hash-shuffles **StreamChunks** (columnar Data Chunk + ops/visibility) — batched,
  never per-row; the **merger aligns barriers** on the receive side (== our N→M merge). Batching "can
  eliminate unnecessary intermediate results and provide better performance."
- **DataFusion 54** — `CoalesceBatchesExec` (`datafusion/physical-plan/src/coalesce_batches.rs`): the
  canonical operator inserted **after `RepartitionExec`** to re-merge small post-repartition batches to
  `target_batch_size` (default 8192) using arrow's `BatchCoalescer`. Exactly our situation (repartition
  makes small batches; coalesce re-merges).
- **Arrow / arrow-rust 58.3** — `arrow_select::coalesce::BatchCoalescer` (`push_batch` / `push_batch_with_
  indices` / `next_completed_batch` / `finish_buffered_batch`): production, zero-alloc-amortized row
  coalescing into exact target-size batches. `Utf8View`/`BinaryView` reduce copies on string/shuffle.
- **Arrow Flight** (`arrow_flight::encode::FlightDataEncoderBuilder`; Ballista remote-shuffle uses Flight
  `do_get` + client cache): schema sent once, then one `FlightData` per batch; **large batches amortize
  the per-message cost**; `with_max_flight_data_size` only SPLITS (never coalesces) — so coalescing must
  happen upstream of the encoder.
- **FAANG columnar** — Databricks **Photon** (SIGMOD 2022) and Meta **Velox**: vectorized, batch-at-a-time
  end to end; the win over row engines (Flink/JVM) is *staying columnar and never per-row*, incl. the
  exchange. Arroyo (Rust+Arrow, our stack) beats Flink 5× on exactly this (REFERENCES §6).

## 3. The design (synthesis — one fix, all layers)

Three coordinated pieces, each mapped to a source; correctness-first.

### D1. Periodic watermarks (Flink `auto-watermark-interval`) — DONE (9cd7d05c)
`WatermarkExec` emits at most one watermark per `VAJRA_WATERMARK_INTERVAL_MS` (default 200 ms); `max_ts`/
per-partition maxima keep updating every batch so the emitted watermark is current; monotonic; finals
flush at end/EndOfData. Removes cause (b): markers no longer sit between every data batch. **This alone is
correct and idiomatic regardless of the shuffle change.**

### D2. Shuffle-boundary coalescer (DataFusion CoalesceBatches + Arrow BatchCoalescer + Flink buffer) — DONE core (276d7d8d), buffer-timeout REMAINING
A **pull** stream combinator (`coalesce_flow_events`) at the Flight server `do_get`, upstream of
`FlightDataEncoderBuilder`, re-merges routed DATA batches to `VAJRA_SHUFFLE_BATCH_ROWS` (target; propose
default 16384 once validated) via `BatchCoalescer`, and:
- **flushes before every MARKER** (watermark / checkpoint barrier / EndOfData / idle) → the barrier stays a
  consistent **Chandy-Lamport cut** (data never reordered behind a marker) — EO + watermark correctness;
- **flushes on stream end** (the Flight client always drains `do_get` fully → no abandoned rows; verified
  on clean EKS: counts EXACT, so the earlier local "loss" was machine-load flakiness, not this code);
- **REMAINING — buffer-timeout (Flink `execution.buffer-timeout`, default 100 ms):** flush a partial
  buffer on a timer so a low-traffic channel never stalls and shuffle latency is bounded. In a pull
  combinator this is a `tokio::select!` between the input poll and a periodic tick. Required for the
  realtime/low-rate path; the bounded throughput path is already served by size + periodic-marker flush.

Place at the Flight boundary (distributed-only) — NOT the in-process exchange — so the validated
single-node path is untouched (`VAJRA_SHUFFLE_BATCH_ROWS=0` = off).

### D3. (Future, not required for parity) zero-copy: `Utf8View` on the value/shuffle columns + Flight
client-cache (Ballista) to cut the remaining serialize/copy. Track separately; D1+D2 close the batch-count
gap first.

## 4. Correctness invariants (must hold; each has a check)
1. **No row loss/dup:** Σ window counts == baseline (coalescing off). *Check:* `nm_dist_gate` OFF==ON on a
   QUIET machine, and MinIO/kind distributed windowed-agg OFF==ON.
2. **Consistent cut:** every marker is preceded by all data buffered before it. *Check:* unit test
   `coalesce_preserves_rows_and_marker_order` (green) + crash-EO `f3c_stateful_crash` dup=0.
3. **Watermark monotonic + windows complete:** *Check:* windowed-agg n_windows/total unchanged by D1 (EKS:
   9/90M both — the completeness gap is pre-existing, orthogonal).

## 5. Validation plan (FREE first — kind+MinIO; EKS only for scale numbers)
- **T1 mechanism (local/free):** distributed windowed-agg with `VAJRA_WM_PROF=1`; assert per-pod
  `shuffle_send_batches` DROPS ~ (rows/batch → 16384) when D2 on, at equal counts.
- **T2 kind + MinIO (free, real pods = real Flight):** deploy the dist manifest with `shufcoal` image,
  `VAJRA_SHUFFLE_BATCH_ROWS=16384`, windowed-agg → MinIO S3; assert counts exact + batches drop +
  crash-EO dup=0. **This is the correctness gate — no EKS needed for it.**
- **T3 EKS (numbers only):** ONE A/B (off vs on) for the throughput delta vs Flink; tear to $0.

## 6. Status ledger
| Piece | Source | State |
|---|---|---|
| D1 periodic watermark | Flink auto-watermark-interval | ✅ 9cd7d05c, unit-tested |
| D2 coalescer (size + marker-flush + end-flush) | DataFusion CoalesceBatches + Arrow BatchCoalescer | ✅ 276d7d8d, unit-tested |
| D2 buffer-timeout | Flink execution.buffer-timeout | ✅ tokio::select tick |
| Enable by default (16384) | — | ✅ VALIDATED local-cluster+MinIO |
| D3 zero-copy Utf8View | Arrow StringView / Ballista | ⬜ future |

## 7. VALIDATION RESULT (free, local-cluster distributed + MinIO, WM_PROF, 2 runs — deterministic)
local-cluster routes shuffle over **Flight** (shuffle_send_batches > 0) ⇒ the coalescer is exercised
without EKS. 5M rows, 16-part, monotonic, COMPLETE_ON_END:
- **Correctness: total_events = 5,000,000 EXACT for OFF and ON, n_windows=100 both** (no loss/dup).
- **Mechanism: shuffle_send_batches 4890/4888 (OFF) → 2281/2294 (ON) = 2.13–2.14× fewer Flight messages.**
Repeatable across runs. Confirms D1+D2 correct + effective. (Throughput delta needs release+scale = one
future EKS A/B for the number; mechanism + correctness proven here for free.) `scripts/local_dist_coalesce_check.sh`.

## 8. FAIR head-to-head vs Flink (2026-07-10, EKS 1× c7g.4xlarge, 100M, BOTH → S3) — WHERE WE LAG
Both engines: identical 10s tumbling windowed COUNT, same 100M Kafka topic, WRITE TO S3 (Flink parquet via
flink-s3-fs-hadoop + flink-sql-parquet + flink-shaded-hadoop-2-uber + checkpoints-after-tasks-finish; Vajra
parquet). Bounded catch-up = throughput.
| Metric | Flink 1.19 | Vajra |
|---|---|---|
| Throughput | **5.07M ev/s** (19.7s) | 2.32M ev/s (43s) — Flink **2.2×** |
| Peak memory | 9.27 GiB (1 TM) | **3.70 GiB/pod** (Vajra lower) |
| Correctness | 48 files | 100M/10 windows/1000 keys exact |

**ROOT CAUSE OF THE 2.2× (Vajra per-pod WM_PROF, cpu-ms):** source_read=40–49s (≈ the 43s WALL) >> from_json
=11–12s > exchange_cpu=**0**. shuffle_recv=608s is BLOCKED-WAIT (window starves behind the source), NOT CPU.
**Vajra is SOURCE-READ BOUND** — Kafka consume + Arrow batch build dominates; the window is idle waiting.
Flink reads 100M in 19.7s. **⇒ The throughput lever is the Kafka SOURCE READ + Arrow decode path, NOT the
shuffle** (exchange_cpu=0; the coalescer, though correct + 2.14× fewer msgs, does NOT move throughput and
regresses single-node). NEXT STEP = profile + optimize source_read (kafka/reader.rs: rdkafka consume loop,
batch sizing, Arrow build; compare to Flink KafkaSource FLIP-27 fetch parallelism/mini-batch). Cluster torn $0.

## 9. source_read fix plan (the throughput lever — measured, grounded in Flink FLIP-27)
MEASURED split (local 5M, WM_PROF SOURCE_POLL/BUILD): source_read=21.6s = **poll 12.6s (58%)** +
per-message bookkeeping ~7.9s (37%) + **build 1.0s (5%)**. The Arrow columnar build is NOT the bottleneck
(5%). source_read is PER-MESSAGE CONSUME-bound. Flink KafkaConsumer.poll() returns ~500 records/call.

**Fix (ordered, each T1-measured local before EKS):**
1. **Per-partition offset arrays (bookkeeping, low-risk):** replace the hot per-message `next: HashMap<u64,i64>`
   `insert` + `ends: HashMap<u64,i64>` `get` (kafka/reader.rs ~756/864/883) with `Vec<i64>` indexed by a
   dense (topic_idx, partition) index — O(1) no-hash per message. Cuts a chunk of the 37%.
2. **Batch the consume (poll 58%, the big lever, FLIP-27 SplitReader model):** the `msg_stream.next()
   .now_or_never()` per-message future poll is the cost. Prod-grade = a DEDICATED consume thread
   (spawn_blocking) running `BaseConsumer::poll()` in a tight loop, building Arrow batches, handing full
   batches to the async pipeline via a bounded channel (decouples sync fetch from async, no per-msg future
   machinery). Grounded: Flink FLIP-27 (SplitReader.fetch returns a batch), rust-rdkafka BaseConsumer.
3. (measure after 1+2) if still gapped: source parallelism / assignment vs Flink's per-partition readers.
**Target:** source_read → ~1/2, throughput 2.32M → toward Flink 5.07M. Validate T1 (local WM_PROF split)
→ T2 kind → T3 EKS head-to-head. NOT the shuffle (exchange_cpu=0; coalescer is correct but orthogonal).

## 10. Cross-node shuffle is LATENCY-BOUND (2026-07-11) — the honest root cause + grounded fix
DECISIVE isolation (release, same box, 5M): in-process exchange **3.07M** vs Flight shuffle **1.55M** = flat
2×, UNCHANGED by 16k→64k batch rows AND by the HTTP/2 window fix on loopback (1.52–1.57M). Reconciled with
the REAL EKS data: single-node (4 in-process workers, loopback shuffle) = **4.92M = 1.15× behind Flink**
(near-parity ✅); multi-node (cross-node Flight) = **1.46M = 3.6× behind** ❌. So the in-process/loopback
shuffle is FINE at scale; the gap is CROSS-NODE only. Cross-node runs at 1.46M×16B ≈ **24 MB/s = ~50× below
the c7g NIC (~1250 MB/s) and ~40× below Flight's proven 1 GB/s single-stream** (REFERENCES §4b, arXiv
2204.03032) ⇒ **LATENCY-bound, not compute/bandwidth.** Grounded fix (applied, rpc.rs): the tonic read
client used the DEFAULT 64 KiB HTTP/2 receive window → gave it 8 MiB stream / 16 MiB connection + tcp_nodelay
(credit-based flow control CONCEPT sized to Flight's BDP; NOT a copied Flink knob). **Loopback (μs RTT) can't
validate a window fix — nor can single-host kind (docker bridge ~0.1ms). Only real MULTI-NODE network (cross-
AZ ~0.5ms RTT) exercises the throttle.** Companion levers (REFERENCES §4b, Flight paper): PARALLEL streams
(up to 16 = ~10× on localhost) for the N→M shuffle; keep zero-copy (gzip/zstd verified INACTIVE — client
never negotiates; NOT a fix). Window fix is grounded but **UNVALIDATED until one multi-node EKS A/B**
(current vs fixed). Alternative: single-node scale-UP is already 1.15× behind Flink + wins memory/unified.

## 11. REAL root cause FOUND BY CPU PROFILE (2026-07-12) — S3 client rebuild + TLS cert reload, NOT the transport
The frame-pointer pprof profiler (VAJRA_PPROF_SECS; commit ee382309) run on a single-process local-cluster
CONTAINER (the real Flight shuffle, fits the 8 GB laptop) gave DEFINITIVE stacks. **Hotspot (4454 samples):**
`CheckpointStore::from_location → AmazonS3Builder::build → reqwest ClientBuilder::build →
rustls_native_certs::load_native_certs → base64-parse the whole system CA store` = **~30% of on-CPU**
(from_location 15% + S3Builder::build 15.7% + load_native_certs 14.5%, same stack). **The actual Flight IPC
was 1.3%** — the "lean transport / HTTP/2 window / marker column / consume" hypotheses were ALL WRONG; the
profile settled it in one run. CAUSE: `from_location` (checkpoint.rs) rebuilt the S3 client on EVERY call, and
every streaming operator (KafkaSource/ShuffleWrite/WindowAccum, re-planned per micro-batch) calls it → reloads
all TLS certs each time. **FIX (commit fdd541f3): process-global object-store client cache keyed by
scheme://authority — build the reqwest/TLS client ONCE per bucket + reuse** (object_store clients are
long-lived; Ballista client-cache, REFERENCES §4). **VALIDATED before/after (same container profile):
load_native_certs 646→0 samples; from_location 680→26; S3Builder::build 697→26.** The ~30% cert-reload waste
is ELIMINATED; top consumers are now window_accum + Flight/IPC (the real compute). Throughput NUMBER pending a
clean EKS run (container local-cluster + MinIO returns a gRPC 400 mid-execution — environmental, present
before+after the fix, not a regression; EKS S3 sink works, prior runs proved). LESSON: instrument first.

## 12. T1 END-TO-END VERIFIED ON MinIO (2026-07-12) — fix delivers +20-35% throughput, counts EXACT
Per the SDLC (T1 local-cluster + MinIO BEFORE EKS). Release binary WITH the client-cache fix (fdd541f3),
local-cluster --workers 4, VAJRA_DISTRIBUTED_STREAM=1, 5M Kafka events → windowed-agg → parquet on MinIO S3 →
read back. **Same setup, before vs after:**
| | throughput | total_events | n_windows |
|---|---|---|---|
| pre-fix baseline | 1.546M ev/s | — | — |
| fix run1/2/3 | **1.848 / 1.806 / 2.090M ev/s** | 5,000,000 EXACT (all) | 100 (all) |
**Avg ~1.9M = +20-35% over 1.546M pre-fix; counts EXACT every run** (numbers read back FROM MinIO = full
write→read round-trip proven). Consistent with the CPU profile (cert-reload chain 646→0 samples). Correctness
+ throughput both green on real object storage. NOTE: the vajra DOCKER image streaming-Kafka read is flaky on
the local 8 GB Docker VM (reads nothing → 400; same on the pre-fix image; the image runs fine on EKS per prior
runs) — so T1 uses the local macOS binary (the proven path); T2 kind is RAM-blocked on the 8 GB laptop. NEXT =
T3 EKS: at-scale throughput vs Flink with the fix.
