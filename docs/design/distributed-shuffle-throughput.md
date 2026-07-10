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
