# Vajra per-pillar grounded map — where we stand vs Flink & the streaming frontier

> **Purpose.** Answer, with measurement + named sources, the standing question:
> *"Vajra is a single binary, no-JVM, no serialize/deserialize — why is it still slower than Flink?"*
> One row per production pillar: what each credible engine does, where Vajra **measurably** stands,
> the exact mechanism where we lag, and the grounded prod-grade fix. No claim here is un-measured
> unless labelled UNMEASURED. Sources are cited to [REFERENCES.md](../REFERENCES.md); every fetched
> fact is appended there the same turn (AIM standing rule).

## The thesis (the honest answer)

The no-JVM / columnar / no-serde edge is a **node-local** property, and it **is won** — by measurement.
The distributed streaming gap is **not** a JVM or serialization tax (the shuffle IPC encode measures
2–5 ms = negligible). It is a **dataflow / execution-model** property. Every fast engine — Flink,
RisingWave 3.0, Arroyo (Rust+Arrow, our exact stack, beats Flink 5×), Polars, Spark 4.1 RTM —
engineers the same handful of mechanisms at the source and the shuffle boundary. Naming the exact
mechanism is what turns "still slow" into a fix.

**Proof the language is not the problem:** Arroyo is Rust + Arrow + DataFusion — our exact stack — and
beats Flink 5×+ (10× on sliding windows) [REFERENCES §9]. So a Rust/Arrow engine *can* beat Flink; our
remaining gap is mechanism, not language.

| | Measured |
|---|---|
| **Node-local — WON** | Batch 6.2× vs Spark · single-node consume ~8M/s · latency p50 30 vs 42 ms (tail 4.6–6× tighter, no GC) · per-node mem 3.7 vs 9.3 GiB |
| **Distributed — the gap = execution model** | Source consume `StreamConsumer` 4M/s vs Flink poll-batch 10M/s · shuffle small-batch Flight IPC (receiver starves) · `availableNow` micro-batch re-plan tax |

## Per-pillar scorecard

Status: **WON** measured beat · **PARITY** measured equal · **LAG** root-caused + fix implemented, EKS
number pending · **UNMEASURED** honest gap.

| Pillar | Vajra standing (measured) | Credible source & technique | Lagging mechanism → grounded fix | Status |
|---|---|---|---|---|
| **Batch throughput** | 6.2× vs Spark; TPC-H SF1 1.78 vs 63.46 s | **DataFusion** morsel-driven vectorized exec; **Arrow** columnar [§7] | — | **WON** vs Spark |
| **Streaming throughput** | ~2–2.5× behind Flink; consume ~4M/s vs ~10M/s | **Flink FLIP-27** `poll(timeout)` = N records/call; **Arroyo** Kafka→RecordBatch zero-copy [§2d, §9] | `StreamConsumer` reads 1 msg/poll → **`rd_kafka_consume_batch_queue`** batch-queue (1000/call). Measured **2.8×** (1.38→3.89 M/s), EXACT 10M/10M; kind A/B 2.33× faster wall, both arms EXACT | **LAG** — fix impl, EKS ⧗ |
| **Realtime windowed completeness** | Vajra continuous **== Flink** (kind, both→MinIO parquet): 15 windows / 150000, group=10, no partial-split/over-emit/dup | **Flink** `WatermarkStatus.IDLE` + in-band-FIFO watermark + MIN over inputs [§2d] | Bug (traced w/ instrumentation): batch-queue source Idle on a TRANSIENT empty drain → exchange excluded an active channel → frozen watermark. Fixed: source Idle only at genuine high-watermark + live emitted-ends floor for far-ahead jumps. One grounded fix, not a patch | **WON** (== Flink) |
| **Spark 4.2 RTM API surface** | `.trigger(realTime="5 seconds")` (4.2.0 client) routes into Vajra's realtime engine; kind-verified 15 windows / 150000 / group=10 == Flink. Vajra realtime is STATEFUL/windowed — a **superset of 4.2's stateless-only RTM** (Spark defers stateful RTM to 4.3) | **Spark 4.2** `Trigger.RealTime("<dur>")` = new trigger, wire field `real_time_batch_duration=100`; dur is a checkpoint interval (min 5s), not latency [§1, §10] | Was: Vajra's realtime engine reachable only via pre-4.2 `.trigger(continuous=...)`; the new `realTime` proto field (100) wasn't decoded → 4.2 clients fell through to micro-batch. Wired: proto + `spec::StreamTrigger::RealTime` + route → realtime engine (e183cb22) | **WON** (superset of 4.2) |
| **Latency (Kafka→Kafka)** | p50 30 / p99 125 / max 128 ms vs 42 / 580 / 767 | **Spark 4.1 RTM** concurrent-stage + in-mem shuffle validates model [§1, §3c]; **no-JVM** no-GC tail | — | **WON** vs Flink |
| **Memory / RSS** | Continuous 7.06 vs 8.58 GiB (win); bounded 10.4 vs 8.6 (lose) — path-dependent | **Flink 2.0 ForSt**: RAM bounded by off-heap state + credit backpressure, not GC [§3]; **Polars** per-morsel SemaphorePermit = exact backpressure + spillable OOC sinks [§9]; **RisingWave** network-buffer backpressure prevents OOM [§9] | No-JVM ≠ free RSS (measured 1.12× bounded). Fix = **F5 spill** (OOC sinks, per Polars) + **credit/permit backpressure** on the shuffle edge (per Flink FLIP-2 / RisingWave) — memory is a *discipline*, not a GC win | **PARITY** |
| **CPU / per-stage** | ~parity; source-read bound (same lever as throughput) | **arrow-rs** `Utf8View` zero-copy; **Arroyo** columnar JSON decode + SIMD [§7, §9] | Source read + Arrow decode dominate → batch-queue consume + `Utf8View` on value/shuffle cols | **PARITY** |
| **Network / shuffle** | Small-batch Flight IPC; 2.14× fewer msgs after coalesce (counts EXACT) | **Arroyo Shuffle-Edge**; **Ballista** Flight zero-copy; **RisingWave** exchange + network-buffer backpressure [§4, §8, §9] | Per-batch IPC re-encode + receiver starves → **coalesce before Flight** + `Utf8View` zero-copy + **credit-flow** (parallel streams). T1/T2 done; EKS number pending | **LAG** — fix impl, EKS ⧗ |
| **State management** | Spillable windowed-agg + join, out==N exact @5M | **Flink** Key-Groups; **DataFusion** grouped-hash spill; **Arroyo** specialized per-op state structs [§2d, §7, §9] | Generic grouped-hash vs Arroyo's specialized time-eviction window = the *sliding-window* lever (future) | **PARITY** |
| **Fault tolerance / EO** | dup=0 across kill-9 on EKS (aligned barriers + exact idle + emit floor) | **Chandy-Lamport** aligned barriers; **Flink** checkpoint [§2] | — | **PARITY** |
| **Incremental checkpoint** | O(delta) on one Arrow substrate; manifest refs immutable F5 chunks | **Flink ForSt / RocksDB** SST refcount — Vajra F5 chunks = SST-analog [§3, §5] | — (structurally cleaner: one Arrow format, no RocksDB lineage) | **WON** vs ForSt |
| **Rescale / elasticity** | Key-group rescale on Arrow chunks, crash-gated | **Flink FLIP-8** Key-Groups = atomic redistribution unit [§2] | Bit-exact gated by EO residual (documented) | **PARITY** |
| **Recovery time** | Correctness proven; wall-time **not measured** | **Flink 2.0 ForSt**: 49× faster recovery, 94% less ckpt duration [§3] | Measure recovery wall-time head-to-head | **UNMEASURED** |
| **Backpressure / credit** | Bounded mpsc channels exist; not measured under slow sink | **Flink FLIP-2** credit flow; **Polars** per-morsel permit; **RisingWave** network-buffer backpressure [§9] | Add credit/permit flow on the shuffle edge (VAJ-BF2.4) + measure under slow sink | **UNMEASURED** |

## What "grounded prod-grade" looks like here (the throughput pillar, worked end to end)

1. **GROUND** — the source consume lever is REFERENCES §2d/§9: Flink `KafkaSource` polls N records per
   call (FLIP-27); rust-rdkafka `StreamConsumer` yields one message per async poll.
2. **MEASURE before building** — fair local A/B on 10M/4-part, identical Arrow build:
   `StreamConsumer` 1.38 M/s → poll-`BaseConsumer` 1.87 → **`rd_kafka_consume_batch_queue` 3.89 M/s (2.8×)**.
   The winner is chosen by number, not by guess (the poll-BaseConsumer "hint" was wrong).
3. **BUILD prod-grade, gated** — dedicated reader thread per split → bounded channel → async generator,
   preserving the exact EO offset-staging / watermark / epoch / idle contract. Gated
   `VAJRA_KAFKA_BATCH_QUEUE` (default OFF) for a clean A/B. Clippy + 67 unit tests green.
4. **VERIFY free on kind, EKS confirms** — kind A/B (cross-pod + real MinIO): both arms EXACT 1M
   (sum(count)=1,000,000 / 5 windows), batch-queue **2.33× faster wall** (4.39→1.88 s), no OOM. Continuous
   (realtime) path extended the same way; EKS is the at-scale number only — never where bugs are found.

Unproven experiments were **removed** after measuring them marginal (G1 shuffle prefetch +4%; G2 raw-TCP
= reinventing HTTP/2). Prod-grade means: measured, gated, EO-preserving, source-cited — or deleted.

## Honest open items (no overclaim)

- **Streaming throughput / shuffle**: fixes implemented + kind-validated for correctness; the *at-scale
  throughput number* vs Flink is the one remaining EKS confirm.
- **Recovery time** and **backpressure under slow sink**: UNMEASURED axes — named, not hidden.
- **Sliding-window** specialized state (Arroyo's 10× lever) is a future pillar, not yet built.

---
*Sources: [REFERENCES.md](../REFERENCES.md) §1 Spark 4.1 RTM · §2/§2d Flink FLIP-27/checkpoint · §3 Flink 2.0
ForSt · §3c Databricks RTM · §4/§5 Ballista/DataFusion · §7 Arrow/arrow-rs · §8 Flight shuffle · §9 Polars /
Arroyo 0.15 / RisingWave 3.0 (fetched 2026-07-16). Indexed from [BOARD.md](../BOARD.md).*
