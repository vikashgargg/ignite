# Streaming Performance Program — beat Flink/RisingWave/Arroyo on every pillar (SDLC board)

**Charter (AIM):** Zelox (single-binary, no-JVM, Arrow+DataFusion) must OBJECTIVELY BEAT Flink (streaming),
RisingWave 3.0, Arroyo 0.15 on throughput, memory, latency, and speed — by learning their proven designs
and eliminating their limitations, not copying. Every epic below is grounded in a named credible source,
gated T1(local)→T2(kind)→T3(EKS), and DONE only when it **beats Flink on the measured number**.

## Honest baseline (measured EKS 100M realtime, 2026-07-18 — the gap we're closing)

| Pillar | Zelox now | Flink | Target (DONE bar) |
|---|---|---|---|
| Correctness | 10 win/100M, byte-identical | 10 win/100M | ✅ **DONE — TIE, 0 mismatch** |
| Throughput | 4.0M ev/s | 10.0M ev/s | **≥ Flink (≥10M)** |
| Memory (windowed) | 12.17 GiB | 9.03 GiB | **≤ Flink** |
| Memory (passthrough) | 13.07 GiB | 3.88 GiB | **≤ Flink** |
| Latency p50 / tail | 101 / 136 ms | 95 / 136 ms | **≤ Flink, win the tail (no-GC)** |

*Zelox WINS memory in CONTINUOUS mode (3.9 vs 4.68) — so the realtime gaps are fixable, not fundamental.
Arroyo (Rust+Arrow+DataFusion) beats Flink 3–5× ⇒ the deficit is our execution model, not the language.*

---

## EPIC-M — Memory: realtime → ≤ Flink (port continuous's bounded discipline)  [P0, in progress]
**Grounded design (learn + eliminate limits):**
- **Flink FLIP-2 credit-based flow control** — receiver grants the sender exact buffer credits; in-flight
  network memory is bounded, backpressure exact. *Limit to eliminate:* Flink's credits are fixed-size
  network buffers (coarse); we do **byte-credits** on Arrow batches (finer, no buffer-count tuning).
- **RisingWave 3.0 network-buffer backpressure** — bounded exchange channels prevent OOM.
- **Polars streaming** — per-morsel `SemaphorePermit` bounding total in-flight BYTES + spillable OOC sinks.
- **Our own continuous mode** — frees plan/stream (reader prefetch + exchange + sink buffers) per
  micro-batch (streaming.rs:415). Realtime holds one unbounded pipeline → accumulates.

**Root cause (grounded):** window STATE already bounded both modes (F5 spill + `bounded_agg_context`); the
gap is realtime PIPELINE BUFFERS: reader prefetch (64 MiB×16≈1 GiB) + exchange N×M fixed 16-batch cap +
Kafka sink producer buffer, all live for the whole run. Passthrough (no windows) = 13 GiB proves it.

**Tickets:**
- M1 [T3-profile] EC2 heap profile (jemalloc) of realtime passthrough → exact heap vs page-cache vs
  fixed-buffer breakdown (page-cache in cgroup memory.peak may inflate the number — must separate).
- M2 credit-based flow control on `StreamExchange` edge: receiver-granted byte-credits, replace the fixed
  `ZELOX_EXCHANGE_CHANNEL_CAP=16`. Grounded FLIP-2/RisingWave.
- M3 per-morsel byte-permit across the realtime pipeline (Polars) — bound total in-flight bytes.
- M4 cap realtime reader prefetch + Kafka sink producer buffer (rebuilt small like continuous).
**DONE:** realtime windowed + passthrough RSS ≤ Flink at EKS 100M.

## EPIC-T — Throughput: realtime 4M → ≥ Flink 10M  [P0]
**Grounded design:**
- **Arroyo** (our exact stack) beats Flink 3–5×: specialized window operators, interpreted Arrow columnar
  + SIMD, **off-path async checkpointing** (checkpoint never stalls the record path). *Limit to eliminate:*
  Arroyo's SQL-only surface — we keep the Spark API.
- **Flink** mini-batch + credit flow + pipelined operators. **DataFusion** morsel-driven vectorized exec.
- **FLIP-27** batch source (we have batch-queue 2.8×, but DEFAULT OFF — make it the one proven path).

**Root cause (traced):** single-node `StreamExchange` on the critical path (hash-route→N sub-channels→
coalesce→MIN-merge) + **synchronous S3 epoch commit** inflating the drain wall (consumption ≈5M/s already).

**Tickets:**
- T1 make batch-queue the DEFAULT source path (remove StreamConsumer/poll/LEGACY_POLL alternates).
- T2 **off-path async epoch commit** (Arroyo model) — S3 commit leaves the record path.
- T3 shuffle-edge: coalesce + `Utf8View` zero-copy; skip the keyed reshuffle when parallelism fits one node.
- T4 parallel Flight streams for true cross-node.
**DONE:** realtime drain ≥ Flink 10M ev/s at EKS 100M, counts exact.

## EPIC-L — Latency: win the tail (no-GC)  [P1]
**Grounded design:** Spark 4.1/4.2 RTM concurrent-stage + in-mem shuffle; no-JVM no-GC should win the tail.
**Root cause:** current 100ms floor is Kafka-linger-bound (masks engine latency); realtime commit cadence
adds a few ms. **Tickets:** L1 linger-isolated latency harness (expose real engine latency); L2 shrink
realtime commit interval off the record path. **DONE:** p50 ≤ Flink AND tail (p99.9/max) < Flink.

## EPIC-C — Cleanup: remove unproven code (prod-grade surface)  [P1, interleave]
Verify-then-remove (look-before-delete; coalesce_flow_events is ACTUALLY wired in Flight shuffle — keep):
`ZELOX_T7_FUSE` (source fusion, opt-in, unproven), `ZELOX_RT_SINGLE` (legacy reader opt-out),
`ZELOX_KAFKA_LEGACY_POLL` (poll opt-out), dual idle → **E4-only** (streaming-watermark.md debt), unused
sweep knobs. **DONE:** one proven path each, clippy+tests green, no behavior regression.

## SDLC / board discipline
Each ticket: ground the source (cite) → design → T1 local self-checking → T2 kind → T3 EKS confirm (tear
$0) → merge + docs same turn. No merge without the measured beat. Board cell in [BOARD.md](../BOARD.md)
updated + commit linked the same turn. NO patch-mode: one prod-grade fix per ticket, grounded first.

---
*Sources: Arroyo blog (why-arrow-and-datafusion; how-arroyo-beats-flink-at-sliding-windows) · Flink FLIP-2
credit flow / FLIP-27 · RisingWave 3.0 · Polars streaming · [REFERENCES.md](../REFERENCES.md) §1-9. Baseline
measured EKS 100M 2026-07-18.*
