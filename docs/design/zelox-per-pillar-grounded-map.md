# Zelox per-pillar grounded map — where we ACTUALLY stand vs Flink (measured, honest)

> **The standing question:** *"Zelox is a single binary, no-JVM, no serialize/deserialize — why is it
> still slower than Flink?"* This doc answers it per pillar with **measured EKS 100M numbers**, the
> **exact mechanism** where Zelox diverges from the credible engines, and the **prod-grade design** to
> close it (built on their proven work, cited). No claim here is un-measured unless labelled UNMEASURED.

## The measured truth (EKS 100M realtime, 2026-07-18, fresh cluster, same c7g.4xlarge node, sequential)

Zelox `.trigger(realTime=…)` (rt43, batch-queue ON) vs Flink 1.19 unbounded streaming, both 100M+16 closers,
identical 10s tumbling keyed COUNT, both output-completeness-timed to the sink:

| Pillar | Zelox | Flink | Verdict |
|---|---|---|---|
| **Correctness / completeness** | 10 win / 100M / per_group=10000 | 10 win / 100M / per_group=10000 | **TIE — byte-identical output, 0 mismatch** |
| **Throughput** (backlog drain→output-complete) | 24.9s = **4.0M ev/s** | 10s = **10.0M ev/s** | **LOSE ~2.5×** |
| **Peak RSS** (windowed agg) | **12.17 GiB** | 9.03 GiB | **LOSE 1.35×** |
| **Peak RSS** (Kafka→Kafka passthrough) | **13.07 GiB** | 3.88 GiB | **LOSE 3.4×** |
| **Latency** Kafka→Kafka p50 / p99 / max | 101 / 131 / 136 ms | 95 / 127 / 136 ms | **LOSE (slight); tail ties** |

**The honest verdict: on this apples-to-apples realtime path Zelox TIES correctness and LOSES every
performance pillar.** The no-JVM/columnar edge is NOT translating into streaming wins. Prior "latency win /
memory win" claims were from *different configs* (kind, low parallelism) and DO NOT hold here — they are
retracted. This is a **dataflow/execution-model** deficit, not a language one — proven by Arroyo (Rust +
Arrow + DataFusion, our exact stack) beating Flink 3–5× [Arroyo blog]. Language is not the excuse.

## Per-pillar: credible design → Zelox's measured lag → exact mechanism → prod-grade fix

### 1. Throughput — LOSE 4.0M vs 10.0M (~2.5×)
- **Credible:** **Arroyo** (Rust/Arrow/DataFusion) beats Flink 3–5× (10× sliding) via interpreted Arrow
  columnar + SIMD, specialized window operators, and async (off-path) checkpointing [arroyo.dev/blog:
  why-arrow-and-datafusion, how-arroyo-beats-flink-at-sliding-windows]. **Flink** pipelined operators +
  credit flow + mini-batch. **DataFusion** morsel-driven vectorized exec.
- **Zelox mechanism (measured/traced):** batch-queue fixed the SOURCE (2.8×), but the **single-node
  `StreamExchange` is on the critical path**: every batch is hash-routed to N sub-channels, markers
  broadcast N×, coalesced, then MIN-merged (`merge_output_subchannels`) — even single-node. Prior EKS
  profiling: `shuffle_recv` blocked-wait dominates. The S3 sink also commits per 5s epoch, inflating the
  output-complete wall (consumption alone ≈ 5M/s).
- **Prod-grade fix (grounded, not yet done):** (a) **specialized tumbling-window operator** that avoids
  the full keyed re-shuffle when parallelism fits one node (Arroyo-style); (b) coalesce + `Utf8View`
  zero-copy on the shuffle edge; (c) async/off-path epoch commit (Arroyo checkpoint model) so S3 latency
  leaves the throughput path; (d) parallel Flight streams for the true cross-node case.
- **Status: LOSE — root-caused, fix designed, NOT implemented.**

### 2. Memory — LOSE 12–13 GiB vs 3.9–9 GiB (up to 3.4×)  ← the most damning, most actionable
- **Credible:** **Flink FLIP-2 credit-based flow control** — the *receiver* grants the sender exact buffer
  credits; network memory is bounded and backpressure is exact, never OOM. **Flink 2.0 ForSt**: state
  off-heap/disaggregated. **Polars** streaming: per-morsel `SemaphorePermit` + spillable out-of-core
  sinks. **RisingWave 3.0**: network-buffer backpressure. [REFERENCES §3, §9]
- **Zelox mechanism (from code, self-admitted):** memory = **live in-flight buffering across N×M
  sub-channels** with a *fixed* mpsc cap of 16 batches/channel ([exchange.rs:30-45] comment) — a COARSE
  analog of credit flow, not real credit flow. Plus **Kafka prefetch 64 MiB × 16 partitions ≈ 1 GiB**
  ([reader.rs:260]). No per-morsel permit; buffers sized by count, not bytes/credits → 13 GiB for a
  passthrough that Flink does in 3.9.
- **Prod-grade fix (grounded):** implement **real credit-based flow control** on the shuffle edge
  (FLIP-2 / RisingWave network-buffer): receiver-granted byte-credits, not a fixed batch cap; **per-morsel
  permits** (Polars) bounding total in-flight bytes; cut default prefetch. This is a **memory-DISCIPLINE**
  build, not a GC win — "no-JVM" never bounded RSS by itself.
- **Status: LOSE — mechanism known, real credit-flow NOT implemented (only the coarse cap exists).**

### 3. Latency — LOSE p50 101 vs 95 ms (slight; tail ties at 136)
- **Credible:** **Spark 4.1/4.2 RTM** concurrent-stage + in-memory shuffle; **Flink** continuous; no-JVM
  no-GC should win the tail. [REFERENCES §1, §3c]
- **Zelox mechanism:** the ~100ms floor on BOTH engines = Kafka sink `linger`/batching dominates at
  20k/s, masking engine latency. Zelox slightly worse — the realtime per-epoch commit cadence + passthrough
  path add a few ms. The prior kind win (30 vs 42) was a lower-rate/parallelism config, not this one.
- **Prod-grade fix:** measure at the engine boundary (not Kafka-linger-bound); shrink the realtime commit
  interval off the record path; confirm the no-GC tail advantage on a linger-controlled harness.
- **Status: LOSE (slight) — needs a linger-isolated measure to expose the real engine latency.**

### 4. Reliability / correctness — TIE (WON the completeness bug)
- Byte-identical windowed output to Flink (0 mismatch), crash-EO dup=0 (prior), aligned barriers
  (Chandy-Lamport), inc-checkpoint O(delta). The watermark flush-before-idle fix (commit 9ae02e7e) closed
  the last completeness gap. **This pillar is genuinely at parity.**

## Unproven / unwanted code to REMOVE (prod-grade cleanup, user-requested)
Measure-or-delete. Candidates (verify each is off the proven path before removing):
- `coalesce_flow_events` combinator — OFF by default, never proven (needs drain guarantee).
- **Dual idle mechanisms** — wall-clock `active_partition_watermark` vs source-signaled `Idle` (E4).
  Target = **E4-only** (see streaming-watermark.md consolidation debt).
- Gated experiments with no proven win: `ZELOX_T7_FUSE` (source fusion), `ZELOX_KAFKA_LEGACY_POLL`,
  `ZELOX_RT_SINGLE`, `ZELOX_KAFKA_PREFETCH_*` sweep knobs, `ZELOX_SHUFFLE_*` sweep knobs,
  `ZELOX_SOURCE_MAX_BATCH_BYTES` if unused.
- Any `G1/G2` prefetch/raw-TCP remnants (were measured marginal).

## Execution sequence (AIM way — design-first, then build, then measure)
1. **Remove** the unproven code above → smaller, prod-grade surface.
2. **Build the two real levers, grounded:** (a) credit-based flow control on the shuffle edge (memory);
   (b) off-path async epoch commit + shuffle coalesce/Utf8View (throughput).
3. **End-to-end retest** batch (vs Spark) + realtime (vs Flink) + latency, with the identical-output check.
4. **Merge + docs** with the new measured numbers.
5. **ONE final EKS** to confirm the pillars moved.

---
*Sources: [Arroyo blog](https://www.arroyo.dev/blog/) (why-arrow-and-datafusion; how-arroyo-beats-flink-at-sliding-windows) · [REFERENCES.md](../REFERENCES.md) §1 Spark RTM · §2/§2d Flink FLIP-27/checkpoint · §3 Flink 2.0 ForSt · §8 Flight shuffle · §9 Polars/Arroyo 0.15/RisingWave 3.0. Measured EKS 100M 2026-07-18.*
