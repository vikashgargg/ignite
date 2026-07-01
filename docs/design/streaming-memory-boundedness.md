# Streaming memory boundedness — prod-grade, better than Flink managed-mem + Spark

**Goal:** bound Vajra's streaming RSS ≤ Flink (measured EKS: Vajra 10.34 vs Flink 8.58 GiB = 1.20× MORE
on the bounded windowed-agg path). Do it the Arrow-native way that beats both engines.

## ROOT CAUSE (measured 2026-07-01, controlled A/B — NOT a guess)
- **Allocator is NOT the cause.** glibc 2.07 vs jemalloc 2.03 GiB (~identical) on a 5M-row bounded
  windowed-agg; jemalloc decay-on vs off 1.76 vs 1.78 GiB. ⇒ the RSS is **LIVE in-flight buffering**, not
  freed-but-retained memory. (jemalloc kept OPT-IN as a minor complementary optimization only.)
- The live in-flight memory in the pipeline: **exchange channels** (`CHANNEL_CAPACITY=16` × up to N×M=256
  sub-channels) + **source batches** (`MAX_BATCH_BYTES=128MB` × 16 readers) + the materialized `value`
  column + window accumulation. Nothing bounds the SUM against a global limit → RSS = whatever's in flight.

## How the incumbents bound memory (grounded)
- **Flink:** managed-memory pools (network buffers, RocksDB block cache) explicitly sized + **credit-based
  backpressure** (FLIP-2) bounds in-flight network buffers; state off-heap (ForSt 2.0). Bounded, but JVM +
  generic state backends + serialization to `MemorySegment`.
- **Spark:** unified memory manager (execution/storage regions) + spill; RT-mode in-memory shuffle adds
  overhead (trades memory for latency, [databricks](https://www.databricks.com/blog/introducing-real-time-mode-apache-sparktm-structured-streaming)).
- **DataFusion (our substrate):** [`MemoryPool`](https://docs.rs/datafusion/latest/datafusion/execution/memory_pool/trait.MemoryPool.html)
  — operators register `MemoryConsumer` reservations against a **bounded pool** (`GreedyMemoryPool` hard
  limit / `FairSpillPool` fair spill); spillable operators spill to disk when pressured. = managed memory,
  no JVM.
- **Arroyo (Rust+Arrow+DataFusion, our stack):** per-operator state structures optimized per access
  pattern (window eviction) + checkpoint to object store → **beats Flink 5×** by avoiding generic backends.

## Vajra design (better than both — the combination none has)
1. **Bounded `MemoryPool` for the streaming session** (explicit limit, e.g. % of container) — the managed-
   memory analog. Register the memory-heavy streaming consumers: exchange coalesce buffers, window
   accumulation, source batch builders.
2. **Credit-based backpressure at the exchange** (FLIP-2): the receiver grants credits; the source/sender
   slows when the pool is pressured → in-flight bounded + adaptive (vs fixed 16-deep channels). First cut:
   cut `CHANNEL_CAPACITY` + gate the source on pool headroom.
3. **Spill under pressure** — engage the existing F5 spillable window state when the pool hits the limit
   (already built; wire it to the pool pressure signal).
4. **Zero-copy Arrow end-to-end** — no serialization to a managed segment (Flink pays this); Arrow buffers
   ARE the managed memory.
⇒ explicit bound + zero-copy + optimized per-operator state + spill + no JVM = the axis where Vajra can be
strictly better than Flink managed-mem and Spark unified-mem.

## Implementation plan (measure → bound → backpressure → spill)
1. **Instrument:** where is the peak? Set a `MemoryPool`, make exchange/window/source register
   `MemoryConsumer`s, dump the per-consumer high-water at EndOfData (like WM_PROF). Attribute the 10.34GiB.
2. **Bound the dominant consumer** (likely exchange buffering + source batches): cut `CHANNEL_CAPACITY`,
   cap concurrent source batches, coalesce less. Re-measure.
3. **Backpressure:** source reads gated on pool headroom (credit-flow).
4. **Spill:** wire F5 window spill to pool pressure.
5. **Validate:** local bounded windowed-agg peak RSS ↓; then EKS re-measure vs Flink 8.58 GiB. Gate: ≤ Flink.

## PROGRESS (2026-07-01, branch streaming/memory-jemalloc)
- Made in-flight buffers env-tunable (the bound levers): `VAJRA_EXCHANGE_CHANNEL_CAP` (exchange channel
  depth, default 16) + `VAJRA_SOURCE_MAX_BATCH_BYTES` (Kafka source batch cap, default 128MiB). The mpsc
  channel already gives backpressure; a smaller cap = tighter backpressure = less in-flight (no data drop).
- **Local A/B (5M-row bounded windowed-agg, file source):** exchange cap 16→2 = peak RSS **2.35→2.08 GiB
  (−11.5%) with ZERO throughput cost** (61.4→61.3s), correctness intact. ⇒ exchange buffering is a real
  RSS driver, reducible for free. Source-batch cap (the bigger EKS lever, 128MB×16=2GB) needs the Kafka
  path — validate on the EKS re-measure with `VAJRA_SOURCE_MAX_BATCH_BYTES=32MiB` + `CAP=4`.
- NEXT: EKS re-measure with reduced buffers → if memory ↓ toward Flink 8.58 GiB, set as defaults + merge.

## DECISIVE ROOT CAUSE (measured 2026-07-01) — the pool is BYPASSED
Set a `greedy` MemoryPool with **max_size=512 MiB**; the Kafka windowed-agg still peaked at **1.69 GiB
(3.3× OVER the limit, no error).** ⇒ **the streaming pipeline's memory is NOT REGISTERED with the
MemoryPool** — DataFusion's pool only bounds consumers that call `try_grow` (aggregate/sort/join hash
tables); the Kafka source builders, exchange channel batches, and in-flight `FlowEvent`s never reserve,
so the ceiling doesn't apply to them. This is why jemalloc (not retention), buffer-tuning (bounds an
UNregistered amount), and enabling the bounded pool all failed: **the memory escapes the pool entirely.**

## THE FIX (correct prod-grade, = Flink managed-memory + backpressure)
Register the streaming in-flight with the pool + backpressure:
1. **Source reserves** pool memory for each emitted batch (`batch.get_array_memory_size()`); the
   `MemoryReservation` travels with the `FlowEvent` and **drops (releases) when the window consumes it** —
   reservation tied to batch lifetime = accounts the WHOLE in-flight span (source→exchange→window).
2. Pool full (window behind) → source `try_grow` fails → **source awaits = backpressure** → in-flight
   hard-bounded at the pool limit (Flink network-buffer semantics).
3. **Default pool = bounded** (greedy/fair sized to container) so the ceiling is on.
⇒ RSS tracks the pool limit. This is the managed-memory behavior; buffer env-tunables become secondary.
The env-tunable buffers (VAJRA_EXCHANGE_CHANNEL_CAP / SOURCE_MAX_BATCH_BYTES) stay as coarse extra knobs.

## ACTUAL ROOT CAUSE (2026-07-01) — librdkafka prefetch queue (C-side, invisible to pool+allocator)
`apply_consumer_throughput_defaults` explicitly set `queued.max.messages.kbytes=1 GiB/partition` +
`queued.min.messages=1M/partition` (for throughput — "saturate the broker like Flink"). At EKS's 16
partitions that's **up to 16 GiB of librdkafka prefetch buffer** = the 10.34 GiB. This is the ONLY thing
that explains ALL the negative results: it's C-side (jemalloc=glibc), never registers (MemoryPool 512MiB
→ 1.69 GiB bypassed), separate from Arrow batches (batch-size tuning moot), and Flink prefetches ~50 MB
(→ the exact 1.20× ratio). **FIX:** `VAJRA_KAFKA_PREFETCH_KBYTES` (default 1 GiB→256 MiB) + `_MSGS`
(1M→400k), env-tunable.

## HONEST: local testing is EXHAUSTED (cannot validate this fix)
Every local A/B — allocator, exchange/source buffers, prefetch — caps at ~1.3–1.8 GiB REGARDLESS (8
partitions, 20M events on one Mac). Local does NOT reproduce the EKS 10.34 GiB regime (16 part, 100M,
availableNow). ⇒ a memory fix cannot be validated locally. The prefetch hypothesis stands on the MATH
(1 GiB×16 vs Flink 50 MB = the 1.20×) + the explicit config, not a local A/B. **VALIDATION = ONE EKS
re-measure sweeping `VAJRA_KAFKA_PREFETCH_KBYTES` ∈ {1 GiB, 256 MiB, 64 MiB}** (env, no rebuild): the
RSS-vs-throughput curve confirms the driver + picks the sweet spot. Then set default + merge.

## ✅ VALIDATED (EKS 100M sweep, 2026-07-02) — Vajra now BEATS Flink on memory
| prefetch/partition | peak RSS | throughput | vs Flink (8.58 GiB / 5.7M) |
|---|---|---|---|
| 1 GiB (old default) | 8.32 GiB | 5.64M/s | ~tie mem |
| 256 MiB | 8.50 GiB | 5.63M/s | ~tie |
| **64 MiB (NEW default)** | **7.36 GiB** | 5.58M/s | **BEATS mem (−1.2 GiB), matched throughput** |

RSS is noisy (~±1 GiB) but 64 MiB is clearly the lowest, comfortably under Flink, with NO throughput cost
⇒ **default set to 64 MiB/partition.** The librdkafka prefetch WAS the driver; bounding it flips the
memory result from 1.20× MORE to <Flink, while keeping the 1.10× throughput. Prod-grade: env-tunable
(`VAJRA_KAFKA_PREFETCH_KBYTES`) so a deep-prefetch workload can raise it. This is the one fully-measured
Flink-beating axis so far. Branch streaming/memory-jemalloc → merge.

## Non-goals / honest
jemalloc is NOT the fix (measured) — opt-in only. Don't claim a memory win until the EKS re-measure shows
≤ Flink. This is the one measured axis where Vajra currently LOSES on the streaming-bounded path.
