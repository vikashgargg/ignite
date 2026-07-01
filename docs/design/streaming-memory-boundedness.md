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

## Non-goals / honest
jemalloc is NOT the fix (measured) — opt-in only. Don't claim a memory win until the EKS re-measure shows
≤ Flink. This is the one measured axis where Vajra currently LOSES on the streaming-bounded path.
