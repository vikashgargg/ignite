# Per-partition watermark (Flink-class) — fix for continuous-mode premature window close

Status: DESIGN + impl step 1 (2026-06-25). Root cause CONFIRMED (docs/STREAMING_ARCHITECTURE.md gap
register; minimal repro `scripts/inc_ckpt_gate.sh PARTS=1`→PASS / `PARTS=4`→partial-count splits).

## Root cause (confirmed)
The realtime/continuous Kafka source is pinned to `parallelism = 1` (kafka/reader.rs:279) because its
EO commit coordination is single-instance (`read_realtime_committed` / `write_staged_epoch_offsets` /
the single `realtime/committed` record). So ONE instance reads ALL N Kafka partitions interleaved out
of event-time order, and the downstream `WatermarkExec` (`withWatermark`) computes a single GLOBAL
`max(event_time)` → it races past slower partitions → windows close before all their events arrive →
partial counts (e.g. 3 then 7 = 10 emitted as two rows). Flink avoids this with **per-partition
watermarks**: each split tracks its own event-time max; the source watermark = MIN across splits, so a
window closes only when EVERY partition has passed it (REFERENCES §2).

## Why NOT multi-instance (option rejected)
Running the realtime source at `parallelism = N` (one instance per partition, like the BOUNDED path,
reusing the proven exchange MIN-merge) would be the obvious reuse — BUT the realtime EO commit is
single-instance (one `realtime/committed`, per-epoch offset staging, sink commits on the aligned
barrier). N instances would clobber that coordination. Deep change → rejected for now.

## Fix: per-partition watermark in `WatermarkExec` (single-instance, contained)
The Kafka source already emits a `partition` Int32 column (reader.rs:217). Make `WatermarkExec`
per-partition aware:
- New optional `(partition_col, num_partitions)`. Default `None` ⇒ today's global-max behavior (no
  regression; non-Kafka / single-partition paths unchanged).
- When set: track `max_et` PER partition value; emit watermark = **MIN over partitions − delay**, and
  **only once all `num_partitions` partitions have been seen** (an unseen partition = −∞ ⇒ withhold —
  this is what stops the race; tracking only seen partitions would still race for not-yet-seen ones).
  `num_partitions` comes from the source (`count_kafka_partitions`), passed by the planner.
- (Follow-up) idleness: a partition with no data for a timeout contributes +∞ so the watermark isn't
  stalled forever by a truly idle partition (Flink `withIdleness`). Not needed for the gate.

### Step 1 (THIS change): the mechanism + unit test
`WatermarkExec::with_partition_watermark(col, n)`; execute tracks `HashMap<i64,i64>` (partition→max_et),
emits monotone MIN−delay once `len==n`. Unit test: partition 0 racing ahead + partition 1 lagging ⇒
emitted watermark tracks the LAGGING partition (min), and nothing emits until both seen.

### Step 2 (integration, next): plumbing + gate
The realtime rewriter must keep the source `partition` column reaching `WatermarkExec` (the user
projection drops it; thread it through, then strip after) and pass `num_partitions`. Then verify
`scripts/inc_ckpt_gate.sh PARTS=4` → 0 dups (NOCRASH) and EO PASS (with crash). Likely also resolves
the F5.3 compaction-multipart + EKS-scale symptoms if they share premature-close.

## Bar vs Flink
Same per-partition-watermark correctness as Flink, but: no JVM/GC, Arrow-columnar, and the watermark
min-merge is unified with the existing `StreamExchangeExec` receiver MIN-merge (one model for source
splits AND keyed-shuffle inputs).
