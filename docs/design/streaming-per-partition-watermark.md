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

### ⚠️ CRITICAL (found 2026-06-29): withhold-until-all-N has NO idleness fallback → HANG
The general step-2 attempt (rewriter auto-preserves the source `partition` col + sets N) was
implemented and BUILT, but on the gate (general query, PARTS=4) it **HUNG for 3+ hours**: the
per-partition watermark **withholds the watermark until all N partitions are seen**, and when that
never happens (partition not correctly reaching WatermarkExec, OR fewer than N distinct partitions in
the data), the watermark **never advances → windows never close → `availableNow` never terminates**.
A pipeline that can **stall forever is WORSE than the dup bug** it fixes. **This latent risk is in the
MECHANISM itself** — the prove-it only passed because all N partitions got data.
**REQUIRED before enabling per-partition anywhere:** an **idleness/timeout guard** (Flink
`withIdleness`) — a partition with no data for a bound contributes +∞ so the MIN advances; never stall.
AND verify partition detection actually reaches WatermarkExec (the hang suggests it may not have in the
general query). Step-2 populator REVERTED (commit clean); mechanism + gated prove-it + step-1 plumbing
remain. Redo step-2 ONLY after the idleness guard + a SHORT bounded test (never a 40s×continuous run
that can hang for hours).

### Step 1 (THIS change): the mechanism + unit test
`WatermarkExec::with_partition_watermark(col, n)`; execute tracks `HashMap<i64,i64>` (partition→max_et),
emits monotone MIN−delay once `len==n`. Unit test: partition 0 racing ahead + partition 1 lagging ⇒
emitted watermark tracks the LAGGING partition (min), and nothing emits until both seen.

### Step 2 — CONFIRMED a real multi-component change, not plumbing (2026-06-25)
Blockers found (one batched read; record so never re-derived):
- `WatermarkNode` is created at SQL resolution (`resolver/query/misc.rs:205`) — BEFORE the streaming
  rewriter, so no source/partition-count context there.
- `rewrite_streaming_plan` is SYNC; `count_kafka_partitions` is ASYNC; `StreamSource` has NO
  partition-count accessor. So the exact `N` can't be threaded at plan time without new machinery.
- The user projection drops the source `partition` column before the watermark.
CRUX = get the exact Kafka partition count `N` to `WatermarkExec` (withhold-until-all-N-seen needs the
real N: an over-estimate stalls forever, under-estimate still races). Options:
  (a) add a sync partition-count accessor to the Kafka `StreamSource` (cached from metadata) + thread
      `N` + `partition_col` onto `WatermarkNode` in the rewriter (which has the source), planner passes
      to `WatermarkExec.with_partition_watermark`; AND preserve the `partition` column to the watermark
      (augment the projection chain, or have the realtime decode keep it).
  (b) EXECUTION-TIME discovery: `WatermarkExec` learns `N` from the source via a one-time control
      signal (e.g. the realtime source emits an initial `partitions=N` marker) — avoids plan-time async.
  Recommendation: (b) is cleaner (no async-at-plan, no projection surgery for N) but needs a new
  control marker; (a) reuses existing markers but needs projection preservation + a sync accessor.
CHEAP PROVE-IT path (if validating the mechanism before the full change): gate query keeps `partition`
(`select k, et, partition`) + planner enables per-partition when `partition` ∈ input schema with N from
an env override; confirm `inc_ckpt_gate.sh PARTS=4` → 0 dups. Then generalize.

### Step 2 (integration): plumbing + gate
The realtime rewriter must keep the source `partition` column reaching `WatermarkExec` (the user
projection drops it; thread it through, then strip after) and pass `num_partitions`. Then verify
`scripts/inc_ckpt_gate.sh PARTS=4` → 0 dups (NOCRASH) and EO PASS (with crash). Likely also resolves
the F5.3 compaction-multipart + EKS-scale symptoms if they share premature-close.

## Bar vs Flink
Same per-partition-watermark correctness as Flink, but: no JVM/GC, Arrow-columnar, and the watermark
min-merge is unified with the existing `StreamExchangeExec` receiver MIN-merge (one model for source
splits AND keyed-shuffle inputs).
