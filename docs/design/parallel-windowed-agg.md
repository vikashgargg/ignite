# Parallel keyed windowed aggregation — closing the throughput gap vs Flink

Status: design (grounded in Flink 1.19, Spark 4.1 Real-Time Mode, DataFusion, Arrow).
Owner: streaming. Supersedes the single-threaded watermark funnel in the keyed
windowed-agg path.

## Problem (measured, EKS c7g.4xlarge, 100M-event 10s tumbling COUNT)

Verified head-to-head on a dedicated 2-node topology (Kafka isolated):

| Engine | wall | throughput | peak RSS | reads 100M? |
|---|--:|--:|--:|:--:|
| Flink 1.19 | 17.3 s | 5.8M ev/s | 8.6 GiB | yes (verified) |
| Zelox (before) | 65 s | 1.5M ev/s | 1.3 GiB | **no — 95.07M (under-read, fixed separately)** |

Zelox is ~3.8× slower **on this workload** despite 6.4× less memory. The earlier
"Zelox 1.33× faster" was a co-location artifact (Kafka starved Flink's CPU); with a
dedicated Kafka node Flink shows its real speed.

## Root cause (read from the plan, not guessed)

Current keyed windowed-agg physical plan:

```
KafkaSource(N) → [StreamBarrierAlign N→1] → WatermarkExec(1) → [StreamExchange 1→N hash] → WindowAccumExec(N) → [align N→1] → sink
```

1. **N→1 funnel before the watermark** (`planner.rs` WatermarkNode): source + JSON
   parse run N-way, then *all 100M rows collapse to a single instance* for watermark
   derivation **and** the exchange's input side. `StreamExchangeExec` reads only input
   partition 0 (`exchange.rs:154`) — it is 1→N by construction — so the per-row hash of
   100M keys runs on one core. This is why `--workers 4` and `--workers 16` measured
   identically (65 s): the bottleneck is a single-threaded stage, not worker count.
2. **No pre-shuffle aggregation.** All 100M parsed rows cross the exchange. Flink shuffles
   ~1.27M (measured: its source vertex emitted 1.27M, not 100M) because of **local-global
   aggregation**.

## How the best engines do it (authoritative)

- **Flink — watermarks in parallel dataflows**
  ([generating_watermarks](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/event-time/generating_watermarks/)):
  watermarks are generated **per Kafka partition** and "merged in the same way as
  watermarks are merged on stream shuffles." A downstream operator with multiple input
  channels sets its event-time clock to **the minimum** across channels, and "must
  completely process a watermark before forwarding it" (fire closed windows, then forward).
  Idle partitions are marked idle (`withIdleness`) so one silent partition never stalls the
  global watermark. → **per-partition watermark + MIN merge + idleness**, never a funnel.
- **Flink — local-global (two-phase) aggregation**
  ([tuning](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/tuning/)):
  a **local** stage accumulates same-key inputs into partial accumulators (MapReduce
  combine), a **global** stage merges them — "significantly reduces network shuffle and the
  cost of state access," and de-skews hot keys. Depends on mini-batch buffering.
- **Spark 4.1 Real-Time Mode**
  ([blog](https://www.databricks.com/blog/introducing-real-time-mode-apache-sparktm-structured-streaming)):
  concurrent (not sequential) stage scheduling and **in-memory streaming shuffle** of
  pre-reduced data; process records on arrival; minimize coordination/serialization.
- **DataFusion / Arrow**: native two-phase aggregation (`AggregateMode::Partial` →
  `Final`) and vectorized hashing/`GroupedHashAggregateStream`; Arrow columnar batches keep
  the partial state compact.

## Zelox design (one engine, native, better than both)

Reuse the existing, tested 1→N `StreamExchangeExec` and N→1 `StreamBarrierAlignExec`, but
move the heavy work **above** the funnel so it runs N-way in parallel, and shrink what the
funnel/exchange carry by ~80× via pre-shuffle local aggregation:

```
KafkaSource(N)
  → WatermarkExec(N, parallel; per-instance source tag, idleness)         # parse + wm, N-way
  → WindowAccumExec[LocalPartial](N)   # fold rows → (window,key) partials, pass watermarks
  → [StreamBarrierAlign N→1 : MIN-merge watermarks, all-N EndOfData]      # carries ~1.27M, not 100M
  → [StreamExchange 1→N hash by key]                                      # hashes ~1.27M
  → WindowAccumExec[GlobalFinal](N)    # merge partials, fire windows on watermark, emit
  → [StreamBarrierAlign N→1]
  → sink
```

### Three-mode WindowAccumExec (mirrors DataFusion AggregateMode)

- `Single` (unchanged): partial+final in one operator — the non-parallel path.
- `LocalPartial`: run the existing per-batch `AggregateMode::Partial` reduction, but on
  watermark/EndOfData **flush accumulated partials downstream** (partial schema) and forward
  watermark markers; **no** final-merge, **no** emit-mask, **no** watermark-driven eviction.
  Output is the `Partial` schema (group cols incl. the window struct + partial state cols).
- `GlobalFinal`: input is already partial state — skip the partial step, accumulate incoming
  partials, and on watermark run `AggregateMode::Final` + emit windows with `end ≤ watermark`
  exactly once (existing `finalize_and_emit`). Owns the open-window state + checkpoint snapshot.

### Watermark MIN-merge in the funnel (Flink parallel-watermark rule)

`StreamBarrierAlignExec` today forwards only input-0's watermark (correct for one stream,
wrong for N). Change: track the latest watermark per input channel; the merged watermark =
**min over non-idle inputs**; emit a watermark only when that min strictly advances
(monotonic). An input that has ended is treated as +∞ (no longer holds back the min); an
input that has produced data but no watermark yet holds the min at −∞ (nothing closes) —
this is exactly Flink's "min across input channels," and prevents a fast partition from
prematurely closing another partition's windows. Checkpoint-barrier alignment and all-N
`EndOfData` handling are unchanged (EO preserved).

### Idleness

A bounded (`availableNow`) read has all partitions active, so idleness is a no-op there. For
the realtime/continuous path, a partition with no data within the watermark interval is
marked idle (emits an Idle marker) so it does not hold the global min back — matching
`withIdleness`. (Implemented as a follow-up; not needed for the bounded head-to-head.)

## Exactly-once / correctness invariants (must not regress)

- Source offset staging per instance is unchanged (the under-read fix already landed).
- `GlobalFinal` owns window state + per-partition checkpoint snapshot on `EndOfData`, exactly
  as the current `WindowAccumExec` does — EO recovery semantics preserved.
- Window results must remain **bit-for-bit equal to Spark** on the 6-probe suite
  (`scripts/dist_streaming_smoke.py`): windowed_file=97, dedup=50, join=200, etc.
- The funnel still emits exactly one aligned `Checkpoint{e}` per epoch and one `EndOfData`.

## Validation plan

1. Local (debug): `dist_streaming_smoke.py` 6/6 unchanged; `ZELOX_COUNT == produced` at
   10M and the windowed-agg result count equals the pre-change result.
2. EKS (release, dedicated 2-node): re-run the verified head-to-head — expect Zelox
   throughput to reach/beat Flink with reads=100M and unchanged memory advantage; re-run the
   EO chaos test (100000/100000 across kill).
