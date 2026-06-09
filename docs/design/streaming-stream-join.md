# Design: stateful stream–stream join

Status: **inner equi-join SHIPPED 2026-06-09** (`StreamJoinNode` + `StreamJoinExec`,
verified: correct cross-batch matches, no duplicates, watermark min-merge). Remaining
follow-ups: **watermark-based state eviction** (buffers are currently unbounded), outer
joins + residual filter, interval-join time bounds. Other join shapes still fail loud
with `not_impl_err`. Builds on the marker-based watermark foundation
([streaming-watermark.md](streaming-watermark.md)).

## ✅ Interval join SHIPPED 2026-06-10 (bounded state, Flink-aligned)
`StreamJoinExec` now supports the **interval join**: an inner equi-join with a residual
time-range condition. Verified (pyspark): `a.join(b, "a.ka=b.kb AND b.tb BETWEEN a.ta-2s
AND a.ta+2s")` → matches satisfy the key **and** the time window; state is **bounded** by
evicting per the Flink rule (`bounds=(-2s,+2s)`, event-time indices resolved, confirmed).

- **Residual filter applied** to matched pairs (post-equi-join filter on the output) —
  qualified column refs (`a."#2"`) are stripped to unqualified internal ids; the output
  schema preserves the input relation qualifiers so the consuming plan resolves.
- **Bounds extracted** from the filter (`extract_interval_bounds`): canonical
  `right_ts CMP left_ts ± <duration>` → `(lower, upper)` micros (handles
  `DurationMicrosecond` / `IntervalDayTime`).
- **Eviction (Flink rule):** drop left rows when `right_wm > left.ts + upper`, right rows
  when `left_wm > right.ts - lower`, via the watermark min-merge. No recognizable bound →
  unbounded (= Spark default), matches still correct.
- Plain inner equi-join (no filter) keeps unbounded state (= Spark), still correct.

Remaining follow-ups: outer joins (emit unmatched on eviction); session/processing-time;
push the time filter into the hash join for efficiency.

## Bounded state = interval join (grounded in Spark/Flink, 2026-06-09 research)
**Eviction requires a time-range join condition** — confirmed against both engines:

- **Flink `IntervalJoinOperator`**: emits pairs where `right.ts ∈ [left.ts + lowerBound,
  left.ts + upperBound]`. On each element it buffers + probes the other side, and
  **registers a per-element cleanup timer**. A left row is evicted once the **right
  watermark passes `left.ts + upperBound`** (no future right row can match); a right row
  once the **left watermark passes `right.ts − lowerBound`**.
- **Spark `StreamingSymmetricHashJoinExec.getStateWatermarkPredicates`**: builds a per-
  side **state watermark predicate** — a *key* watermark when the join key is the
  event-time column, else a *value* watermark derived from a **range condition on the
  event-time columns**. State rows below the predicate are removed
  (`removeByKeyCondition` / `removeByValueCondition`).
- **Both engines: no time-range condition ⇒ unbounded state.** So a plain equi-join
  cannot be evicted correctly — this is inherent, not a gap.

### Therefore the prod-grade plan (Flink-aligned)
1. **Allow a residual time-range filter** on the stream×stream join (currently rejected);
   apply it to matches (post-equi-join filter on the output, or a `JoinFilter`).
2. **Extract `(left_ts_col, right_ts_col, lowerBound, upperBound)`** from the condition
   (`right.ts BETWEEN left.ts + L AND left.ts + U`, or the `>=`/`<=` conjunction form).
3. **Evict (Flink rule):** drop left rows when `right_watermark > left.ts + upperBound`;
   drop right rows when `left_watermark > right.ts − lowerBound`. Both watermarks come
   from the existing min-merge.
4. If no time-range condition → keep unbounded (documented, = Spark default) — never
   silently drop matches.

### Known prerequisite
A streaming interval-join query (`withWatermark` both sides + aliased time-range `expr`)
currently fails to resolve before reaching the rewriter (batch interval joins work).
Fix that resolution path first, then implement eviction.

## What shipped (inner equi-join, unbounded — = Spark default)
- `StreamJoinExec` buffers each side's batches; when a batch arrives on one side it is
  joined against the **accumulated** other-side batches via DataFusion `HashJoinExec`
  (`CollectLeft`), so each pair is emitted exactly once (when the second row arrives).
- Operator watermark = **min(left_wm, right_wm)**, forwarded as it advances.
- Rewriter detects stream×stream (`contains_stream_source` both sides) → `StreamJoinNode`
  for inner equi-join without residual filter. Planner does 2-input wiring + builds the
  equi-key exprs against each side's decoded data schema.

## Why it's a major feature
A correct stream×stream join is a **2-input stateful operator** — the largest in the
streaming stack. It must buffer rows from *both* sides keyed by the join key, emit
matches as rows arrive on either side, and **bound state via watermarks** (otherwise
state grows forever). Flink/Spark both treat this as a first-class, carefully-tested
operator (interval joins, outer-join state, watermark coordination, state TTL).

## Pieces to build
1. **`StreamJoinExec`** (new, 2-input physical operator):
   - Decode both flow-event inputs; merge them **side-tagged** (`select` over
     `Left`/`Right`-tagged streams).
   - Keyed dual state: `HashMap<JoinKey, Vec<Row>>` for each side (compute the key from
     the equi-join `on` expressions against each side's data schema).
   - On a Data row from side A: probe side-B buffer for the key, emit matched output
     rows (left ++ right columns, applying any residual `filter`), then insert the A row
     into side-A buffer. Symmetric for side B. (Inner equi-join first; LEFT/RIGHT/FULL
     OUTER add "unmatched on eviction" emission — a follow-up.)
   - Output schema = `join.schema()` re-wrapped as a flow-event schema.
2. **Watermark min-merge** (the key correctness primitive): track each input's latest
   `FlowMarker::Watermark`; the operator's watermark = **min(left_wm, right_wm)**
   (Flink semantics). Forward a Watermark marker downstream when the min advances.
3. **Time-bound eviction** (bounds state): require a time constraint (Spark needs a
   watermark + a range condition, e.g. `b.t BETWEEN a.t AND a.t + INTERVAL`). Evict
   buffered rows whose timestamp is older than `watermark − interval` (they can no
   longer match). Without a time bound, error (matches Spark, which rejects unbounded
   stream-stream joins).
4. **Rewriter**: detect stream×stream (`contains_stream_source` on both sides — already
   added) and build a `StreamJoinNode` (feed both flow-event inputs; carry `on`,
   `filter`, `join_type`, and the parsed time-bound). **Planner**: 2-input wiring,
   build key exprs against the decoded data schemas.

## Correctness tests (write FIRST)
- Inner equi-join of two rate streams on `k`: every cross-batch match emitted exactly
  once; non-matches not emitted.
- Interval join: a row matches only within the time bound; outside → no match.
- State stays **bounded** over a long run (eviction past `min_watermark − interval`).
- Watermark forwarded downstream = min of the two inputs.
- Regression: stream×static joins and all batch joins unchanged.

## Files
- new `crates/sail-physical-plan/src/streaming/stream_join.rs` (`StreamJoinExec`)
- new `crates/sail-logical-plan/src/streaming/stream_join.rs` (`StreamJoinNode`)
- `crates/sail-plan/src/streaming/rewriter.rs` (Join arm → `StreamJoinNode`)
- `crates/sail-session/src/planner.rs` (2-input wiring, key exprs vs data schema)

The 2-input watermark **min-merge** here is the same primitive multi-stage pipelines
will reuse — implement it generically.
