# Design: stateful stream–stream join

Status: **design (ready to build)**. Today stream×stream joins are guarded with a clear
`not_impl_err` (the per-micro-batch path silently produced 0 cross-batch matches);
stream×static joins still work. This is the plan for the real, watermark-bounded
operator. Builds on the marker-based watermark foundation
([streaming-watermark.md](streaming-watermark.md)).

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
