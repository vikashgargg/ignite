# All-in-one validation — combined batch + streaming (2026-06-11)

A single sweep validating Vajra as one batch+streaming engine after the session's work
(keyed-windowed fix, Phase-2 parallelism, exactly-once, crash recovery). Local, debug build,
current `HEAD`. Harness: `/tmp/allinone.py`.

## Result: 12 / 12 pass (dropDuplicates fixed 2026-06-11)

| Area | Test | Result |
|---|---|---|
| **Batch** | arithmetic, group-by, join, window function, CTE | ✅ all 5 |
| **Streaming** | stateless (rate→filter) | ✅ |
| | windowed agg, no key | ✅ |
| | **windowed agg, keyed** (the session's fix) | ✅ all keys present |
| | stream-stream join | ✅ matches all distinct |
| | **`dropDuplicates`** (all-cols + subset) | ✅ **fixed** (was a pre-existing gap) |
| **Reliability** | exactly-once (availableNow ×2 → contiguous, no dup) | ✅ |
| **Combined** | batch SQL reads the streaming-written parquet | ✅ |

**Core product is solid:** batch correctness, the just-fixed keyed windowed aggregation,
joins, exactly-once, and the all-in-one flow (batch reading streaming output) all pass.

## The one gap — streaming `dropDuplicates` (pre-existing, root-caused)
Both forms fail (not touched this session):
1. **`dropDuplicates()` (all columns)** → `No field named "?table?"."#0"` — the same
   **qualifier-resolution** bug class fixed for windowed agg, but in the
   `Distinct → StreamDeduplicateNode` path (key columns resolved against the unqualified
   data schema).
2. **`dropDuplicates([subset])`** → `Cannot execute pipeline breaking queries, AggregateExec
   Partial`. **Root cause:** `resolve_and_execute_plan` runs the streaming rewriter on the
   **already-optimized** plan (`session_state.optimize` first), so DataFusion's
   `ReplaceDistinctWithAggregate` logical rule has already rewritten the `Distinct` into a
   **global `Aggregate`** before the streaming rewriter can route it to
   `StreamDeduplicateNode`. The rewriter then treats it as a (non-windowed) streaming
   aggregate → pipeline-breaking on unbounded input.

### Fix (focused follow-up — touches the plan pipeline, so not rushed)
- Preserve `Distinct` for streaming: detect streaming **before** `optimize`, or run a
  streaming-specific logical optimizer that **disables `ReplaceDistinctWithAggregate`**
  (and any rule that breaks streaming-operator routing), so `Distinct`/`DistinctOn` reaches
  `StreamDeduplicateNode`.
- Apply the qualifier strip in the `Distinct → dedup` path (same pattern as the windowed-agg
  fix).
- Gate with a differential: streaming `dropDuplicates()` and `dropDuplicates([k])` →
  correct distinct output.

## Conclusion
The combined batch+streaming product validates cleanly on its core capabilities (11/12).
The single failure is a **precisely-root-caused, pre-existing** `dropDuplicates` gap whose
fix is a focused plan-pipeline change — tracked, not rushed.
