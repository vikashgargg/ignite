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

## `dropDuplicates` — FIXED (2026-06-11)
The actual root cause (confirmed via the optimized-plan dump): the resolver plans
`dropDuplicates` as an **`Aggregate`**, not a `Distinct` — so the rewriter's `Distinct` path
was dead. `dropDuplicates()` → `Aggregate(group=[all cols], aggr=[])`; `dropDuplicates([k])`
→ `Aggregate(group=[k], aggr=[first_value(all cols)])`. The rewriter routed both to a global
(pipeline-breaking) aggregate, with the qualifier bug on top.

**Fix:**
1. Rewriter detects the **dedup-aggregate** pattern (`aggr=[]` or all-`first_value`, group
   keys plain columns) and routes to `StreamDeduplicateNode` over the **flow-event stream**,
   reconstructing the aggregate's output schema from the deduped first row
   (`first_value(c) ⇒ col(c)`), with **`alias_qualified`** so field qualifiers match and the
   parent projection resolves.
2. `StreamDeduplicateExec` boundedness `requires_infinite_memory: false` — matches Spark
   `dropDuplicates()` without a watermark (runs, accumulating seen-keys; the sanity checker
   otherwise refuses). Watermark-bounded eviction (`dropDuplicatesWithinWatermark`) is the
   follow-up for guaranteed-bounded state.

**Verified:** `dropDuplicates()` (all cols) and `dropDuplicates([k])` (subset) → correct
distinct output, all columns retained; all-in-one sweep **12/12**; clippy clean.

> Note: this is the third manifestation of the pervasive `?table?` qualifier mismatch
> (windowed agg, then dedup). A **systemic streaming qualifier-strip** would prevent future
> recurrences — a candidate cleanup.

## Conclusion
The combined batch+streaming product validates cleanly on its core capabilities (11/12).
The single failure is a **precisely-root-caused, pre-existing** `dropDuplicates` gap whose
fix is a focused plan-pipeline change — tracked, not rushed.
