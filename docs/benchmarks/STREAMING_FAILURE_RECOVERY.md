# Streaming failure-injection recovery (SIGKILL crash, not clean restart)

Tests exactly-once under a **hard crash** (`kill -9` mid-stream), not just a clean shutdown —
the harder reliability bar. Local, debug build, rate → parquet, `availableNow` + checkpoint.

## Results (2026-06-11)
| Scenario | Outcome |
|---|---|
| **Crash before output durable** (kill ~2 s into a 50M-row write) | `committed=NONE`, `staged=50M`, **0 orphan/temp files**. Restart → replay from committed → **clean** (2M rows, no dup, contiguous). |
| **Crash at/after commit** (watch-and-kill the instant a `.parquet` appears) | offset was **already committed** (the durable→commit window is sub-poll, a few ms). Restart → no replay → clean. |
| **3 random-timed crashes + 1 clean run** (cumulative) | **total=distinct=2,000,000, min=0, max=1,999,999, no dup, contiguous.** Exactly-once **held through 3 crashes**; no orphan duplicates. |

## Stateful crash recovery (2026-06-11) — windowed agg + joins under SIGKILL
Same harness, but with **operator state** (the harder case: a crash that emitted output
durably but didn't commit the state could *re-emit* on replay).

| Operator | Test | Outcome |
|---|---|---|
| **Windowed aggregation** | 6 rounds (`availableNow` windowed COUNT → parquet + checkpoint), **2 crashed** mid-round (one before-commit → replayed) | **each window emitted exactly once** (4 windows, 4 distinct, `emitted_once=True`); no re-emission despite replay |
| **Stream-stream join** | 3 rounds (equi-join → parquet + checkpoint), **1 crashed** mid-round | **120,000 matches, all distinct keys, contiguous 0–119,999** — no duplicate matches, no loss |

The `emitted_ends` window state + the join buffers + the source offset all recover together,
so a crashed-and-replayed round neither double-emits nor loses results.

## Conclusion
**Exactly-once is robust under hard crash-recovery (SIGKILL), not just clean restart — for
stateless, windowed-aggregation, and join pipelines.** The offset WAL + operator-state
snapshot, committed only after durable output and restored on restart (Spark
`MicroBatchExecution` / `StateStore` model), replays uncommitted work cleanly; no duplicates
or losses were observed across repeated SIGKILLs at random points.

## Remaining (honest) gap — file-sink commit log
There is a **tiny theoretical window**: a crash *after* the parquet file is durable but
*before* the offset commits would leave an **orphan parquet** that the file reader (which
scans the output dir directly) would include → a duplicate on replay. We **could not hit it
empirically** — the window is only a few ms and every random crash landed before-durable.
The robust fix (matches Spark `_spark_metadata` / Flink): a **file commit log** that records
committed output files so readers ignore orphans. Low practical probability; needed for an
*absolute* exactly-once guarantee on the file sink.

## Next reliability items (roadmap)
- **24-hour endurance soak** (memory/latency/throughput drift) — only minutes tested so far.
- **Continuous exactly-once** (re-plan loop) + **checkpoint barrier alignment** so
  exactly-once holds under the intra-node parallelism (Phase 2).
- File-sink commit log (close the few-ms orphan window above).
- ✅ **Stateful** crash-recovery (windowed/join under SIGKILL) — **done (2026-06-11)**, above.
