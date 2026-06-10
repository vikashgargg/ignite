# Streaming failure-injection recovery (SIGKILL crash, not clean restart)

Tests exactly-once under a **hard crash** (`kill -9` mid-stream), not just a clean shutdown —
the harder reliability bar. Local, debug build, rate → parquet, `availableNow` + checkpoint.

## Results (2026-06-11)
| Scenario | Outcome |
|---|---|
| **Crash before output durable** (kill ~2 s into a 50M-row write) | `committed=NONE`, `staged=50M`, **0 orphan/temp files**. Restart → replay from committed → **clean** (2M rows, no dup, contiguous). |
| **Crash at/after commit** (watch-and-kill the instant a `.parquet` appears) | offset was **already committed** (the durable→commit window is sub-poll, a few ms). Restart → no replay → clean. |
| **3 random-timed crashes + 1 clean run** (cumulative) | **total=distinct=2,000,000, min=0, max=1,999,999, no dup, contiguous.** Exactly-once **held through 3 crashes**; no orphan duplicates. |

## Conclusion
**Stateless exactly-once is robust under hard crash-recovery, not just clean restart.** The
offset WAL → commit-after-durable → restore-on-restart protocol (Spark `MicroBatchExecution`
model) replays uncommitted batches cleanly; no duplicates were observed across repeated
SIGKILLs at random points.

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
- **Stateful** crash-recovery (windowed/join state under SIGKILL) — same offset+state
  protocol, not yet crash-injection-tested.
- File-sink commit log (close the orphan window above).
