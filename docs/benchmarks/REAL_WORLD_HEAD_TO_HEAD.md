# Real-world head-to-head — Vajra vs Spark vs Flink (2026-06-11)

One AWS **c7g.2xlarge** Graviton node (8 vCPU, 15 GiB, ap-south-1), Ubuntu 22.04 ARM64.
Vajra = the `phase5` image (all recent streaming work — dropDuplicates, keyed windows,
systemic qualifier-strip — **verified present** before benchmarking). Spark 3.5.3 (pip,
`local[*]`), Flink 1.18.1 (standalone, parallelism 1). Engines run **sequentially**.
Cost-disciplined: node torn down after (~$0.90).

## Headline numbers

| Workload | Vajra | Spark 3.5.3 | Flink 1.18 | Read |
|---|--:|--:|--:|---|
| **Batch** (20M ⨝ 100k + group-by, warm) | **0.28 s** | 0.70 s | — | **Vajra ~2.5×** |
| **Streaming windowed** (1s tumbling count, single-partition) | **~5.5M rows/s** | ≥3M/s (capped) | ~3.55M rows/s | **Vajra ~1.55× Flink** |
| **Streaming ETL** (read→filter→write, 50M parquet, availableNow) | n/a (gap, below) | 6.0M rows/s | — | — |

### Batch — clean win
20M-row fact joined to a 100k-row dim, then group-by aggregation; identical query both
engines (range-generated, no I/O confound), warm. Vajra **0.28 s vs Spark 0.70 s ≈ 2.5×**,
both correct (20M rows, 10 groups). Consistent with the DataFusion-core advantage.

### Streaming windowed — Vajra ahead, with a methodology caveat
1-second tumbling windowed count. Vajra's processing **saturates ~5.5M rows/s** (window
counts plateaued: 1.74M→4.12M→5.48M as the rate cap rose 5M→20M→40M — diminishing, i.e.
processing-bound). Flink (datagen, parallelism 1) sustained **~3.55M rows/s** (source
`numRecordsOut` delta). Spark Structured Streaming **kept up with a 3M/s cap** but its rate
source caps *generation*, so its windowed ceiling wasn't saturated here.
**Caveat:** the rate/datagen sources make this a per-engine ceiling estimate, not a
byte-identical load test — treat as directional. The dedicated, more rigorous streaming
comparison is in [FLINK_HEAD_TO_HEAD.md](FLINK_HEAD_TO_HEAD.md).

## Real-world findings (the point of this exercise — feed the industry-grade roadmap)
The run surfaced concrete Vajra streaming gaps to close before a clean 3-engine streaming
benchmark is possible:

1. **No file-source streaming** — `spark.readStream.parquet(...)` fails with *"streaming
   query must write data to a sink"*. Only `rate`/Kafka sources work today. **Blocks**
   file-based streaming ETL (and the clean bounded-file throughput method). **P1.**
2. ~~**`SELECT window.start` in SQL** fails~~ — **FIXED (2026-06-12):** the windowed-agg
   struct column is now named `window` (Spark convention) instead of the call display, so
   `window.start` resolves via SQL and DataFrame (batch + streaming).
3. **No `recentProgress.processedRowsPerSecond`** over Spark Connect — streaming progress
   metrics aren't exposed, so throughput/lag can't be read the Spark way. **P2.**
4. ~~**Container binds `127.0.0.1`**~~ — **CORRECTED (not a defect):** the image's
   `CMD` already binds `0.0.0.0` (`server --ip 0.0.0.0 --port 50051`), and the bare binary
   keeps a secure loopback default (`--ip`, default `127.0.0.1`) — the right prod-grade
   design (container boundary is the isolation, like k8s pods). The earlier loopback was a
   benchmark-harness error: the `docker run … server --port 50051` invocation *overrode* the
   `CMD` and dropped `--ip 0.0.0.0`. Verified: default `CMD` is reachable via `-p` without
   `--network host`.

## Not yet run (deferred with the gaps above)
- **Stream-stream join** and **mixed batch+streaming** head-to-head — gated on the file-source
  gap and a stable streaming throughput harness. Tracked for the next iteration once #1 lands.

## Realtime continuous Kafka → durable file sink, exactly-once (2026-06-15, local)

Same pipeline on **both** engines: real Kafka (`apache/kafka`, single-partition topic, 50,000
JSON messages) → `CAST(value AS STRING)` → write **parquet** with a checkpoint location. 8-core
MacBook Air; Vajra = persistent `vajra server` (debug build); Spark 3.5.3 `local[*]` with
`spark-sql-kafka-0-10`. Same input, same sink format — a true like-for-like.

| Mode | Vajra | Spark 3.5.3 | Read |
|---|--:|--:|---|
| **availableNow micro-batch** (drain 50k → parquet) | **50,000 rows, exactly-once, 0.51 s, ~98.7k rows/s** | 50,000 rows, exactly-once, 8.78 s, ~5.7k rows/s | throughput gap real; Spark wall-clock is **startup-inflated** (JVM + local-job spin-up) — see caveat |
| **continuous → durable file sink, exactly-once** | **✅ 50,000 rows, exactly-once** (+ EO **across restart** measured separately: wave1 5k → stop → wave2 5k → 10,000 distinct, contiguous) | **❌ not possible** — `java.lang.IllegalStateException: Unknown type of trigger: ContinuousTrigger(1000)`; Spark Continuous sinks are **Kafka/Memory/Console only, at-least-once** (official docs) | **capability Vajra has and Spark does not** |

### Honest reading
- The **headline is the capability gap**, not the micro-batch wall-clock. Spark *cannot* run
  continuous processing into a durable file sink (it throws), and Spark Continuous is at-least-once
  even where it does run; Vajra does continuous **exactly-once** to parquet, recoverable across
  restart (Flink-class EO via in-band epoch barriers + a single atomic commit, no alignment latency
  for stateless). This is the F1b result, measured.
- The micro-batch **0.51 s vs 8.78 s** ratio is inflated by Spark's local-mode/JVM startup; a warm
  Spark narrows it. But Vajra's **~98.7k rows/s** sustained on a debug build (no-JVM, Arrow core) is
  the dependable number, consistent with the ~2.5× batch win above.
- Single node, single Kafka partition. **Multi-partition / distributed** realtime is F2/F3 (the
  Chandy-Lamport alignment primitive `StreamBarrierAlignExec` is built + unit-tested; cross-node
  Arrow-Flight shuffle + continuous stateful remain). A Kafka→file **Flink** streaming comparison is
  the next external head-to-head (the Iceberg-sink run earlier showed Vajra 0.26 s vs Flink 20.27 s
  on equivalent input).

## Conclusion
**Batch is a clean, fair ~2.5× win over Spark on real Graviton hardware.** Streaming windowed
throughput is ahead of Flink per-core (directional, ~1.55×), but the exercise's real value was
surfacing **four concrete streaming gaps** (file source, SQL window access, progress metrics,
container bind) — exactly the punch-list to make Vajra's streaming industry-grade. File-source
streaming (#1) is the top priority; it unblocks both real-world ETL and the clean streaming
benchmark.
