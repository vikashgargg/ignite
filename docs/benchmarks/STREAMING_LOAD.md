# Streaming load test — release build, sustained, single node

Measured 2026-06-10 on a release build (thin-LTO, opt-level 3), single 8-core laptop,
to validate the product-goal streaming properties **before** any Flink head-to-head:
Flink-class **latency**, **bounded state** under load, **low memory** — in one binary.

## 1. Latency — Flink-class ✅ (the headline)
Continuous windowed aggregation, release build, RUST_LOG=info latency instrumentation
(source-emit → sink-process):

| Workload | p50 | p99 | max |
|---|--:|--:|--:|
| windowed agg, continuous | **0.0 ms** | **0.0–0.1 ms** | **0.1 ms** |

Sub-millisecond, sustained — Flink-class (Flink p99 ~tens of ms), not Spark's ~100 ms–1 s
micro-batch class. Vajra's tick is ~1–10 ms with sub-ms per-batch processing.

## 2. Bounded state under load ✅ (the key reliability property)
Stateful operators must keep memory bounded over long runs. Measured **peak RSS over
time** (release):

| Workload (sustained) | RSS trajectory | Verdict |
|---|---|---|
| Windowed agg, 50k/s, 40 s | steady **~66–90 MB** | bounded ✅ |
| **Interval join, isolated state** (non-matching keys, 50k/s/side, 45 s) | climbs to ~66 MB by t=6 s, then **flat at 66 MB** for 39 s | **bounded ✅** |

The interval join with *non-matching keys* isolates pure join **state** (≈0 output): RSS is
rock-steady at 66 MB — **watermark + interval eviction keeps state bounded under sustained
high-cardinality load** (the Flink interval-join cleanup rule, working in practice). No
RocksDB needed at our target scale.

**Caveat (not a leak):** a *matching-keys* join into a **memory sink** grows RSS linearly
(503 MB @ 50k/s after 40 s) — that's the memory sink **buffering all 369,900 output rows**
(a sink property; Flink sinking to memory grows too), *not* join state. With state isolated,
memory is flat.

## 3. Throughput — honestly source-limited (not the engine)
Requesting `rowsPerSecond=1,000,000`, the windowed agg ingested only **~27k rows/s**
(586k rows over 22 s; per-window counts 14k–78k). The **rate source** is the bottleneck:
its micro-batch model caps at ~1000 batches/s and per-batch overhead limits effective
continuous ingestion to ~27k/s — the **engine is far from saturated** (that's why latency
stays sub-ms). The earlier ~28M rows/s figure is **bounded/batch** throughput
(`availableNow` count), a different measurement.

**Action item:** to measure the engine's continuous-throughput *ceiling* (and for a
throughput-focused Flink comparison), the rate source needs larger batches per tick
(fewer, bigger batches at high rates). This is the #1 prerequisite for a throughput benchmark.

## Flink head-to-head readiness (scoped to our product goal)
- **Ready now (measured, real):** Flink-class **latency** + **bounded state** + **low memory**
  on supported queries (windowed aggregation, interval join). A scoped head-to-head on
  *latency + memory* for these queries is defensible.
- **Not ready / deferred:** continuous **throughput** comparison (fix the rate source first);
  **endurance** (24 h soak) and **failure recovery** (Flink's strengths) — untested, so we
  do **not** claim reliability superiority yet.
- **Recommended first comparison:** release build, single node, generator-based, **one
  windowed-agg + one interval-join query**, reporting **latency + peak RSS** vs Flink local —
  explicitly scoped, not a full Nexmark or reliability claim.

## Known pre-existing issue (separate)
`stream × static` join into a memory sink hits a resolver-level error (`?table?.k`,
unnamed rate-stream relation + string-key join) — pre-existing (it never worked
end-to-end; the memory-sink catalog bug previously masked it). Out of scope here; tracked
as a follow-up. Does not affect stream×stream / interval joins, windowed agg, or batch.
