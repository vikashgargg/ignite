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

## 3. Throughput — diagnosed + fixed (windowed-agg was the bottleneck, now 6.5×)
Initial reading: windowed agg ingested only ~27k rows/s. **Diagnosis** (the important
part): raw `rate → memory` does **436k rows/s** and `rate → filter → memory` ~424k/s — so
the source and stateless engine are *not* the bottleneck. The cap was **`WindowAccumExec`
re-aggregating *all* buffered raw rows on every watermark** (O(pending) per batch,
quadratic within a window).

**Fix (production best-practice — Flink `AggregateFunction` / Spark stateful / DataFusion
two-phase):** incrementally pre-aggregate each batch to **partial state** on arrival (one
partial per window-group, not raw rows), and merge with `Final` mode only when a window
closes.

| Windowed agg, continuous | Before | After (incremental) |
|---|--:|--:|
| Throughput (release) | ~27k rows/s | **~177k rows/s (6.5×)** |
| Throughput (debug) | ~27k/s | ~94k/s |
| Latency | sub-ms | **sub-ms (unchanged)** |
| Peak RSS | bounded | **68 MB (bounded)** |

Remaining headroom toward the ~436k/s stateless ceiling is per-watermark `Final`-merge +
per-batch plan-construction overhead — addressable by throttling the merge to window-close
cadence (Flink emits on trigger, not per element). The earlier ~28M rows/s figure is
**bounded/batch** throughput (`availableNow` count), a different measurement.

## Flink head-to-head readiness (scoped to our product goal)
- **Ready now (measured, real):** Flink-class **latency** + **bounded state** + **low memory**
  on supported queries (windowed aggregation, interval join). A scoped head-to-head on
  *latency + memory* for these queries is defensible.
- **Throughput:** windowed agg now ~177k/s (6.5× after the incremental-agg fix), sub-ms,
  bounded memory — a scoped throughput comparison is now reasonable. Further headroom
  (toward ~436k/s) via Final-merge throttling is a follow-up.
- **Not ready / deferred:** **endurance** (24 h soak) and **failure recovery** (Flink's
  strengths) — untested, so we do **not** claim reliability superiority yet.
- **Recommended first comparison:** release build, single node, generator-based, **one
  windowed-agg + one interval-join query**, reporting **latency + peak RSS** vs Flink local —
  explicitly scoped, not a full Nexmark or reliability claim.

## Known pre-existing issue (separate)
`stream × static` join into a memory sink hits a resolver-level error (`?table?.k`,
unnamed rate-stream relation + string-key join) — pre-existing (it never worked
end-to-end; the memory-sink catalog bug previously masked it). Out of scope here; tracked
as a follow-up. Does not affect stream×stream / interval joins, windowed agg, or batch.
