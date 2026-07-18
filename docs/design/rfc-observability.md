# RFC-observability (P0 prerequisite) — make optimization evidence-based

**Problem (proven this session):** we optimized blind. The realtime throughput number was a commit-cadence
artifact (5s → 4M, 1s → 6M, wall-time → 7.18M); the M1 heap profile measured an idle server. Per the
first principle *"no optimization without evidence"*, no memory/transport/source RFC may start until the
harness produces real before/after metrics. This RFC builds that harness.

## What the leaders expose (grounded)
- **DataFusion** — `ExecutionPlan::metrics()` → `MetricsSet` per operator: `output_rows`, `elapsed_compute`,
  spill_count/bytes, custom `Count`/`Gauge`/`Time`. The canonical Rust per-operator framework.
- **Flink** — per-operator `numRecordsIn/OutPerSecond`, `busyTimeMsPerSecond` (backpressure), latency
  markers; the metrics groups + REST/UI.
- **RisingWave** — Prometheus per-actor/fragment; **Polars** — `.profile()` per-node timing.
- **Rust heap** — jemalloc `prof` (tikv-jemallocator, already an opt-in dep) / dhat. BUT: a 2026-07-01 A/B
  (Cargo.toml note) proved Vajra's streaming RSS gap is **LIVE IN-FLIGHT BUFFERING, not allocator** — so
  the dominant tool here is **in-flight byte counters at the buffering points**, jemalloc secondary.

## Vajra today
- `WM_PROF` (env-gated atomics): per-stage CPU-ns (source_read/poll, from_json, exchange_cpu/wait, encode,
  shuffle_send/recv). Works — it's how we know source+parse is the ~7M ceiling. Ad-hoc (log dump), not a
  framework, no BYTE/memory metric, no per-operator MetricsSet.

## Deliverables (this RFC)
1. **Consumption-rate drain (throughput truth)** — the harness must measure how fast the query CONSUMES
   the backlog (Spark `StreamingQueryProgress.numInputRows` accumulated to N), NOT S3 output completeness
   (cadence-gated). `stream_realtime_drain.py` reports `consume_M/s` (real) alongside completeness.
   *No rebuild.* [DONE below]
2. **In-flight byte instrumentation (memory truth)** — atomic peak-byte gauges at the buffering points:
   exchange channel in-flight (bytes enqueued − dequeued), reader prefetch, sink queue. Dumped at
   EndOfData like WM_PROF. Directly attributes the 12 GiB to a component. [Rust — next build]
3. **Per-stage as proper metric** — promote WM_PROF stage timings + the byte gauges into a single
   `VAJRA_PROF` report (CPU-ns + peak-bytes per stage), always-cheap (atomics), one log line.
4. **Flamegraph target** — `cargo flamegraph`/perf on a profiling build (`profile.profiling`), documented.
5. **jemalloc heap (secondary)** — `--features jemalloc` + `MALLOC_CONF=prof:true,lg_prof_sample:19` +
   jeprof, for allocator/leak questions the byte-gauges can't answer.

## Acceptance
Given a 100M run we can state, with numbers: consume-rate (M/s), per-stage CPU share, peak in-flight bytes
per component. Then — and only then — the memory/transport/source RFCs proceed with before/after gates.
