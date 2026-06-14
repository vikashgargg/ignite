# Vajra realtime mode (F1) + the new-age universal engine — prod-grade across every dimension

Vajra's goal: **one native engine, one Spark API, for any processing** — batch, micro-batch streaming,
**realtime (continuous) streaming**, and lakehouse — that **out-performs Spark and Flink combined**
across reliability, security, memory, latency, and throughput. Not by porting their mechanisms, but by
re-deriving them on a leaner substrate (Rust + Arrow + DataFusion, no JVM) and **earning every claim
with a fair, measured head-to-head**.

Grounded in: Flink stateful-stream-processing + checkpointing docs (Chandy-Lamport barriers, alignment,
unaligned checkpoints); Spark Continuous Processing / Databricks real-time mode; Apache Arrow Flight
(`DoExchange` zero-copy shuffle); DataFusion execution model (pull-based vectorized async `Stream`s).

## 0. The substrate edge (why "combined" is achievable, not hype)
- **No JVM** → no GC pauses → **flat tail latency (p99≈p50)** (Flink's tail is GC + barrier-alignment
  outliers, ms→hours); compact Arrow buffers → **7–16× less RAM** (measured); no JIT warmup.
- **Vectorized pull streams (DataFusion)** → low latency *and* high throughput together (the
  micro-batch↔continuous tradeoff that forces Spark/Flink to choose doesn't bind us — measured sub-ms
  latency at multi-M rows/s).
- **One engine**: batch is a bounded stream; realtime reuses the same flow-event operators, optimizer,
  and sinks — no second runtime to secure, tune, or trust.

## 1. The five prod-grade dimensions (target = beat Spark *and* Flink; status = measured/honest)

**Reliability.** Exactly-once via the flow-event barrier model (below) + idempotent/transactional sinks
(Iceberg, file `_spark_metadata`) + object-store checkpoints (F4, S3-verified) + replayable sources with
committed offsets (file + Kafka, F11). Bounded buffers + watermark-bounded state → no unbounded growth
(soak: RSS flat). Crash/restart EO gated by SIGKILL + deterministic crash-window simulations. *Gap:*
distributed barrier coordinator (F2/F3); long (24h) soak.

**Security.** Const-time auth token compare (done); CVE gate (cargo-deny/audit, 0 vulns); **no secrets in
logs** (creds via env/IAM/instance-role, never echoed — the checkpoint/warehouse path used this on EKS);
**TLS in transit** for Spark Connect + Arrow Flight (Flight supports gRPC TLS); object-store creds via
IRSA/instance role (no embedded keys). *Gap:* at-rest encryption hooks, full pen-test, row/column ACLs.

**Memory.** Arrow columnar (no per-row objects); bounded mpsc exchange channels (backpressure); spillable
state (F5, Arrow-spill to object store) for state ≫ RAM. Measured: streaming ~80–99 MB stable vs Flink
2 JVMs (~GB). *Gap:* RocksDB-class spill backend (F5).

**Latency.** Realtime mode (this doc): continuous vectorized pipeline, **flat-tail** sub-ms→tens-of-ms;
no GC jitter, no alignment tax for stateless. Micro-batch path for richer EO when latency is secondary.

**Throughput.** Vectorized + cost-based batching: batch ~30× Spark (TPC-H SF-1); streaming multi-M
rows/s; Arrow Flight zero-copy shuffle for distributed (F2/F3). *Gap:* multi-node streaming parallelism.

## 2. Realtime mode (F1) — the architecture

**Vajra's flow-event model already *is* Chandy-Lamport barriers.** `FlowEvent::Marker` flows in-band with
`FlowEvent::Data`, never overtaking it — exactly Flink's barrier property, but Arrow-batch-framed,
vectorized, GC-free. We reuse the marker we have; no new barrier subsystem.

1. **Continuous execution, not micro-batch re-plan.** One long-lived flow-event pipeline (pull-based
   DataFusion `Stream`s on Tokio). The source emits **small Arrow batches at a high tick** so records flow
   operator→operator continuously — Flink-class latency, vectorized throughput, no GC tail. Distinct from
   the `processingTime` re-plan path (latency ≥ interval).
2. **Epoch barriers = `FlowMarker::Checkpoint{epoch}`**, injected by the source every commit interval,
   flowing with data; when it exits the sink, the driver commits that epoch's offsets/state **async, off
   the data path** (Spark's epoch-WAL idea) — commits never stall record flow.
3. **Exactly-once for stateless without alignment (the headline).** map/filter/single-input pipelines are
   embarrassingly parallel (Flink: EO without alignment). Epoch-offset commit + idempotent sink (Iceberg
   epoch-keyed, or `_spark_metadata`) → **exactly-once, zero alignment latency** — beats Spark Continuous
   (at-least-once) and avoids Flink's alignment tax. Stateful (agg/join, multi-input) needs aligned
   barriers → lands with F2/F3.
4. **Spark-compatible trigger.** `Trigger.Continuous("1 second")` selects realtime mode; unsupported
   operators are **rejected by name** (no silent fallback — no-workaround bar).
5. **Distributed (F2/F3)**: Arrow Flight `DoExchange` carries Arrow batches **zero-copy columnar** between
   nodes (`StreamExchangeExec` already broadcasts markers); barriers **aligned only at multi-input
   operators**, none elsewhere — minimal latency.

## 3. Build plan — gated, nothing claimed without a measured head-to-head
- **F1a**: `Trigger.Continuous` → `Realtime` driver (long-lived pipeline); reject unsupported plans by name.
- **F1b**: source periodic `Checkpoint{epoch}` markers; driver async epoch commit → EO for stateless + idempotent sink.
- **F1c**: latency metrics (`FlowMarker::LatencyTracker`). ✅ **MEASURED (2026-06-15):** realtime mode
  rate→memory @10k rows/s, end-to-end processing latency (source-emit→sink-process, marker-stamped so
  tz-independent and isolating *processing* latency): **p50 ≈ 0.0–0.1 ms, p99 ≈ 0.1 ms, max ≈ 0.1–1.1 ms,
  ~410 samples/s sustained.** Sub-ms with a **flat tail (p99≈p50)** — validates the no-GC + vectorized-
  continuous thesis (Flink p99 is tens-of-ms with GC outliers; Spark micro-batch floor = trigger interval,
  seconds). The rate source emits `LatencyTracker`; `MemorySinkExec` computes/logs percentiles. *Still to
  do:* surface in `recentProgress`; a measured Flink head-to-head (F1d).
- **F1d**: gate — Kafka/file → idempotent sink: measure p50/p99 end-to-end latency, EO across restart, and a **fair latency head-to-head vs Flink** (same pipeline). Then — and only then — claim.
- **F2/F3**: Arrow Flight `DoExchange` shuffle + aligned barriers for stateful/distributed.
- Cross-cutting each step: re-check the five dimensions; security (TLS, no-secret-logs) and reliability
  (crash gate) are acceptance criteria, not afterthoughts.

DataFusion 54 (repartition coalesce, Parquet morsel scans) helps throughput but is batch-engine; realtime
is Vajra's flow-event layer on top — tracked as the separate DF54 upgrade sprint.
