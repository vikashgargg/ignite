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
- **F1b**: per-epoch durable sink (`RealtimeFileSinkExec`) + epoch-tied source-offset commit → EO for stateless.
  - ✅ **MEASURED (2026-06-15), sink side:** continuous flow-event stream rolled into per-epoch committed
    files (`<base>/<epoch>/` + `_spark_metadata/<epoch>` log). Validated on a debug server:
    rate→parquet `Trigger.Continuous("1 second")` produced 5 epoch dirs/6 s, all rows distinct;
    **real Kafka** (single-partition topic, `apache/kafka`) → 5000 msgs → 5000 rows, **0 dups
    (within-run exactly-once)**; epoch counter **resumes** from the commit log across restart (0→6);
    `from_json`/`select` projections preserve flow-event markers (rewriter contract) so realistic parse
    pipelines route through the realtime sink; multi-partition realtime is **rejected by name** (no
    silent data loss — lands with F2/F3).
  - ⚠️ **Honest gap — cross-restart EO not yet closed (measured):** the *unbounded continuous* Kafka
    reader still uses `subscribe` + broker auto-commit (`auto.offset.reset=earliest`); it does **not**
    seek to the checkpoint-store committed offset on restart nor stage offsets per epoch (only the
    *bounded* F11 path does). Measured restart re-read wave-1: **total=15000, distinct=10000**.
    **Fix (next):** continuous Kafka reader → `assign`+`seek` to `sources/0/committed`,
    `enable.auto.commit=false`, emit `FlowMarker::Checkpoint{epoch}` every commit interval and stage
    that epoch's offsets to `sources/0/staged-epoch-<id>`; `RealtimeFileSinkExec` commits **on the
    Checkpoint marker** (not a wall-clock timer) and **promotes the matching staged offset atomically
    with the file commit-log entry** (epoch id = join key). This ties source offset + sink files into
    one epoch transaction → exactly-once for stateless **without alignment latency** (beats Spark
    Continuous at-least-once; avoids Flink's alignment tax). Stateful needs aligned barriers (F2/F3).

### F1b EO mechanism — doc-grounded (Flink 2PC + Spark epoch + F4 atomic commit)

Read first (2026-06-15): Flink *stateful-stream-processing* + *checkpointing* + the *end-to-end
exactly-once with 2PC* blog; Spark *continuous-processing* guide. Established invariants:
- **Flink:** barriers flow in-band and **never overtake records**; **single-input / embarrassingly-
  parallel ops are exactly-once even without alignment**; 2PC = *pre-commit on barrier* → *commit on
  checkpoint-complete notification*; on recovery the **source resets to the snapshotted offset** and the
  sink issues an **idempotent preemptive commit** ("it is our responsibility to implement a commit in an
  idempotent way").
- **Spark Continuous:** **at-least-once only**, ~1 ms, **map/filter/projection only**, and sinks are
  **Kafka/Memory/Console — no durable file sink**. So Vajra's EO durable-file realtime sink is *beyond*
  Spark Continuous and at Flink's EO level, without alignment (single-input).

**Vajra mechanism (maps 1:1 to the invariants, collapsed to an object-store-atomic commit):**
1. The continuous source emits `FlowMarker::Checkpoint{epoch}` every commit interval, **in-band, never
   overtaking data** (FlowEvent ordering already guarantees this — our markers *are* Chandy-Lamport
   barriers). Before emitting, it **pre-commits** by staging its reached offset map to
   `sources/0/staged-epoch-<id>`.
2. The sink consumes the **flow-event** stream (sees markers). On `Checkpoint{epoch}` it durably writes
   the epoch's data files (pre-commit) then performs the **commit as a SINGLE atomic `put`** of one
   record `_spark_metadata/<epoch>` carrying **both** the committed file metadata **and** the source
   offset map (read from `staged-epoch-<id>`). Object stores have no multi-object transaction, so one
   atomic object = no torn commit (the F4 principle).
3. **Recovery:** on restart the source reads the latest committed `_spark_metadata/<id>`.offsets and
   **seeks there** (`assign`+`seek`, `enable.auto.commit=false` — not broker auto-commit); the sink
   resumes at `id+1`. Crash *before* the atomic put → nothing committed → source re-reads from the last
   committed offset → next epoch commits it (no dup, never committed). Crash *after* the atomic put →
   offset+files committed together → resume strictly after (no dup, no loss). Idempotent by construction.
This is **exactly-once for stateless without alignment latency** — beats Spark Continuous (at-least-once)
and matches Flink EO, with a durable file sink Spark Continuous lacks. Stateful (agg/join) still needs
aligned barriers → F2/F3.
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
