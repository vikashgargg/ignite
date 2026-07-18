# Vajra Architecture Review — subsystem gap analysis vs Spark / Flink / DataFusion / RisingWave / Arroyo / Polars

**Status:** living master RFC (the root of the engineering program). **Operating model:** production
engineering org — RFC-driven, metrics-first, benchmark-gated. No subsystem is "good enough"; no change
lands without a *measured* before/after against a named credible design. This document is the map;
per-subsystem RFCs (`docs/design/rfc-<subsystem>.md`) carry the depth + acceptance criteria.

## First principle (enforced)
Every change answers, in its RFC/commit: **WHY needed · what METRIC proves it · what BOTTLENECK it
removes · how Spark/Flink/DataFusion/RisingWave/Arroyo solve it · can we do better.** No code because it
"works". No speculative optimization. Root cause from profiling/metrics/flamegraphs, not assumption.

## Measured baseline (fresh EKS 100M realtime, 2026-07-18 — the honest starting line)
- **Correctness: TIE** — Vajra 10 windows/100M byte-identical to Flink, both S3/Kafka-verified (0 mismatch).
- **Throughput: ~6–7M/s vs Flink ~10M = ~1.4×** (the old "4M/2.5×" was a harness cadence artifact — proven).
- **Latency: p50 100 vs 95ms** (~tie; kind low-parallelism showed a Vajra tail win — config-dependent).
- **Memory: 12 GiB vs 3.9(passthrough)/9(windowed)** — the one clear gap; dominant source still UNKNOWN
  (M1 heap profile failed on an idle server → re-profile is a P0 blocker for the memory RFC).
- **Batch: Vajra 6.2× Spark** (200M ETL) — the proven categorical win.
- **Refuted by metric:** single-node inter-worker gRPC (`--mode local`==`local-cluster`); sink-buffer as the
  dominant memory (only ~1 GiB of 12); encode/decode + off-path-commit as throughput movers (measure was blind).

## Performance pillars → owning subsystem (measure everything)
throughput→source/parse+shuffle · latency/tail→scheduler+transport+commit-cadence · memory→state+network
buffers+allocator · cache-locality/zero-copy→execution+transport · recovery/checkpoint→state+FT · scalability→
scheduler+shuffle · reliability→FT+backpressure. Each pillar has a **performance budget** (target = ≤ Flink,
stretch = beat it) tracked in BOARD.md.

## Subsystem gap-analysis matrix
Legend: **P0** on the critical path for the measured gaps (do first, grounded, RFC each). Rows are the map;
depth lives in the linked per-subsystem RFC.

| Subsystem | Vajra today | Leaders' proven design | Gap → ideal | Prio |
|---|---|---|---|---|
| **Source read + parse** | batch-queue (rd_kafka_consume_batch_queue, FLIP-27) ~4–7M/s; from_json CPU-heavy | Flink KafkaSource poll(N) + mini-batch; **arrow-json** SIMD decode; Arroyo columnar JSON | ~7M ceiling = parse+read bound. Ideal: SIMD/`Utf8View` zero-copy JSON, vectorized decode, avoid per-record. **RFC-source** | **P0** |
| **Transport / shuffle** | Arrow-Flight per-batch gRPC (cross-node); in-proc mpsc (single-node) | Flink credit-flow network stack + object-reuse; RisingWave exchange; Arroyo shuffle-edge; **Velox** exchange | per-batch gRPC ≠ Arrow zero-copy ceiling. Ideal: batch-coalesce + `Utf8View` + credit-flow; investigate leaner-than-per-batch-gRPC (Flight-DoExchange stream, or shared-mem same-node). Data-driven, not blind. **RFC-transport** | **P0** |
| **Memory / buffers** | F5 spill bounds STATE; realtime holds unbounded pipeline buffers (12 GiB) | Flink FLIP-2 credit buffers + ForSt off-heap; Polars per-morsel SemaphorePermit + OOC spill; RisingWave network-buffer BP | dominant 12 GiB source UNKNOWN (re-profile P0). Ideal: byte-credit flow + per-morsel permits bound total in-flight. **RFC-memory** | **P0** |
| **Execution model** | DataFusion morsel/vectorized + FlowEvent marker-col per operator boundary | Flink operator chaining (no serialize intra-task); DataFusion pipelined; Velox vectorized | per-operator FlowEvent encode/decode (partly cut). Ideal: **operator chaining** — pass FlowEvent intra-node, encode only at shuffle. **RFC-chaining** | P1 |
| **Scheduler** | Sail task scheduler (morsel-driven) | Flink pipelined-region + credit; Spark DAG stages; DataFusion `target_partitions` | overhead unmeasured — profile before touching. **RFC-scheduler** | P1 |
| **Streaming / watermark** | E1–E5 contract (flush-on-transition fixed); dual idle mechanism | Flink in-band FIFO watermark + WatermarkStatus.IDLE + MIN; RisingWave barrier; Arroyo | consolidate dual-idle → E4-only (debt). Ideal: single canonical protocol. streaming-watermark.md | P1 |
| **State** | F5 spillable Arrow chunks + inc-ckpt O(delta) | Flink ForSt (disaggregated, 49× recovery); RocksDB SST; RisingWave Hummock | competitive; sliding-window specialized state (Arroyo 10×) is future. | P2 |
| **Checkpoint / recovery** | aligned barriers (Chandy-Lamport), inc-ckpt, per-epoch commit | Flink unaligned ckpt + ForSt; Spark offset log | commit was inline (now off-path); measure recovery wall-time (UNMEASURED). | P1 |
| **Fault tolerance / EO** | dup=0 kill-9 (aligned barriers + emit-floor) | Flink EO 2-phase; Spark idempotent sink | at parity (proven). Harden: node-loss/network-partition tests before EKS. | P1 |
| **Backpressure** | bounded mpsc (coarse count cap) | Flink FLIP-2 credit; Polars permit; RisingWave network-buffer | coarse → real byte-credit (ties to memory/transport RFCs). | **P0** |
| **Planner / optimizer** | DataFusion 54 + 3 adopted rules + Sail streaming rewriter | Spark Catalyst; Flink Calcite; DataFusion optimizer; RisingWave | adopt DF54 v0.6.5 features (PIVOT, window_time, lambda). | P2 |
| **Networking** | HTTP/2 8MiB window; Flight | Flink Netty credit; **Flink native S3 FS (2026-06)**; RisingWave | study Flink native-S3 for object-store IO; credit-flow. | P1 |
| **Storage / caching / lakehouse** | Delta+Iceberg, object-store, DF cache | Spark cache; Databricks Photon; RisingWave Hummock | adopt catalog-managed Delta/Iceberg; measure cache. | P2 |
| **Concurrency** | tokio async + Rust threads | Flink task threads; DataFusion tokio | profile scheduler/contention. | P2 |
| **Metrics / observability** | WM_PROF, KAFKA_STATS ad-hoc | Flink metrics + backpressure UI; Spark UI | **build proper per-stage metrics + flamegraph harness (prereq for all P0 root-causing)**. | **P0** |
| **Resource mgmt / autoscaling / elasticity** | key-group rescale (FLIP-8), crash-gated | Flink reactive/adaptive; Spark dynamic alloc; RisingWave | measure rescale wall-time; autoscaling untested. | P2 |
| **Batch** | 6.2× Spark (proven) | Spark/Photon; DataFusion; Velox | keep the win; regression-gate it. | P1 |
| **Realtime** | `.trigger(realTime=…)` wired (superset of Spark 4.2 stateless RTM); ~6–7M/s | Spark 4.2 RTM sub-second; Flink continuous | close throughput 1.4× + memory; sub-second latency target (RTM refs). | **P0** |

## The disciplined sequence (RFC per P0, each with a benchmark gate)
0. **RFC-observability (P0, prerequisite):** proper per-stage throughput/latency/alloc metrics +
   flamegraph + heap-profile harness that actually works (the M1 idle-server failure blocks everything).
   Fix the drain harness to measure **consumption-rate**, not commit-cadence. *No optimization RFC starts
   until this can produce before/after evidence.*
1. **RFC-memory (P0):** re-profile → attribute the 12 GiB (heap vs cache vs which buffer) → byte-credit
   flow (FLIP-2/RisingWave) + per-morsel permits (Polars). Gate: realtime RSS ≤ Flink.
2. **RFC-source-parse (P0):** SIMD/`Utf8View` JSON, vectorized decode. Gate: source+parse ≥ Flink read rate.
3. **RFC-transport (P0):** coalesce + credit-flow + data-driven leaner-than-per-batch-gRPC study. Gate:
   cross-node shuffle ≥ Flink; no correctness/EO regression.
4. **RFC-chaining (P1):** operator chaining (eliminate intra-node encode/decode). Gate: full ≥ Flink.

## Tech-debt removal (continuous, look-before-delete)
Dead/experimental paths measured-marginal or superseded → remove with a green regression suite:
`VAJRA_T7_FUSE`, `VAJRA_KAFKA_LEGACY_POLL`, dual-idle→E4-only, unused sweep knobs (RT_SINGLE already
removed). Keep proven-wired code (e.g. coalesce_flow_events IS in the Flight path — verified).

## EKS gate (production hardening — no surprises, fix before deploy)
Before any EKS: scale + failure + restart + recovery + OOM + network-interruption + node-loss +
backpressure + checkpoint-recovery + autoscaling + resource-limit + long-running-stability tests, all
green on T1(local)/T2(kind) first. EKS confirms the at-scale number only. Every prior EKS surprise
(region redirect, aws-auth corruption, CNI removal, cadence artifact) → a pre-EKS checklist item.

## Reference material (study WHY, improve on limits — do not copy)
Spark 4.2 RTM (databricks.com/blog/introducing-apache-spark-42; ultra-fast-anomaly-detection RTM;
databricks-blogposts/2026-04-rtm-sub-second-latency) · PySpark DataStreamWriter.trigger · Flink native S3
FS (flink.apache.org/2026/06/26) · Flink CDC · DataFusion/Arrow/arrow-rs · RisingWave 3.0 · Arroyo 0.15 ·
Polars streaming · Velox. Distilled facts live in [REFERENCES.md](../REFERENCES.md); append every fetch.
