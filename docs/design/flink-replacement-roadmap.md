# Zelox → prod-grade, better-than-Flink streaming replacement — roadmap

**Thesis:** don't out-feature a decade of Flink. Win decisively on **cost/memory + unified engine +
no-JVM + incremental checkpoint**, reach **parity** on the rest, and make the few **operability**
features that actually gate prod adoption first-class. This doc is the standing strategic register;
streaming work cites a P0/P1 item here + the `STREAMING_ARCHITECTURE.md` gap cell it advances.

## HONEST scorecard (measured; claim only what's verified — see [[feedback_competitive_claims_bar]])
**EKS 100M, c7g.4xlarge, bounded availableNow (2026-06-30):** throughput Zelox 4.92M vs Flink 5.67M
ev/s = **1.15× SLOWER**; memory Zelox 10.38 vs Flink 8.57 GiB = **1.2× MORE** (this path). So we are
**competitive, NOT categorically better — yet.** Per-stage fix targets: source_read>exchange>from_json.

## Where Zelox is ahead / a moat (qualified — not universal)
- **Memory: 6.6× less ON THE CONTINUOUS path** (prior EKS, 1.28 vs 8.5 GiB) — BUT the bounded
  availableNow path shows 1.2× MORE (2026-06-30). ⇒ **path-dependent; do NOT claim "better memory"
  unqualified.**
- **No JVM** → no GC pauses, faster cold start, smaller footprint, predictable tail latency.
- **Incremental checkpoint on ONE Arrow substrate** (F5 spill chunks = checkpoint refs; window + join,
  O(delta) proven) → structurally cheaper than Flink's RocksDB-backed ForSt.
- **One engine, Spark API**: batch + streaming + interactive, no second system to operate.

## At parity (hold the line)
Correctness (per-partition watermark, auto), exactly-once across crash, transactional Kafka/file sinks,
event-time windows, stream-stream joins, spillable bounded-memory state.

## P0 — blockers to a credible "prod-grade replacement" (do these first, ~equal weight)
1. **Throughput** — close the measured ~2.4× windowed-agg gap → ≤1.2× Flink (stretch: beat).
   Plan: [eks-throughput-capstone.md](eks-throughput-capstone.md). Phase A instrument
   from_json/exchange/window stage timers → attribute; Phase B fix the dominant stage. *Necessary, not
   sufficient.*
2. **Rescaling from checkpoint** — restore a job at a **different parallelism** (redistribute keyed
   state). Flink's real operational killer feature; without it you can't grow/shrink a running job.
   **Zelox differentiator angle:** state is already immutable Arrow chunks → rescale = re-assign chunk
   key-ranges, cheaper than re-reading/re-partitioning a RocksDB snapshot. Design:
   `streaming-rescale-from-checkpoint.md` (TODO).
3. **Multi-node EO + soak/endurance + chaos at scale** — finish F2/F3 remainder (streaming Flight
   shuffle, concurrent stage scheduler, per-instance state snapshot) + a **multi-day soak** + random
   kill-chaos gate. "Survives one crash in a demo" ≠ "runs for weeks." See [[project_f2f3_distributed]].
4. **Observability + backpressure** — per-operator metrics (throughput, watermark lag, state size,
   checkpoint duration/size) exported (Prometheus) + a real backpressure mechanism. Ops cannot run a
   streaming engine blind.

## P1 — parity/ecosystem (after P0)
5. **Savepoint-based upgrades** (stop-with-savepoint, restore on new code/version), **state TTL**,
   **state schema evolution**.
6. **Connector breadth** — the few that matter: Kafka ✅, Iceberg/Delta, JDBC, S3 (not all of Flink's).
7. **Close continuous-EO epoch-boundary residual** + flip correctness-gate C6/C7 green (validates the
   correctness claim end-to-end). See [[project_windowed_agg_64k_cap]] / per-partition entry.

## Execution order (current)
1. **Throughput capstone** (P0-1) — biggest credibility lever; plan ready, Phase A is locally buildable.
2. **Rescaling-from-checkpoint design** (P0-2) — highest-leverage NEW differentiator; build on the
   incremental-checkpoint chunk/manifest substrate.
3. **Soak + chaos + metrics gate** (P0-3/4) — extend the correctness gate to multi-day + random kills +
   Prometheus metrics; this is what turns "works" into "prod-grade."

## One-line bar
Throughput is the headline number, but **rescaling-from-checkpoint** and a **soak/observability gate**
are equally P0 — they're what make teams trust a streaming engine in prod. Everything else stages
behind those.
