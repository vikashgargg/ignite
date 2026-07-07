# Vajra BOARD — the master kanban (beat Spark + Flink on EVERY axis)

> **This is the single source of truth for "what's planned vs achieved" against the [CHARTER](../CLAUDE.md)
> aim: one unified engine that OBJECTIVELY BEATS Spark (batch) + Flink (streaming) on every production
> axis.** It is an INDEX over the detailed docs — it does not duplicate them:
> - Strategy / P0-P1: [flink-replacement-roadmap.md](design/flink-replacement-roadmap.md) · [PROD_GRADE_ROADMAP.md](PROD_GRADE_ROADMAP.md)
> - Measured dimension metrics: [prodgrade-dimensions-scorecard.md](design/prodgrade-dimensions-scorecard.md)
> - Distribution / repo GA: [public-ga-readiness-board.md](design/public-ga-readiness-board.md)
> - Streaming spec + gap register: [STREAMING_ARCHITECTURE.md](STREAMING_ARCHITECTURE.md)
> - Active epic: [EPIC-beat-flink-streaming.md](design/EPIC-beat-flink-streaming.md) · [vaj-bf2-distributed-streaming.md](design/vaj-bf2-distributed-streaming.md)
>
> **SDLC law (per charter):** every ticket (a) cites the axis it advances + a named OSS design ref,
> (b) is architect-first (design before code), (c) is DONE only when **T1 local → T2 kind → T3 EKS**
> are green (EKS confirms, never discovers), (d) links the commit the turn it lands, (e) claims ONLY
> measured head-to-head (flag path-dependence). No patch loops; root-cause from official docs.

**Legend:** ✅ done+measured · 🟡 in-progress/partial · 🔴 gap/unmeasured · ⬜ backlog.
Status vs **S**=Spark, **F**=Flink: `>` beats, `=` parity, `<` behind, `?` unmeasured.

---

## 1. Per-axis scorecard (charter axes × measured status)

| Axis | vs S | vs F | State | Evidence (measured) | Owning epic/ticket |
|------|:---:|:---:|:---:|---|---|
| **Batch throughput** | `>` | — | ✅ | P4 200M ETL: 5.92s vs Spark 36.94s = **6.2×**; TPC-H SF1 1.78 vs 63.46s | [P4](design/production-workload-benchmark.md) |
| **Streaming throughput** | — | `<` | 🟡 | EKS 100M bounded: 4.92–5.40M vs Flink 5.58–5.67M ev/s = **~1.05–1.15× behind** (path-dep) | **VAJ-BF2** (distribute exchange) |
| **Latency (e2e event→sink)** | `?` | `?` | 🔴 | UNMEASURED (D2) — likely no-GC win | [D2](design/prodgrade-dimensions-scorecard.md) |
| **Memory** | `>` | `~` | 🟡 | Continuous: 7.06 vs Flink 8.58 GiB (win); bounded: 10.38 vs 8.57 (lose) → **path-dependent** | [D3](design/prodgrade-dimensions-scorecard.md), F5 spill |
| **CPU / per-stage** | — | `~` | 🟡 | Per-stage ranked: from_json 135s > exchange 89.8s > finalize 27s > source_read 4.4s | VAJ-BF2 |
| **Network / shuffle** | — | `?` | 🟡 | Arrow-Flight zero-copy shuffle exists (batch); streaming exchange now distributable (T-BF2.2) | **VAJ-BF2** |
| **State mgmt** | — | `=` | ✅ | Spillable windowed-agg+join state (F5), out==N exact @5M; 64k-cap fixed | [F5](design/streaming-spillable-state-f5.md) |
| **Fault tolerance / EO** | `=` | `=` | ✅ | dup=0 across kill-9 on EKS (aligned barriers + exact idle + emit floor) | [distributed-eo](design/distributed-eo-coordinator-wiring.md) |
| **Recovery time** | — | `?` | 🟡 | Correctness proven; TIME not measured (Flink 2.0 ForSt 49× claim) | [D5](design/prodgrade-dimensions-scorecard.md) |
| **Incremental checkpoint** | — | `>` | ✅ | O(delta) on one Arrow substrate; manifest refs immutable F5 chunks (beats ForSt-RocksDB) | [inc-ckpt](design/streaming-incremental-checkpoint.md) |
| **Rescale / elasticity** | — | `=` | 🟡 | Key-group rescale on Arrow chunks (FLIP-8), crash-gated; bit-exact gated by EO residual | [rescale](design/streaming-rescale-from-checkpoint.md) |
| **K8s-native** | `=` | `=` | ✅ | `kubernetes-cluster` mode: driver dynamically launches worker pods + Flight shuffle | [f2f3 §F3-d](design/distributed-streaming-f2f3.md) |
| **Cost (idle→$0)** | — | — | ✅ | AWS torn to $0 when idle (standing discipline) | — |
| **Completeness** | `=` | `=` | ✅ | EKS 100M: 10 windows/100M matches Flink (VAJRA_COMPLETE_ON_END) | [completeness](design/EPIC-beat-flink-streaming.md) |
| **Parallel Kafka sink** | — | `=` | ✅ | 100M/100M delivered @1.67M msg/s (N parallel tasks, per-task txn.id) | [f2f3](design/distributed-streaming-f2f3.md) |
| **Realtime passthrough latency/thruput** | — | `<` | 🔴 | Vajra ~1.3K/s p50=257ms vs Flink 20K/s p50=98ms (un-batched Kafka sink) | [gap](design/EPIC-beat-flink-streaming.md) |
| **DX / PySpark-compat** | `=` | — | ✅ | PySpark runs unchanged; batch+streaming smoke 6/6 vs Spark 3.5.3 | [f2f3](design/distributed-streaming-f2f3.md) |
| **Interactive SQL** | `~` | — | 🟡 | ClickBench 60.11 vs LakeSail 65.50s (shared core); vs ClickHouse/Trino unmeasured | [clickbench](design/) |
| **AI-native execution** | `?` | `?` | ⬜ | Not started (charter axis; backlog) | — |
| **Lakehouse (Delta/Iceberg)** | `~` | — | 🟡 | Delta 144/163; Iceberg batch+stream partial | [delta](design/) |
| **Backpressure** | — | `?` | 🔴 | Bounded mpsc channels exist; not measured under slow sink (D10); credit-flow = T-BF2.4 | [D10](design/prodgrade-dimensions-scorecard.md) |

**Honest one-liner (per [competitive-claims-bar]):** Vajra **beats Spark decisively on batch**
(6.2×) and is **competitive-not-categorically-better vs Flink on streaming** — parity on
correctness/EO/state/completeness, path-dependent on memory/throughput, behind on realtime
passthrough latency + still-unmeasured on e2e latency/cold-start/recovery-time. The active epic
(VAJ-BF2) targets the one structurally-beatable stage: the distributed exchange.

---

## 2. Active sprint — EPIC VAJ-BF2 (distributed streaming + Arrow-Flight exchange)

**Goal:** beat Flink on streaming throughput by distributing the ranked #2 stage (exchange, 89.8s)
across nodes with no-JVM zero-copy Arrow shuffle — the only stage where Vajra can *structurally* win.
Design: [vaj-bf2-distributed-streaming.md](design/vaj-bf2-distributed-streaming.md).

| Ticket | Axis | Design ref | Backlog | Design | Impl | T1 | T2 | T3 | Commit |
|--------|------|-----------|:---:|:---:|:---:|:---:|:---:|:---:|--------|
| **BF2-Exp1** distributed exec on kind | K8s | f2f3 §F3-d | — | ✅ | ✅ | — | ✅ | — | 417cfc8e(prior) |
| **BF2-Exp2** root-cause: streaming pinned to 1 worker | network | planner.rs | — | ✅ | ✅ | — | ✅ | — | 417cfc8e |
| **T-BF2.2** cut stage boundary at StreamExchangeExec (1→N) | network/shuffle | f2f3 marker-shuffle | — | ✅ | ✅ | ✅ | ✅* | ⬜ | d816eac7 |
| **T-BF2.5** spread a stage's partitions across workers | scale/placement | Flink evenly-spread-out-slots / Spark spreadOut | — | ✅ | ✅ | ✅ | ⬜ | ⬜ | d02670ed |
| **T-BF2.3** N→M cross-network barrier/watermark align (benchmark is N→M) | FT/EO | Chandy-Lamport, RisingWave merger | ✅ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | — |
| **T-BF2.4** credit-based network backpressure | backpressure | Flink FLIP-8/FLIP-2 | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | ⬜ | — |
| **BF2-measure** multi-node exchange profile vs Flink | throughput/CPU | eks_stream_headtohead | ⬜ | ⬜ | — | — | — | ⬜ | — |

**T1 gate for T-BF2.2 (green):** unit tests gate-off→1 stage / gate-on→2 stages; `dist_streaming_smoke`
6/6 gate-ON local-cluster (`windowed_file=97` through the new shuffle) + 6/6 local; clippy `-D` green.
**Deployment parity (green):** local 6/6 · local-cluster 6/6 · kubernetes-cluster worker-launch confirmed.
**T2 result (2026-07-08, *=partial):** stage boundary cut confirmed, BUT (1) the multi-partition Kafka
benchmark is **N→M** (source parallelism = #kafka-partitions) so T-BF2.2's 1→N gate doesn't touch it
→ **T-BF2.3 is critical path**; (2) even 1→N did NOT spread — `TaskSlotAssigner::next()` fill-first-packs
a stage onto one worker → **new critical ticket T-BF2.5 (even placement)**. Cutting the boundary is
necessary but not sufficient. Kind torn down, AWS $0. Detail: [vaj-bf2 §4e](design/vaj-bf2-distributed-streaming.md).
**Critical path now:** T-BF2.5 (spread placement) → T-BF2.3 (N→M align) → T-BF2.4 → T3 EKS.

---

## 2b. Epics registry (the full epic list — done / active / planned)

| Epic | Axis focus | State | Evidence / doc |
|------|-----------|:---:|---|
| **E1** DF54/Arrow58.3 upgrade | foundation | ✅ | main @ merged; 860 tests, clippy green |
| **E2** Distributed batch (driver/worker, Flight shuffle, staged job graph) | scale/network | ✅ | dist_streaming_smoke batch 6/6 |
| **E3** Batch perf vs Spark | throughput/memory | ✅ | P4 6.2×; TPC-H 36×; ClickBench parity |
| **E4** Distributed stateful streaming (F2/F3) | streaming/state | ✅ | 6/6 vs Spark; multi-node KIND; continuous stateful EO across crash |
| **E5** Crash-EO correctness (aligned barriers, exact idle, emit floor) | FT/EO | ✅ | EKS dup=0 exact |
| **E6** Completeness + parallel Kafka sink | completeness | ✅ | EKS 10 windows/100M; 100M delivered @1.67M msg/s |
| **E7** Incremental checkpoint + rescale | ckpt/elasticity | ✅ | O(delta) merged; rescale key-groups (FLIP-8) |
| **E8** F5 spillable state | memory/state | 🟡 | out==N @5M; bounded-peak proof = F5.4 open |
| **E9** GA distribution/repo readiness | DX/ops | 🟡 | [public-ga-readiness-board](design/public-ga-readiness-board.md) |
| **E10** Prod-grade dimensions (D1–D10 measured) | all metrics | 🟡 | [scorecard](design/prodgrade-dimensions-scorecard.md); D2/D4/D10 unmeasured |
| **VAJ-T7** source-fusion throughput | throughput | ✅(null) | measured NO beat; kept opt-in; root-caused |
| **VAJ-BF2** distributed streaming exchange (Arrow-Flight) | network/throughput | 🟡 **ACTIVE** | §2 above; T-BF2.2 T1-green |
| **E-LAT** latency/cold-start/recovery-time measurement | latency | 🔴 | D2/D4/D5 — unmeasured, backlog |
| **E-RT** realtime Kafka→Kafka passthrough | latency/throughput | 🔴 | behind Flink; batch the Kafka sink |
| **E-AI** AI-native execution | AI-native | ⬜ | charter axis; not started |
| **E-LAKE** lakehouse (Delta/Iceberg) parity | lakehouse | 🟡 | Delta 144/163; Iceberg partial |
| **E-PEERS** vs ClickHouse/Trino/DuckDB/Polars | interactive SQL | ⬜ | charter peers; unmeasured |

## 3. Backlog (charter axes not yet in an active epic)
- 🔴 **D2 latency probe** (e2e event→sink p50/p99/p999) — the headline real-time axis, never measured.
- 🔴 **D4 cold start** (launch→first-output) — quick no-JVM win to quantify.
- 🟡 **D5 recovery-time** timing (not just correctness) vs Flink 2.0 ForSt.
- 🔴 **Realtime Kafka→Kafka passthrough** throughput/latency (batch the Kafka sink).
- ⬜ **AI-native execution** (charter axis) — scope from Ray Data / Daft / feature-pipeline patterns.
- 🟡 **Lakehouse** Delta 144/163 → parity; Iceberg batch+stream.
- ⬜ **vs ClickHouse/Trino/DuckDB/Polars** interactive-SQL head-to-heads (charter peers, unmeasured).

---

*Maintenance: update the cell + link the commit the SAME turn work lands. This board is loaded at
orientation (CLAUDE.md). Point-in-time claims must be verified against current code before re-asserting.*
