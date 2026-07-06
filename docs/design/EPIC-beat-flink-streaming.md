# EPIC VAJ-BEAT-FLINK — objectively beat Apache Flink on streaming (not match)

> **Charter mandate** ([MEMORY.md](../../MEMORY.md)): Vajra must *objectively beat* Flink on **every**
> production axis — throughput, latency, memory, EO/recovery, elasticity, K8s-native, cost, DX. This EPIC
> is the streaming-throughput+latency front. Bar: **≤1.0× Flink is the floor; the goal is a new era of
> streaming on Rust** — structurally ahead because no-JVM + Arrow zero-copy + fusion do less work per
> record than JVM+Jackson+GC can. Every ticket: architect-first (design + tests BEFORE code), grounded in
> a named KB source, gated T1→T2→T3 ([three-tier-sdlc.md](three-tier-sdlc.md)), claims MEASURED only.

## 0. Measured reality (the honest baseline — do not re-derive)
Source of truth: [throughput-tickets.md](throughput-tickets.md) (EKS 100M, c7g.4xlarge, vs Flink 1.19).

| Path | Vajra | Flink | Gap | Notes |
|---|---|---|---|---|
| Windowed-agg throughput | **5.37M ev/s** | 5.72M | **1.068× behind** | steady, measured; per-stage CPU below |
| Memory (RSS) | **9.61 GiB** | 8.55 GiB | 1.12× behind | source value-byte copy is the cost |
| Realtime Kafka→Kafka passthrough | ~1.3K/s p50=257ms | 20K/s p50=98ms | **~15× behind** | likely un-batched Kafka sink (separate lever) |
| Crash-EO / completeness | **dup=0, n=exact** | dup=0 | **PARITY** ✓ | already beats-or-ties |

Per-stage CPU (windowed-agg, WM_PROF): **`source_read 100.6s ≫ from_json 31.7s > exchange 66.5s (LOW
ceiling, already optimized by VAJ-T4) > finalize 19.6s`**. **The exchange is NOT the lever** (VAJ-T4
proved shuffle cost = hash+route+send, not the copy). The levers are **source_read + from_json**.

## 1. The structural moat — why Rust+Arrow can BEAT (not just match) Flink
Flink's per-record path (KB [REFERENCES.md](../REFERENCES.md) §Flink): `KafkaDeserializationSchema` →
**Jackson tree parse per record** → object graph → JVM GC → pipelined shuffle. It is fast per-op but
does **irreducible per-record work on the JVM heap**. Vajra's advantage is that we can do **less total
work**: parse the JSON value **directly into typed Arrow columns inside the source**, so the raw value
bytes are **never materialized as a full Arrow Binary column** (~10 GB @100M) and never re-read. No JVM,
no GC, no object graph — columnar all the way. **This is the axis Flink cannot follow** (it has no
zero-copy columnar source). That is the "new era on Rust" thesis, and it is measurement-justified: the
#1 CPU stage is exactly the raw-value-byte copy.

## 2. Tickets (JIRA-style; ranked by MEASURED remaining CPU)

### VAJ-T7 — Source-fusion: parse JSON→typed cols inside the Kafka read *(THE big one)*
- **Rank:** #1 (source_read 100.6s + from_json 31.7s = ~132s of ~206s upstream).
- **Design:** Extend the T7a columnar `ColBuilder` so the Kafka reader parses `value` → typed struct
  columns during `KafkaArrowBuilders` append, keyed by the projected schema. The raw value column is
  produced ONLY when a query actually projects the raw string (rare); windowed-agg never does. Exchange
  then carries narrow parsed cols (also shrinks the 66.5s exchange + the memory gap). Grounded: Flink
  deserialize-in-source + DataFusion projection/expr pushdown into scan.
- **DoD:** WM_PROF `source_read + from_json` combined CPU **< Flink's equivalent**; EKS 100M **≤1.0×
  Flink ev/s** (the beat); RSS ≤ Flink; correctness_gate 6/6 + inc_ckpt dup=0 UNCHANGED; simd-json
  padding + semantic-parity tests (the 20 ColBuilder parity tests extended). Branch note:
  `streaming/t7-parse-fusion` exists — resume there or fold in.

### VAJ-T7b — simd-json direct-to-builder (replace the serde_json tree)
- **Rank:** #2 (the residual `serde_json` PARSE ~27s inside from_json).
- **Design:** Swap the serde_json tree parse for simd-json parse-into-`ColBuilder` (SIMD, padded
  buffers). Semantic parity with Spark JSON (nulls, type coercion, malformed → permissive).
- **DoD:** from_json CPU ~32→~18s (measured); parity tests green; feeds VAJ-T7.

### VAJ-BF1 — Realtime passthrough: batch the Kafka sink writes
- **Rank:** the ~15× realtime gap (separate from windowed-agg).
- **Design:** Measure first (the 1.3K/s was partly the 1/16 partition bug, now fixed — RE-MEASURE clean).
  If still behind, batch the `KafkaSinkExec` produce path (accumulate to a target byte/record budget +
  linger, like Flink's KafkaSink `batch.size`/`linger.ms`), preserving transactional EO. Grounded:
  Flink KafkaSink batching + librdkafka producer batching.
- **DoD:** clean realtime Kafka→Kafka p50/throughput re-measured vs Flink on EKS; document the honest number.

### VAJ-BF2 (stretch — "beyond matching") — Arrow Flight zero-copy shuffle
- **Rank:** the distributed-shuffle ⬜ gap (matrix: "Flight zero-copy").
- **Design:** Replace the in-memory stream shuffle with Arrow Flight `DoGet`/`DoPut` (Ballista 53.0.0
  model) between stages — zero-copy columnar exchange, disaggregated. Marker-aware; receiver MIN-merges
  watermarks. This is where Vajra can EXCEED Flink's network stack (no serialization, no JVM copies).
- **DoD:** EKS multi-node windowed-agg throughput ≥ Flink at ≥16-part; EO preserved; documented.

### VAJ-BF3 (stretch) — concurrent stage scheduling + credit-based flow control w/ metrics
- Pipeline stages instead of block (Spark 4.1 RT-mode shape); explicit credit backpressure + Prometheus
  per-operator throughput/watermark-lag/ckpt metrics (matrix Observability P0 + Backpressure).

## 3. SDLC / Definition of Done (every ticket)
1. **Architect-first:** design + acceptance test cases written and cited to a KB source BEFORE code.
2. **T1 local:** `correctness_gate.sh` 6/6 + `inc_ckpt_gate.sh` dup=0 (EO NEVER regresses) + WM_PROF
   per-stage CPU delta + `local_continuous_scale.sh` self-check. NO behavior change on EO/completeness.
3. **T2 kind:** `kind_streaming_test.sh` (n_windows/sum exact) + `kind_kafka_sink_test.sh` (delivered=N)
   UNCHANGED; throughput measured on real k8s.
4. **T3 EKS (confirm-only, tear to $0):** 100M head-to-head vs Flink 1.19 — the MEASURED beat number.
   Image built on throwaway c7g EC2 → ECR (`eks_build_image.sh`).
5. Claim ONLY the EKS head-to-head number; flag path-dependence; update `throughput-tickets.md` +
   `feedback_competitive_claims_bar` the same turn.

## 4. Sequence
VAJ-T7b (unblocks T7's parser) → **VAJ-T7 source-fusion** (the beat) → VAJ-BF1 (realtime re-measure) →
VAJ-BF2/BF3 (exceed, not just beat). Do NOT interleave with unrelated work — this is the capstone.
