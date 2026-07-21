# EPIC VAJ-BEAT-FLINK — objectively beat Apache Flink on streaming (not match)

> **Charter mandate** ([MEMORY.md](../../MEMORY.md)): Zelox must *objectively beat* Flink on **every**
> production axis — throughput, latency, memory, EO/recovery, elasticity, K8s-native, cost, DX. This EPIC
> is the streaming-throughput+latency front. Bar: **≤1.0× Flink is the floor; the goal is a new era of
> streaming on Rust** — structurally ahead because no-JVM + Arrow zero-copy + fusion do less work per
> record than JVM+Jackson+GC can. Every ticket: architect-first (design + tests BEFORE code), grounded in
> a named KB source, gated T1→T2→T3 ([three-tier-sdlc.md](three-tier-sdlc.md)), claims MEASURED only.

## 0. Measured reality (the honest baseline — do not re-derive)
Source of truth: [throughput-tickets.md](throughput-tickets.md) (EKS 100M, c7g.4xlarge, vs Flink 1.19).

| Path | Zelox | Flink | Gap | Notes |
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
does **irreducible per-record work on the JVM heap**. Zelox's advantage is that we can do **less total
work**: parse the JSON value **directly into typed Arrow columns inside the source**, so the raw value
bytes are **never materialized as a full Arrow Binary column** (~10 GB @100M) and never re-read. No JVM,
no GC, no object graph — columnar all the way. **This is the axis Flink cannot follow** (it has no
zero-copy columnar source). That is the "new era on Rust" thesis, and it is measurement-justified: the
#1 CPU stage is exactly the raw-value-byte copy.

## 2. Tickets (JIRA-style; ranked by MEASURED remaining CPU)

### VAJ-T7 — Source-fusion: parse JSON→typed cols inside the Kafka read *(THE big one)*
- **Rank:** #1 (source_read 100.6s + from_json 31.7s = ~132s of ~206s upstream).
- **Thesis:** the raw `value` column (Arrow `Binary`, ~10 GB @100M) is materialized by the reader, copied
  through the exchange, then re-read by a downstream `from_json` projection. If the reader parses
  `value` → the target struct columns *during* batch build, the raw column is **never materialized**,
  the `from_json` projection + its `CAST(value AS string)` vanish, and the exchange carries only the
  narrow parsed cols. Grounded: Flink `deserialize`-in-source (KB §Flink) + DataFusion projection/expr
  pushdown into scan.

#### Concrete architecture (dep-layering + codec RESOLVED 2026-07-06)
- **Layering (resolved):** `zelox-data-source → zelox-function` is a **clean acyclic downward edge**
  (`zelox-function` deps = zelox-common / zelox-common-datafusion / zelox-sql-analyzer; none reach
  zelox-data-source). So the reader may call the parse routine directly — no relocation to a shared
  crate needed.
- **Step 1 — expose a batch parse API in zelox-function** (`scalar/json/from_json.rs`): promote
  `parse_json_to_struct` to `pub fn parse_json_binary_to_struct(values: &BinaryArray, fields: &Fields,
  options: &SparkFromJsonOptions, tz: &str) -> Result<StructArray>` (Binary variant of the existing
  Utf8 path; UTF-8 lossy per-row like Spark; reuses `ColBuilder` + `simd_parse_value` from T7b). Unit
  test it against the existing `from_json(CAST(value AS string), schema)` output for byte-identity.
- **Step 2 — `KafkaSourceExec` gains `parse_value_as: Option<(Fields, SparkFromJsonOptions)>`**
  (`formats/kafka/reader.rs`): `try_new` param + getter. When `Some`, `execute()`'s three read paths
  (bounded / realtime / continuous) build the value column, then call
  `parse_json_binary_to_struct` and emit the struct's fields (flow-event-wrapped) **instead of**
  `value:Binary`. Output schema = `to_flow_event_schema(project(struct_fields))`. When `None`,
  behaviour is byte-identical to today (default; zero risk to non-JSON pipelines).
- **Step 3 — codec round-trip** (`zelox-execution/src/codec.rs`, `KafkaSourceExecNode`): add proto
  field `parse_value_as` (serialized `Fields` + options); encode in the `downcast_ref::<KafkaSourceExec>`
  arm, decode in `NodeKind::KafkaSource`. Distributed round-trip test (the codec test at ~L4715).
- **Step 4 — physical-optimizer fusion rule** (`zelox-physical-optimizer`, new
  `fuse_streaming_source_parse.rs`): match `ProjectionExec`/unnest whose sole expr over the streaming
  source is `from_json(CAST(value AS Utf8), <schema-literal>)`; when the child is a `KafkaSourceExec`
  with `parse_value_as == None`, rewrite to the source with `parse_value_as = Some(...)` and drop the
  projection + CAST. Conservative: bail (leave plan unchanged) on any expr shape it doesn't recognize
  → correctness can never regress, only the fast path is missed. Register in `lib.rs` after projection
  pushdown, before TracingExec injection.
- **Anti-scope:** the raw string IS still produced when a query projects `value` itself (rule only
  fires when `from_json` is the sole consumer). No change to key/topic/offset/timestamp cols or to
  barrier/watermark emission (parse happens strictly inside batch build, before the FlowEvent adapter).
- **⚠️ SCHEMA-CONTRACT COUPLING (design finding 2026-07-06, Step 1 done):** the fused output schema is
  defined by the *removed* projection — `value:Binary` becomes a single `parsed:Struct(fields)` column
  named by that projection's output alias, alongside whichever kafka cols the query still keeps. So
  `parse_value_as` must carry `(output_field_name, Fields, SparkFromJsonOptions)` and the reader must
  recompute `projected_schema`/`output_schema` (swap value→struct) — and that computed schema MUST
  equal what Step 4's rule expects after dropping the projection. **⇒ Steps 2+3+4 are ONE atomic unit
  (co-design the schema contract; the reader can only be integration-tested via T2 kind once Step 4
  fires the path). Do not land Step 2's field in isolation.** Step 1 (`parse_json_binary_to_struct`,
  DONE 7898f8d3) is the isolated, unit-tested foundation the atomic unit builds on.
- **DoD:** WM_PROF `source_read + from_json` combined CPU **< Flink's equivalent**; EKS 100M **≤1.0×
  Flink ev/s** (the beat); RSS ≤ Flink; correctness_gate 6/6 + inc_ckpt dup=0 **UNCHANGED**; new
  Binary-parse parity test + codec round-trip test green; T2 kind streaming/kafka-sink exact.
- **Sequencing note:** T7b (`simd_parse_value`, DONE 153ae332) is the parser Step 1 reuses. Implement
  T7 as a focused unit (reader path is EO-critical — do not land half-done); Steps 1+3 are low-risk and
  independently testable, Steps 2+4 are the careful part.

#### KB grounding (named sources — cite, don't re-derive)
- **REFERENCES §6 "columnar edge" (the core thesis + a HONEST warning):** Flink's Kafka SplitReader
  deserializes **per-record into JVM objects** (object churn + GC, row-at-a-time) — the structural
  weakness a columnar engine beats. Arroyo (Rust+Arrow, *our* stack) beats Flink **5×+** by staying
  **columnar end-to-end, never row-at-a-time**. **Critical nuance the KB already measured:** an
  arrow-json columnar decoder *in isolation* was **~parity with serde_json** (0.418s) — "helps only if
  fed zero-copy (no NDJSON rebuild)." **⇒ T7b (a faster parser) alone is expected ~parity; the BEAT is
  T7 eliminating the raw-value materialize + exchange-copy + re-read (columnar end-to-end).** Set
  expectations accordingly: do not claim a win from T7b in isolation.
- **Polars (REFERENCES §8):** Arrow-native, zero-copy, vectorized + lazy **projection pushdown into
  scan** — T7's optimizer rule is exactly projection-pushdown of `from_json` into the streaming source.
- **prodgrade-practices Throughput row:** "vectorized batch ops, morsel-parallel, zero-copy shuffle,
  **parallel parse**" — T7 is the parse-in-source half; VAJ-BF2 (Arrow Flight) is the shuffle half.
- **RisingWave 3.0 (REFERENCES §8):** compute/state separation is the streaming frontier — informs
  VAJ-BF3 (not T7), noted so the epic stays honest about where each engine's idea applies.

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

### VAJ-BF2 — multi-node streaming + Arrow Flight zero-copy shuffle *(THE structural beat; chosen 2026-07-07)*
- **Why this is the lever (COMPLETE measured ranking, 2026-07-07, source_read now instrumented):**
  clean 20M/16-part profile — `from_json=135s` (#1, intrinsic JSON tokenize Flink's Jackson also pays,
  already simd-json'd = PARITY, not the differentiator) > **`exchange=89.8s` (#2, the keyed shuffle =
  the BF2 target)** > `finalize=27s` > `source_read=4.4s` (CHEAP — librdkafka bg prefetch; RULED OUT as
  a lever) > `encode=0.3s`; window `STARVED(upstream)`. ⇒ single-node is parse-bound PARITY (Zelox
  ~1.05× behind on identical work); the exchange is the #2 cost and the ONLY stage where Zelox's no-JVM
  Arrow zero-copy NETWORK shuffle can STRUCTURALLY beat Flink's JVM-serialized shuffle — but that only
  shows **multi-node** (here the exchange is in-memory). Measure-first RULED IN exchange, RULED OUT
  source_read + parse.
- **⚠️ SCOPING (code-verified 2026-07-07): BF2 is GREENFIELD, not an optimization.** The streaming path
  is **single-process today**: `StreamExchangeExec` moves data via in-memory `tokio::mpsc` channels
  (`exchange.rs:26`), and the deploy is ONE pod (`--mode local-cluster --workers 4`, `replicas:1`). There
  is NO cross-node streaming shuffle and Arrow Flight is NOT in the streaming path (it IS in the *batch*
  ShuffleWrite/Read path — reuse that transport). So BF2 must ADD distributed streaming.
- **Design (architect-first; grounded in REFERENCES §4 Ballista 53.0.0 Flight + §2d + Flink FLIP-8):**
  1. Distributed streaming topology: N worker pods (not local-cluster-in-one-pod); the driver assigns
     source/exchange/window stages across pods.
  2. `StreamExchangeExec` network transport: replace/augment mpsc with **Arrow Flight `DoGet`/`DoPut`
     (or the existing batch Flight transport)** for cross-pod sub-channels; zero-copy Arrow IPC, no
     JVM copy. Same-pod links stay mpsc (don't pay network for co-located).
  3. Marker/watermark alignment ACROSS the network: the receiver MIN-merges watermarks + buffers
     `Checkpoint{e}` barriers across network sub-channels (extend the existing aligned-barrier logic).
  4. **Credit-based backpressure across the network** (Flink FLIP-8) — bound in-flight Flight batches
     (the mpsc `channel_capacity` is the local analog; make it a network credit).
  5. EO preserved across nodes (barrier-aligned commit already exists — verify across the network cut).
- **Measure-first PREREQ:** build a MULTI-NODE benchmark (≥2 compute nodes, 16-part) + get the CLEAN
  per-stage profile WITH source_read instrumented (**note: `SOURCE_READ_NS` prof_add exists only in the
  BOUNDED read path `reader.rs:886`, NOT the continuous path — wire it there first so the distributed
  profile is complete**). Rank exchange-network vs source_read vs parse BEFORE committing the transport.
- **DoD:** EKS **multi-node** (≥2 compute) windowed-agg throughput **> Flink** at ≥16-part; EO preserved
  (dup=0 across crash + across the network cut); per-stage profile shows the network exchange < Flink's;
  T1 (local multi-process) → T2 kind (multi-pod) → T3 EKS multi-node. Claim only the measured number.
- **HONEST scope:** this is a multi-session, next-generation capability (distributed streaming execution
  + network shuffle + cross-node EO), NOT a patch. Architect-first each sub-part from the cited sources.

### VAJ-BF3 (stretch) — concurrent stage scheduling + credit-based flow control w/ metrics
- Pipeline stages instead of block (Spark 4.1 RT-mode shape); explicit credit backpressure + Prometheus
  per-operator throughput/watermark-lag/ckpt metrics (matrix Observability P0 + Backpressure).

### VAJ-T7 — IMPLEMENTATION STATUS (2026-07-06)
**All 4 steps landed, opt-in behind `ZELOX_T7_FUSE`, default byte-identical. Compiles + clippy
clean + codec round-trip tested. NOT yet behaviourally validated (needs a running plan).**
- Step 1 `parse_json_binary_to_struct` — DONE 7898f8d3 (20/20 parity, byte-identity test).
- Steps 2+3+4 — DONE 47b8206d: reader `parse_value_as` fusion (3 read paths via `fuse_event_stream`);
  codec `KafkaSourceExecNode` round-trip (`test_round_trip_kafka_source_exec_fused` green);
  `rewrite_source_fusion` per-worker rule in `task_runner/core.rs` (matches
  `ProjectionExec[from_json(value)]` over `KafkaSourceExec`, conservative-bail).

**✅ T1 LOCAL GREEN (2026-07-07, a0525f0c):** `inc_ckpt_gate.sh` with `ZELOX_T7_FUSE=1` →
`VAJ-T7 source-fusion: fused from_json -> '#7'` FIRES + `exit=0` (dup=0 EO across kill-9, exact
per-key window counts). The T1 dump pinned the real plan shape —
`ProjectionExec[_marker,_retracted,from_json(value@2) as #7,partition@3] -> CooperativeExec ->
KafkaSourceExec` — and the matcher now targets it exactly (identity-map-except-value, unwrap the
`CooperativeExec`). Next: T2 kind (rule fires on real k8s + WM_PROF source_read drop) → T3 EKS beat.

**✅ T2 KIND GREEN (2026-07-07):** `TAG=t7fuse ZELOX_T7_FUSE=1 kind_streaming_test.sh` on real k8s
(control-plane + kafka + compute nodes) → `T2_COMPLETENESS n_windows=2 sum_count=2000000 ... PASS`
+ `source-fusion: fused` FIRES in the compute pod logs. Confirms the rule matches on a real k8s
scheduler/network, not just local process. (Throughput on kind is not representative; the
source_read-drop measurement is deferred to T3 EKS where it is meaningful vs Flink.)

**🔴 T3 EKS RESULT (2026-07-07, 100M, c7g.4xlarge, vs Flink 1.19) — T7 is CORRECT but NOT a throughput
beat (HONEST negative result):**
| Engine | ev/s | RSS |
|---|---|---|
| Flink 1.19 | **5.580M** | 8.57 GiB |
| Zelox unfused (baseline) | 5.255M | — |
| Zelox **fused (T7)** ×3 | 5.24 / 5.24 / 5.40M | — |
Fusion FIRED (verified `T7FUSE=1` + `fused count:1` + plan dump), EO-safe, counts exact — but
throughput is **unchanged vs unfused**, still ~1.05× behind Flink. **Do NOT flip default-on** (no
measured perf justification; keep opt-in).
- **Root-cause of the null result (mechanistic):** in the *unfused* plan `from_json` already runs in
  the projection BEFORE the exchange, so the raw `value` column was never shuffled. T7 only changes
  WHERE the identical simd-json parse happens and saves one in-memory column materialize — NOT the
  dominant `source_read` cost, which is the **Kafka network read + Arrow decode of the value bytes**
  (unavoidable regardless of parse placement). ⇒ the earlier "source_read 100.6s = the lever"
  attribution conflated the Kafka read with parse-placement. **The real levers are the Kafka read
  path + the exchange, not where from_json sits.** Redirect: VAJ-BF2 (Arrow Flight zero-copy
  exchange) + a Kafka-read/decode profile become the priority; T7 stays as a correct, low-risk,
  opt-in mechanism that may help memory (raw col not materialized) — re-measure RSS specifically.
- **Methodology bug caught (fixed forward):** the head-to-head's `kk set env` AFTER deploy triggered a
  maxSurge=1 rollout deadlock on the 16-vCPU node → the FIRST run silently used the UNFUSED old pod
  (fused count:0). Caught it, patched `maxSurge=0` + forced the fused pod, re-ran (fused count:1).
  Fix forward: set ZELOX_T7_FUSE in the deploy YAML env (not post-hoc set-env), or maxSurge=0 default.

**Remaining validation:**
2. **Validation runbook (T1→T2→T3):**
   - **T1 local:** `inc_ckpt_gate.sh` with `ZELOX_T7_FUSE=1` → dup=0 + correct counts (EO across
     crash on the fused path); and WITHOUT the flag → unchanged (regression guard).
   - **T2 kind:** `kind_streaming_test.sh` + windowed-agg benchmark with `ZELOX_T7_FUSE=1` →
     n_windows/sum exact; confirm the rule fired in worker logs; per-stage WM_PROF shows
     `source_read` dropped (raw-value materialize gone).
   - **T3 EKS:** 100M head-to-head vs Flink with `ZELOX_T7_FUSE=1` → the measured beat
     (source_read+from_json combined CPU < Flink; ev/s ≤1.0× Flink; RSS ≤ Flink). Image via
     `eks_build_image.sh`.
3. **Then flip default-on** once T1+T2+T3 green, and update `throughput-tickets.md` +
   `feedback_competitive_claims_bar` with the measured number (claim ONLY the EKS head-to-head).

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

## 4. Sequence — REVISED by measurement (2026-07-07)
Original plan assumed T7 (parse-in-source) was the beat. **T3 EKS + local WM_PROF proved otherwise**
and redirect the sequence:
- **DONE:** VAJ-T7b (simd-json) + VAJ-T7 (source-fusion) — implemented, T1/T2/T3 green (correct, EO-safe),
  but **MEASURED = no throughput beat** (fused≈unfused; `from_json` already runs pre-exchange).
- **MEASURED bottleneck:** WM_PROF shows the window operator is **`STARVED(upstream)`**
  (`input_wait` ≫ stage CPU) — i.e. the throughput limit is the **source→exchange FEED RATE**, not any
  per-record compute stage. The parse was never the lever.
- **REAL levers (already KB-grounded — cite, don't re-derive):**
  1. **VAJ-BF2 — Arrow Flight zero-copy exchange** (REFERENCES §4, Ballista 53.0.0 `DoGet`/`DoPut`) +
     §2d streaming-shuffle. The exchange feeds the window; a zero-copy columnar shuffle raises feed rate.
  2. **VAJ-BF3 — concurrent-stage scheduling + credit-based flow control** (REFERENCES §2d Spark 4.1
     RT-mode pipelined stages; §Flink FLIP-8 credit backpressure). Decouple source/exchange/window so
     they PIPELINE instead of the window starving on a blocking upstream. This directly attacks
     `input_wait`.
  3. **Kafka read/decode profile** — quantify the read path (librdkafka fetch + Arrow decode) with the
     now-fixed WM_PROF (RUST_LOG must include `sail_physical_plan::streaming::window_accum=info`).
- **PREREQUISITE (measure-first, prod-grade):** before building BF2/BF3, get a CLEAN per-stage EKS
  profile (non-crash, 100M, correct RUST_LOG) to rank source_read vs exchange vs input_wait — pick the
  lever from the ranked number, NOT a guess (the T7 lesson). BF2 vs BF3 priority = whichever the profile
  ranks dominant.
- **VAJ-T7 residual value:** may reduce RSS (raw `value` col not separately materialized) — UNMEASURED;
  re-measure RSS fused vs unfused before any claim. Kept opt-in either way.

**Do NOT interleave; architect-first each ticket from the cited official sources; T1→T2→T3; claim only
measured head-to-head.**
