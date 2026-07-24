# VAJ-BF2 — Distributed streaming + Arrow Flight exchange (architect-first design)

> **Status:** DESIGN (2026-07-07). Design-before-code per the charter. Grounded in the existing Zelox
> code (traced) + REFERENCES §4 (Ballista 53.0.0 Arrow Flight), §2d (Spark 4.1 RT-mode pipelined
> stages), Flink FLIP-8 (credit-based flow control). **Not yet implemented.**

## 0. Why (measured, not assumed)
Complete per-stage profile (clean 20M/16-part, 2026-07-07): `from_json 135s` (#1, intrinsic JSON
tokenize = PARITY with Flink's Jackson) > **`exchange 89.8s` (#2)** > `finalize 27s` >
`source_read 4.4s` (CHEAP, ruled out) > `encode 0.3s`; window `STARVED(upstream)`. Single-node
windowed-agg is parse-bound parity (Zelox ~1.05× behind Flink on identical work). The exchange is the
only stage where Zelox's **no-JVM Arrow zero-copy network shuffle** can *structurally* beat Flink's
JVM-serialized shuffle — but that only manifests **multi-node** (single-node exchange is in-memory).

## 1. The two big de-riskers (code-traced 2026-07-07)
1. **Arrow Flight transport ALREADY EXISTS + is tested.** `zelox-execution/src/stream_service/`:
   `FlightServiceClient` + `FlightRecordBatchStream` (`client.rs`), `TaskStreamFlightServer` + `do_get`
   + `Ticket` (`server.rs`), `test_arrow_flight_shuffle_roundtrip` (`tests.rs`). The **batch** shuffle
   already moves Arrow `RecordBatch` streams cross-node zero-copy over Flight. **Reuse it.**
2. **FlowEvents ALREADY ⟷ RecordBatches.** `zelox-common-datafusion/.../event/encoding.rs`:
   `EncodedFlowEventStream::encode(FlowEvent) -> RecordBatch` (data AND markers — Watermark/Checkpoint/
   Idle/EndOfData — encoded into a special-schema batch with `_marker`/`_retracted` columns);
   `DecodedFlowEventStream` reverses it. So a streaming sub-channel's `FlowEvent` stream **is** a
   `RecordBatch` stream and rides the existing Flight transport **with markers, unchanged**.

⇒ BF2 needs **no new wire format and no new transport**. The exchange payload (data + watermarks +
barriers) already serializes to Arrow RecordBatches, and Flight already carries Arrow RecordBatch
streams zero-copy. This is the no-JVM structural edge, already built for batch.

## 2. What's actually missing (the real work)
Today the streaming path is **single-process**: `StreamExchangeExec` (`zelox-physical-plan/.../
streaming/exchange.rs`) routes via in-memory `tokio::mpsc` channels, and the deploy is ONE pod
(`--mode local-cluster --workers 4`). BF2 must run streaming on the **existing** distributed mode:

- **T-BF2.1 — Multi-pod streaming topology. RESOLVED (2026-07-07): the distributed mode ALREADY
  EXISTS.** `zelox-cli/src/runner.rs`: three modes `local | local-cluster | kubernetes-cluster`;
  **`kubernetes-cluster`** is a real distributed multi-pod mode (`Cluster`/`ClusterRole::Worker` worker
  pods, Flight shuffle, K8s Lease-based scheduler-HA leader election). BUT: **every benchmark (batch AND
  streaming) ran `local-cluster` single-pod** (`k8s/eks/zelox-sf100.yaml`: "local-cluster on the single
  big node"), and the scheduler is run-to-completion (`job_scheduler/core.rs`: final-stages-succeed →
  job-succeeds — fits bounded/availableNow streaming via `EndOfData`; continuous runs forever). ⇒ BF2's
  topology work is **NOT greenfield distributed execution** (it exists) — it's (a) verify streaming runs
  on `kubernetes-cluster` mode across ≥2 worker pods, and (b) the real gap: `StreamExchangeExec` uses
  in-process `mpsc`, so it does NOT distribute — that's T-BF2.2. Investigate whether a streaming DAG
  placed on `kubernetes-cluster` already spreads stages across worker pods (like batch), leaving only
  the exchange transport to swap.
- **T-BF2.2 — cut a distributed stage boundary at `StreamExchangeExec` (the real, root-caused gap;
  Exp 2 2026-07-07).** The distributed planner (`job_graph/planner.rs::build_job_graph`) only cuts
  stage boundaries at `RepartitionExec`/`CoalescePartitionsExec`/`SortPreservingMergeExec`, so a
  streaming plan collapses to ONE stage on ONE worker (Exp 2 §4c: all 8 window partitions ran on
  worker 1; the mpsc exchange never crossed a pod). **Fix:** add a `StreamExchangeExec` arm to
  `build_job_graph` that emits the ALREADY-BUILT, ALREADY-MARKER-AWARE cross-node streaming shuffle
  stack via `create_shuffle` — `ShuffleWriteExec` (broadcasts markers, shuffle_write.rs:202–228) on
  the producer side, `StageInputExec`/`ShuffleReadExec` on the consumer, carrying the
  `FlowEventToData(StreamCoalesce(StreamExchange(child)))` stack that the codec already round-trips
  (codec.rs:4652). Same-pod links can stay mpsc later as an optimization; the FIRST correctness cut is
  simply "make the streaming exchange a real distributed stage." **No new transport, no new wire format
  — one planner arm + source→shuffle partitioning wiring.**
- **T-BF2.3 — Cross-network barrier/watermark alignment.** The receiver already MIN-merges watermarks
  and buffers `Checkpoint{e}` barriers across sub-channels (`merge_output_subchannels`,
  aligned-barrier logic). Verify it operates identically when a sub-channel is a network Flight stream
  (markers arrive as decoded FlowEvents — same code path). The EO barrier-aligned commit must hold
  across the **network cut** (a worker crash mid-epoch).
- **T-BF2.4 — Credit-based network backpressure (Flink FLIP-8).** The mpsc `channel_capacity`
  (`ZELOX_EXCHANGE_CHANNEL_CAP`, default 16) is the local backpressure bound. Over Flight/gRPC there is
  HTTP/2 flow control, but we want *explicit* credit so a fast producer can't unbounded-buffer at the
  receiver. Design an application-level credit (receiver grants N-batch credit; producer blocks when
  exhausted) mirroring the mpsc bound. Grounded: Flink FLIP-8 credit flow control.

## 3. Design decisions (objectively-better checks)
- **Same-pod = mpsc, cross-pod = Flight** (don't serialize co-located data). Better than Flink, which
  serializes even for local channels within a TaskManager unless operator-chained.
- **Zero-copy Arrow all the way** (no JVM heap copy, no GC) — the structural moat. Flight `do_get`
  streams Arrow IPC; the receiver gets Arrow buffers directly.
- **Reuse, don't reinvent** — the batch Flight transport + flow-event encoding are proven; BF2 composes
  them. Lower risk + less code than a bespoke streaming network layer.

## 4. Measure-first + SDLC (per charter)
1. **Multi-node benchmark FIRST** (≥2 compute nodes, 16-part) — the single-node profile can't show the
   network exchange. Build the topology, then profile network-exchange vs parse vs source with the
   now-complete WM_PROF (source_read wired 2026-07-07). RANK before optimizing the transport.
2. **T1 local multi-process** (multiple `zelox` server processes on one host, Flight between them) —
   correctness_gate + inc_ckpt dup=0 across the network cut.
3. **T2 kind multi-pod** (≥2 zelox-stream pods) — n_windows/sum exact; fusion/EO unchanged.
4. **T3 EKS multi-node** (≥2 compute) — windowed-agg throughput **> Flink** at ≥16-part; per-stage
   network-exchange CPU < Flink's shuffle; EO dup=0 across crash + network cut. Claim ONLY the measured
   multi-node head-to-head; tear to $0.

## 4b. Experiment 1 result (kind, 2026-07-07) — distributed execution CONFIRMED (batch); streaming pending
Deployed `k8s/zelox.yaml` (kubernetes-cluster driver, `zelox:t7fuse`) on kind. A trivial distributed
query (`spark.range(0,1e6,1,8).sum`) returned the correct result **and the driver dynamically launched
5 worker pods** (`zelox-worker-*-1..5`) — so kubernetes-cluster worker-pod launch + cross-pod Flight
shuffle + correct result are **PROVEN on kind**. The hard part (distributed worker launch + Flight
shuffle) works. **Still UNOBSERVED:** the STREAMING windowed-agg cross-pod behavior — the streaming run
was blocked by kind Kafka data-path friction (topic empty: producer BOOT namespace, slow single-broker
kind Kafka), NOT a Zelox issue. **Next-session step (skip the friction):** get a small `events` topic
populated in the driver's namespace (fix producer BOOT to `kafka.<ns>.svc`, or pre-create+seed the
topic), run `stream_windowed_agg.py` against the kubernetes-cluster driver, and watch: does the
streaming DAG spread across the launched worker pods, and does `StreamExchangeExec` (mpsc) error /
fall-back / route cross-pod? That single observation scopes T-BF2.2.

## 4c. Experiment 2 result (kind, 2026-07-07) — ROOT-CAUSED: streaming DAG pinned to ONE worker
Seeded a small `events` topic (400k rows, 8 parts, exact `{"k","ts","v"}` scheme) **in the driver's
namespace** (`zelox`), skipping the flaky producer BOOT path, then ran the bounded windowed-agg
(`trigger(availableNow=True)`) against the kubernetes-cluster driver. Observed:
- **The driver launched 5 worker pods for the STREAMING job** (`zelox-worker-nwfrntow45-1..5`, all
  Running) — so worker-pod launch happens for streaming, not just batch.
- **BUT the entire windowed-agg ran on ONE worker.** Worker 1's log shows `F5_PEAK p0..p7` — **all 8
  window partitions on a single pod**; workers 2–5 started their server, did **zero** window work, and
  were aborted at shutdown. The query executed to completion with **no exchange error** — it only
  failed the post-hoc `read.parquet` verification (`No files found in file:///tmp/zelox/wagg_out/`),
  which is expected kind hostPath locality (sink wrote on worker 1's node; driver read its own node),
  **not** a Zelox fault.
- ⇒ `StreamExchangeExec`'s mpsc **never crossed a pod** — not because it errors, but because the whole
  streaming pipeline (source → exchange → window) is co-located on one worker.

**ROOT CAUSE (code-traced, `job_graph/planner.rs::build_job_graph`):** the distributed planner cuts a
cross-node stage boundary — via `create_shuffle` → `ShuffleWriteExec`/`StageInputExec` — **only** for
`RepartitionExec`, `CoalescePartitionsExec`, `SortPreservingMergeExec` (planner.rs:208–210).
`StreamExchangeExec` is **not in that match**, so it falls through the `else` (line 223/278): children
stay in the same stage, plan unchanged. The streaming exchange is therefore a *within-stage* mpsc
exchange, the whole pipeline is one stage, and one-stage → one-worker. **That is the single reason
streaming doesn't distribute** (batch does, because batch shuffles ARE `RepartitionExec`).

**This SHARPENS + DE-RISKS T-BF2.2 dramatically** (see revised §2). The cross-node streaming shuffle
machinery is ALREADY built and marker-aware — `ShuffleWriteExec::shuffle_write` broadcasts markers
(watermark/checkpoint/EndOfData) to all sinks (shuffle_write.rs:202–228), and the codec ALREADY
round-trips the streaming shuffle stack `FlowEventToData(StreamCoalesce(StreamExchange(child)))`
(codec.rs:4652; `StreamCoalesceExec` exists at exchange.rs:255). The **only** missing piece is the
**planner arm**: teach `build_job_graph` to recognize `StreamExchangeExec` as a stage boundary and emit
that already-built streaming shuffle stack via `create_shuffle`. No new transport, no new wire format,
no new shuffle-write path — one planner arm + wiring the source stage's output partitioning to the
shuffle. (Then verify barrier alignment across the network cut = T-BF2.3, credit backpressure =
T-BF2.4.)

## 4d. T-BF2.2 IMPLEMENTED + T1-validated (2026-07-07, commit d816eac7)
Added the `StreamExchangeExec` stage-boundary arm to `build_job_graph` (planner.rs), gated
`ZELOX_DISTRIBUTED_STREAM=1` (resolved once at `try_new`, threaded — not per-node env). It emits the
existing marker-aware Hash shuffle via `create_shuffle` (the exchange's properties already carry
`Partitioning::Hash(keys, N)`), so the N window instances distribute across workers. **Prod-grade
guards:** default keeps the F2/F3-validated inline path (additive/reversible); only the **align-free
1→N** case is cut (single-partition source ⇒ each window instance has one upstream ⇒ broadcast marker
arrives once ⇒ Flink single-input needs no alignment); **N→M is left INLINE** until T-BF2.3 wires
`StreamBarrierAlignExec` at the cross-node receiver, so we never silently mis-align a multi-upstream
barrier. **T1 green:** deterministic unit tests (gate OFF→1 stage, gate ON→2 stages — proves it fires
only when gated); `dist_streaming_smoke` **6/6** with the gate ON (local-cluster, workers=4;
`windowed_file=97` preserved through the new shuffle); clippy `-D warnings` green.

**Deployment-mode parity check (2026-07-07) — Zelox runs the full batch+streaming suite like
Spark/Flink across all three modes:**
| Mode | Spark/Flink analogue | `dist_streaming_smoke` |
|------|----------------------|------------------------|
| `local` (single process) | Spark `local[N]` | **6/6** |
| `local-cluster` (driver + N in-proc workers), gate ON | Spark standalone / Flink local cluster | **6/6** (exercises the new shuffle boundary) |
| `kubernetes-cluster` (driver pod dynamically launches worker pods) | Spark/Flink on k8s / EKS | worker-pod launch + Flight shuffle confirmed on kind (Exp 1/2) |

**Next (T2):** kind multi-pod with the gate ON — confirm the N window instances land on **different**
worker pods (the placement Exp 2 showed pinned to one), counts exact. Then T-BF2.3 (N→M receiver align)
+ T-BF2.4 (credit backpressure) → T3 EKS multi-node vs Flink on the ranked #2 exchange stage.

## 4e. T2 kind multi-pod result (2026-07-08) — TWO real blockers found (before EKS spend)
Ran the gate-ON `zelox:bf2` on a 3-node kind cluster (`kind-multinode.yaml`), driver flag
`ZELOX_DISTRIBUTED_STREAM=1` confirmed, 400k seeded. **Two findings, both root-caused from code:**

1. **The multi-partition Kafka benchmark is the N→M case, NOT 1→N.** The realtime Kafka source sets
   `parallelism = count_kafka_partitions()` (reader.rs:361) = 8 for an 8-partition topic ⇒ plan is
   `KafkaSource(8) → StreamExchange(8→8) → Window(8)`. `distributed_stream_boundary` correctly requires
   `partition_count()==1`, so it does **not** fire (N→M needs receiver align = T-BF2.3). ⇒ T-BF2.2 alone
   never touches the real benchmark; **T-BF2.3 is on the critical path**, not optional-later.
2. **Even the 1→N case does not distribute — the scheduler PACKS a stage onto one worker.** With a
   1-partition topic (`events1` ⇒ `KafkaSource(1) → StreamExchange(1→8)`, gate fires, plan = 2 stages
   per the unit test), a fresh-pool isolated run STILL ran all 8 window partitions (`F5_PEAK p0..p7`) on
   ONE worker pod. **Root cause:** `TaskSlotAssigner::next()` (task_assigner/core.rs:284) is a fill-first
   bin-packer — `self.slots.iter_mut().find_map(|(w,slots)| slots.pop())` drains worker[0]'s slots
   entirely before moving to worker[1]. So an N-partition stage lands wholly on one worker if it has ≥N
   free slots. Flink spreads subtasks across TaskManagers (slot spreading); Spark has spread-out
   scheduling. ⇒ **new ticket T-BF2.5 (spread a stage's partitions across workers) is the actual
   distribution unlock** — cutting the stage boundary is necessary but not sufficient.

**Net (honest):** T-BF2.2 is correct + validated (plan-level 2-stage cut, 6/6 correctness) but
**insufficient alone**. The distributed-streaming beat needs, in order: **T-BF2.5** round-robin/even
task placement (small, surgical, but changes global placement → gate + batch-correctness T1) →
**T-BF2.3** N→M cross-network barrier align (so the 8-partition benchmark distributes) → **T-BF2.4**
credit backpressure → T3 EKS. T2 caught this before EKS spend — exactly its purpose (kind torn down,
AWS $0). *(Secondary harness note: kind cross-node `file:///tmp/zelox` read still flaky for the post-hoc
verify — the F5_PEAK worker logs are the reliable placement signal; use an object-store/S3 sink for the
EKS verify, per f2f3 §F3-d.)*

## 4f. T-BF2.3 architecture (N→M cross-network barrier align) — DESIGN (2026-07-08)
**Goal:** distribute the REAL benchmark — `KafkaSource(N) → StreamExchange(N→M) → Window(M)` — across
workers with correct exactly-once (the N→M case T-BF2.2 deliberately left inline). Grounded:
REFERENCES §2 (Flink Chandy-Lamport: barriers never overtake records; multi-input operators align on
every input before snapshot), §8 (RisingWave merger aligns barriers at multi-upstream actors),
docs/design/streaming-prodgrade-practices.md row "Distributed shuffle" (receiver MIN-merges watermarks).

**What's already built (traced 2026-07-08):**
- Sender half = `ShuffleWriteExec` — hash-routes data + **broadcasts markers** to all M outputs
  (shuffle_write.rs:202–228). Reusable as-is for N→M.
- The in-process `StreamExchangeExec` receiver ALREADY does the correct N→M merge: per output
  partition it keeps N sub-channels, **MIN-merges watermarks** (Flink keyBy) + **aligns Checkpoint
  barriers** + drains-to-max on all-idle. The ONLY problem is those sub-channels are `tokio::mpsc`
  (in-process), so cutting the exchange into a batch shuffle loses them.
- `StreamBarrierAlignExec` (barrier_align.rs) has the Chandy-Lamport align state machine over N
  streams, BUT it **dedups** watermarks (forwards only input-0's copy — correct only when the N inputs
  are broadcast COPIES of one upstream, NOT for N distinct source partitions).

**The blocker (traced):** the generic `ShuffleReadExec` merges the N producer sub-streams with
`MergedRecordBatchStream` = `futures::select_all` = a **naive interleave** (no marker awareness). So a
batch-style shuffle read cannot align N→M: it interleaves N copies of each barrier + N distinct
watermarks with no MIN-merge → wrong epochs / early window fire. This is why T-BF2.2 correctly gates to
1→N (where each consumer has exactly ONE sub-stream, so select_all is trivially correct).

**Design (chosen): a marker-aware aligning merge for streaming shuffle reads.** For a flow-event
shuffle (marker schema present), the consumer partition's `ShuffleReadExec` must merge its N
sub-streams with an operator that (a) **MIN-merges watermarks** across the N sources, (b) **aligns
Checkpoint{e}** (block a source that reached e until all reach e, forward ONE barrier), (c) forwards
`EndOfData` once all N end, (d) supports Flink-style **idleness** (a source with no data excluded from
the watermark MIN so a window still closes). This is exactly `StreamExchangeExec`'s receiver logic —
so the prod-grade path is to **factor that receiver into a reusable `align_flow_event_streams(streams,
mode)` combinator** (mode = `MinMerge` for N→M shuffle / `Dedup` for broadcast N→1), then call it from
BOTH `StreamExchangeExec`'s receiver AND the streaming `ShuffleReadExec` (batch shuffles keep
`select_all`). ONE alignment implementation, no duplication, no new wire format (markers already ride
the shuffle as flow-event batches).

**Increments (each T1-gated; no patch):**
- **T-BF2.3a — factor the align combinator** (behavior-preserving): extract the align state machine so
  `StreamBarrierAlignExec` (Dedup mode) is unchanged (its existing unit tests + `dist_streaming_smoke`
  stay green) and a `MinMerge` mode exists. Unit test both modes on synthetic N-stream inputs.
- **T-BF2.3b — marker-aware `ShuffleReadExec`**: when the shuffle schema is a flow-event schema, use
  `align_flow_event_streams(sub_streams, MinMerge)` instead of `MergedRecordBatchStream`; batch keeps
  select_all. Round-trip any new flag in codec.
- **T-BF2.3c — planner cuts the N→M boundary**: extend `distributed_stream_boundary` to also cut
  `StreamExchangeExec` when input partition_count>1 (now that the aligning read exists); pairs with
  T-BF2.5 even-spread so the M window instances land on different pods.
- **T1 gate:** N→M windowed-agg (multi-partition Kafka/file source) counts EXACT + crash-EO dup=0
  through the aligning cross-network shuffle (correctness_gate + inc_ckpt) → T2 kind pods spread → T3.

**Correctness note (charter anti-patch):** only dup=0 via a CONSISTENT CUT advances the invariant.
The align + MIN-merge is the consistent-cut guarantee across the network; any early-fire/dup means the
alignment is wrong — do not paper over it.

## 4g. T-BF2.3b/c IMPLEMENTED + T1 result (2026-07-08, commit 0a9d631d)
Implemented: `merge_flow_event_streams` (exchange.rs — the validated `merge_output_subchannels`
N→M receiver exposed as a combinator) + streaming `ShuffleReadExec` uses it for flow-event shuffles
(batch keeps `select_all`) + `distributed_stream_boundary` now cuts N→M too. **T1:**
`dist_streaming_smoke` 6/6 gate-ON (flow-event shuffle via the new merge; `windowed_file=97`) + 6/6
local; align/exchange/planner unit tests green; clippy `-D` green.
**N→M probe (4-partition FILE source → windowed-agg, gate OFF vs ON):**
`distinct(window,key)=390` **EXACT in both** ⇒ the alignment + hash-routing is CORRECT. **BUT** gate-ON
`sum=780` = **2× the data** (each row counted twice). Symptom (distinct exact, sum doubled) = a
**SOURCE-read volume dup**, not an alignment error.
- **Root cause (narrowed, NOT yet fixed):** cutting the boundary distributes the multi-partition
  streaming `FileSourceExec` — a path NEVER exercised before (all prior streaming-file tests used
  `coalesce(1)` = 1→N). This is a DF54-morsel-CLASS double-read, but **`partitioned_by_file_group` is
  NOT the fix** — applying it at the streaming scan made the *in-process* baseline WORSE (390→1170),
  so the streaming `FileSourceExec` whole-file partitioning model conflicts with it. Reverted. The real
  fix needs proper root-causing of how the streaming file scan splits/reads under distribution =
  **T-BF2.3d**.
- **Orthogonal to alignment + to the Kafka benchmark:** the real throughput target reads Kafka (no
  parquet scan), so this dup does not apply to it; but Kafka N→M is UNVALIDATED locally (bounded-Kafka
  harness produces no output gate ON **and** OFF — a separate pre-existing harness issue, not this
  change). **Per charter anti-patch, T-BF2.3 is NOT dup=0-validated end-to-end yet.**
- **Next:** T-BF2.3d (root-cause the streaming-file-source distributed double-read) + get a clean Kafka
  N→M dup=0 signal (fix the local bounded-Kafka harness or use the T2 kind path with an S3/shared sink).

## 4h. T-BF2.3d RESOLVED — the "2× dup" was a TEST-HARNESS confound, N→M is dup=0 (2026-07-08)
Root-caused by instrumentation + reading the actual task plan (NOT guessing): `rewrite_parquet_adapters`
fired for the input-**write** scans (groups=4, groups=8) but the streaming READ scan showed **7 file
groups = 21 files with 6 different random-hash prefixes** — i.e. my probe reused `/tmp/nm_probe_in` and
**never `rm`'d it**, so ~6 probe runs of the same 400-row write ACCUMULATED. `distinct(window,key)` was
always exact (alignment correct); only `sum` inflated (390→780→1170→1950→2340) from the duplicate INPUT
files. **The engine had no dup.** Re-tested with a FRESH unique input, identical for both gates:
gate OFF == gate ON == deterministic (`3990/3990/3990` at 4000 rows/50 keys/8 parts;
`90/90/90` at 400 rows). **T-BF2.3 N→M is VALIDATED dup=0** — the MIN-merge + Chandy-Lamport alignment
is correct across the distributed cross-network shuffle. My earlier `partitioned_by_file_group`-at-source
"fix" was chasing this phantom (and made the confounded baseline worse) — **reverted; no engine change
needed.** Permanent self-checking gate: **`scripts/nm_dist_gate.sh`** (fresh input, asserts distributed
== in-process baseline + deterministic). LESSON (charter anti-patch): a fresh, uniquely-named input per
run is mandatory — cross-run file accumulation silently masqueraded as an engine dup. **Remaining T1 for
T-BF2.3: crash-EO N→M dup=0** (counts-exact done) + the Kafka N→M path (real benchmark; no parquet scan).

## 4i. T-BF2.3 crash-EO N→M VALIDATED dup=0 (2026-07-08) — T1 COMPLETE
Ran the existing committed crash-EO gate `f3c_stateful_crash.sh` (continuous stateful windowed-agg over
a **4-partition** Kafka topic = N→M, local-cluster, `kill -9` mid-run → restart → verify) with
`ZELOX_DISTRIBUTED_STREAM=1`. **Result: `F3C_STATEFUL_EO_ACROSS_CRASH PASS` — `no_dup=True`,
`all_counts_10=True`, rows=12/6 windows, IDENTICAL to the gate-OFF baseline.** Confirmed the N→M cut was
genuinely active (not a silent fallback): the task-runner plan dump shows
`ShuffleWriteExec: partitioning=Hash([#8@3], 8)` over the streaming source + a multi-stage job
(`job 1 stage 0/1`) — i.e. the keyed exchange was cut into a distributed cross-network Hash shuffle whose
receiver aligns Chandy-Lamport barriers to a consistent cut. So the aligning shuffle preserves
exactly-once across a hard crash. **Reproducible gate: `ZELOX_DISTRIBUTED_STREAM=1 bash
scripts/f3c_stateful_crash.sh`.** ⇒ T-BF2.3 T1 is COMPLETE (counts-exact `nm_dist_gate` + crash-EO f3c).
Next: T2 kind (pods spread) + the Kafka N→M throughput benchmark → T-BF2.4 → T3 EKS vs Flink.

## 4j. T2 kind result (2026-07-08, zelox:bf3) — cut distributes the SOURCE, NOT the window (T-BF2.6)
Ran the gate-ON `zelox:bf3` (all of T-BF2.2/2.5/2.3) on a 3-node kind cluster, 8-partition Kafka
windowed-agg. **The N→M cut fired** (driver debug: `stage 1 inputs=[StageInput(stage=0, mode=Shuffle)]`,
`Hash([#8@3], 8)`; 2 stages). **Stage 0 (source) ran as 8 distributed tasks.** BUT the window still ran
all 8 `F5_PEAK p0..p7` on ONE pod. Root cause from the driver's TaskRegion + stage-1 plan:
```
stage 1 (ONE output partition => ONE task):
  StreamingSinkCommit → ParquetSink → FlowEventToData → Projection
    → StreamBarrierAlignExec        (N→1 funnel: 8 window instances → 1, for the single sink)
      → WindowAccumExec (8 instances)
        → StageInput(Hash, 8)        (the 8 shuffle partitions from stage 0)
```
So `WindowAccumExec` is **bundled in the same stage as the downstream `StreamBarrierAlignExec` N→1
funnel**, which makes the consumer stage a **single output partition = single task** — the 8 window
instances run in-process on one pod. This is exactly the batch pattern where `CoalescePartitionsExec`
(N→1) is a **stage boundary** so the aggregate distributes *before* the funnel. Streaming's
`StreamBarrierAlignExec` is the N→1 funnel but is NOT a stage boundary.

**T-BF2.6 — make the window distribute (design fork; resolve by reading code before coding):**
- **Option A — cut a stage boundary at `StreamBarrierAlignExec`** (like `CoalescePartitionsExec`), so
  `WindowAccum` runs as its own 8-partition distributed stage (8 tasks) and `StreamBarrierAlign` reads
  the 8 outputs in a funnel stage. **Subtlety:** the align needs the 8 window-instance streams kept
  SEPARATE + identity-preserved (partition p = instance p) to align barriers — so the stage1→stage2
  shuffle must be an **identity N→N forward**, not Hash/RoundRobin (which would re-route/mix rows).
  Need to confirm the shuffle infra supports partition-preserving forward (InputMode::Forward exists).
- **Option B — parallel sink, no funnel** (the f2f3 `ParallelStreamSinkExec` path already used for the
  N-parallel Kafka/file sink): each of the 8 window instances writes its own part-file per epoch and
  the last-to-finish does the global commit — so there is NO `StreamBarrierAlign` funnel, the window
  stage has 8 output partitions → 8 distributed tasks. Matches the already-validated parallel-sink EO.
- Correctness is NOT at risk today (counts-exact + crash-EO already dup=0 with the window on one pod);
  this is purely the **throughput distribution** the epic targets. Pick the option, T1 (task-region
  shows 8 window tasks + nm_dist_gate dup=0), then re-T2 kind (window F5_PEAK across ≥2 pods).

**Honest T2 status:** source distributes (8 tasks, even-spread across pods) + counts-exact + crash-EO —
but the compute-heavy WINDOW does not distribute yet (T-BF2.6). Kind torn down, AWS $0.

## 4k. T-BF2.6 IMPLEMENTED + T1-COMPLETE (2026-07-08, commit 824dbda0)
**Chosen: Option A, refined — cut a stage boundary at `StreamBarrierAlignExec`.** It is the streaming
analog of `CoalescePartitionsExec` (already a boundary in `build_job_graph`), so the fix is one line:
add `StreamBarrierAlignExec` to `distributed_stream_boundary`. Its properties
(`UnknownPartitioning(1)`) drive `create_shuffle` to a **RoundRobin{1} funnel**, so the child
(`WindowAccumExec`, N partitions) runs as **N distributed tasks**; the marker-aware aligning shuffle
read (`merge_flow_event_streams`) does the N→1 barrier-align + watermark MIN that `StreamBarrierAlign`
did in-process (a proven SUPERSET — MIN-merge vs dedup), so the funnel node is subsumed by the shuffle.
**No new distribution variant, no parallel-sink rework** (the identity-N→N-forward subtlety of the naive
Option A, and the sink-path rework of Option B, are both avoided — the RoundRobin{1} funnel + aligning
read handle it). **T1 green:** the WINDOW stage now has **8 tasks (was 1)** — driver plan dump
stage-partitions `0:4 (source) / 1:8 (window) / 2:1 (sink)`; `nm_dist_gate` dup=0 (counts-exact through
the funnel cut); `f3c` crash-EO dup=0 (align via the shuffle read preserves EO); `dist_streaming_smoke`
6/6; 4 stage-boundary unit tests (exchange + funnel × gate on/off); clippy `-D` green.

**T2 kind (zelox:bf4) — WINDOW DISTRIBUTES CONFIRMED + a new scheduler gap (T-BF2.7):**
- With the DEFAULT `worker_task_slots=8` the 8 window instances still ran on ONE pod. Root-caused from
  the driver log (not guessed): the region is 8 task-sets `{stage0 pi, stage1 pi}` (pipeline-co-located),
  but (a) `worker_task_slots=8` ⇒ **one worker can hold the entire 8-task region**, and (b) the region
  is **assigned before the other workers register** (`ScheduleTaskRegion` at 06:57:37, workers register
  after) — so even-spread has only the first worker to place on → packs. This is a distributed-scheduler
  gap analogous to **Spark `spark.scheduler.minRegisteredResourcesRatio` / `maxRegisteredResourcesWaitingTime`**
  and **Flink slot-wait** (wait for the required slots/resources before deploying). = **T-BF2.7.**
- **PROOF the window distributes** (root-cause confirmed, no rebuild): set `ZELOX_CLUSTER__WORKER_TASK_SLOTS=2`
  so the 8-task region can't fit on one worker → the 8 window instances spread across **4 pods, 2
  partitions each** — `p1/p5, p2/p6, p3/p7, p0/p4` = a clean even-spread round-robin (T-BF2.5 working).
  ⇒ **T-BF2.6 is validated: the window compute distributes across worker pods.**
- (Pod→NODE spreading is a separate k8s-scheduler concern — kind packed the 4 pods on one node; the
  window compute is on 4 distinct pods = 4 processes = distributed. On EKS with node anti-affinity /
  more nodes the pods spread across nodes too.)
- **T-BF2.7 (next):** wait for the requested workers to register before assigning. See §4l.

## 4l. T-BF2.7 IMPLEMENTED + T2-VALIDATED (2026-07-08, commit b812100b) — window distributes at DEFAULT slots
**Fix:** gate `assign_tasks` on `requested_worker_count == 0` — don't assign while requested workers are
still registering (`run_tasks` fires `assign_tasks` on EVERY registration, so the first worker to arrive
grabbed the whole region). Wait until every requested worker has registered OR been marked failed, so
even-spread distributes across ALL of them. **Deadlock-safe:** the pending-worker probe
(`handle_probe_pending_worker → track_worker_failed_to_start`) times out a worker that never starts and
decrements the count, so it always reaches 0 within `task_launch_timeout`. No-op when no workers were
requested. = Spark `spark.scheduler.minRegisteredResourcesRatio` / Flink slot-wait.
**T2 kind (zelox:bf5, DEFAULT `worker_task_slots=8`, no workaround):** the 8 window instances now spread
across **4 pods, 2 partitions each** — `p0/p4, p1/p5, p2/p6, p3/p7`, clean even-spread. The earlier
provisioning worry was unfounded: ~4 workers ARE provisioned for the streaming query; the ONLY problem
was the assign-before-register timing race, which the wait-gate fixes. **T1:** nm_dist_gate dup=0 +
smoke 6/6 (gate doesn't break local-cluster) + clippy green. ⇒ **the WINDOW compute distributes across
worker pods at default config.** (All 4 pods landed on one kind node — pod→NODE anti-affinity is a
separate k8s-scheduler concern; on EKS with ≥2 compute nodes + anti-affinity they spread across nodes.)
Kind torn down, AWS $0. **VAJ-BF2 distribution is COMPLETE (T1+T2): source + exchange + window all
distribute across workers, counts-exact + crash-EO dup=0. Next: T-BF2.4 credit backpressure → T3 EKS
multi-node throughput vs Flink (the beat measurement).**

## 4m. T-BF2.4 credit-based backpressure — root-caused; naive fix MEASURED LOSSY, reverted (2026-07-08)
**The gap (confirmed, grounded in Flink FLIP-2):** the task-stream (shuffle) memory sink
`MemoryStreamReplicaSender::write` (`stream_manager/local.rs`) uses non-blocking `try_send`, and when
the receiver's bounded channel (`cluster.task_stream_buffer`, default 16) is full it spills to an
**UNBOUNDED `overflow: VecDeque`** "to avoid blocking sending for slow senders." So a fast producer
buffers WITHOUT BOUND at a slow receiver — exactly the streaming in-flight memory regression vs Flink
(no credit-based flow control on the cross-stage/cross-pod shuffle; the in-process exchange already has
coarse bounded-blocking credit via `channel_capacity`).

**Naive fix attempted + MEASURED (opt-in `ZELOX_CREDIT_BACKPRESSURE`, blocking `send().await` instead
of overflow):** f3c crash-EO PASSED (no deadlock — the align receiver drains non-barriered inputs, as
predicted), BUT `nm_dist_gate` showed **non-deterministic DATA LOSS** (distributed availableNow file
source: 3990 correct then 3630 = −360 rows, same config = a race). **Reverted — will not ship lossy
code (charter: only dup=0 via a consistent cut advances the invariant).**

**Root cause of the loss (why the naive fix is wrong):** the `overflow` VecDeque serves a DUAL purpose —
(1) bounding slow-receiver buffering (what we want to fix) AND (2) buffering data around the
subscription/close LIFECYCLE (data produced before the consumer subscribes, or a straggler producer vs
an early-finishing merge). Blocking `send().await` drops the in-flight batch (`Err` = receiver gone)
when the receiver isn't yet connected or has already closed — the lifecycle race. So credit backpressure
cannot simply replace the overflow.

**RESOLVED (commit pending): bounded-overflow credit — the Flink FLIP-2 model, one prod solution.** Keep the existing lossless try_send+overflow (it never drops during operation + drains on close), then BOUND the overflow: when it exceeds the credit cap, block draining it FIFO via `tx.reserve().await` — an ATOMIC permit (`permit.send` is infallible, so no batch is ever dropped) that awaits the receiver making room = wait-for-credit. Opt-in `ZELOX_CREDIT_BACKPRESSURE=<cap>` (0=off default). **MEASURED: nm_dist_gate dup=0 AND deterministic (3990×3) + f3c crash-EO PASS** (vs the naive blocking-send which was non-deterministically lossy). The earlier lifecycle race is gone because the new batch is always parked in `overflow` first (never consumed by a blocking send that can hit a closed receiver); only the FIFO drain blocks, via an atomic permit. This is credit-based flow control — keep transient
pre-subscription/close buffering, but BOUND the slow-receiver case: cap `overflow` at a credit limit and
block (await channel capacity) only once the receiver is *connected and actively consuming*, so a
straggler-vs-closed-receiver never loses data. Grounded in Flink FLIP-2 (receiver grants credit;
producer blocks when credit exhausted) — the receiver-connected condition is the credit grant. Validate:
`nm_dist_gate` dup=0 AND deterministic + f3c crash-EO dup=0 + a slow-sink memory-bound test (RSS
plateaus, doesn't grow O(N)). **Not correctness-critical for the throughput beat** (distribution +
counts-exact + crash-EO already done); it's the MEMORY-bound lever (bounds in-flight vs Flink).


## 4n. Kind PENETRATION (E2E prod-grade verify, 2026-07-08, zelox:bf6 = all VAJ-BF2 incl T-BF2.4)
Full-stack verify on a 3-node kind cluster with ALL gates on (kubernetes-cluster, ZELOX_DISTRIBUTED_STREAM=1,
ZELOX_CREDIT_BACKPRESSURE=16) so EKS has no unknowns. **Merge-readiness gates GREEN first:** `cargo test
--workspace` EXIT=0 (all pass) + `cargo clippy --workspace --all-targets -D warnings` 0 issues.
**Penetration result:** the distributed windowed-agg RAN E2E on real k8s — source + exchange + window
DISTRIBUTE across **4 worker pods** (2 window partitions each: p0/p4 p1/p5 p2/p6 p3/p7 = clean
even-spread), the query **completed without hang/deadlock** (credit backpressure T-BF2.4 active, no loss),
even-spread + wait-for-workers place at default `worker_task_slots=8`. **The EKS unknowns are cleared:
the full distributed topology + credit + placement all work on real k8s.** Counts-exact on kind's FILE
sink is NOT verifiable here — the distributed file-sink part files land on the sink pod's local fs, not
the shared hostPath (the DOCUMENTED "distributed sink needs an object store" limitation; kind hostPath is
an imperfect S3 substitute). Counts-exact is proven at T1 (`nm_dist_gate`: distributed == in-process
baseline, dup=0, deterministic) with IDENTICAL shuffle/align/window code + codec round-trip tests, and
EKS uses S3 (prior EKS runs proved counts + crash-EO to S3). ⇒ EKS = scale + Flink head-to-head
measurement, not discovery. Kind torn down, AWS $0.


## 4o. T3 EKS multi-node result (2026-07-08, zelox:bf6, 2x c7g.2xlarge compute + kafka) — HONEST NEGATIVE
Ran the distributed windowed-agg (100M/16-part Kafka -> S3) on a 2-compute-node EKS cluster,
kubernetes-cluster mode, all gates on (DISTRIBUTED=1 + CREDIT=16). **What WORKED (capability proven at
scale):** (1) the window DISTRIBUTES CROSS-NODE — 4 worker pods, 2 per compute node (verified `get pods
-o wide`); (2) the **S3 sink + read work** (the kind-penetration gap CLOSED) — counts read back from S3:
`groups=9000 total=90M n_windows=9 n_keys=1000` (9 windows = the known no-COMPLETE_ON_END last-window
drop, not a dup; distinct exact). **What did NOT (the beat thesis):** **throughput = 1.734M ev/s**, vs
the single-node c7g.4xlarge ~5M ev/s (both 16 vCPU total). **Distributing across 2 nodes REGRESSED
throughput ~3x** — the cross-node Flight shuffle overhead (+ likely single-Kafka-broker read bound)
outweighs the parallelism gain at this scale/config. **The "distribute the exchange to structurally beat
Flink" thesis is NOT validated by measurement** — the distribution is CORRECT (cross-node, counts-exact,
S3 EO) but SLOWER than single-node. HONEST like the VAJ-T7 null result. **Opens (for the beat, not done):**
(a) fair Flink-2-TM head-to-head (Flink also pays network cost multi-node — the real comparison, not run);
(b) root-cause the 3x regression (WM_PROF didn't emit on EKS — the dump-site fix is a prerequisite; is it
Flight shuffle serialize/copy, Kafka-broker-bound, or credit throttling?); (c) same-node mpsc vs cross-node
Flight (§3 design: co-located links should stay mpsc — verify the exchange isn't serializing same-node).
**Cluster torn down to $0** (eksctl delete). Distribution CAPABILITY is prod-grade + validated; the
throughput BEAT is unproven and currently a regression.


## 4p. T3 EKS FAIR A/B — Zelox distributed vs Flink 2-TM, SAME 2-node cluster (2026-07-08) — DECISIVE NEGATIVE
Recreated the cluster and ran BOTH engines sequentially on the SAME 2x c7g.2xlarge compute (16 vCPU
total) + same 100M/16-part Kafka topic + IDENTICAL 10s tumbling-window keyed COUNT (Flink SQL == Zelox
stream_windowed_agg). Fair head-to-head:
| Engine (2-node, 16 vCPU) | wall_s | throughput |
|--------------------------|--------|------------|
| **Zelox distributed** (kubernetes-cluster, all gates) | 68.67 | **1.456M ev/s** (counts exact via S3; 8 workers 4/4 across nodes) |
| **Flink 2-TM** (2 TMs, one per node, parallelism 16) | 19.17 | **5.22M ev/s** |
**Flink is ~3.6x FASTER than Zelox at equal 2-node resources.** The "distribute the exchange to
STRUCTURALLY BEAT Flink" thesis (the whole VAJ-BF2 premise) is **REFUTED BY MEASUREMENT** — Zelox's
distributed streaming is CORRECT (cross-node, EO, S3 counts-exact) but ~3.6x SLOWER than Flink's mature
network stack, not faster. (Honest, like the VAJ-T7 null result — claim only measured.) Cluster torn to $0.

**What this means (honest):** VAJ-BF2 delivered a genuinely prod-grade *distributed streaming capability*
(merged to main, T1+T2+T3-correct) but NOT a throughput win. The ~3.6x gap is large and needs deep
root-causing IF the distributed-throughput beat is still the goal: candidates = (a) the Arrow-IPC Flight
shuffle serialize/copy per batch (vs Flink's optimized network + operator chaining that avoids
serialization for chained/local ops); (b) NON-FUSED stages (each cut stage is a separate task with its
own scheduling/stream overhead — Flink fuses/chains); (c) same-node links NOT staying mpsc (the §3
design says co-located should skip serialization — verify); (d) credit backpressure throttling; (e)
WM_PROF didn't emit on EKS so NONE of this is attributed yet (fixing the dump-site is prerequisite #1).
**Strategic honesty:** Zelox's PROVEN wins are batch (6.2x vs Spark), memory (path-dependent), unified
single-engine, no-JVM. Distributed streaming THROUGHPUT is NOT a win and would need major work to become
one; the board should reflect this and the team should decide whether to root-cause the 3.6x or invest
in the proven axes.


## 4q. ROOT-CAUSE of the throughput gap — MEASURED (2026-07-08, local WM_PROF + gate A/B) — the honest, complete picture
Frustration warranted; here is what the MEASUREMENTS say (not guesses), ruling causes IN and OUT:
- **Micro-batch loop: NOT the cause.** A/B on the distributed path: `maxOffsetsPerTrigger=4M` (5 micro-batches)
  0.531M vs `=20M` (1 micro-batch) 0.500M ev/s — identical. (The KB's suspected availableNow re-plan/commit
  loop is NOT the lever.)
- **Same-node distributed shuffle logic: cheap (~16%).** Local-cluster gate-OFF (in-process exchange) 0.542M
  vs gate-ON (distributed cut) 0.465M — the shuffle machinery (cut+align+credit) costs only ~16% same-node
  (Arc-clone, no IPC). ⇒ the EKS 3.4× regression is ENTIRELY the CROSS-NODE Flight/gRPC transport, NOT the
  shuffle logic.
- **Small-batch IPC: NOT the cause.** The Kafka source emits LARGE batches (`MAX_BATCH_BYTES=128 MiB`), so the
  cross-node Flight sends are large — not a per-small-batch framing problem.
- **Single-node per-stage CPU (WM_PROF, 20M, summed): `exchange=267.8s > from_json=131.7s > source_read=52s
  > finalize=18s > encode=0.2s`, `input_wait=610%`.** BUT the "exchange" timer WRAPS `senders[owner].send().await`,
  which BLOCKS on the bounded channel when the window is slow — so **most of the 267s is BACKPRESSURE-WAIT, not
  CPU.** `input_wait=610%` confirms the window is INPUT-STARVED. The genuine CPU bottleneck is **`from_json` (the
  parse, 131s)** — and per KB §6 Zelox's Rust parse is already **~parity with Flink's Jackson** (being Rust not
  JVM is the edge, already realized).

**HONEST CONCLUSION (the prod-bar truth):** on this windowed-COUNT workload Zelox is **parse-bound at ~PARITY
with Flink single-node** (1.05–1.15× — small + intrinsic), and **distribution does NOT help — the workload does
not scale with nodes** (Flink 2-node 5.22M ≈ Flink 1-node 5.58M; neither engine gains), while Zelox's cross-node
shuffle is less optimized so it REGRESSES. **This is not a single missed-prod-bar bug to "fix" — it is a workload
where neither engine's advantages dominate, and Zelox is already competitive.** To genuinely BEAT Flink the lever
is NOT this workload's raw parse-throughput; it is (a) workloads that leverage Zelox's REAL edges — columnar
vectorized compute-heavy aggregation, no-GC tail latency (D2, unmeasured), memory-bounded state; or (b) a
CONFIRMED exchange route-CPU optimization (the per-row key-group loop + N-way `take` in `distribute`) — but that
needs finer profiling to separate route-CPU from backpressure-wait first (the current timer conflates them; fix
= time the route separately from the send-await). WM_PROF worker-env now fixed (was driver-only) so a future
distributed profile can attribute per-worker. **Do NOT claim a distributed-streaming throughput beat — measured
false. Zelox's measured wins remain batch (6.2× vs Spark), memory, unified, no-JVM.**


## 4r. DECISIVE: split exchange CPU vs backpressure-wait (2026-07-08) — the exchange is ~FREE, parse is the bottleneck
Instrumented `distribute()` to separate route-CPU from `send().await` backpressure (the old timer conflated
them). Single-node WM_PROF, 20M: **`exchange_cpu=6.1s` (route/hash/take = NEGLIGIBLE) vs `exchange_wait=272s`
(blocked on the bounded channel).** So the "exchange=267s" was 98% BACKPRESSURE-WAIT, not our CPU. Real CPU:
**`from_json=135s` (parse, 64% of real work) > source_read=56s > finalize=20s > exchange_cpu=6s.** The parse is
already SIMD (simd-json) and ~parity with Flink's Jackson (KB §6, being Rust not JVM = the edge, already realized).
**⇒ single-node is PARSE-BOUND at PARITY — there is NO exchange/route inefficiency to fix (proven 6s). The 3.6×
distributed regression is the cross-node Flight/gRPC transport (a SEPARATE path: `do_get` FlightDataEncoderBuilder
IPC + gRPC, only exercised cross-node — NOT `exchange_cpu`), but distribution does not help this workload anyway
(Flink flat 1->2 nodes). NO missed-prod-bar bug exists on windowed-COUNT; Zelox is competitive (parity), not
lagging, single-node.** Instrumentation kept (EXCHANGE_WAIT_NS) for future distributed profiling. Beat lever is
NOT this workload's parse throughput; it is Zelox's real edges (columnar compute-heavy agg per Arroyo's 10x sliding
windows; no-GC latency D2; memory).

## 5. Risks / open questions (resolve before coding each ticket)
- Does the driver run long-lived multi-stage streaming tasks across pods, or is streaming pinned to
  one pod by design? (T-BF2.1 is the gating unknown — investigate first.)
- EO across a network cut: the barrier-aligned commit is proven single-process; does a worker crash
  mid-epoch on a remote sub-channel recover correctly? (Extends the existing crash-EO proof.)
- Keyed routing stability across rescale when sub-channels are remote (key-group ownership already
  rescale-stable — verify it survives pod reassignment).
- Flight overhead for small streaming batches (latency vs the mpsc path) — measure; may need batch
  coalescing before Flight send (like the batch shuffle's IPC batching).

## 6. First step when building (T-BF2.1 resolved → sharpened)
The distributed mode (`kubernetes-cluster`) exists AND a distributed manifest exists: **`k8s/zelox.yaml`**
= driver Deployment (`ZELOX_MODE=kubernetes-cluster`) + Service + RBAC (Role/ServiceAccount/RoleBinding
so the driver launches worker pods via the k8s API) + a worker pod-template patch. The driver
DYNAMICALLY launches worker pods (`KubernetesWorkerManager::launch_worker`); workers `register_worker`
back (`driver/actor/handler.rs:62`). So the experiment adapts `k8s/zelox.yaml`, not greenfield. Concrete
first move:
1. **On kind, deploy `k8s/zelox.yaml` (kubernetes-cluster driver, image `zelox:TAG`) + run the
   windowed-agg** so the driver launches ≥2 worker pods. Observe: does the streaming DAG spread its
   stages across worker pods (like batch), and what happens at the `StreamExchangeExec` boundary — does
   it error (mpsc can't cross pods), fall back, or already route via Flight? This ONE experiment tells
   us exactly how much of T-BF2.2 is needed. (Watch for: RBAC on kind, worker image pull policy, the
   realtime source pinned `parallelism=1` — memory — which may force the source stage onto one pod.)
2. Based on that: swap `StreamExchangeExec` cross-pod sub-channels to the existing Flight `do_get`
   transport (carrying `EncodedFlowEventStream` RecordBatches), same-pod stays mpsc, behind an env gate
   (`ZELOX_DISTRIBUTED_STREAM`). T1 multi-process (multiple `zelox` processes on one host) first —
   correctness_gate + inc_ckpt dup=0 across the real network cut — then T2 kind multi-pod → T3 EKS.
3. Then credit backpressure (T-BF2.4) + cross-network EO validation (T-BF2.3).

**Measure-first still governs:** before optimizing the Flight path, get the multi-node profile
(source_read now instrumented) and confirm the network exchange is the ranked cost vs Flink.
