# VAJ-BF2 — Distributed streaming + Arrow Flight exchange (architect-first design)

> **Status:** DESIGN (2026-07-07). Design-before-code per the charter. Grounded in the existing Vajra
> code (traced) + REFERENCES §4 (Ballista 53.0.0 Arrow Flight), §2d (Spark 4.1 RT-mode pipelined
> stages), Flink FLIP-8 (credit-based flow control). **Not yet implemented.**

## 0. Why (measured, not assumed)
Complete per-stage profile (clean 20M/16-part, 2026-07-07): `from_json 135s` (#1, intrinsic JSON
tokenize = PARITY with Flink's Jackson) > **`exchange 89.8s` (#2)** > `finalize 27s` >
`source_read 4.4s` (CHEAP, ruled out) > `encode 0.3s`; window `STARVED(upstream)`. Single-node
windowed-agg is parse-bound parity (Vajra ~1.05× behind Flink on identical work). The exchange is the
only stage where Vajra's **no-JVM Arrow zero-copy network shuffle** can *structurally* beat Flink's
JVM-serialized shuffle — but that only manifests **multi-node** (single-node exchange is in-memory).

## 1. The two big de-riskers (code-traced 2026-07-07)
1. **Arrow Flight transport ALREADY EXISTS + is tested.** `sail-execution/src/stream_service/`:
   `FlightServiceClient` + `FlightRecordBatchStream` (`client.rs`), `TaskStreamFlightServer` + `do_get`
   + `Ticket` (`server.rs`), `test_arrow_flight_shuffle_roundtrip` (`tests.rs`). The **batch** shuffle
   already moves Arrow `RecordBatch` streams cross-node zero-copy over Flight. **Reuse it.**
2. **FlowEvents ALREADY ⟷ RecordBatches.** `sail-common-datafusion/.../event/encoding.rs`:
   `EncodedFlowEventStream::encode(FlowEvent) -> RecordBatch` (data AND markers — Watermark/Checkpoint/
   Idle/EndOfData — encoded into a special-schema batch with `_marker`/`_retracted` columns);
   `DecodedFlowEventStream` reverses it. So a streaming sub-channel's `FlowEvent` stream **is** a
   `RecordBatch` stream and rides the existing Flight transport **with markers, unchanged**.

⇒ BF2 needs **no new wire format and no new transport**. The exchange payload (data + watermarks +
barriers) already serializes to Arrow RecordBatches, and Flight already carries Arrow RecordBatch
streams zero-copy. This is the no-JVM structural edge, already built for batch.

## 2. What's actually missing (the real work)
Today the streaming path is **single-process**: `StreamExchangeExec` (`sail-physical-plan/.../
streaming/exchange.rs`) routes via in-memory `tokio::mpsc` channels, and the deploy is ONE pod
(`--mode local-cluster --workers 4`). BF2 must run streaming on the **existing** distributed mode:

- **T-BF2.1 — Multi-pod streaming topology. RESOLVED (2026-07-07): the distributed mode ALREADY
  EXISTS.** `sail-cli/src/runner.rs`: three modes `local | local-cluster | kubernetes-cluster`;
  **`kubernetes-cluster`** is a real distributed multi-pod mode (`Cluster`/`ClusterRole::Worker` worker
  pods, Flight shuffle, K8s Lease-based scheduler-HA leader election). BUT: **every benchmark (batch AND
  streaming) ran `local-cluster` single-pod** (`k8s/eks/vajra-sf100.yaml`: "local-cluster on the single
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
  (`VAJRA_EXCHANGE_CHANNEL_CAP`, default 16) is the local backpressure bound. Over Flight/gRPC there is
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
2. **T1 local multi-process** (multiple `vajra` server processes on one host, Flight between them) —
   correctness_gate + inc_ckpt dup=0 across the network cut.
3. **T2 kind multi-pod** (≥2 vajra-stream pods) — n_windows/sum exact; fusion/EO unchanged.
4. **T3 EKS multi-node** (≥2 compute) — windowed-agg throughput **> Flink** at ≥16-part; per-stage
   network-exchange CPU < Flink's shuffle; EO dup=0 across crash + network cut. Claim ONLY the measured
   multi-node head-to-head; tear to $0.

## 4b. Experiment 1 result (kind, 2026-07-07) — distributed execution CONFIRMED (batch); streaming pending
Deployed `k8s/sail.yaml` (kubernetes-cluster driver, `vajra:t7fuse`) on kind. A trivial distributed
query (`spark.range(0,1e6,1,8).sum`) returned the correct result **and the driver dynamically launched
5 worker pods** (`sail-worker-*-1..5`) — so kubernetes-cluster worker-pod launch + cross-pod Flight
shuffle + correct result are **PROVEN on kind**. The hard part (distributed worker launch + Flight
shuffle) works. **Still UNOBSERVED:** the STREAMING windowed-agg cross-pod behavior — the streaming run
was blocked by kind Kafka data-path friction (topic empty: producer BOOT namespace, slow single-broker
kind Kafka), NOT a Vajra issue. **Next-session step (skip the friction):** get a small `events` topic
populated in the driver's namespace (fix producer BOOT to `kafka.<ns>.svc`, or pre-create+seed the
topic), run `stream_windowed_agg.py` against the kubernetes-cluster driver, and watch: does the
streaming DAG spread across the launched worker pods, and does `StreamExchangeExec` (mpsc) error /
fall-back / route cross-pod? That single observation scopes T-BF2.2.

## 4c. Experiment 2 result (kind, 2026-07-07) — ROOT-CAUSED: streaming DAG pinned to ONE worker
Seeded a small `events` topic (400k rows, 8 parts, exact `{"k","ts","v"}` scheme) **in the driver's
namespace** (`vajra`), skipping the flaky producer BOOT path, then ran the bounded windowed-agg
(`trigger(availableNow=True)`) against the kubernetes-cluster driver. Observed:
- **The driver launched 5 worker pods for the STREAMING job** (`sail-worker-nwfrntow45-1..5`, all
  Running) — so worker-pod launch happens for streaming, not just batch.
- **BUT the entire windowed-agg ran on ONE worker.** Worker 1's log shows `F5_PEAK p0..p7` — **all 8
  window partitions on a single pod**; workers 2–5 started their server, did **zero** window work, and
  were aborted at shutdown. The query executed to completion with **no exchange error** — it only
  failed the post-hoc `read.parquet` verification (`No files found in file:///tmp/sail/wagg_out/`),
  which is expected kind hostPath locality (sink wrote on worker 1's node; driver read its own node),
  **not** a Vajra fault.
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
`VAJRA_DISTRIBUTED_STREAM=1` (resolved once at `try_new`, threaded — not per-node env). It emits the
existing marker-aware Hash shuffle via `create_shuffle` (the exchange's properties already carry
`Partitioning::Hash(keys, N)`), so the N window instances distribute across workers. **Prod-grade
guards:** default keeps the F2/F3-validated inline path (additive/reversible); only the **align-free
1→N** case is cut (single-partition source ⇒ each window instance has one upstream ⇒ broadcast marker
arrives once ⇒ Flink single-input needs no alignment); **N→M is left INLINE** until T-BF2.3 wires
`StreamBarrierAlignExec` at the cross-node receiver, so we never silently mis-align a multi-upstream
barrier. **T1 green:** deterministic unit tests (gate OFF→1 stage, gate ON→2 stages — proves it fires
only when gated); `dist_streaming_smoke` **6/6** with the gate ON (local-cluster, workers=4;
`windowed_file=97` preserved through the new shuffle); clippy `-D warnings` green.

**Deployment-mode parity check (2026-07-07) — Vajra runs the full batch+streaming suite like
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
Ran the gate-ON `vajra:bf2` on a 3-node kind cluster (`kind-multinode.yaml`), driver flag
`VAJRA_DISTRIBUTED_STREAM=1` confirmed, 400k seeded. **Two findings, both root-caused from code:**

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
AWS $0). *(Secondary harness note: kind cross-node `file:///tmp/sail` read still flaky for the post-hoc
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
`VAJRA_DISTRIBUTED_STREAM=1`. **Result: `F3C_STATEFUL_EO_ACROSS_CRASH PASS` — `no_dup=True`,
`all_counts_10=True`, rows=12/6 windows, IDENTICAL to the gate-OFF baseline.** Confirmed the N→M cut was
genuinely active (not a silent fallback): the task-runner plan dump shows
`ShuffleWriteExec: partitioning=Hash([#8@3], 8)` over the streaming source + a multi-stage job
(`job 1 stage 0/1`) — i.e. the keyed exchange was cut into a distributed cross-network Hash shuffle whose
receiver aligns Chandy-Lamport barriers to a consistent cut. So the aligning shuffle preserves
exactly-once across a hard crash. **Reproducible gate: `VAJRA_DISTRIBUTED_STREAM=1 bash
scripts/f3c_stateful_crash.sh`.** ⇒ T-BF2.3 T1 is COMPLETE (counts-exact `nm_dist_gate` + crash-EO f3c).
Next: T2 kind (pods spread) + the Kafka N→M throughput benchmark → T-BF2.4 → T3 EKS vs Flink.

## 4j. T2 kind result (2026-07-08, vajra:bf3) — cut distributes the SOURCE, NOT the window (T-BF2.6)
Ran the gate-ON `vajra:bf3` (all of T-BF2.2/2.5/2.3) on a 3-node kind cluster, 8-partition Kafka
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
The distributed mode (`kubernetes-cluster`) exists AND a distributed manifest exists: **`k8s/sail.yaml`**
= driver Deployment (`SAIL_MODE=kubernetes-cluster`) + Service + RBAC (Role/ServiceAccount/RoleBinding
so the driver launches worker pods via the k8s API) + a worker pod-template patch. The driver
DYNAMICALLY launches worker pods (`KubernetesWorkerManager::launch_worker`); workers `register_worker`
back (`driver/actor/handler.rs:62`). So the experiment adapts `k8s/sail.yaml`, not greenfield. Concrete
first move:
1. **On kind, deploy `k8s/sail.yaml` (kubernetes-cluster driver, image `vajra:TAG`) + run the
   windowed-agg** so the driver launches ≥2 worker pods. Observe: does the streaming DAG spread its
   stages across worker pods (like batch), and what happens at the `StreamExchangeExec` boundary — does
   it error (mpsc can't cross pods), fall back, or already route via Flight? This ONE experiment tells
   us exactly how much of T-BF2.2 is needed. (Watch for: RBAC on kind, worker image pull policy, the
   realtime source pinned `parallelism=1` — memory — which may force the source stage onto one pod.)
2. Based on that: swap `StreamExchangeExec` cross-pod sub-channels to the existing Flight `do_get`
   transport (carrying `EncodedFlowEventStream` RecordBatches), same-pod stays mpsc, behind an env gate
   (`VAJRA_DISTRIBUTED_STREAM`). T1 multi-process (multiple `vajra` processes on one host) first —
   correctness_gate + inc_ckpt dup=0 across the real network cut — then T2 kind multi-pod → T3 EKS.
3. Then credit backpressure (T-BF2.4) + cross-network EO validation (T-BF2.3).

**Measure-first still governs:** before optimizing the Flight path, get the multi-node profile
(source_read now instrumented) and confirm the network exchange is the ranked cost vs Flink.
