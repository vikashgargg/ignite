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
