# EKS Throughput Capstone — closing the windowed-agg gap vs Flink

**Status:** planned (not yet run). **Owner:** streaming. **Cost discipline:** brief EKS, tear to **$0**
when idle; mask 12-digit account IDs in all artifacts.

## 1. Why this exists (the one remaining gap)

Across every other axis Vajra matches or beats Flink (correctness: per-partition watermark + EO across
crash; **memory: 6.6× less**, 1.28 vs 8.5 GiB measured EKS; **incremental checkpoint**: window + join,
O(delta) proven). The **single** open gap is **throughput**:

| Run (c7g.4xlarge, Flink 1.19, shared 100M-event Kafka, 10s tumbling COUNT) | Throughput | Memory |
|---|---|---|
| Flink 1.19 baseline | **11.36M ev/s** @ 41.8s | 8.5 GiB |
| Vajra (ordered-100M v2) | ~2.4× slower wall (17.5→41.8s class) | **1.28 GiB** |

Localized (prior A/B, per-msg-timer): the gap is **downstream of Kafka read** — in
`from_json → WatermarkExec → StreamExchangeExec → WindowAccumExec`. Kafka read is **not** the
bottleneck. **Local throughput is noise** (swings 2× both directions by file count — see
[[local_headtohead]]) so this MUST be measured on controlled EKS.

**Goal:** close windowed-agg throughput to **≤1.2× Flink (stretch: beat it)** while keeping the memory
win. Grounded in: Spark 4.1 Real-Time Mode (concurrent stage scheduling, in-memory streaming shuffle),
Flink pipelined exchange, Arrow Flight zero-copy shuffle, DataFusion vectorized aggregation
(REFERENCES §1/§4/§5).

## 2. Method — LOCALIZE before optimizing (no guessing)

The prior run gave a coarse "downstream" verdict. **Phase A is instrumentation** to attribute the wall
time to a specific stage, then **Phase B** fixes the dominant one and re-measures. Do not optimize a
stage we haven't proven dominant.

### Phase A — stage attribution (1 EKS run + instrumentation)
Add **env-gated** cumulative stage timers (pattern: existing `KAFKA_BENCH` read-bench + `F5_PEAK`),
behind `VAJRA_WM_PROF`, reporting per-operator µs + rows at `EndOfData`:
- `from_json` decode (scalar UDF / arrow-json) — **prime suspect**: the realtime Kafka source is pinned
  `parallelism=1` (`kafka/reader.rs:279`, single instance reads ALL partitions for single-instance EO),
  so `from_json` + `WatermarkExec` run **single-threaded before the exchange** while Flink parses per
  partition in parallel.
- `StreamExchangeExec` keyed shuffle (in-process channel + re-encode cost).
- `WindowAccumExec` accumulate vs **finalize** (`AggregateExec(Final)`, per-partition single-threaded).

Deliverable: a stage breakdown (e.g. "from_json 55%, exchange 20%, finalize 15%, rest 10%").

### Phase B — fix the dominant stage (ranked by likelihood, all grounded)
1. **Parallelize parse/decode before the exchange** (most likely win). The single-instance source
   serializes `from_json`. Options, in order of preference:
   - Multi-thread the decode **within** the single source instance (rayon over record batches) — keeps
     single-instance EO commit intact (the constraint per [[project_realtime_eo]]).
   - Or insert an early **round-robin repartition** of *raw* rows so `from_json` runs on N threads, then
     the keyed exchange (note: can't key-shuffle before parsing the key — round-robin only).
2. **Zero-copy / pipelined exchange** — replace per-batch re-encode in `StreamExchangeExec` with Arrow
   IPC/Flight zero-copy buffers (REFERENCES Arrow Flight shuffle; Spark 4.1 in-memory streaming shuffle).
3. **Morsel-parallel / vectorized finalize** — parallelize `AggregateExec(Final)` across the M exchange
   outputs and confirm `shuffle.partitions` ≥ vCPU; vectorized partial→final merge (DataFusion
   `AggregateMode`). Ensure we're not finalizing on 1 partition.
4. **Concurrent stage scheduling** (Spark 4.1 RT-mode) — pipeline source→exchange→window so stages
   overlap rather than serialize. Larger architectural change; only if 1–3 fall short.

Each fix is A/B'd on the SAME harness; keep it only if it moves ev/s without regressing memory/EO.

## 3. Like-for-like harness (no shortcuts — [[feedback_no_workarounds]])

Reuse `k8s/stream/` (`eks-stream-cluster`, `kafka`, `flink-session`, `producer`, `vajra-stream`):
- **Identical** for both engines: c7g.4xlarge node(s), shared Kafka topic, **100M events**, **10s
  tumbling COUNT** windowed agg, same partition count, same parallelism = vCPU.
- Measure **throughput = ev/s over the job-compute window** (Flink: REST `/jobs` job-duration to EXCLUDE
  JVM/cluster startup — the honest comparison per [[local_headtohead]]) and **peak memory** (both).
- ≥3 runs each; report median + spread (the metric is only credible controlled — never claim a single
  noisy number).

## 4. Success criteria
- **Primary:** Vajra windowed-agg throughput **≥ 0.83× Flink** (≤1.2× slower); **stretch: ≥ 1.0×**.
- **Guardrail:** memory stays **≤ 2 GiB** (preserve the 6.6× win) and EO/correctness unchanged
  (every group exact, 0 loss) — re-run the correctness assertions, not just timing.

## 5. Pre-flight (do FIRST, cheap)
- [ ] Confirm the **i32 offset-overflow at 100M** is fixed (first EKS run hit it on the distributed
  Flight/state path; v2 ordered-100M succeeded — re-verify before trusting 100M numbers).
- [ ] `dev_cleanup.sh` mindset for the cloud: scripted **teardown to $0** + verify (no lingering EKS
  nodes / NAT / Kafka EBS). Build images on a brief EC2 box, not locally.
- [ ] EKS gotchas (from [[project_streaming_vs_flink_eks]]): Ubuntu 24.04 (not AL2023), setuptools<81,
  16-vCPU account limit, Flink blob port 6124, java17 arm64 image.

## 6. Risks
- Single-instance EO constraint limits source-read parallelism → parse parallelism must live *inside*
  the instance (don't break EO to chase throughput).
- The gap may be split across stages (no single 50% culprit) → need 2 fixes; Phase A decides.
- EKS cost/time → keep runs tight, tear down immediately, capture artifacts for offline analysis.

## 7. Sequence
1. Pre-flight (above). 2. Phase A instrumentation (local build) → 1 EKS run → stage breakdown.
3. Phase B fix #1 → A/B EKS run. 4. Iterate fixes 2–4 only as the breakdown demands. 5. Final
like-for-like capstone run + memory/EO guardrail check. 6. Tear to $0; record results in
[[project_streaming_vs_flink_eks]] + STREAMING_ARCHITECTURE gap register.
