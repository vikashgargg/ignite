# Throughput robustness review — what it takes to beat Flink (end-to-end)

**Purpose:** before building Phase B step 2, audit the WHOLE realtime throughput path and list every gap
to ≤1.2×-and-then-beat Flink — so we fix the right things, in order. Grounded in Phase A
(`VAJRA_WM_PROF`: window STARVED, bottleneck UPSTREAM) + KB §2d (FLIP-27 / Spark 4.1 RT-mode / Arrow
Flight shuffle / Ballista / FAANG).

## The realtime path, stage by stage (where each stands)
`KafkaSource → decode → from_json → WatermarkExec → StreamExchangeExec (N→M) → WindowAccumExec → sink`

| Stage | Today | Beats-Flink requirement | Gap |
|---|---|---|---|
| **Source read** | 1 instance (realtime) reads all N partitions | N readers, 1/partition (FLIP-27) | **Step 1 DONE (gated)** — 4× rows |
| **decode + from_json** | single-threaded on the 1 instance | parallel across N readers | fixed BY step 1 (rides the N readers) |
| **Watermark** | per-partition MIN + discovery-grace workaround | per-instance single-partition → monotone | step 3 (drop workaround once 1:1) |
| **Exchange (shuffle)** | **in-process tokio mpsc, single-node** | **multi-node zero-copy = Arrow Flight** | **GAP for multi-node EKS throughput** |
| **Window finalize** | M-way parallel, fast (Phase A: 0%) | already not the bottleneck | none |
| **EO commit** | single-coordinator (`realtime/committed`) | N-instance union commit | **Step 2 (correctness, gates multi-instance)** |
| **Encoding** | Arrow-IPC FlowEvent encode/decode per batch hop | minimize copies; batch sizing | measure (Phase A says not window; check exchange encode) |
| **Stage scheduling** | operators pipelined via tokio tasks | Spark-RT concurrent stages (no stage blocks) | likely OK (async), confirm no head-of-line block |

## The gaps to beat Flink, RANKED
1. **Step 2 — N-instance EO commit union** (correctness; UNBLOCKS multi-instance being usable). Without
   it, step 1's 4× is unsafe across crash. The delicate part of Phase B. Local crash-gate first.
2. **Streaming Arrow Flight shuffle** (multi-node EKS throughput). `StreamExchangeExec` is in-process
   tokio channels = single-node only. On multi-node EKS the keyed N→M shuffle must go over **Arrow
   Flight (DoGet/DoPut, zero-copy)** — the F2/F3 "streaming Flight shuffle" still-open item. **If the
   EKS test is single big node, this isn't needed yet** (in-process parallelism within one node = step
   1). If multi-node, it's required. ⇒ DECIDE the EKS topology first (1 big node vs multi-node).
3. **Step 3 — drop the per-partition-WM workaround** (1:1 instance:partition → monotone watermark;
   simplifies + closes the last-window edge). Easy once step 2 lands.
4. **Encode/batch efficiency** — confirm the exchange's per-batch IPC encode isn't a hidden cost
   (Phase A localized to "upstream" broadly; a second prof split (read vs from_json vs exchange-encode)
   would pinpoint — only if step 1+2 don't already close the gap on EKS).
5. **Concurrent stage scheduling** (Spark 4.1 RT-mode) — only if a stage head-of-line-blocks; async
   tokio likely already pipelines. Lowest priority; confirm via EKS profile.

## Robustness verdict on the FLIP-27 design
**Sound for single-node throughput** (step 1 proves N parallel readers work, window isn't the
bottleneck). **For multi-node EKS it needs the streaming Flight shuffle (#2)** — that's the one
structural addition the current design doc under-specifies. **Correctness hinges on step 2** (the
N-instance commit union under crash — the same multi-partition-commit race the gap register flags).

## EKS TOPOLOGY CONFIRMED 2026-06-30 — SINGLE NODE
`k8s/stream/`: Vajra `replicas:1` + `--mode local-cluster --workers 4`, `eks-stream-cluster
desiredCapacity:1`, `role:compute` single node. ⇒ **(a) streaming Arrow Flight shuffle (#2) is NOT on
the critical path** for this test — in-process exchange across 4 in-node workers is fine; defer Flight to
true multi-node scale-out. **(b) The throughput NUMBER needs only step 1 + an EKS NO-CRASH run** — a
throughput measurement never crashes, so the N-instance EO commit union (step 2) is NOT required to
answer "did step 1 close the 2.4× gap vs Flink?" Step 2 is for the crash-EO *correctness* claim,
separable from the throughput number. **This is much cheaper than assumed.**

## ⚠️ CRITICAL PATH-MISMATCH FOUND 2026-06-30 (before any EKS spend)
The EKS throughput harness (`scripts/stream_windowed_agg.py`, `state_scale_stress.py`) uses
**`trigger(availableNow=True)` — the BOUNDED path**, which ALREADY runs one-instance-per-Kafka-partition
(16 readers, `reader.rs:270`). So: **(1)** Phase A's "single-instance source STARVED" was profiled on the
**CONTINUOUS** path (`inc_ckpt_gate`), a DIFFERENT path. **(2)** `VAJRA_RT_MULTI` / Phase B multi-instance
only helps CONTINUOUS — it does NOT touch the bounded path the EKS 2.4× gap was measured on. ⇒ **The EKS
throughput gap is NOT the realtime single-instance source.** Must RE-PROFILE the BOUNDED (availableNow)
windowed-agg to find ITS bottleneck (from_json / exchange / window — all already parallel-read at 16).
Phase B remains valid for *continuous-mode* throughput, but the headline EKS number needs the bounded
profile first. (This is exactly the robustness check paying off — caught before EKS $$.)

## BOUNDED-PATH PROFILE 2026-06-30 — the REAL EKS gap is `from_json` + exchange (NOT parallelism/window)
Profiled `stream_windowed_agg.py` (availableNow, 16 partitions/16 readers, VAJRA_WM_PROF) locally:
window **STILL STARVED** — input_wait ≈75%/instance, finalize only **17–20%**, throughput 0.26M ev/s
(local, modest). ⇒ **even with 16 parallel readers, upstream (`from_json` parse + exchange) can't feed
the window fast enough.** So the ~2.4×-vs-Flink gap is **per-unit `from_json`/exchange throughput**, NOT
read parallelism (already maxed at 16) and NOT the window finalize (~20%). **THE fix to beat Flink =
`from_json` parse throughput (+ exchange efficiency).** JSON parse is the canonical hotspot; Flink's
JSON deserialize vs our DataFusion `from_json` UDF / arrow-json is the likely delta.

### Fix targets for the bounded/EKS gap (ranked)
1. **`from_json` parse** — vectorized/SIMD JSON (simd-json / arrow-json fast path), or avoid re-parsing
   (parse once; project needed fields). Confirm its share with a split timer or with/without-from_json A/B.
2. **Exchange efficiency** — the 16→M keyed shuffle's per-batch IPC re-encode; minimize copies.
3. (Multi-instance / Flight shuffle remain CONTINUOUS-path / multi-node items, not this gap.)

## Recommended order (RE-REVISED after the mismatch — superseded by the bounded profile above)
1. **Profile the BOUNDED availableNow windowed-agg** (VAJRA_WM_PROF, EndOfData dump) over Kafka locally
   → find the real EKS-path bottleneck (window busy? exchange? from_json even at 16 readers?).
2. Fix the dominant bounded-path stage (the actual EKS gap).
3. THEN EKS A/B on the bounded path vs Flink.
(Phase B continuous multi-instance stays banked for continuous-mode throughput / correctness.)

## Recommended order (REVISED — superseded by the path-mismatch above)
1. **EKS throughput A/B FIRST** (answers the headline cheaply): deploy step-1 `VAJRA_RT_MULTI=1` on the
   single-node cluster, no-crash, measure ev/s vs the single-instance baseline AND vs Flink 1.19. Confirms
   whether parallel read+from_json closes/beats the gap. Pre-flight: i32-overflow at scale, teardown-$0.
2. **Step 2 (N-instance EO commit union)** + local crash-gate — the correctness claim, once throughput
   is confirmed worth it.
3. **Step 3** drop per-partition-WM workaround; **multi-node Flight shuffle** only if/when scaling out.

## Recommended order (original, superseded by the single-node finding above)
1. **Decide EKS topology** (1 big c7g node vs multi-node) — determines if streaming Flight shuffle is
   on the critical path now. (Prior EKS run was c7g.4xlarge — confirm single vs multi.)
2. **Step 2: N-instance EO commit union** + local crash-gate (`inc_ckpt_gate PARTS=4` no-dup/no-loss).
3. **Step 3: drop per-partition-WM workaround** + continuous gate bit-exact.
4. **If multi-node: streaming Arrow Flight shuffle** for `StreamExchangeExec`.
5. **EKS A/B** vs single-instance baseline + vs Flink. Target ≤1.2× then beat; keep 6.6× memory.
