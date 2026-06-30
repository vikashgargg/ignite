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

## Recommended order
1. **Decide EKS topology** (1 big c7g node vs multi-node) — determines if streaming Flight shuffle is
   on the critical path now. (Prior EKS run was c7g.4xlarge — confirm single vs multi.)
2. **Step 2: N-instance EO commit union** + local crash-gate (`inc_ckpt_gate PARTS=4` no-dup/no-loss).
3. **Step 3: drop per-partition-WM workaround** + continuous gate bit-exact.
4. **If multi-node: streaming Arrow Flight shuffle** for `StreamExchangeExec`.
5. **EKS A/B** vs single-instance baseline + vs Flink. Target ≤1.2× then beat; keep 6.6× memory.
