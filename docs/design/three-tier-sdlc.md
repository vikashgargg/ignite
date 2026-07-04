# Three-tier SDLC — architect-first, kind-before-EKS (STANDING, user-directed 2026-07-04)

The repeated failure mode this project hit: a change is **green locally but fails on EKS**, then we burn a
cluster spin-up + image build + deploy to *discover* the bug, then patch, then re-discover. That is the
patch-and-fix loop the [charter](../../MEMORY.md) forbids. Root cause: there was **no tier between a local
`--workers N` process and a real EKS cluster** to catch **Kubernetes-specific** failures. Every EKS surprise
was a k8s issue a local process cannot express:

- `flink-jm` stuck **Pending** because `vajra-client` squatted 1 CPU on the 8-vCPU kafka node (scheduling).
- Flink SQL **update-vs-append sink** error (`GROUP BY window_start` planned as a retracting GroupAggregate).
- The continuous **over-emit / final-window** behaviour under real multi-pod parallelism + real Kafka.

## The three tiers — a change is DONE only when all three are green, IN ORDER

| Tier | What | Cost | Catches |
|---|---|---|---|
| **T1 — local process** | `cargo test` + prod-representative gates (`scripts/local_continuous_scale.sh`, `correctness_gate.sh`, `inc_ckpt_gate.sh`): 16-part, `--workers 4`, tens-of-M events, continuous, clean+crash, SELF-CHECKING (assert produced==N, output non-empty — fail LOUD) | free, seconds–minutes | logic, scale correctness, EO, over-emit |
| **T2 — kind** (`k8s/kind/`, `scripts/kind_up.sh` + `scripts/kind_streaming_test.sh`) | REAL Kubernetes in Docker, LOCAL. Deploy the SAME manifests (`kafka.yaml`, `vajra-stream.yaml`, `flink-session.yaml`, jobs) + SAME nodeSelectors (`role=kafka`/`role=compute` worker nodes). Resources scaled down (laptop ≠ c7g.4xlarge) — T2 proves topology/scheduling/networking/Kafka/Flink-SQL, not scale. | free, minutes | pod scheduling, multi-pod CPU/mem contention, service networking, Kafka broker, Flink deploy + SQL, manifest correctness, object-store path (MinIO) |
| **T3 — EKS** (`eks_*` scripts) | FINAL like-for-like confirmation: real spot hardware, real S3, real scale, real Flink 1.19 head-to-head. | $ (tear to $0 after) | scale-only + real-cloud behaviour |

**Rule: T3 CONFIRMS, it never DISCOVERS.** Run EKS ONCE per milestone that is already green on T1 **and** T2.
If something fails on EKS that T2 could have caught, the fix is to add it to the T2 gate, not to keep
iterating on EKS.

## Architect-first

Before writing code for a milestone: (1) research the design from the knowledge base (official Flink/Spark/
DataFusion/RisingWave docs + [REFERENCES.md](../REFERENCES.md) + [streaming-prodgrade-practices.md](streaming-prodgrade-practices.md));
(2) write the prod-grade **design** + the **test cases** (the T1 gates and the T2 kind manifests/asserts)
FIRST; (3) implement to make them pass; (4) T1 green → T2 green → T3 confirm. No symptom-patching; a change
that only moves a metric without satisfying the invariant is rejected.

## Open milestone (designed next, this method): close the two EKS-measured gaps
1. **Final-window completeness** — Vajra emits 9 windows / 90M where Flink emits 10 / 100M: Vajra does not
   flush the last boundary window at end-of-input. Design from Flink's `MAX_WATERMARK` on end-of-input +
   Spark's `availableNow` final trigger. T1: assert `n_windows` and `sum == N`. T2: same on kind.
2. **Realtime Kafka→Kafka passthrough throughput** — Vajra ~1.3K/s p50=257ms vs Flink 20K/s p50=98ms (EKS,
   clean); reproduced on T1 local (~1.4K/s). **MEASURED (2026-07-04, isolated backlog-drain, `scripts/
   kafka_sink_gate.sh`): the Kafka SINK is NOT the bottleneck** — an availableNow Kafka passthrough drains at
   **~122K/s** (single ThreadedProducer, librdkafka-batched). Windowed-agg reads Kafka at **5.5M/s**. So the
   ~1K/s is specific to the **CONTINUOUS/realtime execution path** (per-epoch coordination + aligned barriers
   + tiny 5ms-flush batches + single-task funnel), NOT the raw producer, and NOT fixed by "parallelize the
   sink" (that was the wrong layer — RETRACTED before implementing). Flush-cadence tuning (5/100/500ms) did
   NOT move it (disproved). ALSO found: the availableNow Kafka passthrough delivered only 125K of 2M
   (INCOMPLETE) — a separate Kafka-sink-under-availableNow delivery gap. **This is a DEEP architectural
   milestone (continuous realtime throughput), not a one-change fix.** Next: (a) build a CORRECT isolated
   continuous-throughput harness (the latency harness co-locates loadgen+engine; the availableNow one loses
   data); (b) per-stage profile the continuous path (VAJRA_WM_PROF); (c) design from Flink pipelined-exchange
   + credit-based flow-control + Spark 4.1 RT-mode. Do NOT rush it into a mixed session.
