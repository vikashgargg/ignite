# Streaming prod-grade practices — consult FIRST, update as you learn

**Workflow (standing):** at the START of any streaming fix/feature, read the relevant row here + its
canonical source, cite it in the work, and meet its bar. When you learn something new (a paper, a Flink
doc, a measured result), add/refine a row the SAME turn. This is the cite-don't-re-derive contract —
saves tokens and keeps work prod-grade. Pairs with [REFERENCES.md](../REFERENCES.md) (distilled facts)
and [CODEMAP.md](../CODEMAP.md) (where the code is).

## The bar, per concern (canonical source → what prod-grade means → Vajra status)

| Concern | Canonical source | Prod-grade bar | Vajra |
|---|---|---|---|
| **Event-time / watermarks** | Akidau et al., *The Dataflow Model* (VLDB 2015); MillWheel (VLDB 2013); Flink `withIdleness` | Per-key/partition event-time, low-watermark = MIN across inputs, idle sources excluded (liveness vs completeness), allowed-lateness | ✅ per-partition WM + idleness + pure-time grace (REFERENCES §2) |
| **Exactly-once** | Chandy-Lamport (1985) snapshots = Flink barriers; Kreps *The Log* (2013); Spark `MicroBatchExecution` WAL→commit | Barrier-aligned snapshot OR WAL→atomic-commit; idempotent/transactional sinks; offsets+state commit atomically | ✅ EO across crash, transactional Kafka/file sink |
| **State backend** | Flink RocksDB/ForSt; FLIP-8 key-groups; LSM (O'Neil 1996) | Bounded memory (spill), incremental checkpoint (O(delta)), key-group rescale, TTL | ✅ F5 spill + inc-ckpt O(delta) + key-group rescale primitives; ⬜ TTL, operator wiring |
| **Checkpointing** | Flink incremental ckpt (SST sharing + SharedStateRegistry); Spark 4.1 RT-mode (5-min ckpt decoupled from latency) | Async, off the critical path; incremental (don't re-copy state); refcount-GC shared files | ✅ manifest-refs-immutable-chunks, O(delta) proven |
| **Backpressure** | Flink credit-based flow control (FLIP-1/network stack) | Bounded buffers + credit/blocking that propagates upstream; never unbounded queue | 🟡 bounded channels in exchange; ⬜ explicit credit + metrics |
| **Throughput** | Arrow columnar; Leis et al. *Morsel-Driven Parallelism* (SIGMOD 2014); Arrow Flight zero-copy shuffle; DataFusion vectorized agg; Arroyo columnar-JSON (REFERENCES §6) | Vectorized batch ops, morsel/NUMA-parallel operators, zero-copy shuffle, parallel parse-in-source | 🟡 **~1.068× Flink** measured (EKS 100M: 5.37M vs 5.72M ev/s), NOT 2.4×. Levers: source_read 100.6s ≫ from_json 31.7s > exchange 66.5s. **VAJ-T7b simd-json DONE** (153ae332); **VAJ-T7 source-fusion** = the beat (elide raw-value materialize; EPIC-beat-flink-streaming.md) |
| **Distributed shuffle** | Arrow Flight; Spark 4.1 in-memory streaming shuffle; Ballista | Zero-copy, pipelined, marker-aware; receiver MIN-merges watermarks | ✅ StreamExchangeExec (keyed, marker-broadcast, MIN-merge); ⬜ Flight zero-copy |
| **Correctness validation** | (discipline) adversarial testing | Standing gate: scrambled order, multi-partition skew, crash, bounded-mem, vs batch ground-truth; GREEN/XFAIL | ✅ correctness gate (C1-C7); ⬜ rescale + soak gates |
| **Rescaling** | Flink FLIP-8 + Stefan Richter *Rescalable State* (2017) | Key-groups; restore at M′≠M redistributes keyed state; cheap (range re-assign) | ✅ primitives proven (REFERENCES §2b); ⬜ operator wiring |
| **Observability** | Flink metrics; USE/RED method | Per-operator throughput, watermark lag, state size, ckpt duration/size exported (Prometheus) | ⬜ P0 gap |
| **Endurance** | (discipline) soak + chaos | Multi-day soak, random kill-chaos, no leak/drift; not just one-crash demo | ⬜ P0 gap |

## Rules of thumb (cheap to recall, expensive to relearn)
- **Multi-partition keyed correctness is THE recurring bug class** (per-partition WM dup, F5.3 compaction-multipart, EKS-scale): passes trivially at N=1/PARTS=1, breaks at scale/skew. Always test PARTS≥4 + scrambled order before claiming correct.
- **Never trust local throughput numbers** (noisy ±2×); only controlled EKS. Memory numbers ARE stable.
- **A stall-forever is worse than a dup** — every withhold/wait needs an idleness/timeout escape.
- **Single-instance constraints** (realtime EO source = parallelism 1) push parallelism *inside* the instance, not by breaking EO.
- **Bound every test run in `timeout`**; never an unbounded continuous loop (3h-hang lesson).
