# Throughput: beat Flink — the STRUCTURAL board (2026-07-12, code-verified + KB-mapped)

## Root cause (why no-JVM/columnar/no-serde is still 1.4× behind Flink) — VERIFIED IN CODE
Vajra is NOT actually no-serde. **Every streaming operator DECODES its input FlowEvent → runs the DataFusion
RecordBatch op → ENCODES its output FlowEvent** (`DecodedFlowEventStream` + `EncodedFlowEventStream`, seen in
dedup/collector/limit/window_accum/exchange). The `source→from_json→watermark→exchange→window→sink` pipeline
does ~5–6 encode + 5–6 decode PER BATCH; each `encode_inner` allocates a marker column
(`new_null_array(Binary, N)`) + each decode does a per-row scan. This is the `__rdl_alloc` top-of-profile
churn (kind cross-pod, 2026-07-12). **Flink CHAINS operators (1 fused task, object-reuse, ZERO encode/decode
between).** Arroyo (our exact Rust+Arrow+DataFusion stack) beats Flink 5× because it keeps Arrow batches
flowing with no per-operator re-encode. We regress by bridging FlowEvent↔RecordBatch at every step. THAT is
the per-batch tax that eats the columnar advantage. Measured facts: workers ~92% on-CPU (NOT Kafka-wait);
cert-load = per-worker STARTUP, amortizes (not the lever); Flight IPC ~1.3%; scattered leaves = the encode/
decode alloc+scan churn spread across operators.

## The board (ranked, each grounded + with a profile gate)
| # | task | grounded in | expected | gate |
|---|---|---|---|---|
| **T-1 P0** | **Eliminate per-operator FlowEvent encode/decode** — pass `FlowEvent` enums DIRECTLY between in-process operators; encode to RecordBatch ONLY at the Flight shuffle boundary (cross-process). | Flink operator-chaining; Arroyo keeps Arrow flowing; the FlowEvent model itself | removes ~4–5 encode/decode + marker-col allocs + scans per batch = the bulk of the per-batch tax | local-cluster WM_PROF: encode/decode + `__rdl_alloc` share drops; throughput up; counts EXACT |
| **T-2 P1** | **Zero-alloc marker** — if a boundary must encode, represent the marker with a buffer-less `NullArray` (data batches carry no per-row marker payload) instead of `new_null_array(Binary,N)`. | Arrow `NullArray` (no buffers); Flink out-of-band barriers | kills the per-batch Binary-offsets alloc even where encode remains | flamegraph: `new_null_array`/`base64`-class allocs gone |
| **T-3 P1** | **StringView/Utf8View** on value/key so from_json + shuffle don't copy variable-length data. | arrow-rs `Utf8View` (REFERENCES §277); Polars zero-copy | cuts the remaining alloc/copy top-leaves | micro-bench + WM_PROF |
| **T-4 P2** | **Morsel-driven scheduling** on the single-node hot path (work-stealing, small morsels). | Polars/DataFusion morsel (Leis/Neumann, REFERENCES §358) | better core utilization, less handoff | at-scale profile |
| **T-5 P2** | **Cold-start warmup** — first-run is 2× slower than warm (measured EKS: 3.05M cold vs 6.7M warm); Flink has no such penalty. | measured; JIT/connection warmup | closes the cold-start gap (a real prod axis) | EKS first-vs-warm |

## Method (AIM, no scattershot): ONE task at a time, profile-gated. T-1 first (biggest, code-verified).
Validate each: local-cluster (free, WM_PROF + counts) → lean kind (real cross-pod, counts) → EKS number.
