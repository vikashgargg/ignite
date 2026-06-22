# Vajra CODEMAP — module index

**Read this first to orient** (instead of grepping). One line per key module: path ↔
responsibility ↔ key types. Keep it current when modules move. Deep streaming contracts live in
[docs/STREAMING_ARCHITECTURE.md](STREAMING_ARCHITECTURE.md). Vajra = no-JVM Rust Spark(batch)+Flink(streaming)
on Arrow + DataFusion (fork of LakeSail/Sail).

## Request flow (Spark Connect → result)

`sail-server` (gRPC) → `sail-spark-connect` (proto→spec, executor) → `sail-plan` (resolve spec→logical,
streaming rewrite) → `sail-logical-plan`/`-optimizer` → `sail-session` planner (logical→physical) →
`sail-physical-plan`/`-optimizer` → `sail-execution` (distributed: Arrow Flight, codec) → results.

## Core crates

| Crate | Role |
|---|---|
| `sail-cli` | `vajra` binary (`server` subcommand); entrypoint |
| `sail-server` | gRPC server hosting Spark Connect |
| `sail-spark-connect` | Spark Connect proto ↔ `spec` conversion; `service/plan_executor.rs` runs commands incl. `handle_execute_write_stream_operation_start` (trigger/outputMode/options → run flags) |
| `sail-common` | `spec::*` plan IR (the resolved-but-pre-DataFusion representation) |
| `sail-common-datafusion` | shared DataFusion glue; **streaming/event** (FlowEvent model), checkpoint store, stream source trait |
| `sail-plan` | `spec` → DataFusion `LogicalPlan` (resolver); `streaming/rewriter.rs` rewrites optimized plan into streaming operators; `lib.rs::resolve_and_execute_plan_with_options` is the entry |
| `sail-logical-plan` | custom logical nodes (incl. `streaming/window_accum.rs` `WindowAccumNode`, watermark, dedup, join, sinks) |
| `sail-session` | `planner.rs`: logical → physical plan (maps streaming nodes → execs) |
| `sail-physical-plan` | custom physical execs (see streaming below) |
| `sail-execution` | distributed execution; `codec.rs` (de)serializes physical plans across workers (Arrow Flight) |
| `sail-data-source` | source/sink formats (kafka, file_stream, parquet, json, delta, iceberg, rate, socket, console…) |
| `sail-function` / `sail-python-udf` | scalar/agg functions; Python UDFs |
| `sail-delta-lake` / `sail-iceberg` / `sail-vortex` | lakehouse table formats |
| `sail-catalog*` | catalog backends (memory, glue, hms, iceberg, unity, onelake, system) |

## Streaming hot path (most-edited)

| File | What |
|---|---|
| `sail-common-datafusion/src/streaming/event/` | `FlowEvent::{Data{batch,retracted}, Marker(Watermark/Checkpoint/EndOfData/...)}`; encode/decode; `MARKER_FIELD_NAME`. The changelog primitive (retract stream). |
| `sail-common-datafusion/src/streaming/{checkpoint,coordinator,source}.rs` | checkpoint store; epoch coordinator; `StreamSource` trait |
| `sail-data-source/src/formats/kafka/reader.rs` | `KafkaSourceExec`: bounded(availableNow)/realtime(continuous EO)/unbounded paths; 1 instance/partition (FLIP-27); `VAJRA_KAFKA_LEGACY_POLL` kill-switch; `KAFKA_BENCH` read micro-bench |
| `sail-data-source/src/formats/kafka/sink.rs` | `KafkaSinkExec` transactional EO producer |
| `sail-physical-plan/src/streaming/watermark.rs` | `WatermarkExec`: per-partition `maxTs−delay`, monotonic |
| `sail-physical-plan/src/streaming/exchange.rs` | `StreamExchangeExec`: keyed N→M shuffle; receiver MIN-merges watermarks |
| `sail-physical-plan/src/streaming/barrier_align.rs` | `StreamBarrierAlignExec`: N→1 Chandy-Lamport barrier alignment |
| `sail-physical-plan/src/streaming/window_accum.rs` | `WindowAccumExec` + `WindowOutputMode{Append,Update}` + `with_output_mode`; `emit_changelog` (retract+insert); `AccumState` |
| `sail-physical-plan/src/streaming/{dedup,stream_join,filter,limit}.rs` | other stateful operators |
| `sail-physical-plan/src/streaming/collector.rs` | `StreamCollectorExec`: materialize bounded changelog → table (net by row-identity) |
| `sail-physical-plan/src/streaming/state_io.rs` | operator state stage/restore (EO recovery) |
| `sail-plan/src/streaming/rewriter.rs` | `rewrite_streaming_plan` + `StreamingRewriter`; threads bounded/checkpoint/realtime/update_mode/allowed_lateness |
| `sail-session/src/planner.rs` | maps `WindowAccumNode`→`WindowAccumExec` etc. (search `window_output_mode` / `with_output_mode`) |
| `sail-execution/src/codec.rs` | physical-plan (de)serialization for distributed/local-cluster (search `WindowAccumExec::try_new` decode arm) |

## Benchmarks / harness

| Path | What |
|---|---|
| `scripts/diff_test*` | differential test vs real Spark 3.5.3 (see [[project_test_harness]]) |
| `scripts/stream_windowed_agg.py` | Vajra windowed-agg head-to-head harness |
| `k8s/stream/` | EKS head-to-head (kafka, flink-session, producer, vajra-stream, eks-stream-cluster) |
| `KAFKA_BENCH=1 ... cargo test -p sail-data-source --release kafka_read_bench` | local Kafka read throughput micro-bench |

## Conventions

- clippy lane: `--all-targets -D warnings`; workspace denies `unwrap/expect/panic/allow_attributes`.
  Test mods use `#[expect(clippy::unwrap_used)]`. Comply, don't loosen ([[project_clippy_green]]).
- Streaming correctness contract + feature matrix + gaps: **always check `docs/STREAMING_ARCHITECTURE.md` first.**
- **Index hygiene**: reference *symbols* (grep targets like `fn emit_changelog`), never line numbers — lines rot, symbols don't. Validate paths exist when editing this file.
