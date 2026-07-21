# Zelox CODEMAP — module index

**Read this first to orient** (instead of grepping). One line per key module: path ↔
responsibility ↔ key types. Keep it current when modules move. Deep streaming contracts live in
[docs/STREAMING_ARCHITECTURE.md](STREAMING_ARCHITECTURE.md). Zelox = no-JVM Rust Spark(batch)+Flink(streaming)
on Arrow + DataFusion (fork of LakeSail/Sail).

## Request flow (Spark Connect → result)

`sail-server` (gRPC) → `sail-spark-connect` (proto→spec, executor) → `sail-plan` (resolve spec→logical,
streaming rewrite) → `sail-logical-plan`/`-optimizer` → `sail-session` planner (logical→physical) →
`sail-physical-plan`/`-optimizer` → `sail-execution` (distributed: Arrow Flight, codec) → results.

## Core crates

| Crate | Role |
|---|---|
| `sail-cli` | `zelox` binary (`server` subcommand); entrypoint |
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
| `sail-common-datafusion/src/streaming/event/` | `FlowEvent::{Data{batch,retracted}, Marker(Watermark/Checkpoint/EndOfData/...)}`; encode/decode (`encoding.rs`); `MARKER_FIELD_NAME`. The changelog primitive (retract stream). **THROUGHPUT NOTE (2026-06-30):** `encode()` prepends a `new_null_array(Binary, num_rows)` marker col + retracted bool to EVERY data batch at EVERY hop (O(N) per-batch alloc; Flink doesn't tag per-record) — candidate cost for the EKS throughput gap (window STARVED at 16 readers; read/from_json/parallelism ruled out). CONFIRM with a timer before optimizing. See throughput-robustness-review.md. |
| `sail-common-datafusion/src/streaming/{checkpoint,coordinator,source}.rs` | checkpoint store; epoch coordinator; `StreamSource` trait |
| `sail-data-source/src/formats/kafka/reader.rs` | `KafkaSourceExec`: bounded(availableNow)/realtime(continuous EO)/unbounded paths; 1 instance/partition (FLIP-27); `ZELOX_KAFKA_LEGACY_POLL` kill-switch; `KAFKA_BENCH` read micro-bench |
| `sail-data-source/src/formats/kafka/sink.rs` | `KafkaSinkExec` transactional EO producer |
| `sail-physical-plan/src/streaming/watermark.rs` | `WatermarkExec`: per-partition `maxTs−delay`, monotonic |
| `sail-physical-plan/src/streaming/exchange.rs` | `StreamExchangeExec`: keyed N→M shuffle; receiver MIN-merges watermarks (`merge_output_subchannels`). Hash keys = group-by keys minus the window (planner.rs:459, `group_exprs.skip(1)` → routes by `k`). **`coalesce_flow_events`** (pub, unit-tested): marker-aware PULL combinator re-merging routed batches to `ZELOX_SHUFFLE_BATCH_ROWS` (default 16384) via arrow `BatchCoalescer` + flush-before-marker + end-flush + Flink buffer-timeout (`ZELOX_SHUFFLE_BUFFER_TIMEOUT_MS` 100ms); applied at Flight `do_get` (stream_service/server.rs) to cut distributed shuffle tiny-batch IPC. Design+validation: docs/design/distributed-shuffle-throughput.md |
| `sail-physical-plan/src/streaming/watermark.rs` | `WatermarkExec`: emits `Watermark` markers = `max(event_time)−delay`. **PERIODIC** emission (`ZELOX_WATERMARK_INTERVAL_MS`, default 200ms = Flink `auto-watermark-interval`) — NOT per-batch (per-batch put a marker between every data batch, defeating the shuffle coalescer). **Per-partition (Flink)**: `with_partition_watermark(col,N)` → MIN over partitions, withheld until all N seen, with Flink withIdleness (idle partition excluded → never stalls) + startup grace + periodic tick; default global. Fixes premature window close |
| `sail-common-datafusion/src/streaming/event/encoding.rs` + `sail-execution/src/stream_service/{server,client}.rs` | **WM_PROF distributed instrumentation** (`ZELOX_WM_PROF=1`): per-process `ensure_process_dumper` thread logs every stage counter (source_read/from_json/exchange/shuffle_send/recv) to stderr every 10s on EVERY pod (`WM_PROF_PROC[...]`) — so distributed per-stage cost is visible (was blind); `SHUFFLE_SEND_NS/RECV_NS` + batch counts time the cross-pod Flight IPC |
| **Watermark/source wiring** | `WatermarkNode` (logical, `sail-logical-plan/streaming/watermark.rs`) created at `resolver/query/misc.rs:205` (PRE-rewriter) — carries `partition_col:Option<String>`+`num_partitions` (`with_partition_watermark`). `WatermarkExec` built at `planner.rs:~383`: prefers WatermarkNode per-partition fields (general path), else prove-it heuristic (`ZELOX_WM_PARTITIONS` env + lone-Int32 col). Realtime Kafka source pinned `parallelism=1` at `kafka/reader.rs:279` (single-instance EO commit) = the per-partition gap. **OPEN (step2):** rewriter populator must detect realtime-Kafka windowed + PRESERVE partition col to the watermark input (user proj drops it; streaming schemas use #N names) + thread N (`count_kafka_partitions` async vs rewriter sync). **FINDING 2026-06-29 (WM_DBG instrumented, then reverted):** a populator hooked on `TableScan`+`get_stream_source_opt` NEVER fires — the streaming Kafka source is NOT a plain `TableScan` in the rewriter, it's wrapped (`StreamSourceWrapperNode`/`StreamSourceAdapterNode`), and `WatermarkNode` is pre-created at `resolver/query/misc.rs` (rewriter sees it as passthrough). ⇒ the fix (DONE+committed 2026-06-29): in the `TableScan` handler, force `partition` back into the scan projection via `provider.schema()` (survives pruning) + record its name; Projection handler carries it up; WatermarkNode handler attaches partition_col+N. ENGAGES generally now: gate dups rows 1194→~1800 (general query that DROPS partition). AUTO-N DONE 2026-06-29: dropped `ZELOX_WM_PARTITIONS` — startup grace is PURE-TIME (withhold first wm for grace so all partitions report; then MIN-over-active+idleness), no partition-count needed; planner enables on partition-col present (dropped n>1 gate). Validated NO env: rows=1803/1800. Idleness guard = can't hang. **REMAINING: close the continuous epoch-boundary residual (~3 dups, separate from the partition race), full exact-zero validation on a longer/EKS run, flip correctness-gate C6/C7.** |
| **Streaming file sink (EO)** | `streaming_decode.rs`: `RealtimeFileSinkExec` (continuous: per-epoch `<out>/<epoch>/` + `_spark_metadata/<epoch>` + `realtime/committed`), `StreamingSinkCommitExec` (micro-batch batch_id). `streaming_sink_log.rs` = `_spark_metadata` commit log; read honors it via `listing/source.rs` `committed_urls_if_logged`. `Trigger.Continuous`→realtime at `resolver/command/write_stream.rs:163` |
| `sail-physical-plan/src/streaming/barrier_align.rs` | `StreamBarrierAlignExec`: N→1 Chandy-Lamport barrier alignment |
| `sail-physical-plan/src/streaming/window_accum.rs` | `WindowAccumExec` + `WindowOutputMode{Append,Update}` + `with_output_mode`; `emit_changelog` (retract+insert); `AccumState`. **F5 spillable state**: `SpillSourceExec` (lazy spill-reading input, one chunk at a time) + `bounded_agg_context` (FairSpillPool + DiskManager so the Final agg spills its hash table) + `begin_finalize`/`consume_merge_batch` (resumable merge driven one batch/poll via `AccumState.active_merge` → incremental emit, bounded `buf`) + `rebuild_retained_state` (re-spill + COMPACT open windows). **F5.3 compaction**: `compact_partials` (DataFusion `AggregateMode::PartialReduce` = partial→partial merge) collapses duplicate (window,key) partials → O(distinct); wired compact-before-spill + compact-on-retain. **OPT-IN `ZELOX_F5_COMPACT=1` (default OFF)** — open bug: compact-THEN-spill loses closed-window state on unique keys (silent no-emit); see F5 design gap register. `F5_PEAK` log = operator peak resident state (bounded-memory proof). `gather_partials` still used for the durable snapshot (EndOfData/Checkpoint). Budget: `ZELOX_STREAMING_STATE_BUDGET_BYTES` (default 128 MiB, per-partition); `ZELOX_F5_DEBUG` logs spills. **P1 fix**: `drop_late_rows` (drop data past end+lateness) + watermark prunes `emitted_ends` (bounded, no re-emit). **inc-ckpt** (gated `ZELOX_INC_CKPT`): Checkpoint{epoch}→`stage_epoch_incremental` (manifest refs chunks) + in-mem SharedStateRegistry GC; restore via `restore_epoch_incremental` |
| `sail-physical-plan/src/streaming/{dedup,filter,limit}.rs` | other stateful operators |
| `sail-physical-plan/src/streaming/stream_join.rs` | `StreamJoinExec` (inner equi/interval join). **F5-join SPILL**: per-side buffer (`JoinAccum`) spills over `ZELOX_STREAMING_STATE_BUDGET_BYTES` to object-store (`state_io`); join builds the hash on the small INCOMING batch and **streams the other side's buffer (in-RAM + spilled) as the probe** via `SpillSourceExec` → join memory bounded by batch, not buffer; right-arrival swaps keys + `reorder_right_left_to_left_right`; interval eviction is spill-aware (`evict_respill_side`). **Snapshot**: legacy `gather_side`+`stage_state` (full); under `ZELOX_INC_CKPT` **inc-ckpt.2b** stages incrementally (in-RAM residual + manifest referencing already-spilled chunks, via `stage_epoch_incremental`; restore folds residual++chunks) — O(delta), same as the window operator. `ZELOX_F5_DEBUG` logs `F5_JOIN_SPILL` |
| `sail-physical-plan/src/streaming/collector.rs` | `StreamCollectorExec`: materialize bounded changelog → table (net by row-identity) |
| `sail-physical-plan/src/streaming/state_io.rs` | operator state stage/restore (EO recovery) |
| `sail-plan/src/streaming/rewriter.rs` | `rewrite_streaming_plan` + `StreamingRewriter`; threads bounded/checkpoint/realtime/update_mode/allowed_lateness |
| `sail-session/src/planner.rs` | maps `WindowAccumNode`→`WindowAccumExec` etc. (search `window_output_mode` / `with_output_mode`) |
| `sail-execution/src/codec.rs` | physical-plan (de)serialization for distributed/local-cluster (search `WindowAccumExec::try_new` decode arm) |

## Benchmarks / harness

| Path | What |
|---|---|
| `scripts/diff_test*` | differential test vs real Spark 3.5.3 (see [[project_test_harness]]) |
| `scripts/stream_windowed_agg.py` | Zelox windowed-agg head-to-head harness |
| `scripts/tri_engine_scorecard.sh` | 2-phase (streaming\|batch) Zelox-vs-Flink-vs-Spark scorecard (Nexmark / TPC-DS methodology); authoritative head-to-head |
| `scripts/batch_s3_bench.py` | **P4** batch Parquet-on-S3 (gen→write S3→read+agg, count/sum correctness+timing); runs on Zelox (`SPARK_REMOTE=sc://`) OR classic Spark (`BENCH_REMOTE=local[16]` — NOT the magic `SPARK_REMOTE`, which forces Connect-mode) |
| `scripts/eks_p1_s3_eo.sh` | **P1** Kafka→windowed-agg→Parquet-on-S3 exactly-once, incl. kill-9 crash gate (EO across restart) |
| `scripts/eks_build_image.sh` | fast arm64 image build via throwaway c7g EC2 (native, auto-terminate) — avoids slow local docker |
| `scripts/correctness_gate.sh` | STANDING adversarial streaming correctness harness (cardinality/partitions/scrambled/crash) vs batch ground-truth |
| `k8s/stream/` | EKS streaming head-to-head (kafka, flink-session, producer, zelox-stream, eks-stream-cluster) |
| `k8s/eks/` | EKS batch (spark-tpcds-job, spark-s3-job, zelox-sf100/client); Spark-on-S3 needs hadoop-aws:3.3.4 + aws-java-sdk-bundle + `InstanceProfileCredentialsProvider` + s3→s3a; py3.12 needs `setuptools<81` |
| `KAFKA_BENCH=1 ... cargo test -p sail-data-source --release kafka_read_bench` | local Kafka read throughput micro-bench |

Streaming perf/observability knobs (see also [[project_f2f3_distributed]] / throughput tickets):
`KafkaStatsContext` (`sail-data-source/.../kafka/reader.rs`, `ZELOX_KAFKA_STATS` → logs fetchq_size/prefetch/lag);
`ZELOX_WM_PROF` (per-stage streaming CPU breakdown); jemalloc allocator is **opt-in** (`sail-cli` `--features jemalloc`,
default off — glibc/jemalloc measured equivalent, so not the memory driver).

## Conventions

- clippy lane: `--all-targets -D warnings`; workspace denies `unwrap/expect/panic/allow_attributes`.
  Test mods use `#[expect(clippy::unwrap_used)]`. Comply, don't loosen ([[project_clippy_green]]).
- Streaming correctness contract + feature matrix + gaps: **always check `docs/STREAMING_ARCHITECTURE.md` first.**
- **Index hygiene**: reference *symbols* (grep targets like `fn emit_changelog`), never line numbers — lines rot, symbols don't. Validate paths exist when editing this file.
