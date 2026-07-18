use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, Int32Builder, Int64Builder, RecordBatch,
    StringBuilder, TimestampMillisecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{arrow_datafusion_err, exec_datafusion_err, plan_err, Result};
use futures::{FutureExt, StreamExt};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, ConsumerContext, StreamConsumer};
use rdkafka::message::{Message, Timestamp};
use rdkafka::{ClientContext, Statistics};
use sail_common_datafusion::streaming::checkpoint::CheckpointStore;
use sail_common_datafusion::streaming::event::encoding::EncodedFlowEventStream;
use sail_common_datafusion::streaming::event::schema::to_flow_event_schema;
use sail_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;
use sail_common_datafusion::streaming::event::FlowEvent;
use sail_common_datafusion::streaming::source::StreamSource;
use sail_function::scalar::json::from_json::{parse_json_binary_to_struct, SparkFromJsonOptions};

use crate::formats::kafka::options::KafkaReadOptions;

/// Byte budget per emitted Arrow RecordBatch. Flush when the accumulated
/// variable-length payload (value + key + topic) reaches this, regardless of row
/// count, so no Utf8/Binary column can approach Arrow's i32 `OffsetBuffer` limit
/// (2 GiB). 128 MiB keeps batches small for cache locality while bounding the
/// 2 GiB array risk by ~16x headroom — the byte-driven analogue of DataFusion's
/// row-based `batch_size`.
const MAX_BATCH_BYTES: usize = 128 * 1024 * 1024;

/// Runtime-tunable source batch byte cap (streaming-RSS lever: 16 readers × this = source in-flight;
/// smaller = less RSS, possibly more per-batch overhead). Default `MAX_BATCH_BYTES`; floor 1 MiB to keep
/// the i32-offset safety. `VAJRA_SOURCE_MAX_BATCH_BYTES`. docs/design/streaming-memory-boundedness.md.
fn max_batch_bytes() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("VAJRA_SOURCE_MAX_BATCH_BYTES")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n >= 1024 * 1024)
            .unwrap_or(MAX_BATCH_BYTES)
    })
}

/// Wall-clock stall tolerance (ms) for a bounded read: once `remaining > 0` but no message
/// has arrived for this long, the residual offsets are deemed unreachable (log compaction /
/// aborted-txn control records inflating the high watermark) and the read completes. Derived
/// into a consecutive-empty-poll budget from `fetch_timeout_ms`, so the tolerance is the same
/// wall time regardless of poll granularity. Large enough that a transient fetch hiccup never
/// ends a read early; small enough to bound the tail wait on genuinely gapped topics.
const BOUNDED_STALL_TOLERANCE_MS: u64 = 10_000;

/// Realtime data-flush cadence (ms) — DECOUPLED from the epoch/commit interval. The
/// realtime source emits accumulated rows this often (no barrier) so records flow with
/// ~ms latency (Spark Real-Time Mode: records on arrival), while epochs/commits stay at
/// the larger `Trigger.Continuous` interval for exactly-once efficiency. At high rates the
/// row/byte cap flushes first, so throughput is unaffected.
const LOW_LATENCY_FLUSH_MS: u64 = 5;

/// Per-instance checkpoint key for the bounded source's offsets. With parallelism 1 we keep
/// the legacy `sources/0/{staged,committed}` keys (back-compat with existing checkpoints); with
/// N>1 each instance uses a disjoint `sources/0/inst-<i>/...` key so the N parallel readers never
/// clobber each other's offsets. The generic commit (`commit_source_offsets`) promotes any
/// `.../staged` → `.../committed`, so per-instance keys need no commit-side change.
fn offset_key(suffix: &str, inst: usize, parallelism: usize) -> String {
    if parallelism <= 1 {
        format!("sources/0/{suffix}")
    } else {
        format!("sources/0/inst-{inst}/{suffix}")
    }
}

/// Read committed per-(topic,partition) offsets for this instance from the checkpoint store
/// (JSON map `"topic:partition" -> next-offset`). Committed staged→committed by the runner
/// after the batch output is durable.
async fn read_committed_offsets(
    ck: &CheckpointStore,
    inst: usize,
    parallelism: usize,
) -> std::collections::HashMap<(String, i32), i64> {
    let Some(bytes) = ck
        .get(&offset_key("committed", inst, parallelism))
        .await
        .ok()
        .flatten()
    else {
        return std::collections::HashMap::new();
    };
    let map: std::collections::BTreeMap<String, i64> =
        serde_json::from_slice(&bytes).unwrap_or_default();
    map.into_iter()
        .filter_map(|(k, v)| {
            let (t, p) = k.rsplit_once(':')?;
            Some(((t.to_string(), p.parse().ok()?), v))
        })
        .collect()
}

/// Stage (write-ahead) the per-(topic,partition) offsets reached by this instance's micro-batch.
async fn write_staged_offsets(
    ck: &CheckpointStore,
    offsets: &std::collections::HashMap<(String, i32), i64>,
    inst: usize,
    parallelism: usize,
) {
    let map: std::collections::BTreeMap<String, i64> = offsets
        .iter()
        .map(|((t, p), o)| (format!("{t}:{p}"), *o))
        .collect();
    if let Ok(body) = serde_json::to_vec(&map) {
        let _ = ck
            .put(
                &offset_key("staged", inst, parallelism),
                bytes::Bytes::from(body),
            )
            .await;
    }
}

/// Realtime (`Trigger.Continuous`) committed checkpoint: the single source-of-truth object the sink
/// writes atomically per epoch (`realtime/committed`, JSON `{epoch, offsets:{"t:p"->next-offset}}`).
/// Both the sink (epoch resume) and the source (offset seek) read it on restart — one atomic object
/// = no torn commit (F4 principle). See docs/design/streaming-realtime-mode.md (F1b).
#[derive(serde::Serialize, serde::Deserialize, Default)]
struct RealtimeCommitted {
    epoch: u64,
    offsets: std::collections::BTreeMap<String, i64>,
}

/// Read the realtime committed record (epoch + per-(topic,partition) offsets), if present.
async fn read_realtime_committed(
    ck: &CheckpointStore,
) -> Option<(u64, std::collections::HashMap<(String, i32), i64>)> {
    let bytes = ck.get("realtime/committed").await.ok().flatten()?;
    let rec: RealtimeCommitted = serde_json::from_slice(&bytes).ok()?;
    let offsets = rec
        .offsets
        .into_iter()
        .filter_map(|(k, v)| {
            let (t, p) = k.rsplit_once(':')?;
            Some(((t.to_string(), p.parse().ok()?), v))
        })
        .collect();
    Some((rec.epoch, offsets))
}

/// Pre-commit (write-ahead) the offsets reached at epoch `epoch` to a per-epoch staged object the
/// sink reads when it commits that epoch's files. Keyed by epoch so a replayed epoch overwrites
/// idempotently and concurrent epochs never clobber each other.
async fn write_staged_epoch_offsets(
    ck: &CheckpointStore,
    inst: usize,
    epoch: u64,
    offsets: &std::collections::HashMap<(String, i32), i64>,
) {
    let map: std::collections::BTreeMap<String, i64> = offsets
        .iter()
        .map(|((t, p), o)| (format!("{t}:{p}"), *o))
        .collect();
    if let Ok(body) = serde_json::to_vec(&map) {
        // T-EO-3: PER-INSTANCE staged key. With N realtime readers each staging its OWN partitions,
        // a single shared key (`sources/0/staged-epoch-<epoch>`) let instances CLOBBER each other
        // (last-writer-wins) so only one partition's offset survived the commit — on restart the other
        // instances re-read from offset 0 (measured as full-window duplicates). Each instance now
        // stages `sources/0/inst-<i>/staged-epoch-<epoch>`; the sink UNIONS them at commit. inst=0 for
        // the single-instance default (unchanged behavior).
        let _ = ck
            .put(
                &format!("sources/0/inst-{inst}/staged-epoch-{epoch}"),
                bytes::Bytes::from(body),
            )
            .await;
    }
}

/// Count the total partitions across the subscribed topics (one source instance per partition —
/// Spark `KafkaSourceRDD` / Flink FLIP-27). Returns `None` if metadata can't be fetched (caller
/// falls back to `target_partitions`). Runs the blocking rdkafka metadata RPC off the async runtime.
async fn count_kafka_partitions(options: &KafkaReadOptions) -> Option<usize> {
    let topics_csv = options.subscribe.clone()?;
    let bootstrap = options.bootstrap_servers.clone();
    let group = options.group_id.clone();
    let extra = options.extra.clone();
    tokio::task::spawn_blocking(move || {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &bootstrap);
        cfg.set("group.id", &group);
        cfg.set("enable.auto.commit", "false");
        for (k, v) in &extra {
            cfg.set(k.as_str(), v.as_str());
        }
        let consumer: StreamConsumer = cfg.create().ok()?;
        let timeout = Duration::from_secs(30);
        let mut total = 0usize;
        for topic in topics_csv
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
        {
            let md = consumer.fetch_metadata(Some(topic), timeout).ok()?;
            let t = md.topics().iter().find(|t| t.name() == topic)?;
            total += t.partitions().len();
        }
        (total > 0).then_some(total)
    })
    .await
    .ok()
    .flatten()
}

/// Apply high-throughput consumer defaults (librdkafka tuning) BEFORE user `kafka.*` options, so
/// users can still override. Vajra's bounded catch-up read needs to saturate the broker (Flink
/// reaches ~10M ev/s via aggressive fetch settings; rdkafka defaults prefetch too little). These
/// raise the prefetch queue + per-fetch byte budget + socket buffers and shorten the fetch wait.
/// See docs/STREAMING_ARCHITECTURE.md P0 throughput.
/// Consumer context that logs librdkafka's per-partition fetch-queue (PREFETCH) size — the C-side buffer
/// that is the dominant streaming-RSS driver (invisible to the allocator + DataFusion MemoryPool). Enabled
/// by `VAJRA_KAFKA_STATS` (sets `statistics.interval.ms`); prod-grade observability (Flink-equivalent
/// consumer metrics) + DIRECT proof of the prefetch memory. docs/design/streaming-memory-boundedness.md.
#[derive(Clone, Default)]
struct KafkaStatsContext;
impl ClientContext for KafkaStatsContext {
    fn stats(&self, s: Statistics) {
        let (mut bytes, mut msgs, mut lag) = (0i64, 0i64, 0i64);
        for t in s.topics.values() {
            for p in t.partitions.values() {
                bytes += p.fetchq_size as i64;
                msgs += p.fetchq_cnt;
                if p.consumer_lag >= 0 {
                    lag += p.consumer_lag;
                }
            }
        }
        log::info!(
            "KAFKA_STATS prefetch_bytes={bytes} prefetch_gib={:.3} prefetch_msgs={msgs} lag={lag}",
            bytes as f64 / 1_073_741_824.0
        );
    }
}
impl ConsumerContext for KafkaStatsContext {}

fn apply_consumer_throughput_defaults(cfg: &mut ClientConfig) {
    if std::env::var("VAJRA_KAFKA_STATS").is_ok() {
        cfg.set("statistics.interval.ms", "2000"); // fire KafkaStatsContext::stats every 2s
    }
    // librdkafka PREFETCH QUEUE = the dominant streaming-RSS driver (measured 2026-07-01): this C-side
    // buffer is invisible to the DataFusion MemoryPool + the Rust allocator, so it's why the bounded pool
    // was bypassed and the RSS exceeded Flink's. Default was 1 GiB/partition (× 16 parts on EKS = up to
    // 16 GiB) — vastly more than Flink's ~50 MiB total fetch. Bound it (env-tunable) to a Flink-comparable
    // per-partition budget so RSS ≈ prefetch×partitions, trading a little prefetch depth for bounded
    // memory. `VAJRA_KAFKA_PREFETCH_KBYTES` (default 65536 = 64 MiB/partition; set 1048576 for the old
    // 1 GiB throughput-max). docs/design/streaming-memory-boundedness.md.
    let prefetch_kbytes = std::env::var("VAJRA_KAFKA_PREFETCH_KBYTES")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&n| n >= 1024)
        .unwrap_or(65536); // 64 MiB/partition (was 1 GiB) — DEFENSIVE bound. MEASURED EKS 2026-07-02: for a
                           // fast consumer the prefetch queue stays ~44MB regardless of this cap (RSS flat
                           // 7.14/7.32/7.33 across 1GiB/256/64MiB), so this is NOT the memory driver — it
                           // only bounds the WORST case if a consumer falls far behind. Keep bounded +
                           // env-tunable (VAJRA_KAFKA_STATS logs the actual fetchq_size for observability).
    let prefetch_msgs = std::env::var("VAJRA_KAFKA_PREFETCH_MSGS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&n| n >= 1000)
        .unwrap_or(400_000); // was 1M/partition
    cfg.set("queued.max.messages.kbytes", prefetch_kbytes.to_string());
    cfg.set("queued.min.messages", prefetch_msgs.to_string());
    cfg.set("fetch.message.max.bytes", "10485760"); // 10 MiB (matches broker message.max.bytes)
    cfg.set("fetch.wait.max.ms", "50"); // don't idle 500ms waiting to fill a fetch
    cfg.set("socket.receive.buffer.bytes", "16777216"); // 16 MiB socket rx buffer
}

/// Spark-compatible Kafka source schema (7 columns).
pub fn kafka_data_schema() -> Schema {
    Schema::new(vec![
        Field::new("key", DataType::Binary, true),
        Field::new("value", DataType::Binary, true),
        Field::new("topic", DataType::Utf8, false),
        Field::new("partition", DataType::Int32, false),
        Field::new("offset", DataType::Int64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("timestampType", DataType::Int32, false),
    ])
}

// ---------------------------------------------------------------------------
// KafkaStreamSource
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct KafkaStreamSource {
    options: KafkaReadOptions,
    schema: SchemaRef,
}

impl KafkaStreamSource {
    pub fn new(options: KafkaReadOptions) -> Self {
        Self {
            options,
            schema: Arc::new(kafka_data_schema()),
        }
    }
}

#[async_trait]
impl StreamSource for KafkaStreamSource {
    fn data_schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
        // `bounded` (availableNow/once, or each continuous re-plan micro-batch): read only
        // `[committed_offset, current_end_offset)` per partition, then `EndOfData`.
        bounded: bool,
        // With a checkpoint location, per-(topic,partition) offsets are committed/restored via the
        // CheckpointStore for exactly-once recovery (Spark `KafkaMicroBatchStream` model).
        checkpoint_location: Option<&str>,
        realtime_interval_ms: Option<u64>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields.len()).collect());
        // Parallel source read for the bounded (availableNow / micro-batch) path: ONE execution
        // instance per Kafka partition (Spark `KafkaSourceRDD` one-task-per-TopicPartition / Flink
        // FLIP-27 one-split-per-partition). This is REQUIRED for event-time correctness: an
        // instance reading multiple partitions interleaves their records out of event-time order,
        // so the per-instance `max` watermark races past a slower partition's data and the keyed
        // window operator drops it as "late" (measured ~7% loss at 16 partitions / 8 instances).
        // One partition per instance keeps each instance's stream event-time ordered, so its
        // watermark is monotone and the downstream MIN-merge is correct.
        //
        // BOTH bounded (availableNow) AND realtime/continuous now default to N = one reader per Kafka
        // partition (FLIP-27). The realtime multi-instance path is now exactly-once complete: T-EO-1
        // per-instance split assignment, T-EO-3 per-instance staged offsets + sink union commit, and
        // T-EO-3.5 exchange watermark idleness + all-idle drain-to-max. Validated: correctness_gate C6
        // (no-dup + complete, 1800 rows / 6 windows, 3/3) + C7 (EO across hard kill -9, 2/2). See
        // docs/design/continuous-stateful-eo-fix.md. `VAJRA_RT_SINGLE` opts out to the legacy
        // single-instance realtime reader (NOT EO-complete for multi-partition) as a safety escape.
        // EPIC-C: the parallel N-reader path (one reader per Kafka partition) is the proven realtime
        // path (EO-complete, correctness_gate C6/C7). The `VAJRA_RT_SINGLE` opt-out to the legacy
        // single-instance reader (NOT EO-complete for multi-partition) was an unproven escape hatch —
        // removed. Always partition-count parallelism.
        let parallelism = count_kafka_partitions(&self.options)
            .await
            .unwrap_or_else(|| state.config().target_partitions())
            .max(1);
        Ok(Arc::new(KafkaSourceExec::try_new(
            self.options.clone(),
            Arc::clone(&self.schema),
            projection,
            bounded,
            checkpoint_location.map(str::to_string),
            realtime_interval_ms,
            parallelism,
        )?))
    }
}

// ---------------------------------------------------------------------------
// KafkaSourceExec
// ---------------------------------------------------------------------------

/// VAJ-T7 source-fusion spec: parse the Kafka `value` column into a single struct column
/// **in-source**, so the raw `value:Binary` column is never materialized past batch build and the
/// downstream `from_json` projection + its `CAST` are elided (REFERENCES §6, columnar
/// end-to-end). Set by the `fuse_streaming_source_parse` physical-optimizer rule when a
/// `from_json(CAST(value AS string), schema).alias(name)` projection sits directly over the source.
/// The emitted batch has exactly one column, `output_field: Struct(fields)`.
#[derive(Debug, Clone)]
pub struct ValueParseSpec {
    /// Output struct column name = the elided projection's alias (e.g. `e`).
    pub output_field: String,
    /// The target struct fields (read off the `from_json` physical expr's resolved return type —
    /// no schema-string re-parse).
    pub fields: Fields,
    /// Spark timestamp/date format options carried from the original `from_json` call.
    pub options: SparkFromJsonOptions,
}

impl ValueParseSpec {
    /// Fused output DATA schema: `source_schema` with the `value:Binary` field replaced IN PLACE by
    /// `output_field: Struct(fields)`, every other projected column (e.g. `partition`) kept as-is.
    /// This equals the dropped `from_json` projection's data output (marker/retracted are added by
    /// flow-event encoding downstream, not here).
    fn output_data_schema(&self, source_schema: &Schema) -> Result<SchemaRef> {
        let idx = source_schema
            .index_of("value")
            .map_err(|e| arrow_datafusion_err!(e))?;
        let mut fields: Vec<Arc<Field>> = source_schema.fields().iter().cloned().collect();
        fields[idx] = Arc::new(Field::new(
            &self.output_field,
            DataType::Struct(self.fields.clone()),
            true,
        ));
        Ok(Arc::new(Schema::new(fields)))
    }

    /// Parse the `value` column of `batch` into a struct column IN PLACE, keeping all other columns
    /// (position + values unchanged). `batch` is the raw source batch (projected to include `value`
    /// plus whatever other columns the query keeps, e.g. `partition`).
    fn fuse(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let idx = batch
            .schema()
            .index_of("value")
            .map_err(|_| exec_datafusion_err!("VAJ-T7 fusion: source batch missing `value` column"))?;
        let values = batch
            .column(idx)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| exec_datafusion_err!("VAJ-T7 fusion: `value` column is not Binary"))?;
        let struct_arr = parse_json_binary_to_struct(values, &self.fields, &self.options, "UTC")?;
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns[idx] = Arc::new(struct_arr);
        RecordBatch::try_new(self.output_data_schema(batch.schema().as_ref())?, columns)
            .map_err(|e| arrow_datafusion_err!(e))
    }

    /// Fuse a data [`FlowEvent`] (parse its `value` column → the struct column); markers pass
    /// through untouched. Row count is preserved, so the `retracted` mask stays valid.
    fn fuse_event(&self, ev: FlowEvent) -> Result<FlowEvent> {
        match ev {
            FlowEvent::Data { batch, retracted } => Ok(FlowEvent::Data {
                batch: self.fuse(&batch)?,
                retracted,
            }),
            marker => Ok(marker),
        }
    }
}

/// Wrap a source event stream so that, when VAJ-T7 fusion is enabled, every data batch is parsed
/// `value` → struct in-source (markers untouched). No-op when `spec` is `None` — the reader's
/// non-fused behaviour is byte-identical to before.
fn fuse_event_stream(
    events: impl futures::Stream<Item = Result<FlowEvent>>,
    spec: Option<ValueParseSpec>,
) -> impl futures::Stream<Item = Result<FlowEvent>> {
    events.map(move |ev| match (&spec, ev) {
        (Some(s), Ok(fe)) => s.fuse_event(fe),
        (_, other) => other,
    })
}

#[derive(Debug)]
pub struct KafkaSourceExec {
    options: KafkaReadOptions,
    original_schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Vec<usize>,
    /// When set (VAJ-T7), parse `value` -> a single struct column in-source; see [`ValueParseSpec`].
    parse_value_as: Option<ValueParseSpec>,
    /// Bounded micro-batch read (availableNow/once or each continuous re-plan): read up to the
    /// current end offsets, then `EndOfData`.
    bounded: bool,
    /// Streaming `checkpointLocation`, when set — restore/stage per-partition offsets for EO.
    checkpoint_location: Option<String>,
    /// `Trigger.Continuous` epoch interval (millis), when set — realtime EO: seek to the committed
    /// offset on start, emit `Checkpoint{epoch}` + pre-commit per-epoch offsets at this cadence.
    realtime_interval_ms: Option<u64>,
    /// Source read parallelism: the number of execution partitions. Each instance `i`
    /// owns the Kafka partitions whose stable global index `% parallelism == i` (Spark
    /// `KafkaSourceRDD` one-task-per-partition / Flink FLIP-27 split assignment), giving
    /// parallel read + `from_json` parsing. The bounded (availableNow / micro-batch) path
    /// uses this; the realtime continuous path stays at 1 (per-epoch barrier coordination
    /// across N instances is handled separately). Each instance stages its offsets under a
    /// per-instance checkpoint key for exactly-once.
    parallelism: usize,
    properties: Arc<PlanProperties>,
}

impl KafkaSourceExec {
    pub fn try_new(
        options: KafkaReadOptions,
        schema: SchemaRef,
        projection: Vec<usize>,
        bounded: bool,
        checkpoint_location: Option<String>,
        realtime_interval_ms: Option<u64>,
        parallelism: usize,
    ) -> Result<Self> {
        let projected_schema = Arc::new(schema.project(&projection)?);
        let output_schema = Arc::new(to_flow_event_schema(&projected_schema));
        let boundedness = if bounded {
            Boundedness::Bounded
        } else {
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            }
        };
        let parallelism = parallelism.max(1);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(parallelism),
            EmissionType::Both,
            boundedness,
        ));
        Ok(Self {
            options,
            original_schema: schema,
            projected_schema,
            projection,
            bounded,
            checkpoint_location,
            realtime_interval_ms,
            parallelism,
            parse_value_as: None,
            properties,
        })
    }

    /// VAJ-T7 — enable in-source `value` parse fusion. Keeps the existing read projection (which
    /// already includes `value` plus any other kept columns, e.g. `partition`), and sets the plan's
    /// output to that projected schema with `value` replaced in place by the parsed struct column
    /// (flow-event wrapped). Records the [`ValueParseSpec`] used at execute time. The optimizer rule
    /// calls this after verifying a `from_json(value)` projection sits directly over the source and
    /// that the projection's other outputs are passthrough columns (so it can be dropped).
    pub fn with_parse_value_as(mut self, spec: ValueParseSpec) -> Result<Self> {
        // `value` must be among the projected columns (it is — the from_json projection needs it).
        if self.projected_schema.index_of("value").is_err() {
            return plan_err!(
                "VAJ-T7 fusion requires `value` in the source projection, got {:?}",
                self.projected_schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
            );
        }
        let output_schema = Arc::new(to_flow_event_schema(
            spec.output_data_schema(self.projected_schema.as_ref())?.as_ref(),
        ));
        let boundedness = if self.bounded {
            Boundedness::Bounded
        } else {
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            }
        };
        self.properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(self.parallelism),
            EmissionType::Both,
            boundedness,
        ));
        self.parse_value_as = Some(spec);
        Ok(self)
    }

    /// The in-source parse-fusion spec, when VAJ-T7 fusion is enabled.
    pub fn parse_value_as(&self) -> Option<&ValueParseSpec> {
        self.parse_value_as.as_ref()
    }

    /// Source read parallelism (number of execution partitions).
    pub fn parallelism(&self) -> usize {
        self.parallelism
    }

    pub fn options(&self) -> &KafkaReadOptions {
        &self.options
    }

    pub fn original_schema(&self) -> &SchemaRef {
        &self.original_schema
    }

    pub fn projection(&self) -> &[usize] {
        &self.projection
    }

    pub fn bounded(&self) -> bool {
        self.bounded
    }

    pub fn checkpoint_location(&self) -> Option<&str> {
        self.checkpoint_location.as_deref()
    }

    pub fn realtime_interval_ms(&self) -> Option<u64> {
        self.realtime_interval_ms
    }
}

impl DisplayAs for KafkaSourceExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "{}: bootstrap_servers={}, subscribe={:?}",
            self.name(),
            self.options.bootstrap_servers,
            self.options.subscribe,
        )
    }
}

impl ExecutionPlan for KafkaSourceExec {
    fn name(&self) -> &str {
        Self::static_name()
    }


    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            plan_err!("{} cannot have children", self.name())
        } else {
            Ok(self)
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let parallelism = self.parallelism.max(1);
        if partition >= parallelism {
            return plan_err!(
                "{} partition {partition} out of range (parallelism {parallelism})",
                self.name()
            );
        }
        let inst = partition; // this execution instance's index

        let options = self.options.clone();
        let projection = self.projection.clone();
        let full_schema = Arc::new(kafka_data_schema());
        let projected_schema = self.projected_schema.clone();
        // VAJ-T7 source-fusion: when set, the generator emits raw `[value:Binary]` batches
        // (projected_schema is `[value]`) and `fuse_event_stream` parses each to the single
        // `[output_field:Struct]` column; `emit_schema` is what the plan/adapter declare.
        let parse_spec = self.parse_value_as.clone();
        let emit_schema = match parse_spec.as_ref() {
            Some(s) => s.output_data_schema(projected_schema.as_ref())?,
            None => projected_schema.clone(),
        };
        let max_batch = options.max_batch_size;
        // Per-poll fetch timeout (small -> responsive batching / low latency).
        let timeout = Duration::from_millis(options.fetch_timeout_ms);
        // Broker control-plane calls (metadata, watermarks, committed offsets) are
        // request/response round-trips, NOT data polls — they must NOT inherit the small
        // poll timeout (at e.g. 10ms they'd spuriously fail before any data is read). Use a
        // generous fixed floor so these never flake, independent of poll granularity.
        let meta_timeout = timeout.max(Duration::from_secs(30));

        // Bounded micro-batch read with exactly-once offsets (Spark `KafkaMicroBatchStream` model):
        // assign + seek to committed (or earliest/latest) start offsets, read up to the current end
        // offsets, stage the offsets reached, then `EndOfData`. The runner commits staged→committed
        // (via CheckpointStore.promote) after the output is durable.
        if self.bounded {
            let checkpoint_location = self.checkpoint_location.clone();
            let events = async_stream::stream! {
                let mut cfg = ClientConfig::new();
                cfg.set("bootstrap.servers", &options.bootstrap_servers);
                cfg.set("group.id", &options.group_id);
                cfg.set("enable.auto.commit", "false");
                apply_consumer_throughput_defaults(&mut cfg);
                for (k, v) in &options.extra { cfg.set(k.as_str(), v.as_str()); }
                // Stats context logs the librdkafka prefetch queue bytes (VAJRA_KAFKA_STATS) — direct
                // measurement of the streaming-RSS driver. No overhead when stats disabled (no interval).
                let consumer: StreamConsumer<KafkaStatsContext> =
                    match cfg.create_with_context(KafkaStatsContext) {
                    Ok(c) => c,
                    Err(e) => { yield Err(exec_datafusion_err!("failed to create Kafka consumer: {e}")); return; }
                };
                let Some(topics_csv) = options.subscribe.clone() else {
                    yield Err(exec_datafusion_err!("bounded/exactly-once Kafka read requires `subscribe` (explicit topics)"));
                    return;
                };
                let topics: Vec<String> = topics_csv.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();

                // Restore committed per-(topic,partition) offsets, if any.
                let ck = checkpoint_location.as_deref().and_then(|l| CheckpointStore::from_location(l).ok());
                let committed: std::collections::HashMap<(String, i32), i64> = match &ck {
                    Some(ck) => read_committed_offsets(ck, inst, parallelism).await,
                    None => std::collections::HashMap::new(),
                };
                let earliest = options.starting_offsets.eq_ignore_ascii_case("earliest");

                // Resolve assignments (topic, partition, start offset, end=high watermark) in a SYNC
                // step that returns owned data — so rdkafka's non-Send `Metadata` is never held
                // across an await/yield in this stream.
                //
                // Parallel-source assignment: collect ALL (topic, partition) pairs, sort into a
                // stable global order, and keep only those whose global index `% parallelism == inst`
                // (Spark KafkaSourceRDD one-task-per-partition / Flink FLIP-27 round-robin split
                // assignment). Deterministic, so on restart instance `i` resumes exactly its own
                // partitions (its per-instance committed offsets).
                let resolve = || -> std::result::Result<Vec<(String, i32, i64, i64)>, String> {
                    let mut pairs: Vec<(String, i32)> = vec![];
                    for topic in &topics {
                        let md = consumer
                            .fetch_metadata(Some(topic), meta_timeout)
                            .map_err(|e| format!("fetch_metadata({topic}): {e}"))?;
                        let Some(t) = md.topics().iter().find(|t| t.name() == topic) else { continue };
                        for p in t.partitions() {
                            pairs.push((topic.clone(), p.id()));
                        }
                    }
                    pairs.sort();
                    let mut out = vec![];
                    for (g, (topic, part)) in pairs.into_iter().enumerate() {
                        if g % parallelism != inst {
                            continue; // owned by another instance
                        }
                        let (low, high) = consumer
                            .fetch_watermarks(&topic, part, meta_timeout)
                            .map_err(|e| format!("fetch_watermarks({topic},{part}): {e}"))?;
                        let start = committed
                            .get(&(topic.clone(), part))
                            .copied()
                            .unwrap_or(if earliest { low } else { high });
                        out.push((topic, part, start, high));
                    }
                    Ok(out)
                };
                let assignments = match resolve() {
                    Ok(a) => a,
                    Err(e) => { yield Err(exec_datafusion_err!("Kafka {e}")); return; }
                };
                let mut tpl = rdkafka::TopicPartitionList::new();
                // Hot-path offset state keyed by a packed (topic-idx, partition) u64; topics interned
                // once. The durable (topic, partition) commit format is rebuilt at staging.
                let mut topic_names: Vec<String> = Vec::new();
                let mut idx_of: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
                let mut ends: std::collections::HashMap<u64, i64> = std::collections::HashMap::new();
                let mut next: std::collections::HashMap<u64, i64> = std::collections::HashMap::new();
                for (topic, part, start, high) in &assignments {
                    if let Err(e) = tpl.add_partition_offset(topic, *part, rdkafka::Offset::Offset(*start)) {
                        yield Err(exec_datafusion_err!("Kafka assign({topic},{part}@{start}): {e}")); return;
                    }
                    let idx = *idx_of.entry(topic.clone()).or_insert_with(|| {
                        topic_names.push(topic.clone());
                        (topic_names.len() - 1) as u32
                    });
                    let k = pack_tp(idx, *part);
                    ends.insert(k, *high);
                    next.insert(k, *start);
                }

                // FLIP-27 batch-queue path (gated VAJRA_KAFKA_BATCH_QUEUE): spawn one dedicated
                // reader thread per split, each draining via rd_kafka_consume_batch_queue (2.8x the
                // StreamConsumer per-message stream, measured). The async generator only wraps the
                // received batches in FlowEvents + preserves the EO offset-staging + EndOfData
                // contract — identical semantics, faster consume. The StreamConsumer above is used
                // only for metadata/watermark resolution here (cheap control-plane calls).
                if batch_queue_enabled() {
                    let (btx, mut brx) = tokio::sync::mpsc::channel::<ReaderEvent>(8);
                    for (topic, part, start, high) in &assignments {
                        let Some(&idx) = idx_of.get(topic) else { continue };
                        let mut jcfg = ClientConfig::new();
                        jcfg.set("bootstrap.servers", &options.bootstrap_servers);
                        jcfg.set("group.id", &options.group_id);
                        jcfg.set("enable.auto.commit", "false");
                        apply_consumer_throughput_defaults(&mut jcfg);
                        for (k, v) in &options.extra { jcfg.set(k.as_str(), v.as_str()); }
                        spawn_partition_batch_reader(PartitionReaderJob {
                            cfg: jcfg,
                            topic: topic.clone(),
                            partition: *part,
                            start: *start,
                            end: *high,
                            key: pack_tp(idx, *part),
                            target_batch: max_batch,
                            max_bytes: max_batch_bytes(),
                            full_schema: Arc::clone(&full_schema),
                            projection: projection.clone(),
                            timeout_ms: options.fetch_timeout_ms.max(1) as i32,
                            max_empty_polls: ((BOUNDED_STALL_TOLERANCE_MS / options.fetch_timeout_ms.max(1)).max(5)) as u32,
                            emit_idle: false,
                        }, btx.clone());
                    }
                    drop(btx); // so the channel closes once every reader thread finishes
                    while let Some(item) = brx.recv().await {
                        match item {
                            ReaderEvent::Batch(pb) => {
                                next.insert(pb.key, pb.next_off);
                                yield Ok(FlowEvent::append_only_data(pb.batch));
                            }
                            ReaderEvent::Idle => {} // not emitted in bounded mode (emit_idle=false)
                            ReaderEvent::Err(e) => { yield Err(exec_datafusion_err!("Kafka batch-reader: {e}")); return; }
                        }
                    }
                    // Stage the offsets reached (write-ahead) — identical to the StreamConsumer path.
                    if let Some(ck) = &ck {
                        let durable: std::collections::HashMap<(String, i32), i64> = next
                            .iter()
                            .map(|(&k, &o)| { let (idx, part) = unpack_tp(k); ((topic_names[idx as usize].clone(), part), o) })
                            .collect();
                        write_staged_offsets(ck, &durable, inst, parallelism).await;
                    }
                    yield Ok(FlowEvent::Marker(sail_common_datafusion::streaming::event::marker::FlowMarker::EndOfData));
                    return;
                }

                if let Err(e) = consumer.assign(&tpl) {
                    yield Err(exec_datafusion_err!("Kafka assign: {e}")); return;
                }

                // Read until every partition reaches its end offset (or no more messages).
                let remaining = |next: &std::collections::HashMap<u64, i64>| -> i64 {
                    ends.iter().map(|(k, e)| (e - next.get(k).copied().unwrap_or(*e)).max(0)).sum()
                };
                // Last-seen topic cache: resolve the interned idx in the hot loop without a per-message
                // String alloc (str compare; single-topic = always a hit).
                let mut last_topic = String::new();
                let mut last_idx: u32 = 0;
                let mut msg_stream = consumer.stream();
                // A bounded (availableNow / micro-batch) read MUST reach each partition's
                // captured end offset — that snapshot is exactly what Spark's
                // `KafkaMicroBatchStream` and Flink's bounded Kafka source read to. A
                // transient fetch timeout (slow broker, cross-node fetch latency, rebalance)
                // is NOT end-of-data: treating one empty poll as "done" silently under-reads
                // (measured ~5% short at 100M on EKS). So we KEEP polling while `remaining > 0`
                // and only conclude the residual offsets are unreachable after a bounded
                // wall-clock stall with zero progress (covers genuine offset gaps from log
                // compaction / aborted-txn control records, so we never hang nor under-read).
                let timeout_ms = options.fetch_timeout_ms.max(1);
                let max_empty_polls: u32 =
                    ((BOUNDED_STALL_TOLERANCE_MS / timeout_ms).max(5)) as u32;
                let mut empty_polls: u32 = 0;
                // Drain librdkafka's buffered messages without arming a per-message timer
                // (the throughput path). `VAJRA_KAFKA_LEGACY_POLL=1` forces the old
                // one-timer-per-message poll + small batch — a kill-switch / A-B measurement lever.
                let fast_drain =
                    std::env::var("VAJRA_KAFKA_LEGACY_POLL").ok().as_deref() != Some("1");
                // Arrow batch row target. MEASURED 2026-06-21 (EKS 100M): bumping this to 128 Ki
                // for the catch-up read did NOT help wall time (44s vs 41.8s) and raised peak
                // memory (2.4 GiB vs 1.3 GiB) — per-batch operator overhead is not the bottleneck
                // (that's the Kafka consumer read path; see docs/STREAMING_ARCHITECTURE.md P0). So
                // keep the configured size (DataFusion default 8 Ki), bounded by the byte cap below.
                let target_batch = max_batch;
                while remaining(&next) > 0 {
                    // Throughput attribution (VAJRA_WM_PROF): time the per-batch read (poll + append +
                    // finish) so the EKS profile pinpoints the source-read share. Zero cost when unset.
                    let _rd = sail_common_datafusion::streaming::event::encoding::wm_prof_enabled()
                        .then(std::time::Instant::now);
                    let mut builders = KafkaArrowBuilders::with_capacity(target_batch, &projection);
                    // Flush on EITHER a row cap OR a byte cap. The byte cap is the
                    // real safety guarantee: Arrow Utf8/Binary use i32 offsets (2 GiB
                    // per array), and the overflow is byte-driven, so a row count alone
                    // is too coarse (e.g. 262k rows x 8 KiB = 2 GiB). Bounding bytes keeps
                    // every variable-length column safely under the i32 limit regardless
                    // of payload size — matching how Arrow/DataFusion size-bound batches.
                    let mut batch_bytes: usize = 0;
                    let mut stream_ended = false;
                    let (mut poll_ns, mut build_ns) = (0u64, 0u64); // source_read split (poll vs build)
                    while builders.len() < target_batch
                        && batch_bytes < max_batch_bytes()
                        && remaining(&next) > 0
                    {
                        // Fast path: take a message librdkafka's fetch thread already buffered,
                        // WITHOUT arming a per-message tokio timer (at 1e8 msgs the timer
                        // registration dominates CPU). Only when nothing is buffered do we either
                        // flush the rows we have or — if empty — await with the poll timeout, which
                        // drives stall detection / EndOfData.
                        let _tp = _rd.map(|_| std::time::Instant::now());
                        let msg_opt = if fast_drain {
                            match msg_stream.next().now_or_never() {
                                Some(item) => item,
                                None => {
                                    if builders.len() > 0 {
                                        break; // buffer drained: flush this batch, don't block
                                    }
                                    match tokio::time::timeout(timeout, msg_stream.next()).await {
                                        Ok(item) => item,
                                        Err(_) => break, // transient poll timeout: outer stall budget
                                    }
                                }
                            }
                        } else {
                            // Legacy: arm a fresh timeout per message (kill-switch / A-B baseline).
                            match tokio::time::timeout(timeout, msg_stream.next()).await {
                                Ok(item) => item,
                                Err(_) => break,
                            }
                        };
                        if let Some(t) = _tp {
                            poll_ns = poll_ns.saturating_add(t.elapsed().as_nanos() as u64);
                        }
                        match msg_opt {
                            Some(Ok(msg)) => {
                                empty_polls = 0; // made progress -> reset the stall budget
                                let part = msg.partition();
                                let topic = msg.topic();
                                // Resolve the interned topic idx without a per-message String alloc:
                                // librdkafka returns the same topic repeatedly, so a str compare to
                                // last-seen is the common path (single-topic = always a hit).
                                if topic != last_topic {
                                    let Some(&i) = idx_of.get(topic) else { continue };
                                    last_idx = i;
                                    last_topic.clear();
                                    last_topic.push_str(topic);
                                }
                                let k = pack_tp(last_idx, part);
                                let end = ends.get(&k).copied().unwrap_or(i64::MIN);
                                let off = msg.offset();
                                if off >= end { continue; } // past this batch's snapshot
                                let (ts_ms, ts_type) = match msg.timestamp() {
                                    Timestamp::NotAvailable => (-1i64, -1i32),
                                    Timestamp::CreateTime(ms) => (ms, 0i32),
                                    Timestamp::LogAppendTime(ms) => (ms, 1i32),
                                };
                                let key = msg.key();
                                let value = msg.payload();
                                batch_bytes += value.map_or(0, |v| v.len())
                                    + key.map_or(0, |k| k.len())
                                    + topic.len();
                                // Append borrowed bytes straight into the Arrow buffers (no to_vec).
                                let _tb = _rd.map(|_| std::time::Instant::now());
                                builders.append(key, value, topic, part, off, ts_ms, ts_type);
                                if let Some(t) = _tb {
                                    build_ns = build_ns.saturating_add(t.elapsed().as_nanos() as u64);
                                }
                                next.insert(k, off + 1);
                            }
                            Some(Err(e)) => { yield Err(exec_datafusion_err!("Kafka error: {e}")); return; }
                            None => { stream_ended = true; break; } // consumer stream closed
                        }
                    }
                    if builders.len() > 0 {
                        if let Some(t) = _rd {
                            use sail_common_datafusion::streaming::event::encoding as prof;
                            prof::prof_add(&prof::SOURCE_READ_NS, t.elapsed().as_nanos() as u64);
                            prof::prof_add(&prof::SOURCE_POLL_NS, poll_ns);
                            prof::prof_add(&prof::SOURCE_BUILD_NS, build_ns);
                        }
                        match builders.finish_projected(&full_schema, &projection) {
                            Ok(batch) => yield Ok(FlowEvent::append_only_data(batch)),
                            Err(e) => { log::error!("kafka source finish_projected error: {e}"); yield Err(e); return; }
                        }
                        continue;
                    }
                    // Empty batch with `remaining > 0`: either the consumer stream closed,
                    // or a poll timed out before any data arrived. The latter is transient —
                    // retry until we either make progress or exhaust the stall budget, so a
                    // momentary fetch hiccup never prematurely ends a bounded read.
                    if stream_ended {
                        break;
                    }
                    empty_polls += 1;
                    if empty_polls >= max_empty_polls {
                        break; // genuinely stalled: residual offsets are unreachable (gaps)
                    }
                }

                // Stage the offsets actually reached (write-ahead); runner commits after durable.
                if let Some(ck) = &ck {
                    // Rebuild the durable (topic, partition) -> offset commit format from packed keys.
                    let durable: std::collections::HashMap<(String, i32), i64> = next
                        .iter()
                        .map(|(&k, &o)| {
                            let (idx, part) = unpack_tp(k);
                            ((topic_names[idx as usize].clone(), part), o)
                        })
                        .collect();
                    write_staged_offsets(ck, &durable, inst, parallelism).await;
                }
                yield Ok(FlowEvent::Marker(sail_common_datafusion::streaming::event::marker::FlowMarker::EndOfData));
            };
            let events = fuse_event_stream(events, parse_spec);
            let stream = Box::pin(FlowEventStreamAdapter::new(emit_schema, events));
            return Ok(Box::pin(EncodedFlowEventStream::new(stream)));
        }

        // Realtime (`Trigger.Continuous`) exactly-once path: one long-lived pipeline that
        // `assign`s + `seek`s to the committed offset on start (NOT broker auto-commit), reads
        // continuously, and every `realtime_interval_ms` emits a `Checkpoint{epoch}` barrier — after
        // pre-committing that epoch's reached offsets to `sources/0/staged-epoch-<epoch>`. The
        // barrier flows in-band (never overtaking data, Flink invariant); the realtime sink commits
        // the epoch's files and the matching offset atomically on the marker. Single-input/stateless
        // ⇒ exactly-once without alignment latency. See docs/design/streaming-realtime-mode.md (F1b).
        if let Some(interval_ms) = self.realtime_interval_ms {
            use sail_common_datafusion::streaming::event::marker::FlowMarker;
            let checkpoint_location = self.checkpoint_location.clone();
            let events = async_stream::stream! {
                let Some(cl) = checkpoint_location else {
                    yield Err(exec_datafusion_err!("realtime (continuous) exactly-once Kafka read requires checkpointLocation")); return;
                };
                let ck = match CheckpointStore::from_location(&cl) {
                    Ok(ck) => ck,
                    Err(e) => { yield Err(exec_datafusion_err!("checkpoint store {cl}: {e}")); return; }
                };
                let Some(topics_csv) = options.subscribe.clone() else {
                    yield Err(exec_datafusion_err!("realtime/exactly-once Kafka read requires `subscribe` (explicit topics)")); return;
                };
                let topics: Vec<String> = topics_csv.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();

                // Restore the committed epoch + offsets (the single atomic record the sink wrote).
                let (mut epoch, committed) = match read_realtime_committed(&ck).await {
                    Some((e, o)) => (e + 1, o),
                    None => (0u64, std::collections::HashMap::new()),
                };

                let mut cfg = ClientConfig::new();
                cfg.set("bootstrap.servers", &options.bootstrap_servers);
                cfg.set("group.id", &options.group_id);
                cfg.set("enable.auto.commit", "false");
                // Native EOF signal: librdkafka emits PartitionEOF exactly when the consumer reaches the
                // partition high-watermark (genuinely caught up = no more data) — the CORRECT, exact
                // source of Flink `WatermarkStatus.IDLE`. A slow-but-behind partition keeps delivering
                // data (never EOF), so it is never mis-marked idle under load (the timeout heuristic's
                // bug that closed windows early → over-emit). New data after EOF simply resumes delivery.
                cfg.set("enable.partition.eof", "true");
                apply_consumer_throughput_defaults(&mut cfg);
                for (k, v) in &options.extra { cfg.set(k.as_str(), v.as_str()); }
                // Stats context logs the librdkafka prefetch queue bytes (VAJRA_KAFKA_STATS) — direct
                // measurement of the streaming-RSS driver. No overhead when stats disabled (no interval).
                let consumer: StreamConsumer<KafkaStatsContext> =
                    match cfg.create_with_context(KafkaStatsContext) {
                    Ok(c) => c,
                    Err(e) => { yield Err(exec_datafusion_err!("failed to create Kafka consumer: {e}")); return; }
                };
                let earliest = options.starting_offsets.eq_ignore_ascii_case("earliest");

                // Resolve start offsets per partition (committed if present, else earliest/latest
                // watermark) in a SYNC step — rdkafka's non-Send `Metadata` never crosses an await.
                // (per-(topic,partition) start offsets for this instance, total partition count).
                type Resolved = (Vec<(String, i32, i64)>, usize);
                let resolve = || -> std::result::Result<Resolved, String> {
                    // FLIP-27 per-instance split assignment (SAME as the bounded path): collect ALL
                    // (topic, partition) pairs, sort into a stable global order, and keep only those
                    // whose global index `% parallelism == inst`. This is REQUIRED for multi-instance
                    // realtime correctness — without it every instance reads every partition (measured
                    // as an N× over-count). parallelism=1 ⇒ instance 0 owns all (single-instance path
                    // unchanged). Each instance thus reads its partitions in event-time order ⇒ monotone
                    // per-instance watermark ⇒ the downstream keyed exchange MIN-merge is exact.
                    // Also returns `pairs.len()` = the TOTAL partition count = the number of offsets a
                    // GLOBALLY-COMPLETE checkpoint must cover (the sink gates its commit on this).
                    let mut pairs: Vec<(String, i32)> = vec![];
                    for topic in &topics {
                        let md = consumer.fetch_metadata(Some(topic), meta_timeout)
                            .map_err(|e| format!("fetch_metadata({topic}): {e}"))?;
                        let Some(t) = md.topics().iter().find(|t| t.name() == topic) else { continue };
                        for p in t.partitions() {
                            pairs.push((topic.clone(), p.id()));
                        }
                    }
                    pairs.sort();
                    let total_partitions = pairs.len();
                    let mut out = vec![];
                    for (g, (topic, part)) in pairs.into_iter().enumerate() {
                        if g % parallelism != inst {
                            continue; // owned by another instance
                        }
                        // Recovery precedence (EO Kafka sink first):
                        //  1. the consumer GROUP's committed offset — for an EO Kafka sink this is
                        //     the records' atomic commit point (sink commits offsets INTO its txn
                        //     via send_offsets_to_transaction); an auto-generated group (file sink)
                        //     has none here, so this is skipped and the next source is used;
                        //  2. the object-store `realtime/committed` record (file-sink EO model);
                        //  3. the earliest/latest watermark (fresh start).
                        let mut one = rdkafka::TopicPartitionList::new();
                        let _ = one.add_partition(&topic, part);
                        let group_off = consumer
                            .committed_offsets(one, meta_timeout)
                            .ok()
                            .and_then(|t| t.find_partition(&topic, part).map(|e| e.offset()))
                            .and_then(|o| match o {
                                rdkafka::Offset::Offset(v) => Some(v),
                                _ => None,
                            });
                        let start = match group_off
                            .or_else(|| committed.get(&(topic.clone(), part)).copied())
                        {
                            Some(o) => o,
                            None => {
                                let (low, high) = consumer.fetch_watermarks(&topic, part, meta_timeout)
                                    .map_err(|e| format!("fetch_watermarks({topic},{part}): {e}"))?;
                                if earliest { low } else { high }
                            }
                        };
                        out.push((topic, part, start));
                    }
                    Ok((out, total_partitions))
                };
                let (assignments, total_partitions) = match resolve() {
                    Ok(a) => a,
                    Err(e) => { yield Err(exec_datafusion_err!("Kafka {e}")); return; }
                };
                // T-EO diagnostic: which (partition@start) this realtime instance owns. Correct
                // FLIP-27 assignment ⇒ every partition owned by EXACTLY ONE instance across the run.
                log::debug!(
                    "realtime source inst={inst}/{parallelism} owns={:?}",
                    assignments.iter().map(|(_, p, s)| (*p, *s)).collect::<Vec<_>>()
                );
                // W3 (distributed-EO): publish the TOTAL partition count so the sink can gate its commit
                // on GLOBAL COMPLETENESS — only advance `realtime/committed` when the offset set covers
                // ALL `total_partitions` (a globally-consistent checkpoint), never a partial one that
                // would drop a partition and re-read it on crash. Idempotent (every instance writes the
                // same value); tiny.
                let _ = ck
                    .put(
                        "sources/0/expected",
                        bytes::Bytes::from(total_partitions.to_string()),
                    )
                    .await;
                let mut tpl = rdkafka::TopicPartitionList::new();
                let mut next: std::collections::HashMap<(String, i32), i64> = std::collections::HashMap::new();
                for (topic, part, start) in &assignments {
                    if let Err(e) = tpl.add_partition_offset(topic, *part, rdkafka::Offset::Offset(*start)) {
                        yield Err(exec_datafusion_err!("Kafka assign({topic},{part}@{start}): {e}")); return;
                    }
                    next.insert((topic.clone(), *part), *start);
                }
                if let Err(e) = consumer.assign(&tpl) {
                    yield Err(exec_datafusion_err!("Kafka assign: {e}")); return;
                }
                // Stage the INITIAL (resume) offsets IMMEDIATELY, before consuming, so the cumulative
                // `realtime/committed` covers EVERY partition from the very first commit. Without this, a
                // reader that crashes before its first epoch tick never staged any offset → it is absent
                // from the committed record → resumes at offset 0 → re-reads its whole partition → the
                // already-committed windows re-emit (measured: 3/16 partitions still hit this after the
                // cumulative fix). Staging the start position means such a reader resumes at exactly where
                // it began (no re-read of uncommitted data). Chandy-Lamport: every input's position is
                // known at every checkpoint, including the initial one.
                write_staged_epoch_offsets(&ck, inst, epoch, &next).await;

                // FLIP-27 batch-queue continuous path (gated VAJRA_KAFKA_BATCH_QUEUE): one dedicated
                // reader thread per split draining rd_kafka_consume_batch_queue (2.8× the StreamConsumer
                // per-message stream, measured). The thread emits row/byte-capped Arrow batches on its
                // poll cadence (= low-latency flush) and signals Idle when caught up to the head. This
                // select! preserves the exact EO contract: epoch barrier stages the offsets RECEIVED so
                // far + emits Checkpoint (biased first = a consistent cut; any un-received batch belongs
                // to epoch+1), and Idle → the downstream watermark-MIN drops this instance (Flink IDLE).
                if batch_queue_enabled() {
                    use sail_common_datafusion::streaming::event::marker::FlowMarker;
                    let (btx, mut brx) = tokio::sync::mpsc::channel::<ReaderEvent>(8);
                    let flush_ms = LOW_LATENCY_FLUSH_MS.min(interval_ms.max(1)).max(1);
                    for (topic, part, start) in &assignments {
                        let mut jcfg = ClientConfig::new();
                        jcfg.set("bootstrap.servers", &options.bootstrap_servers);
                        jcfg.set("group.id", &options.group_id);
                        jcfg.set("enable.auto.commit", "false");
                        apply_consumer_throughput_defaults(&mut jcfg);
                        for (k, v) in &options.extra { jcfg.set(k.as_str(), v.as_str()); }
                        spawn_partition_batch_reader(PartitionReaderJob {
                            cfg: jcfg,
                            topic: topic.clone(),
                            partition: *part,
                            start: *start,
                            end: i64::MAX,          // continuous (unbounded)
                            key: 0,                 // continuous stages by (topic, partition)
                            target_batch: max_batch,
                            max_bytes: max_batch_bytes(),
                            full_schema: Arc::clone(&full_schema),
                            projection: projection.clone(),
                            timeout_ms: flush_ms as i32, // poll cadence = low-latency flush
                            max_empty_polls: 0,     // unused in continuous
                            emit_idle: true,
                        }, btx.clone());
                    }
                    drop(btx);
                    let idle_src = format!("kafka:{inst}");
                    let mut idle_signaled = false;
                    let mut timer = tokio::time::interval(Duration::from_millis(interval_ms.max(1)));
                    timer.tick().await; // discard the immediate first tick
                    loop {
                        tokio::select! {
                            biased;
                            _ = timer.tick() => {
                                write_staged_epoch_offsets(&ck, inst, epoch, &next).await;
                                yield Ok(FlowEvent::Marker(FlowMarker::Checkpoint { id: epoch }));
                                epoch += 1;
                            }
                            ev = brx.recv() => {
                                match ev {
                                    Some(ReaderEvent::Batch(pb)) => {
                                        idle_signaled = false;
                                        next.insert((pb.topic, pb.partition), pb.next_off);
                                        yield Ok(FlowEvent::append_only_data(pb.batch));
                                    }
                                    Some(ReaderEvent::Idle) => {
                                        if !idle_signaled {
                                            idle_signaled = true;
                                            yield Ok(FlowEvent::Marker(FlowMarker::Idle { source: idle_src.clone() }));
                                        }
                                    }
                                    Some(ReaderEvent::Err(e)) => { yield Err(exec_datafusion_err!("Kafka batch-reader: {e}")); return; }
                                    None => break, // all readers ended (unexpected for continuous)
                                }
                            }
                        }
                    }
                    return;
                }

                let mut msg_stream = consumer.stream();
                let mut builders = KafkaArrowBuilders::with_capacity(max_batch, &projection);
                let mut batch_bytes: usize = 0; // byte budget (see MAX_BATCH_BYTES)
                let mut timer = tokio::time::interval(Duration::from_millis(interval_ms.max(1)));
                timer.tick().await; // discard the immediate first tick
                // Low-latency data-flush timer, decoupled from the epoch timer (never coarser
                // than the epoch interval). Emits accumulated rows on arrival for ~ms latency.
                let flush_ms = LOW_LATENCY_FLUSH_MS.min(interval_ms.max(1)).max(1);
                let mut flush_timer = tokio::time::interval(Duration::from_millis(flush_ms));
                flush_timer.tick().await;
                // Source-signaled idleness (Flink WatermarkStatus.IDLE): when a flush interval passes with
                // NO data consumed (genuinely caught up to the partition head), emit ONE `Idle` marker so
                // the downstream N→M merge excludes this partition from the watermark MIN. This replaces
                // the exchange's wall-clock idle inference, which at scale wrongly marked a slow-but-active
                // (unscheduled/backpressured) reader idle → premature window close → over-emit. Any new
                // data clears the idle state (and re-activates the channel downstream).
                // Source-signaled idleness (Flink WatermarkStatus.IDLE) driven by librdkafka PartitionEOF:
                // emit `Idle` ONCE when a partition reaches its high-watermark (caught up = no more data),
                // and re-activate on the next data. Exact — never mis-marks a slow-but-behind partition.
                let idle_src = format!("kafka:{inst}");
                let mut idle_signaled = false;
                // Source-read CPU attribution (VAJRA_WM_PROF): the continuous path is event-driven, so
                // we accumulate the per-record append + batch-build time and flush to SOURCE_READ_NS at
                // each emit (the bounded path times the whole read loop; this is the streaming analog).
                // Without this the continuous-mode profile shows source_read=0, hiding the read cost.
                let prof = sail_common_datafusion::streaming::event::encoding::wm_prof_enabled();
                let mut read_acc: u64 = 0;
                loop {
                    tokio::select! {
                        biased;
                        // Epoch boundary: flush buffered data, then pre-commit offsets + emit barrier.
                        // `biased` + data-flush-first guarantees the marker never overtakes its data.
                        _ = timer.tick() => {
                            if builders.len() > 0 {
                                let b = std::mem::replace(&mut builders, KafkaArrowBuilders::with_capacity(max_batch, &projection));
                                let _f = prof.then(std::time::Instant::now);
                                match b.finish_projected(&full_schema, &projection) {
                                    Ok(batch) => {
                                        if let Some(t) = _f { read_acc += t.elapsed().as_nanos() as u64; }
                                        yield Ok(FlowEvent::append_only_data(batch));
                                    }
                                    Err(e) => { yield Err(e); return; }
                                }
                                batch_bytes = 0;
                            }
                            // Flush accumulated source-read CPU (append + batch-build) once per epoch.
                            if prof && read_acc > 0 {
                                sail_common_datafusion::streaming::event::encoding::prof_add(
                                    &sail_common_datafusion::streaming::event::encoding::SOURCE_READ_NS,
                                    std::mem::take(&mut read_acc),
                                );
                            }
                            write_staged_epoch_offsets(&ck, inst, epoch, &next).await;
                            yield Ok(FlowEvent::Marker(FlowMarker::Checkpoint { id: epoch }));
                            epoch += 1;
                        }
                        // Low-latency flush: emit accumulated rows (no barrier) so records flow
                        // with ~ms latency instead of waiting for the (coarser) epoch tick.
                        _ = flush_timer.tick() => {
                            if builders.len() > 0 {
                                let b = std::mem::replace(&mut builders, KafkaArrowBuilders::with_capacity(max_batch, &projection));
                                match b.finish_projected(&full_schema, &projection) {
                                    Ok(batch) => yield Ok(FlowEvent::append_only_data(batch)),
                                    Err(e) => { yield Err(e); return; }
                                }
                                batch_bytes = 0;
                            }
                        }
                        msg = msg_stream.next() => {
                            match msg {
                                Some(Ok(m)) => {
                                    idle_signaled = false; // data flowing again → active
                                    let (ts_ms, ts_type) = match m.timestamp() {
                                        Timestamp::NotAvailable => (-1i64, -1i32),
                                        Timestamp::CreateTime(ms) => (ms, 0i32),
                                        Timestamp::LogAppendTime(ms) => (ms, 1i32),
                                    };
                                    let topic = m.topic();
                                    let part = m.partition();
                                    let off = m.offset();
                                    let key = m.key();
                                    let value = m.payload();
                                    batch_bytes += value.map_or(0, |v| v.len())
                                        + key.map_or(0, |k| k.len())
                                        + topic.len();
                                    next.insert((topic.to_string(), part), off + 1);
                                    // Append borrowed bytes straight into the Arrow buffers (no to_vec).
                                    let _a = prof.then(std::time::Instant::now);
                                    builders.append(key, value, topic, part, off, ts_ms, ts_type);
                                    if let Some(t) = _a {
                                        read_acc += t.elapsed().as_nanos() as u64;
                                    }
                                    // Flush on row OR byte cap for throughput (epoch still delimits
                                    // the commit; mid-epoch batches just carry data forward). Byte cap
                                    // keeps Utf8/Binary columns under Arrow's i32 offset limit.
                                    if builders.len() >= max_batch || batch_bytes >= max_batch_bytes() {
                                        let b = std::mem::replace(&mut builders, KafkaArrowBuilders::with_capacity(max_batch, &projection));
                                        match b.finish_projected(&full_schema, &projection) {
                                            Ok(batch) => yield Ok(FlowEvent::append_only_data(batch)),
                                            Err(e) => { yield Err(e); return; }
                                        }
                                        batch_bytes = 0;
                                    }
                                }
                                Some(Err(rdkafka::error::KafkaError::PartitionEOF(_))) => {
                                    // Caught up to the partition high-watermark = genuinely idle. Flush any
                                    // buffered rows first (never let the Idle marker overtake its data),
                                    // then signal `Idle` ONCE so the downstream merge excludes this
                                    // partition from the watermark MIN (Flink WatermarkStatus.IDLE).
                                    if builders.len() > 0 {
                                        let b = std::mem::replace(&mut builders, KafkaArrowBuilders::with_capacity(max_batch, &projection));
                                        match b.finish_projected(&full_schema, &projection) {
                                            Ok(batch) => yield Ok(FlowEvent::append_only_data(batch)),
                                            Err(e) => { yield Err(e); return; }
                                        }
                                        batch_bytes = 0;
                                    }
                                    if !idle_signaled {
                                        idle_signaled = true;
                                        yield Ok(FlowEvent::Marker(FlowMarker::Idle { source: idle_src.clone() }));
                                    }
                                }
                                Some(Err(e)) => { yield Err(exec_datafusion_err!("Kafka error: {e}")); return; }
                                None => break, // stream ended (unexpected for continuous)
                            }
                        }
                    }
                }
            };
            let events = fuse_event_stream(events, parse_spec);
            let stream = Box::pin(FlowEventStreamAdapter::new(emit_schema, events));
            return Ok(Box::pin(EncodedFlowEventStream::new(stream)));
        }

        let output = async_stream::stream! {
            // Build rdkafka config.
            let mut cfg = ClientConfig::new();
            cfg.set("bootstrap.servers", &options.bootstrap_servers);
            cfg.set("group.id", &options.group_id);
            cfg.set("enable.auto.commit", "true");
            cfg.set("session.timeout.ms", "6000");
            apply_consumer_throughput_defaults(&mut cfg);
            cfg.set(
                "auto.offset.reset",
                match options.starting_offsets.to_lowercase().as_str() {
                    "earliest" => "earliest",
                    _ => "latest",
                },
            );
            for (k, v) in &options.extra {
                cfg.set(k.as_str(), v.as_str());
            }

            let consumer: StreamConsumer = match cfg.create() {
                Ok(c) => c,
                Err(e) => {
                    yield Err(exec_datafusion_err!("failed to create Kafka consumer: {e}"));
                    return;
                }
            };

            // Subscribe.
            if let Some(ref topics_csv) = options.subscribe {
                let topics: Vec<&str> = topics_csv.split(',').map(str::trim).collect();
                if let Err(e) = consumer.subscribe(&topics) {
                    yield Err(exec_datafusion_err!(
                        "failed to subscribe to Kafka topics {:?}: {e}",
                        topics
                    ));
                    return;
                }
            } else if let Some(ref pattern) = options.subscribe_pattern {
                // rdkafka interprets topics starting with '^' as regex patterns.
                let regex_topic = if pattern.starts_with('^') {
                    pattern.clone()
                } else {
                    format!("^{pattern}")
                };
                if let Err(e) = consumer.subscribe(&[regex_topic.as_str()]) {
                    yield Err(exec_datafusion_err!(
                        "failed to subscribe to Kafka pattern '{pattern}': {e}"
                    ));
                    return;
                }
            } else {
                yield Err(exec_datafusion_err!(
                    "subscribe or subscribePattern is required for Kafka source"
                ));
                return;
            }

            let mut msg_stream = consumer.stream();

            // Collect messages into micro-batches.
            loop {
                let mut builders = KafkaArrowBuilders::with_capacity(max_batch, &projection);
                let mut batch_bytes: usize = 0; // byte budget (see MAX_BATCH_BYTES)
                let deadline = tokio::time::Instant::now() + timeout;

                while builders.len() < max_batch && batch_bytes < max_batch_bytes() {
                    let remaining =
                        deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    match tokio::time::timeout(remaining, msg_stream.next()).await {
                        Ok(Some(Ok(msg))) => {
                            let (ts_ms, ts_type) = match msg.timestamp() {
                                Timestamp::NotAvailable => (-1i64, -1i32),
                                Timestamp::CreateTime(ms) => (ms, 0i32),
                                Timestamp::LogAppendTime(ms) => (ms, 1i32),
                            };
                            let key = msg.key();
                            let value = msg.payload();
                            batch_bytes += value.map_or(0, |v| v.len())
                                + key.map_or(0, |k| k.len())
                                + msg.topic().len();
                            // Append borrowed bytes straight into the Arrow buffers (no to_vec).
                            builders.append(
                                key,
                                value,
                                msg.topic(),
                                msg.partition(),
                                msg.offset(),
                                ts_ms,
                                ts_type,
                            );
                        }
                        Ok(Some(Err(e))) => {
                            yield Err(exec_datafusion_err!("Kafka error: {e}"));
                            return;
                        }
                        Ok(None) => return, // stream ended
                        Err(_) => break,    // timeout — flush partial batch
                    }
                }

                if builders.len() == 0 {
                    continue;
                }

                match builders.finish_projected(&full_schema, &projection) {
                    Ok(batch) => yield Ok(batch),
                    Err(e) => yield Err(e),
                }
            }
        };

        let output = output.map(|x| Ok(FlowEvent::append_only_data(x?)));
        let output = fuse_event_stream(output, parse_spec);
        let stream = Box::pin(FlowEventStreamAdapter::new(emit_schema, output));
        Ok(Box::pin(EncodedFlowEventStream::new(stream)))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Builds the Spark Kafka batch by appending each message's bytes DIRECTLY into Arrow
/// builders — one copy into the final contiguous Arrow buffer, with no per-message `Vec`
/// allocation, no intermediate `KafkaRow`, and no second copy in `build_batch`. This is the
/// zero-extra-copy consume path (Arrow-native; cf. docs/REFERENCES.md §4/§5) and the fix for
/// the measured consumer-read-path bottleneck (docs/STREAMING_ARCHITECTURE.md P0 throughput).
struct KafkaArrowBuilders {
    key: Option<BinaryBuilder>,
    value: Option<BinaryBuilder>,
    topic: Option<StringBuilder>,
    partition: Option<Int32Builder>,
    offset: Option<Int64Builder>,
    timestamp: Option<TimestampMillisecondBuilder>,
    timestamp_type: Option<Int32Builder>,
    rows: usize,
}

impl KafkaArrowBuilders {
    /// Projection-aware: only the columns in `projection` are allocated + appended, so pruned columns
    /// cost nothing — esp. the CONSTANT `topic` string, which the EKS `source_read` profile showed was
    /// being copied 100M× then projected away. Column indices match `kafka_data_schema()`: 0=key
    /// 1=value 2=topic 3=partition 4=offset 5=timestamp 6=timestampType. (Spark/DataFusion projection
    /// pushdown + Arrow bulk-columnar build; REFERENCES §170 — don't re-introduce per-record waste.)
    fn with_capacity(n: usize, projection: &[usize]) -> Self {
        let has = |i: usize| projection.contains(&i);
        Self {
            key: has(0).then(|| BinaryBuilder::with_capacity(n, 0)),
            value: has(1).then(|| BinaryBuilder::with_capacity(n, 0)),
            topic: has(2).then(|| StringBuilder::with_capacity(n, 0)),
            partition: has(3).then(|| Int32Builder::with_capacity(n)),
            offset: has(4).then(|| Int64Builder::with_capacity(n)),
            timestamp: has(5).then(|| TimestampMillisecondBuilder::with_capacity(n)),
            timestamp_type: has(6).then(|| Int32Builder::with_capacity(n)),
            rows: 0,
        }
    }

    fn len(&self) -> usize {
        self.rows
    }

    /// Append one Kafka message (borrowed bytes copied straight into the projected Arrow buffers;
    /// pruned columns are skipped — no work, no allocation).
    fn append(
        &mut self,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        topic: &str,
        partition: i32,
        offset: i64,
        ts_ms: i64,
        ts_type: i32,
    ) {
        if let Some(b) = self.key.as_mut() {
            b.append_option(key);
        }
        if let Some(b) = self.value.as_mut() {
            b.append_option(value);
        }
        if let Some(b) = self.topic.as_mut() {
            b.append_value(topic);
        }
        if let Some(b) = self.partition.as_mut() {
            b.append_value(partition);
        }
        if let Some(b) = self.offset.as_mut() {
            b.append_value(offset);
        }
        if let Some(b) = self.timestamp.as_mut() {
            b.append_value(ts_ms);
        }
        if let Some(b) = self.timestamp_type.as_mut() {
            b.append_value(ts_type);
        }
        self.rows += 1;
    }

    /// Finish into the projected Spark Kafka record batch — builds ONLY the projected columns, in
    /// `projection` order (no full-7-column build followed by `.project()`).
    fn finish_projected(
        mut self,
        full_schema: &SchemaRef,
        projection: &[usize],
    ) -> Result<RecordBatch> {
        let projected_schema =
            Arc::new(full_schema.project(projection).map_err(|e| arrow_datafusion_err!(e))?);
        let columns: Vec<ArrayRef> = projection
            .iter()
            .map(|&i| -> Result<ArrayRef> {
                let missing = || exec_datafusion_err!("kafka builder: column {i} not allocated");
                let arr: ArrayRef = match i {
                    0 => Arc::new(self.key.take().ok_or_else(missing)?.finish()),
                    1 => Arc::new(self.value.take().ok_or_else(missing)?.finish()),
                    2 => Arc::new(self.topic.take().ok_or_else(missing)?.finish()),
                    3 => Arc::new(self.partition.take().ok_or_else(missing)?.finish()),
                    4 => Arc::new(self.offset.take().ok_or_else(missing)?.finish()),
                    5 => Arc::new(self.timestamp.take().ok_or_else(missing)?.finish()),
                    6 => Arc::new(self.timestamp_type.take().ok_or_else(missing)?.finish()),
                    other => return Err(exec_datafusion_err!("kafka builder: column {other} out of range")),
                };
                Ok(arr)
            })
            .collect::<Result<_>>()?;
        RecordBatch::try_new(projected_schema, columns).map_err(|e| arrow_datafusion_err!(e))
    }
}


/// Pack (topic-index, partition) into a u64 hot-loop offset key — avoids a per-message
/// `(String, i32)` alloc + String re-hash at 1e8 msgs (Flink tracks offsets per-split, not
/// per-record). The durable `(topic, partition)` commit format is reconstructed at staging.
fn pack_tp(idx: u32, part: i32) -> u64 {
    ((idx as u64) << 32) | (part as u32 as u64)
}
fn unpack_tp(k: u64) -> (u32, i32) {
    ((k >> 32) as u32, k as u32 as i32)
}

/// FLIP-27 batch-queue Kafka consume (gated `VAJRA_KAFKA_BATCH_QUEUE`, default OFF). The default
/// `StreamConsumer` path reads ONE message per async-stream poll; this drains librdkafka's fetch
/// queue via `rd_kafka_consume_batch_queue` (up to 1000 messages per FFI call — the true Flink
/// `KafkaConsumer.poll(timeout)` analog). Measured 2.8× the StreamConsumer per-message stream on a
/// local 10M/4-part fair A/B (identical Arrow build); grounded in REFERENCES §8 (FLIP-27 one
/// SourceReader per split). Kept behind a flag for an EKS A/B before it becomes the default.
fn batch_queue_enabled() -> bool {
    std::env::var("VAJRA_KAFKA_BATCH_QUEUE").as_deref() == Ok("1")
}

/// One flushed Arrow batch from a single-partition batch-queue reader thread, tagged with the packed
/// (topic-idx, partition) key, the source topic/partition, and the next offset reached (= last
/// consumed offset + 1) so the async generator preserves the exact EO offset-staging contract of the
/// `StreamConsumer` path (bounded stages by packed key; continuous stages by `(topic, partition)`).
struct PartitionBatch {
    batch: RecordBatch,
    key: u64,
    topic: String,
    partition: i32,
    next_off: i64,
}

/// Events a reader thread sends to the async generator. `Idle` is the continuous-path
/// caught-up-to-high-watermark signal (= Flink `WatermarkStatus.IDLE`, driven here by an empty
/// batch-queue drain rather than librdkafka PartitionEOF, since the batch FFI does not surface EOF).
enum ReaderEvent {
    Batch(PartitionBatch),
    Idle,
    Err(String),
}

/// Per-split reader inputs (a struct to keep the spawn call under the arg-count lint without an
/// `#[allow]`, which the workspace denies). `end = i64::MAX` + `emit_idle = true` selects the
/// continuous (unbounded) mode; a finite `end` + `emit_idle = false` is the bounded snapshot read.
struct PartitionReaderJob {
    cfg: ClientConfig,
    topic: String,
    partition: i32,
    start: i64,
    end: i64,
    key: u64,
    target_batch: usize,
    max_bytes: usize,
    full_schema: SchemaRef,
    projection: Vec<usize>,
    timeout_ms: i32,
    max_empty_polls: u32,
    emit_idle: bool,
}

/// Spawn a dedicated OS thread that owns a single-partition `BaseConsumer` and drains it with the
/// librdkafka batch-queue FFI, building row/byte-capped Arrow batches and sending them over `tx`
/// (blocking send = channel backpressure). Bounded read: stops at the captured `end` high-watermark
/// or after `max_empty_polls` empty drains (offset gaps / compaction). Continuous read
/// (`emit_idle`): never stops, and sends `Idle` once each time it catches up to the head (cleared by
/// the next data). Errors are sent as `Err` then the thread exits (dropping its `tx` clone). Mirrors
/// the measured `kafka_read_bench_batch` design.
fn spawn_partition_batch_reader(
    job: PartitionReaderJob,
    tx: tokio::sync::mpsc::Sender<ReaderEvent>,
) {
    std::thread::spawn(move || {
        use rdkafka::bindings;
        use rdkafka::consumer::{BaseConsumer, Consumer};
        use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

        let PartitionReaderJob {
            cfg,
            topic,
            partition,
            start,
            end,
            key,
            target_batch,
            max_bytes,
            full_schema,
            projection,
            timeout_ms,
            max_empty_polls,
            emit_idle,
        } = job;

        // Send an error and stop. If the receiver is gone the send fails — nothing left to do.
        macro_rules! fail {
            ($e:expr) => {{
                let _ = tx.blocking_send(ReaderEvent::Err($e));
                return;
            }};
        }

        let consumer: BaseConsumer = match cfg.create() {
            Ok(c) => c,
            Err(e) => fail!(format!("create BaseConsumer({topic}/{partition}): {e}")),
        };
        let mut tpl = TopicPartitionList::new();
        if let Err(e) = tpl.add_partition_offset(&topic, partition, Offset::Offset(start)) {
            fail!(format!("assign {topic}/{partition}@{start}: {e}"));
        }
        if let Err(e) = consumer.assign(&tpl) {
            fail!(format!("assign {topic}/{partition}: {e}"));
        }

        let rk = consumer.client().native_ptr();
        // SAFETY: `rk` is the live `rd_kafka_t` of this `BaseConsumer`; get its consumer queue.
        let queue = unsafe { bindings::rd_kafka_queue_get_consumer(rk) };
        if queue.is_null() {
            fail!(format!("null consumer queue {topic}/{partition}"));
        }

        let batch_sz = 1000usize;
        let mut msgs: Vec<*mut bindings::rd_kafka_message_t> =
            vec![std::ptr::null_mut(); batch_sz];
        let mut next_off = start;
        let mut empty_polls: u32 = 0;
        let mut idle_signaled = false;
        // Cached partition high-watermark (offset of the head). `Idle` = genuinely caught up to it (a
        // TRANSIENT empty drain mid-backlog must NOT signal Idle — that wrongly excludes an active
        // channel from the downstream watermark MIN-merge = the Flink WatermarkStatus.IDLE contract).
        let mut cached_hi = consumer
            .fetch_watermarks(&topic, partition, std::time::Duration::from_millis(2000))
            .map(|(_, h)| h)
            .unwrap_or(i64::MAX);

        // Emit one row/byte-capped Arrow batch per iteration. Bounded stops at `end`; continuous
        // (`end == i64::MAX`) runs until the receiver drops.
        'outer: while next_off < end {
            let mut builders = KafkaArrowBuilders::with_capacity(target_batch, &projection);
            let mut batch_bytes = 0usize;
            while builders.len() < target_batch && batch_bytes < max_bytes && next_off < end {
                // SAFETY: `queue` is valid; `msgs` is a `batch_sz`-length out-array for message ptrs.
                let n = unsafe {
                    bindings::rd_kafka_consume_batch_queue(
                        queue,
                        timeout_ms,
                        msgs.as_mut_ptr(),
                        batch_sz,
                    )
                };
                if n <= 0 {
                    if builders.len() > 0 {
                        break; // buffer momentarily empty: flush what we have
                    }
                    if emit_idle {
                        // Continuous: signal Idle ONLY when genuinely caught up to the partition
                        // high-watermark (Flink WatermarkStatus.IDLE). A transient empty drain while
                        // `next_off < hi` (more data buffered/coming) must NOT signal Idle. Re-fetch the
                        // head when we think we've reached it (the head advances for a live topic).
                        if next_off >= cached_hi {
                            if let Ok((_, h)) = consumer.fetch_watermarks(
                                &topic, partition, std::time::Duration::from_millis(1000),
                            ) {
                                cached_hi = h;
                            }
                        }
                        if next_off >= cached_hi && !idle_signaled {
                            idle_signaled = true;
                            if tx.blocking_send(ReaderEvent::Idle).is_err() {
                                break 'outer;
                            }
                        }
                        continue; // still below head = transient empty; keep polling, stay ACTIVE
                    }
                    empty_polls += 1;
                    if empty_polls >= max_empty_polls {
                        break 'outer; // bounded: residual offsets unreachable (gaps)
                    }
                    continue;
                }
                empty_polls = 0;
                idle_signaled = false; // data flowing again → active
                for &m in msgs.iter().take(n as usize) {
                    // SAFETY: `m` is a valid message ptr from librdkafka; its payload/key bytes are
                    // borrowed only for the append copy, then the message is destroyed.
                    unsafe {
                        let msg = &*m;
                        if msg.err as i32 == 0 {
                            let off = msg.offset;
                            if off < end {
                                let payload = (!msg.payload.is_null()).then(|| {
                                    std::slice::from_raw_parts(msg.payload as *const u8, msg.len)
                                });
                                let mkey = (!msg.key.is_null()).then(|| {
                                    std::slice::from_raw_parts(msg.key as *const u8, msg.key_len)
                                });
                                let mut tstype =
                                    bindings::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE;
                                let ts = bindings::rd_kafka_message_timestamp(m, &mut tstype);
                                let ts_type = tstype as i32 - 1; // -1 n/a, 0 Create, 1 LogAppend
                                batch_bytes += payload.map_or(0, <[u8]>::len)
                                    + mkey.map_or(0, <[u8]>::len)
                                    + topic.len();
                                builders.append(
                                    mkey,
                                    payload,
                                    &topic,
                                    partition,
                                    off,
                                    if ts_type < 0 { -1 } else { ts },
                                    ts_type,
                                );
                                next_off = off + 1;
                            }
                        }
                        bindings::rd_kafka_message_destroy(m);
                    }
                }
            }
            if builders.len() > 0 {
                match builders.finish_projected(&full_schema, &projection) {
                    Ok(batch) => {
                        let ev = ReaderEvent::Batch(PartitionBatch {
                            batch,
                            key,
                            topic: topic.clone(),
                            partition,
                            next_off,
                        });
                        if tx.blocking_send(ev).is_err() {
                            break; // receiver dropped: stop reading
                        }
                    }
                    Err(e) => fail!(format!("finish_projected {topic}/{partition}: {e}")),
                }
            }
        }
        // SAFETY: `queue` came from `rd_kafka_queue_get_consumer`; release it before the consumer drops.
        unsafe { bindings::rd_kafka_queue_destroy(queue) };
    });
}

#[cfg(test)]
mod offset_key_tests {
    use super::{pack_tp, unpack_tp};
    #[test]
    fn pack_tp_roundtrips() {
        for (idx, part) in [(0u32, 0i32), (3, 17), (255, 0), (1, 4095), (7, 65535)] {
            assert_eq!(unpack_tp(pack_tp(idx, part)), (idx, part));
        }
    }
}

// ---------------------------------------------------------------------------
// Local read-throughput micro-benchmark (ignored unless KAFKA_BENCH=1).
// Drives KafkaSourceExec directly across all partitions against a pre-loaded
// local Kafka topic and reports rows/sec — used to A/B the bounded read path
// (now_or_never drain + topic intern + larger batch) without any cloud cost.
// Run: KAFKA_BENCH=1 BENCH_BOOTSTRAP=localhost:9092 BENCH_TOPIC=repro_under \
//      BENCH_PARTS=16 cargo test -p sail-data-source --release kafka_read_bench -- --nocapture --ignored
// ---------------------------------------------------------------------------
#[expect(clippy::expect_used)]
#[cfg(test)]
mod bench {
    use std::sync::Arc;
    use std::time::Instant;

    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::ExecutionPlan;
    use futures::StreamExt;

    use super::{kafka_data_schema, KafkaArrowBuilders, KafkaSourceExec};
    use crate::formats::kafka::options::KafkaReadOptions;

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn kafka_read_bench() {
        if std::env::var("KAFKA_BENCH").ok().as_deref() != Some("1") {
            eprintln!("set KAFKA_BENCH=1 to run");
            return;
        }
        let boot = std::env::var("BENCH_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".into());
        let topic = std::env::var("BENCH_TOPIC").unwrap_or_else(|_| "repro_under".into());
        let parts: usize = std::env::var("BENCH_PARTS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(16);

        let options = KafkaReadOptions::from_options(vec![
            ("kafka.bootstrap.servers".into(), boot),
            ("subscribe".into(), topic.clone()),
            ("startingOffsets".into(), "earliest".into()),
        ])
        .expect("options");

        let schema = Arc::new(kafka_data_schema());
        let projection: Vec<usize> = (0..schema.fields().len()).collect();
        let exec = Arc::new(
            KafkaSourceExec::try_new(options, schema, projection, true, None, None, parts)
                .expect("exec"),
        );

        let t0 = Instant::now();
        let mut handles = vec![];
        for p in 0..parts {
            let exec = Arc::clone(&exec);
            handles.push(tokio::spawn(async move {
                let ctx = Arc::new(TaskContext::default());
                let mut s = exec.execute(p, ctx).expect("execute");
                let mut rows: u64 = 0;
                let mut batches: u64 = 0;
                while let Some(b) = s.next().await {
                    let b = b.expect("batch");
                    rows += b.num_rows() as u64;
                    batches += 1;
                }
                (rows, batches)
            }));
        }
        let mut total_rows = 0u64;
        let mut total_batches = 0u64;
        for h in handles {
            let (r, b) = h.await.expect("join");
            total_rows += r;
            total_batches += b;
        }
        let dt = t0.elapsed().as_secs_f64();
        eprintln!(
            "KAFKA_READ_BENCH topic={topic} parts={parts} rows={total_rows} batches={total_batches} \
             wall_s={dt:.3} throughput={:.3}M_rows/s",
            total_rows as f64 / dt / 1e6
        );
    }

    /// C1 variant: the FLIP-27 prod-grade design — a DEDICATED std::thread per partition running a
    /// BaseConsumer (rust-rdkafka: BaseConsumer is considerably faster than StreamConsumer for small
    /// messages) in a tight `poll()` loop, building the SAME columnar Arrow batches (KafkaArrowBuilders +
    /// finish, so build cost is counted). A/B vs `kafka_read_bench` (StreamConsumer.now_or_never) proves
    /// the source_read consume win in isolation — NO EO/marker/idle complexity, no cloud cost.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn kafka_read_bench_baseconsumer() {
        if std::env::var("KAFKA_BENCH").ok().as_deref() != Some("1") {
            eprintln!("set KAFKA_BENCH=1 to run");
            return;
        }
        let boot = std::env::var("BENCH_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".into());
        let topic = std::env::var("BENCH_TOPIC").unwrap_or_else(|_| "repro_under".into());
        let parts: usize = std::env::var("BENCH_PARTS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(16);
        let schema = Arc::new(kafka_data_schema());
        let full_schema = Arc::clone(&schema);
        let projection: Vec<usize> = (0..schema.fields().len()).collect();

        let t0 = Instant::now();
        let mut handles = vec![];
        for p in 0..parts {
            let (boot, topic) = (boot.clone(), topic.clone());
            let projection = projection.clone();
            let full_schema = Arc::clone(&full_schema);
            handles.push(std::thread::spawn(move || {
                use std::time::Duration;

                use rdkafka::config::ClientConfig;
                use rdkafka::consumer::{BaseConsumer, Consumer};
                use rdkafka::message::Timestamp;
                use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
                use rdkafka::Message;

                let mut cfg = ClientConfig::new();
                cfg.set("bootstrap.servers", &boot)
                    .set("group.id", format!("bench-bc-{p}-{}", std::process::id()))
                    .set("enable.auto.commit", "false");
                super::apply_consumer_throughput_defaults(&mut cfg); // SAME tuning as the current path (fair)
                let consumer: BaseConsumer = cfg.create().expect("base consumer");
                let mut tpl = TopicPartitionList::new();
                tpl.add_partition_offset(&topic, p as i32, Offset::Beginning)
                    .expect("tpl");
                consumer.assign(&tpl).expect("assign");
                let (_lo, hi) = consumer
                    .fetch_watermarks(&topic, p as i32, Duration::from_secs(15))
                    .expect("watermarks");
                let target = 8192usize;
                let mut rows: u64 = 0;
                let mut batches: u64 = 0;
                let mut builders = KafkaArrowBuilders::with_capacity(target, &projection);
                let mut empty = 0u32;
                loop {
                    // Non-blocking drain (fair vs the baseline's now_or_never): poll(0) takes a buffered
                    // message; only block-wait (poll 100ms) when the prefetch buffer is momentarily empty.
                    let msg_opt = match consumer.poll(Duration::from_millis(0)) {
                        Some(item) => Some(item),
                        None => consumer.poll(Duration::from_millis(100)),
                    };
                    match msg_opt {
                        Some(Ok(msg)) => {
                            empty = 0;
                            let off = msg.offset();
                            let (ts_ms, ts_type) = match msg.timestamp() {
                                Timestamp::NotAvailable => (-1i64, -1i32),
                                Timestamp::CreateTime(ms) => (ms, 0i32),
                                Timestamp::LogAppendTime(ms) => (ms, 1i32),
                            };
                            builders.append(
                                msg.key(),
                                msg.payload(),
                                msg.topic(),
                                msg.partition(),
                                off,
                                ts_ms,
                                ts_type,
                            );
                            if builders.len() >= target {
                                rows += builders.len() as u64;
                                batches += 1;
                                let _ = builders.finish_projected(&full_schema, &projection);
                                builders = KafkaArrowBuilders::with_capacity(target, &projection);
                            }
                            if off + 1 >= hi {
                                break; // reached the partition high-watermark (bounded read)
                            }
                        }
                        Some(Err(_)) => break,
                        None => {
                            empty += 1;
                            if empty > 30 {
                                break; // drained
                            }
                        }
                    }
                }
                if builders.len() > 0 {
                    rows += builders.len() as u64;
                    batches += 1;
                    let _ = builders.finish_projected(&full_schema, &projection);
                }
                (rows, batches)
            }));
        }
        let (mut total_rows, mut total_batches) = (0u64, 0u64);
        for h in handles {
            let (r, b) = h.join().expect("join");
            total_rows += r;
            total_batches += b;
        }
        let dt = t0.elapsed().as_secs_f64();
        eprintln!(
            "KAFKA_READ_BENCH_BASECONSUMER topic={topic} parts={parts} rows={total_rows} \
             batches={total_batches} wall_s={dt:.3} throughput={:.3}M_rows/s",
            total_rows as f64 / dt / 1e6
        );
    }

    /// C1b variant: librdkafka BATCH consume (`rd_kafka_consume_batch_queue`, N messages per CALL — the
    /// true Flink `KafkaConsumer.poll(500)` analog) via FFI, building ONE columnar Arrow batch from the
    /// message array. Tests whether cutting the per-CALL count beats the per-message poll ceiling (~2.5M).
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn kafka_read_bench_batch() {
        if std::env::var("KAFKA_BENCH").ok().as_deref() != Some("1") {
            eprintln!("set KAFKA_BENCH=1 to run");
            return;
        }
        let boot = std::env::var("BENCH_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".into());
        let topic = std::env::var("BENCH_TOPIC").unwrap_or_else(|_| "repro_under".into());
        let parts: usize = std::env::var("BENCH_PARTS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(16);
        let schema = Arc::new(kafka_data_schema());
        let full_schema = Arc::clone(&schema);
        let projection: Vec<usize> = (0..schema.fields().len()).collect();

        let t0 = Instant::now();
        let mut handles = vec![];
        for p in 0..parts {
            let (boot, topic) = (boot.clone(), topic.clone());
            let projection = projection.clone();
            let full_schema = Arc::clone(&full_schema);
            handles.push(std::thread::spawn(move || {
                use std::time::Duration;

                use rdkafka::bindings;
                use rdkafka::config::ClientConfig;
                use rdkafka::consumer::{BaseConsumer, Consumer};
                use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

                let mut cfg = ClientConfig::new();
                cfg.set("bootstrap.servers", &boot)
                    .set("group.id", format!("bench-batch-{p}-{}", std::process::id()))
                    .set("enable.auto.commit", "false");
                super::apply_consumer_throughput_defaults(&mut cfg);
                let consumer: BaseConsumer = cfg.create().expect("consumer");
                let mut tpl = TopicPartitionList::new();
                tpl.add_partition_offset(&topic, p as i32, Offset::Beginning)
                    .expect("tpl");
                consumer.assign(&tpl).expect("assign");
                let (_lo, hi) = consumer
                    .fetch_watermarks(&topic, p as i32, Duration::from_secs(15))
                    .expect("wm");

                let rk = consumer.client().native_ptr();
                // SAFETY: `rk` is a valid rd_kafka_t from the live BaseConsumer; get its consumer queue.
                let queue = unsafe { bindings::rd_kafka_queue_get_consumer(rk) };
                assert!(!queue.is_null(), "consumer queue");

                let batch_sz = 1000usize;
                let mut msgs: Vec<*mut bindings::rd_kafka_message_t> =
                    vec![std::ptr::null_mut(); batch_sz];
                let target = 8192usize;
                let mut builders = KafkaArrowBuilders::with_capacity(target, &projection);
                let (mut rows, mut batches, mut empty, mut last_off) = (0u64, 0u64, 0u32, -1i64);
                loop {
                    // SAFETY: `queue` valid; `msgs` is a `batch_sz`-length out-array for message pointers.
                    let n = unsafe {
                        bindings::rd_kafka_consume_batch_queue(queue, 100, msgs.as_mut_ptr(), batch_sz)
                    };
                    if n <= 0 {
                        empty += 1;
                        if empty > 30 {
                            break;
                        }
                        continue;
                    }
                    empty = 0;
                    for &m in msgs.iter().take(n as usize) {
                        // SAFETY: `m` is a valid message pointer from librdkafka; read its fields (payload/
                        // key are borrowed for the append copy) then destroy it.
                        unsafe {
                            let msg = &*m;
                            if msg.err as i32 == 0 {
                                let payload = (!msg.payload.is_null()).then(|| {
                                    std::slice::from_raw_parts(msg.payload as *const u8, msg.len)
                                });
                                let key = (!msg.key.is_null()).then(|| {
                                    std::slice::from_raw_parts(msg.key as *const u8, msg.key_len)
                                });
                                let mut tstype =
                                    bindings::rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE;
                                let ts = bindings::rd_kafka_message_timestamp(m, &mut tstype);
                                let ts_type = tstype as i32 - 1; // 0->-1, 1->0(Create), 2->1(LogAppend)
                                builders.append(
                                    key,
                                    payload,
                                    &topic,
                                    msg.partition,
                                    msg.offset,
                                    if ts_type < 0 { -1 } else { ts },
                                    ts_type,
                                );
                                last_off = msg.offset;
                            }
                            bindings::rd_kafka_message_destroy(m);
                        }
                    }
                    if builders.len() >= target {
                        rows += builders.len() as u64;
                        batches += 1;
                        let _ = builders.finish_projected(&full_schema, &projection);
                        builders = KafkaArrowBuilders::with_capacity(target, &projection);
                    }
                    if last_off + 1 >= hi {
                        break;
                    }
                }
                if builders.len() > 0 {
                    rows += builders.len() as u64;
                    batches += 1;
                    let _ = builders.finish_projected(&full_schema, &projection);
                }
                // SAFETY: `queue` was obtained from rd_kafka_queue_get_consumer; release it.
                unsafe { bindings::rd_kafka_queue_destroy(queue) };
                (rows, batches)
            }));
        }
        let (mut total_rows, mut total_batches) = (0u64, 0u64);
        for h in handles {
            let (r, b) = h.join().expect("join");
            total_rows += r;
            total_batches += b;
        }
        let dt = t0.elapsed().as_secs_f64();
        eprintln!(
            "KAFKA_READ_BENCH_BATCH topic={topic} parts={parts} rows={total_rows} \
             batches={total_batches} wall_s={dt:.3} throughput={:.3}M_rows/s",
            total_rows as f64 / dt / 1e6
        );
    }

    /// PROD-component gate: drive the real `spawn_partition_batch_reader` (the gated
    /// VAJRA_KAFKA_BATCH_QUEUE path) end-to-end — one reader thread per partition, shared channel,
    /// per-partition captured end high-watermark — and assert it delivers EXACTLY the topic's rows
    /// (no over/under-read, end-bound honored). This validates the prod wiring, not just the raw FFI
    /// bench. `KAFKA_BENCH=1 BENCH_TOPIC=repro_under BENCH_PARTS=4`.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn kafka_batch_queue_prod_reader() {
        if std::env::var("KAFKA_BENCH").ok().as_deref() != Some("1") {
            eprintln!("set KAFKA_BENCH=1 to run");
            return;
        }
        use rdkafka::config::ClientConfig;
        use rdkafka::consumer::{BaseConsumer, Consumer};

        use super::{pack_tp, spawn_partition_batch_reader, PartitionReaderJob, ReaderEvent};
        let boot = std::env::var("BENCH_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".into());
        let topic = std::env::var("BENCH_TOPIC").unwrap_or_else(|_| "repro_under".into());
        let parts: usize = std::env::var("BENCH_PARTS").ok().and_then(|s| s.parse().ok()).unwrap_or(4);
        let schema = Arc::new(kafka_data_schema());
        let projection: Vec<usize> = (0..schema.fields().len()).collect();

        // Resolve each partition's [low, high) snapshot with a throwaway consumer.
        let mut wm = ClientConfig::new();
        wm.set("bootstrap.servers", &boot).set("group.id", "wm-probe");
        let probe: BaseConsumer = wm.create().expect("probe");
        let mut expected = 0i64;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ReaderEvent>(8);
        let t0 = Instant::now();
        for p in 0..parts {
            let (low, high) = probe
                .fetch_watermarks(&topic, p as i32, std::time::Duration::from_secs(15))
                .expect("wm");
            expected += (high - low).max(0);
            let mut cfg = ClientConfig::new();
            cfg.set("bootstrap.servers", &boot)
                .set("group.id", format!("prod-batch-{p}-{}", std::process::id()))
                .set("enable.auto.commit", "false");
            super::apply_consumer_throughput_defaults(&mut cfg);
            spawn_partition_batch_reader(
                PartitionReaderJob {
                    cfg,
                    topic: topic.clone(),
                    partition: p as i32,
                    start: low,
                    end: high,
                    key: pack_tp(0, p as i32),
                    target_batch: 8192,
                    max_bytes: 128 * 1024 * 1024,
                    full_schema: Arc::clone(&schema),
                    projection: projection.clone(),
                    timeout_ms: 100,
                    max_empty_polls: 50,
                    emit_idle: false,
                },
                tx.clone(),
            );
        }
        drop(tx);
        let (mut rows, mut batches) = (0i64, 0u64);
        while let Some(item) = rx.recv().await {
            match item {
                ReaderEvent::Batch(pb) => { rows += pb.batch.num_rows() as i64; batches += 1; }
                ReaderEvent::Idle => {}
                ReaderEvent::Err(e) => { eprintln!("reader err: {e}"); break; }
            }
        }
        let dt = t0.elapsed().as_secs_f64();
        eprintln!(
            "KAFKA_BATCH_QUEUE_PROD topic={topic} parts={parts} rows={rows} expected={expected} \
             batches={batches} wall_s={dt:.3} throughput={:.3}M_rows/s EXACT={}",
            rows as f64 / dt / 1e6,
            rows == expected
        );
        assert_eq!(rows, expected, "prod batch-queue reader must deliver EXACTLY the snapshot rows");
    }

    /// GROUNDED-FIX gate (Flink WatermarkStatus.IDLE = genuinely caught up, not a wall-clock/transient
    /// gap): the continuous batch-queue reader (`emit_idle=true`) must emit `Idle` ONLY after it has
    /// consumed every message up to the partition high-watermark — NEVER on a transient empty drain
    /// mid-backlog (which would wrongly exclude an active channel from the downstream watermark MIN-merge,
    /// freezing/corrupting the watermark = the measured continuous-completeness bug). Asserts: rows
    /// received BEFORE the first `Idle` == the full topic count. `KAFKA_BENCH=1`, topic `idle_test`.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn kafka_idle_only_on_high_watermark() {
        if std::env::var("KAFKA_BENCH").ok().as_deref() != Some("1") {
            eprintln!("set KAFKA_BENCH=1 to run");
            return;
        }
        use rdkafka::config::ClientConfig;
        use rdkafka::consumer::{BaseConsumer, Consumer};

        use super::{spawn_partition_batch_reader, PartitionReaderJob, ReaderEvent};
        let boot = std::env::var("BENCH_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".into());
        let topic = std::env::var("BENCH_TOPIC").unwrap_or_else(|_| "idle_test".into());
        let schema = Arc::new(kafka_data_schema());
        let projection: Vec<usize> = (0..schema.fields().len()).collect();
        let mut wm = ClientConfig::new();
        wm.set("bootstrap.servers", &boot).set("group.id", "idle-probe");
        let probe: BaseConsumer = wm.create().expect("probe");
        let (low, high) = probe
            .fetch_watermarks(&topic, 0, std::time::Duration::from_secs(15))
            .expect("wm");
        let expected = (high - low).max(0);

        let (tx, mut rx) = tokio::sync::mpsc::channel::<ReaderEvent>(8);
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &boot)
            .set("group.id", format!("idle-{}", std::process::id()))
            .set("enable.auto.commit", "false");
        super::apply_consumer_throughput_defaults(&mut cfg);
        spawn_partition_batch_reader(
            PartitionReaderJob {
                cfg,
                topic: topic.clone(),
                partition: 0,
                start: low,
                end: i64::MAX, // continuous
                key: 0,
                target_batch: 8192,
                max_bytes: 128 * 1024 * 1024,
                full_schema: Arc::clone(&schema),
                projection: projection.clone(),
                timeout_ms: 50,
                max_empty_polls: 0,
                emit_idle: true,
            },
            tx,
        );
        let mut rows = 0i64;
        let mut rows_at_first_idle: Option<i64> = None;
        while let Some(ev) = rx.recv().await {
            match ev {
                ReaderEvent::Batch(pb) => rows += pb.batch.num_rows() as i64,
                ReaderEvent::Idle => { rows_at_first_idle = Some(rows); break; }
                ReaderEvent::Err(e) => { eprintln!("reader err: {e}"); break; }
            }
        }
        eprintln!(
            "KAFKA_IDLE_GATE rows_at_first_idle={rows_at_first_idle:?} expected={expected} \
             GENUINE_ONLY={}",
            rows_at_first_idle == Some(expected)
        );
        assert_eq!(
            rows_at_first_idle,
            Some(expected),
            "Idle must fire ONLY at the high-watermark (all {expected} consumed), never transiently"
        );
    }

    /// from_json proxy: parse every `value` (Binary, col 1) as `serde_json::Value` — the dominant per-row
    /// compute the downstream from_json operator does. Used by the OVERLAP gate (C2) to give the pipeline a
    /// realistic CPU stage to overlap the Kafka fetch against.
    fn parse_values(batch: &datafusion::arrow::record_batch::RecordBatch) -> u64 {
        use datafusion::arrow::array::{Array, BinaryArray};
        let vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("value col is Binary");
        let mut n = 0u64;
        for i in 0..vals.len() {
            if vals.is_valid(i) {
                let _v: serde_json::Value =
                    serde_json::from_slice(vals.value(i)).unwrap_or(serde_json::Value::Null);
                n += 1;
            }
        }
        n
    }

    /// Read one partition to its high-watermark with a tuned BaseConsumer, building Arrow batches of
    /// ~`target` rows and handing each finished batch to `on_batch`. Shared by the serial/overlap gate.
    fn read_partition<F: FnMut(datafusion::arrow::record_batch::RecordBatch)>(
        boot: &str,
        topic: &str,
        p: i32,
        target: usize,
        projection: &[usize],
        full_schema: &Arc<datafusion::arrow::datatypes::Schema>,
        mut on_batch: F,
    ) {
        use std::time::Duration;

        use rdkafka::config::ClientConfig;
        use rdkafka::consumer::{BaseConsumer, Consumer};
        use rdkafka::message::Message;
        use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", boot)
            .set("group.id", format!("bench-pipe-{p}-{}", std::process::id()))
            .set("enable.auto.commit", "false");
        super::apply_consumer_throughput_defaults(&mut cfg);
        let consumer: BaseConsumer = cfg.create().expect("consumer");
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(topic, p, Offset::Beginning)
            .expect("tpl");
        consumer.assign(&tpl).expect("assign");
        let (_lo, hi) = consumer
            .fetch_watermarks(topic, p, Duration::from_secs(15))
            .expect("wm");
        let mut builders = KafkaArrowBuilders::with_capacity(target, projection);
        let mut empty = 0u32;
        loop {
            // Blocking poll (100ms) so the first broker fetch lands; returns early once data is queued, so
            // saturated throughput is unaffected — only the tail (past high-watermark) waits.
            match consumer.poll(Duration::from_millis(100)) {
                Some(Ok(m)) => {
                    empty = 0;
                    let (ts_ms, ts_type) = match m.timestamp() {
                        rdkafka::message::Timestamp::CreateTime(ms) => (ms, 0i32),
                        rdkafka::message::Timestamp::LogAppendTime(ms) => (ms, 1i32),
                        rdkafka::message::Timestamp::NotAvailable => (-1i64, -1i32),
                    };
                    let off = m.offset();
                    builders.append(m.key(), m.payload(), topic, p, off, ts_ms, ts_type);
                    if builders.len() >= target {
                        let b = builders.finish_projected(full_schema, projection).expect("finish");
                        on_batch(b);
                        builders = KafkaArrowBuilders::with_capacity(target, projection);
                    }
                    if off + 1 >= hi {
                        break;
                    }
                }
                Some(Err(e)) => { eprintln!("poll err: {e}"); break; }
                None => {
                    empty += 1;
                    if empty > 50 {
                        break;
                    }
                }
            }
        }
        if builders.len() > 0 {
            let b = builders.finish_projected(full_schema, projection).expect("finish");
            on_batch(b);
        }
    }

    /// C2 gate — SERIAL (today's model): one thread per partition does fetch → build → from_json-proxy
    /// INLINE (no overlap). A/B partner of `kafka_pipe_bench_overlap`. Run with BENCH_PARTS < cores so the
    /// overlap variant has spare cores (that is exactly the resource the fetch thread needs).
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn kafka_pipe_bench_serial() {
        if std::env::var("KAFKA_BENCH").ok().as_deref() != Some("1") {
            eprintln!("set KAFKA_BENCH=1 to run");
            return;
        }
        let boot = std::env::var("BENCH_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".into());
        let topic = std::env::var("BENCH_TOPIC").unwrap_or_else(|_| "bench_src".into());
        let parts: usize = std::env::var("BENCH_PARTS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(4);
        let schema = Arc::new(kafka_data_schema());
        let projection: Vec<usize> = (0..schema.fields().len()).collect();
        let t0 = Instant::now();
        let mut handles = vec![];
        for p in 0..parts {
            let (boot, topic, projection, schema) =
                (boot.clone(), topic.clone(), projection.clone(), Arc::clone(&schema));
            handles.push(std::thread::spawn(move || {
                let mut rows = 0u64;
                read_partition(&boot, &topic, p as i32, 8192, &projection, &schema, |b| {
                    rows += parse_values(&b);
                });
                rows
            }));
        }
        let total: u64 = handles.into_iter().map(|h| h.join().expect("join")).sum();
        let dt = t0.elapsed().as_secs_f64();
        eprintln!(
            "KAFKA_PIPE_BENCH_SERIAL topic={topic} parts={parts} rows={total} wall_s={dt:.3} \
             throughput={:.3}M_rows/s",
            total as f64 / dt / 1e6
        );
    }

    /// C2 gate — OVERLAP (FLIP-27 model): per partition a FETCH thread does fetch → build → send over a
    /// BOUNDED sync_channel, while a COMPUTE thread does the from_json-proxy — so fetch overlaps compute.
    /// If this materially beats `kafka_pipe_bench_serial`, the production dedicated-fetch-thread is justified.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn kafka_pipe_bench_overlap() {
        if std::env::var("KAFKA_BENCH").ok().as_deref() != Some("1") {
            eprintln!("set KAFKA_BENCH=1 to run");
            return;
        }
        let boot = std::env::var("BENCH_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".into());
        let topic = std::env::var("BENCH_TOPIC").unwrap_or_else(|_| "bench_src".into());
        let parts: usize = std::env::var("BENCH_PARTS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(4);
        let schema = Arc::new(kafka_data_schema());
        let projection: Vec<usize> = (0..schema.fields().len()).collect();
        let t0 = Instant::now();
        let mut handles = vec![];
        for p in 0..parts {
            let (boot, topic, projection, schema) =
                (boot.clone(), topic.clone(), projection.clone(), Arc::clone(&schema));
            handles.push(std::thread::spawn(move || {
                // Bounded handover = backpressure (Flink credit-flow analog); cap = a few batches.
                let (tx, rx) =
                    std::sync::mpsc::sync_channel::<datafusion::arrow::record_batch::RecordBatch>(4);
                let (b2, t2, pr2, s2) =
                    (boot.clone(), topic.clone(), projection.clone(), Arc::clone(&schema));
                let fetch = std::thread::spawn(move || {
                    read_partition(&b2, &t2, p as i32, 8192, &pr2, &s2, |b| {
                        let _ = tx.send(b);
                    });
                });
                let mut rows = 0u64;
                while let Ok(b) = rx.recv() {
                    rows += parse_values(&b);
                }
                fetch.join().expect("fetch join");
                rows
            }));
        }
        let total: u64 = handles.into_iter().map(|h| h.join().expect("join")).sum();
        let dt = t0.elapsed().as_secs_f64();
        eprintln!(
            "KAFKA_PIPE_BENCH_OVERLAP topic={topic} parts={parts} rows={total} wall_s={dt:.3} \
             throughput={:.3}M_rows/s",
            total as f64 / dt / 1e6
        );
    }
}
