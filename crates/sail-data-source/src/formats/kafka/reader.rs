use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, Int32Builder, Int64Builder, RecordBatch, StringBuilder,
    TimestampMillisecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
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
        // watermark is monotone and the downstream MIN-merge is correct. The realtime continuous
        // path stays single (parallelism=1) — its per-epoch barrier coordination is separate.
        let parallelism = if bounded {
            let n_parts = count_kafka_partitions(&self.options).await;
            // Fall back to target_partitions if metadata is unavailable; never below 1.
            n_parts
                .unwrap_or_else(|| state.config().target_partitions())
                .max(1)
        } else if std::env::var("VAJRA_RT_MULTI").is_ok() {
            // Throughput Phase B (FLIP-27): N realtime readers, one per Kafka partition, to parallelize
            // source read + from_json (Phase A showed the window STARVED on the single-instance path).
            // GATED off by default — the N-instance per-epoch EO commit union is steps 2-3 (single-
            // coordinator commit is still wired); enable only to profile/validate. Each instance reads
            // ONE partition in event-time order ⇒ monotone watermark (also closes the per-partition
            // watermark edge). See docs/design/streaming-realtime-multi-instance.md.
            count_kafka_partitions(&self.options)
                .await
                .unwrap_or_else(|| state.config().target_partitions())
                .max(1)
        } else {
            1
        };
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

#[derive(Debug)]
pub struct KafkaSourceExec {
    options: KafkaReadOptions,
    original_schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Vec<usize>,
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
            properties,
        })
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

    fn as_any(&self) -> &dyn Any {
        self
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
                for (topic, part, start, high) in assignments {
                    if let Err(e) = tpl.add_partition_offset(&topic, part, rdkafka::Offset::Offset(start)) {
                        yield Err(exec_datafusion_err!("Kafka assign({topic},{part}@{start}): {e}")); return;
                    }
                    let idx = *idx_of.entry(topic.clone()).or_insert_with(|| {
                        topic_names.push(topic.clone());
                        (topic_names.len() - 1) as u32
                    });
                    let k = pack_tp(idx, part);
                    ends.insert(k, high);
                    next.insert(k, start);
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
                    while builders.len() < target_batch
                        && batch_bytes < max_batch_bytes()
                        && remaining(&next) > 0
                    {
                        // Fast path: take a message librdkafka's fetch thread already buffered,
                        // WITHOUT arming a per-message tokio timer (at 1e8 msgs the timer
                        // registration dominates CPU). Only when nothing is buffered do we either
                        // flush the rows we have or — if empty — await with the poll timeout, which
                        // drives stall detection / EndOfData.
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
                                builders.append(key, value, topic, part, off, ts_ms, ts_type);
                                next.insert(k, off + 1);
                            }
                            Some(Err(e)) => { yield Err(exec_datafusion_err!("Kafka error: {e}")); return; }
                            None => { stream_ended = true; break; } // consumer stream closed
                        }
                    }
                    if builders.len() > 0 {
                        if let Some(t) = _rd {
                            sail_common_datafusion::streaming::event::encoding::prof_add(
                                &sail_common_datafusion::streaming::event::encoding::SOURCE_READ_NS,
                                t.elapsed().as_nanos() as u64,
                            );
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
            let stream = Box::pin(FlowEventStreamAdapter::new(projected_schema, events));
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
                let resolve = || -> std::result::Result<Vec<(String, i32, i64)>, String> {
                    // FLIP-27 per-instance split assignment (SAME as the bounded path): collect ALL
                    // (topic, partition) pairs, sort into a stable global order, and keep only those
                    // whose global index `% parallelism == inst`. This is REQUIRED for multi-instance
                    // realtime correctness — without it every instance reads every partition (measured
                    // as an N× over-count). parallelism=1 ⇒ instance 0 owns all (single-instance path
                    // unchanged). Each instance thus reads its partitions in event-time order ⇒ monotone
                    // per-instance watermark ⇒ the downstream keyed exchange MIN-merge is exact.
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
                    Ok(out)
                };
                let assignments = match resolve() {
                    Ok(a) => a,
                    Err(e) => { yield Err(exec_datafusion_err!("Kafka {e}")); return; }
                };
                // T-EO diagnostic: which (partition@start) this realtime instance owns. Correct
                // FLIP-27 assignment ⇒ every partition owned by EXACTLY ONE instance across the run.
                log::debug!(
                    "realtime source inst={inst}/{parallelism} owns={:?}",
                    assignments.iter().map(|(_, p, s)| (*p, *s)).collect::<Vec<_>>()
                );
                let mut tpl = rdkafka::TopicPartitionList::new();
                let mut next: std::collections::HashMap<(String, i32), i64> = std::collections::HashMap::new();
                for (topic, part, start) in assignments {
                    if let Err(e) = tpl.add_partition_offset(&topic, part, rdkafka::Offset::Offset(start)) {
                        yield Err(exec_datafusion_err!("Kafka assign({topic},{part}@{start}): {e}")); return;
                    }
                    next.insert((topic, part), start);
                }
                if let Err(e) = consumer.assign(&tpl) {
                    yield Err(exec_datafusion_err!("Kafka assign: {e}")); return;
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
                loop {
                    tokio::select! {
                        biased;
                        // Epoch boundary: flush buffered data, then pre-commit offsets + emit barrier.
                        // `biased` + data-flush-first guarantees the marker never overtakes its data.
                        _ = timer.tick() => {
                            if builders.len() > 0 {
                                let b = std::mem::replace(&mut builders, KafkaArrowBuilders::with_capacity(max_batch, &projection));
                                match b.finish_projected(&full_schema, &projection) {
                                    Ok(batch) => yield Ok(FlowEvent::append_only_data(batch)),
                                    Err(e) => { yield Err(e); return; }
                                }
                                batch_bytes = 0;
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
                                    builders.append(key, value, topic, part, off, ts_ms, ts_type);
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
                                Some(Err(e)) => { yield Err(exec_datafusion_err!("Kafka error: {e}")); return; }
                                None => break, // stream ended (unexpected for continuous)
                            }
                        }
                    }
                }
            };
            let stream = Box::pin(FlowEventStreamAdapter::new(projected_schema, events));
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
        let stream = Box::pin(FlowEventStreamAdapter::new(projected_schema, output));
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

    use super::{kafka_data_schema, KafkaSourceExec};
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
}
