use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BinaryArray, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{arrow_datafusion_err, exec_datafusion_err, plan_err, Result};
use futures::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Message, Timestamp};
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
    epoch: u64,
    offsets: &std::collections::HashMap<(String, i32), i64>,
) {
    let map: std::collections::BTreeMap<String, i64> = offsets
        .iter()
        .map(|((t, p), o)| (format!("{t}:{p}"), *o))
        .collect();
    if let Ok(body) = serde_json::to_vec(&map) {
        let _ = ck
            .put(
                &format!("sources/0/staged-epoch-{epoch}"),
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
                for (k, v) in &options.extra { cfg.set(k.as_str(), v.as_str()); }
                let consumer: StreamConsumer = match cfg.create() {
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
                let mut ends: std::collections::HashMap<(String, i32), i64> = std::collections::HashMap::new();
                let mut next: std::collections::HashMap<(String, i32), i64> = std::collections::HashMap::new();
                for (topic, part, start, high) in assignments {
                    if let Err(e) = tpl.add_partition_offset(&topic, part, rdkafka::Offset::Offset(start)) {
                        yield Err(exec_datafusion_err!("Kafka assign({topic},{part}@{start}): {e}")); return;
                    }
                    ends.insert((topic.clone(), part), high);
                    next.insert((topic, part), start);
                }
                if let Err(e) = consumer.assign(&tpl) {
                    yield Err(exec_datafusion_err!("Kafka assign: {e}")); return;
                }

                // Read until every partition reaches its end offset (or no more messages).
                let remaining = |next: &std::collections::HashMap<(String, i32), i64>| -> i64 {
                    ends.iter().map(|(k, e)| (e - next.get(k).copied().unwrap_or(*e)).max(0)).sum()
                };
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
                while remaining(&next) > 0 {
                    let mut rows: Vec<KafkaRow> = Vec::with_capacity(max_batch);
                    // Flush on EITHER a row cap OR a byte cap. The byte cap is the
                    // real safety guarantee: Arrow Utf8/Binary use i32 offsets (2 GiB
                    // per array), and the overflow is byte-driven, so a row count alone
                    // is too coarse (e.g. 262k rows x 8 KiB = 2 GiB). Bounding bytes keeps
                    // every variable-length column safely under the i32 limit regardless
                    // of payload size — matching how Arrow/DataFusion size-bound batches.
                    let mut batch_bytes: usize = 0;
                    let mut stream_ended = false;
                    while rows.len() < max_batch
                        && batch_bytes < MAX_BATCH_BYTES
                        && remaining(&next) > 0
                    {
                        match tokio::time::timeout(timeout, msg_stream.next()).await {
                            Ok(Some(Ok(msg))) => {
                                empty_polls = 0; // made progress -> reset the stall budget
                                let tp = (msg.topic().to_string(), msg.partition());
                                let end = ends.get(&tp).copied().unwrap_or(i64::MIN);
                                if msg.offset() >= end { continue; } // past this batch's snapshot
                                let (ts_ms, ts_type) = match msg.timestamp() {
                                    Timestamp::NotAvailable => (-1i64, -1i32),
                                    Timestamp::CreateTime(ms) => (ms, 0i32),
                                    Timestamp::LogAppendTime(ms) => (ms, 1i32),
                                };
                                let key = msg.key().map(|k| k.to_vec());
                                let value = msg.payload().map(|v| v.to_vec());
                                batch_bytes += value.as_ref().map_or(0, |v| v.len())
                                    + key.as_ref().map_or(0, |k| k.len())
                                    + msg.topic().len();
                                rows.push(KafkaRow {
                                    key,
                                    value,
                                    topic: msg.topic().to_string(),
                                    partition: msg.partition(),
                                    offset: msg.offset(),
                                    timestamp_ms: ts_ms,
                                    timestamp_type: ts_type,
                                });
                                next.insert(tp, msg.offset() + 1);
                            }
                            Ok(Some(Err(e))) => { yield Err(exec_datafusion_err!("Kafka error: {e}")); return; }
                            Ok(None) => { stream_ended = true; break; } // consumer stream closed
                            Err(_) => break, // transient poll timeout: flush partial, then retry
                        }
                    }
                    if !rows.is_empty() {
                        match build_batch(&full_schema, &projection, &rows) {
                            Ok(batch) => yield Ok(FlowEvent::append_only_data(batch)),
                            Err(e) => { yield Err(e); return; }
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
                    write_staged_offsets(ck, &next, inst, parallelism).await;
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
                for (k, v) in &options.extra { cfg.set(k.as_str(), v.as_str()); }
                let consumer: StreamConsumer = match cfg.create() {
                    Ok(c) => c,
                    Err(e) => { yield Err(exec_datafusion_err!("failed to create Kafka consumer: {e}")); return; }
                };
                let earliest = options.starting_offsets.eq_ignore_ascii_case("earliest");

                // Resolve start offsets per partition (committed if present, else earliest/latest
                // watermark) in a SYNC step — rdkafka's non-Send `Metadata` never crosses an await.
                let resolve = || -> std::result::Result<Vec<(String, i32, i64)>, String> {
                    let mut out = vec![];
                    for topic in &topics {
                        let md = consumer.fetch_metadata(Some(topic), meta_timeout)
                            .map_err(|e| format!("fetch_metadata({topic}): {e}"))?;
                        let Some(t) = md.topics().iter().find(|t| t.name() == topic) else { continue };
                        for p in t.partitions() {
                            let part = p.id();
                            // Recovery precedence (EO Kafka sink first):
                            //  1. the consumer GROUP's committed offset — for an EO Kafka sink this is
                            //     the records' atomic commit point (sink commits offsets INTO its txn
                            //     via send_offsets_to_transaction); an auto-generated group (file sink)
                            //     has none here, so this is skipped and the next source is used;
                            //  2. the object-store `realtime/committed` record (file-sink EO model);
                            //  3. the earliest/latest watermark (fresh start).
                            let mut one = rdkafka::TopicPartitionList::new();
                            let _ = one.add_partition(topic, part);
                            let group_off = consumer
                                .committed_offsets(one, meta_timeout)
                                .ok()
                                .and_then(|t| t.find_partition(topic, part).map(|e| e.offset()))
                                .and_then(|o| match o {
                                    rdkafka::Offset::Offset(v) => Some(v),
                                    _ => None,
                                });
                            let start = match group_off
                                .or_else(|| committed.get(&(topic.clone(), part)).copied())
                            {
                                Some(o) => o,
                                None => {
                                    let (low, high) = consumer.fetch_watermarks(topic, part, meta_timeout)
                                        .map_err(|e| format!("fetch_watermarks({topic},{part}): {e}"))?;
                                    if earliest { low } else { high }
                                }
                            };
                            out.push((topic.clone(), part, start));
                        }
                    }
                    Ok(out)
                };
                let assignments = match resolve() {
                    Ok(a) => a,
                    Err(e) => { yield Err(exec_datafusion_err!("Kafka {e}")); return; }
                };
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
                let mut rows: Vec<KafkaRow> = Vec::with_capacity(max_batch);
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
                            if !rows.is_empty() {
                                match build_batch(&full_schema, &projection, &rows) {
                                    Ok(batch) => yield Ok(FlowEvent::append_only_data(batch)),
                                    Err(e) => { yield Err(e); return; }
                                }
                                rows.clear();
                                batch_bytes = 0;
                            }
                            write_staged_epoch_offsets(&ck, epoch, &next).await;
                            yield Ok(FlowEvent::Marker(FlowMarker::Checkpoint { id: epoch }));
                            epoch += 1;
                        }
                        // Low-latency flush: emit accumulated rows (no barrier) so records flow
                        // with ~ms latency instead of waiting for the (coarser) epoch tick.
                        _ = flush_timer.tick() => {
                            if !rows.is_empty() {
                                match build_batch(&full_schema, &projection, &rows) {
                                    Ok(batch) => yield Ok(FlowEvent::append_only_data(batch)),
                                    Err(e) => { yield Err(e); return; }
                                }
                                rows.clear();
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
                                    let tp = (m.topic().to_string(), m.partition());
                                    next.insert(tp, m.offset() + 1);
                                    let key = m.key().map(|k| k.to_vec());
                                    let value = m.payload().map(|v| v.to_vec());
                                    batch_bytes += value.as_ref().map_or(0, |v| v.len())
                                        + key.as_ref().map_or(0, |k| k.len())
                                        + m.topic().len();
                                    rows.push(KafkaRow {
                                        key,
                                        value,
                                        topic: m.topic().to_string(),
                                        partition: m.partition(),
                                        offset: m.offset(),
                                        timestamp_ms: ts_ms,
                                        timestamp_type: ts_type,
                                    });
                                    // Flush on row OR byte cap for throughput (epoch still delimits
                                    // the commit; mid-epoch batches just carry data forward). Byte cap
                                    // keeps Utf8/Binary columns under Arrow's i32 offset limit.
                                    if rows.len() >= max_batch || batch_bytes >= MAX_BATCH_BYTES {
                                        match build_batch(&full_schema, &projection, &rows) {
                                            Ok(batch) => yield Ok(FlowEvent::append_only_data(batch)),
                                            Err(e) => { yield Err(e); return; }
                                        }
                                        rows.clear();
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
                let mut rows: Vec<KafkaRow> = Vec::with_capacity(max_batch);
                let mut batch_bytes: usize = 0; // byte budget (see MAX_BATCH_BYTES)
                let deadline = tokio::time::Instant::now() + timeout;

                while rows.len() < max_batch && batch_bytes < MAX_BATCH_BYTES {
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
                            let key = msg.key().map(|k| k.to_vec());
                            let value = msg.payload().map(|v| v.to_vec());
                            batch_bytes += value.as_ref().map_or(0, |v| v.len())
                                + key.as_ref().map_or(0, |k| k.len())
                                + msg.topic().len();
                            rows.push(KafkaRow {
                                key,
                                value,
                                topic: msg.topic().to_string(),
                                partition: msg.partition(),
                                offset: msg.offset(),
                                timestamp_ms: ts_ms,
                                timestamp_type: ts_type,
                            });
                        }
                        Ok(Some(Err(e))) => {
                            yield Err(exec_datafusion_err!("Kafka error: {e}"));
                            return;
                        }
                        Ok(None) => return, // stream ended
                        Err(_) => break,    // timeout — flush partial batch
                    }
                }

                if rows.is_empty() {
                    continue;
                }

                match build_batch(&full_schema, &projection, &rows) {
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

struct KafkaRow {
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    topic: String,
    partition: i32,
    offset: i64,
    timestamp_ms: i64,
    timestamp_type: i32,
}

fn build_batch(
    full_schema: &SchemaRef,
    projection: &[usize],
    rows: &[KafkaRow],
) -> Result<RecordBatch> {
    let keys: Vec<Option<&[u8]>> = rows.iter().map(|r| r.key.as_deref()).collect();
    let values: Vec<Option<&[u8]>> = rows.iter().map(|r| r.value.as_deref()).collect();
    let topics: Vec<&str> = rows.iter().map(|r| r.topic.as_str()).collect();
    let partitions: Vec<i32> = rows.iter().map(|r| r.partition).collect();
    let offsets: Vec<i64> = rows.iter().map(|r| r.offset).collect();
    let timestamps: Vec<i64> = rows.iter().map(|r| r.timestamp_ms).collect();
    let timestamp_types: Vec<i32> = rows.iter().map(|r| r.timestamp_type).collect();

    let all_columns: Vec<ArrayRef> = vec![
        Arc::new(BinaryArray::from(keys)) as ArrayRef,
        Arc::new(BinaryArray::from(values)) as ArrayRef,
        Arc::new(StringArray::from(topics)) as ArrayRef,
        Arc::new(Int32Array::from(partitions)) as ArrayRef,
        Arc::new(Int64Array::from(offsets)) as ArrayRef,
        Arc::new(TimestampMillisecondArray::from(timestamps)) as ArrayRef,
        Arc::new(Int32Array::from(timestamp_types)) as ArrayRef,
    ];

    let full_batch = RecordBatch::try_new(Arc::clone(full_schema), all_columns)
        .map_err(|e| arrow_datafusion_err!(e))?;

    full_batch
        .project(projection)
        .map_err(|e| arrow_datafusion_err!(e))
}
