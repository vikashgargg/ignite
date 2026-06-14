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
use sail_common_datafusion::streaming::event::encoding::EncodedFlowEventStream;
use sail_common_datafusion::streaming::checkpoint::CheckpointStore;
use sail_common_datafusion::streaming::event::schema::to_flow_event_schema;
use sail_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;
use sail_common_datafusion::streaming::event::FlowEvent;
use sail_common_datafusion::streaming::source::StreamSource;

use crate::formats::kafka::options::KafkaReadOptions;

/// Read committed per-(topic,partition) offsets from the checkpoint store (single object
/// `sources/0/committed`, a JSON map `"topic:partition" -> next-offset`). The runner commits
/// staged→committed via `CheckpointStore.promote` after the batch output is durable.
async fn read_committed_offsets(
    ck: &CheckpointStore,
) -> std::collections::HashMap<(String, i32), i64> {
    let Some(bytes) = ck.get("sources/0/committed").await.ok().flatten() else {
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

/// Stage (write-ahead) the per-(topic,partition) offsets reached by this micro-batch.
async fn write_staged_offsets(
    ck: &CheckpointStore,
    offsets: &std::collections::HashMap<(String, i32), i64>,
) {
    let map: std::collections::BTreeMap<String, i64> = offsets
        .iter()
        .map(|((t, p), o)| (format!("{t}:{p}"), *o))
        .collect();
    if let Ok(body) = serde_json::to_vec(&map) {
        let _ = ck.put("sources/0/staged", bytes::Bytes::from(body)).await;
    }
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
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
        // `bounded` (availableNow/once, or each continuous re-plan micro-batch): read only
        // `[committed_offset, current_end_offset)` per partition, then `EndOfData`.
        bounded: bool,
        // With a checkpoint location, per-(topic,partition) offsets are committed/restored via the
        // CheckpointStore for exactly-once recovery (Spark `KafkaMicroBatchStream` model).
        checkpoint_location: Option<&str>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields.len()).collect());
        Ok(Arc::new(KafkaSourceExec::try_new(
            self.options.clone(),
            Arc::clone(&self.schema),
            projection,
            bounded,
            checkpoint_location.map(str::to_string),
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
    properties: Arc<PlanProperties>,
}

impl KafkaSourceExec {
    pub fn try_new(
        options: KafkaReadOptions,
        schema: SchemaRef,
        projection: Vec<usize>,
        bounded: bool,
        checkpoint_location: Option<String>,
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
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(1),
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
            properties,
        })
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
        if partition != 0 {
            return plan_err!("{} only supports a single partition", self.name());
        }

        let options = self.options.clone();
        let projection = self.projection.clone();
        let full_schema = Arc::new(kafka_data_schema());
        let projected_schema = self.projected_schema.clone();
        let max_batch = options.max_batch_size;
        let timeout = Duration::from_millis(options.fetch_timeout_ms);

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
                    Some(ck) => read_committed_offsets(ck).await,
                    None => std::collections::HashMap::new(),
                };
                let earliest = options.starting_offsets.eq_ignore_ascii_case("earliest");

                // Resolve assignments (topic, partition, start offset, end=high watermark) in a SYNC
                // step that returns owned data — so rdkafka's non-Send `Metadata` is never held
                // across an await/yield in this stream.
                let resolve = || -> std::result::Result<Vec<(String, i32, i64, i64)>, String> {
                    let mut out = vec![];
                    for topic in &topics {
                        let md = consumer
                            .fetch_metadata(Some(topic), timeout)
                            .map_err(|e| format!("fetch_metadata({topic}): {e}"))?;
                        let Some(t) = md.topics().iter().find(|t| t.name() == topic) else { continue };
                        for p in t.partitions() {
                            let part = p.id();
                            let (low, high) = consumer
                                .fetch_watermarks(topic, part, timeout)
                                .map_err(|e| format!("fetch_watermarks({topic},{part}): {e}"))?;
                            let start = committed
                                .get(&(topic.clone(), part))
                                .copied()
                                .unwrap_or(if earliest { low } else { high });
                            out.push((topic.clone(), part, start, high));
                        }
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
                while remaining(&next) > 0 {
                    let mut rows: Vec<KafkaRow> = Vec::with_capacity(max_batch);
                    while rows.len() < max_batch && remaining(&next) > 0 {
                        match tokio::time::timeout(timeout, msg_stream.next()).await {
                            Ok(Some(Ok(msg))) => {
                                let tp = (msg.topic().to_string(), msg.partition());
                                let end = ends.get(&tp).copied().unwrap_or(i64::MIN);
                                if msg.offset() >= end { continue; } // past this batch's snapshot
                                let (ts_ms, ts_type) = match msg.timestamp() {
                                    Timestamp::NotAvailable => (-1i64, -1i32),
                                    Timestamp::CreateTime(ms) => (ms, 0i32),
                                    Timestamp::LogAppendTime(ms) => (ms, 1i32),
                                };
                                rows.push(KafkaRow {
                                    key: msg.key().map(|k| k.to_vec()),
                                    value: msg.payload().map(|v| v.to_vec()),
                                    topic: msg.topic().to_string(),
                                    partition: msg.partition(),
                                    offset: msg.offset(),
                                    timestamp_ms: ts_ms,
                                    timestamp_type: ts_type,
                                });
                                next.insert(tp, msg.offset() + 1);
                            }
                            Ok(Some(Err(e))) => { yield Err(exec_datafusion_err!("Kafka error: {e}")); return; }
                            Ok(None) => break,
                            Err(_) => break, // timeout: no more messages available right now
                        }
                    }
                    if rows.is_empty() { break; }
                    match build_batch(&full_schema, &projection, &rows) {
                        Ok(batch) => yield Ok(FlowEvent::append_only_data(batch)),
                        Err(e) => { yield Err(e); return; }
                    }
                }

                // Stage the offsets actually reached (write-ahead); runner commits after durable.
                if let Some(ck) = &ck {
                    write_staged_offsets(ck, &next).await;
                }
                yield Ok(FlowEvent::Marker(sail_common_datafusion::streaming::event::marker::FlowMarker::EndOfData));
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
                let deadline = tokio::time::Instant::now() + timeout;

                while rows.len() < max_batch {
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
                            rows.push(KafkaRow {
                                key: msg.key().map(|k| k.to_vec()),
                                value: msg.payload().map(|v| v.to_vec()),
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
