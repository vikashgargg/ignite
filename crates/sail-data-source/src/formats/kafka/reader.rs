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
use sail_common_datafusion::streaming::event::schema::to_flow_event_schema;
use sail_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;
use sail_common_datafusion::streaming::event::FlowEvent;
use sail_common_datafusion::streaming::source::StreamSource;

use crate::formats::kafka::options::KafkaReadOptions;

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
        // Bounded (availableNow/once) reads are not yet implemented for Kafka; the
        // source runs continuously. Tracked in docs/STREAMING.md.
        _bounded: bool,
        // Kafka offset commit/restore is a follow-up (native partition offsets).
        _checkpoint_location: Option<&str>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields.len()).collect());
        Ok(Arc::new(KafkaSourceExec::try_new(
            self.options.clone(),
            Arc::clone(&self.schema),
            projection,
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
    properties: Arc<PlanProperties>,
}

impl KafkaSourceExec {
    pub fn try_new(
        options: KafkaReadOptions,
        schema: SchemaRef,
        projection: Vec<usize>,
    ) -> Result<Self> {
        let projected_schema = Arc::new(schema.project(&projection)?);
        let output_schema = Arc::new(to_flow_event_schema(&projected_schema));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        ));
        Ok(Self {
            options,
            original_schema: schema,
            projected_schema,
            projection,
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
