//! `KafkaSinkExec` — streaming Kafka sink (`writeStream.format("kafka")`).
//!
//! Consumes the **flow-event** input, decodes it, and **produces each data row to Kafka
//! on arrival** (record-paced → low latency, the gap vs Flink the file sink couldn't
//! close), flushing on each epoch (`Checkpoint`) / `EndOfData` boundary.
//!
//! Delivery: **at-least-once** (the Spark `KafkaStreamWriter` / Flink `KafkaSink`
//! default) — produce non-blocking into librdkafka's queue, `flush()` at the epoch
//! boundary so the micro-batch's records are acknowledged before the source's offsets are
//! committed. (Exactly-once via Kafka transactions tied to the per-epoch offset commit is
//! the documented next step — see docs/design/kafka-sink-and-low-latency.md.)
//!
//! Column mapping (Spark-compatible): `value` (required), optional `key`. The value/key
//! column is cast to Utf8 and produced as its UTF-8 bytes.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{exec_datafusion_err, internal_err, plan_err, Result};
use futures::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::producer::{BaseRecord, Producer, ThreadedProducer};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::util::Timeout;
use rdkafka::{Offset, TopicPartitionList};
use sail_common_datafusion::streaming::checkpoint::CheckpointStore;
use sail_common_datafusion::streaming::event::encoding::DecodedFlowEventStream;
use sail_common_datafusion::streaming::event::marker::FlowMarker;
use sail_common_datafusion::streaming::event::stream::FlowEventStream;
use sail_common_datafusion::streaming::event::FlowEvent;

/// Streaming Kafka sink operator.
#[derive(Debug)]
pub struct KafkaSinkExec {
    input: Arc<dyn ExecutionPlan>,
    bootstrap_servers: String,
    topic: String,
    /// Column to use as the record value (default: `value`, else the single data column).
    value_col: Option<String>,
    /// Optional column to use as the record key (default: `key` if present).
    key_col: Option<String>,
    /// Extra `kafka.*` producer options (prefix already stripped).
    extra: HashMap<String, String>,
    /// `true` = exactly-once via Kafka transactions (Flink `KafkaSink` EXACTLY_ONCE /
    /// read-process-write): per epoch, `send_offsets_to_transaction(source offsets, group)`
    /// then `commit_transaction`, so produced records and consumed offsets commit atomically;
    /// a stable `transactional.id` fences orphaned txns on restart. `read_committed`
    /// consumers never see aborted/in-flight records. `false` = at-least-once (default).
    exactly_once: bool,
    /// Consumer group whose offsets are committed inside the transaction (EO). Shared with
    /// the source so recovery reads the group's committed offsets = the records' commit point.
    group_id: Option<String>,
    /// Checkpoint location — EO reads the source's per-epoch staged offsets
    /// (`sources/0/staged-epoch-<epoch>`) to put into the transaction.
    checkpoint_location: Option<String>,
    /// This sink task's index (0 for a single sink; `i` for the i-th of N parallel tasks). Used for a
    /// per-task `transactional.id` (`vajra-sink-<topic>-<idx>`) so parallel EO producers each fence their
    /// own orphaned transactions independently (Flink KafkaSink per-subtask transactional.id).
    sink_partition: usize,
    properties: Arc<PlanProperties>,
}

impl KafkaSinkExec {
    #[expect(clippy::too_many_arguments)]
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        bootstrap_servers: String,
        topic: String,
        value_col: Option<String>,
        key_col: Option<String>,
        extra: HashMap<String, String>,
        exactly_once: bool,
        group_id: Option<String>,
        checkpoint_location: Option<String>,
        sink_partition: usize,
    ) -> Result<Self> {
        if bootstrap_servers.is_empty() {
            return plan_err!("kafka sink requires kafka.bootstrap.servers");
        }
        if topic.is_empty() {
            return plan_err!("kafka sink requires the `topic` option");
        }
        if exactly_once && group_id.is_none() {
            return plan_err!(
                "kafka sink exactly-once requires `kafka.group.id` (shared with the source)"
            );
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::empty())),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            input.properties().boundedness,
        ));
        Ok(Self {
            input,
            bootstrap_servers,
            topic,
            value_col,
            key_col,
            extra,
            exactly_once,
            group_id,
            checkpoint_location,
            sink_partition,
            properties,
        })
    }

    pub fn sink_partition(&self) -> usize {
        self.sink_partition
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }
    pub fn topic(&self) -> &str {
        &self.topic
    }
    pub fn value_col(&self) -> Option<&str> {
        self.value_col.as_deref()
    }
    pub fn key_col(&self) -> Option<&str> {
        self.key_col.as_deref()
    }
    pub fn extra(&self) -> &HashMap<String, String> {
        &self.extra
    }
    pub fn exactly_once(&self) -> bool {
        self.exactly_once
    }
    pub fn group_id(&self) -> Option<&str> {
        self.group_id.as_deref()
    }
    pub fn checkpoint_location(&self) -> Option<&str> {
        self.checkpoint_location.as_deref()
    }
}

/// Resolve the value column index: explicit name, else `value`, else the sole column.
fn resolve_value_idx(schema: &SchemaRef, requested: Option<&str>) -> Result<usize> {
    if let Some(name) = requested {
        return schema
            .index_of(name)
            .map_err(|_| exec_datafusion_err!("kafka sink: value column `{name}` not found"));
    }
    if let Ok(i) = schema.index_of("value") {
        return Ok(i);
    }
    if schema.fields().len() == 1 {
        return Ok(0);
    }
    plan_err!(
        "kafka sink: no `value` column and input has {} columns; set the value column explicitly",
        schema.fields().len()
    )
}

/// Read the source's per-epoch staged offsets (`sources/0/staged-epoch-<epoch>`, a JSON
/// map `"topic:partition" -> next-offset`) into a `TopicPartitionList` for
/// `send_offsets_to_transaction` (the committed position = next offset to consume).
async fn read_staged_epoch_offsets(ck: &CheckpointStore, epoch: u64) -> Result<TopicPartitionList> {
    let mut tpl = TopicPartitionList::new();
    let Some(bytes) = ck
        .get(&format!("sources/0/staged-epoch-{epoch}"))
        .await
        .ok()
        .flatten()
    else {
        return Ok(tpl);
    };
    let map: std::collections::BTreeMap<String, i64> =
        serde_json::from_slice(&bytes).unwrap_or_default();
    for (k, off) in map {
        if let Some((topic, part)) = k.rsplit_once(':') {
            if let Ok(p) = part.parse::<i32>() {
                tpl.add_partition_offset(topic, p, Offset::Offset(off))
                    .map_err(|e| exec_datafusion_err!("kafka sink: offset list: {e}"))?;
            }
        }
    }
    Ok(tpl)
}

impl DisplayAs for KafkaSinkExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "KafkaSinkExec: topic={}", self.topic)
    }
}

impl ExecutionPlan for KafkaSinkExec {
    fn name(&self) -> &str {
        "KafkaSinkExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!("KafkaSinkExec requires exactly one child");
        }
        Ok(Arc::new(KafkaSinkExec::try_new(
            children.remove(0),
            self.bootstrap_servers.clone(),
            self.topic.clone(),
            self.value_col.clone(),
            self.key_col.clone(),
            self.extra.clone(),
            self.exactly_once,
            self.group_id.clone(),
            self.checkpoint_location.clone(),
            self.sink_partition,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("KafkaSinkExec: invalid partition {partition}");
        }
        let input = Arc::clone(&self.input);
        let bootstrap = self.bootstrap_servers.clone();
        let topic = self.topic.clone();
        let value_col = self.value_col.clone();
        let key_col = self.key_col.clone();
        let extra = self.extra.clone();
        let exactly_once = self.exactly_once;
        let group_id = self.group_id.clone();
        let checkpoint_location = self.checkpoint_location.clone();
        let sink_partition = self.sink_partition;
        let empty = Arc::new(Schema::empty());
        let empty_out = empty.clone();

        let out = async_stream::stream! {
            // librdkafka producer: non-blocking enqueue (background-thread delivery) → low
            // latency; bounded queue applies back-pressure (we await on QueueFull).
            let mut cfg = ClientConfig::new();
            cfg.set("bootstrap.servers", &bootstrap);
            cfg.set("linger.ms", "5");
            cfg.set("queue.buffering.max.messages", "2000000");
            cfg.set("compression.type", "lz4");
            if exactly_once {
                // Transactional producer (Flink KafkaSink EXACTLY_ONCE). Stable transactional.id
                // per sink task → on restart a new producer with the same id FENCES + aborts any
                // orphaned in-flight txn, so read_committed consumers never see partial output.
                cfg.set("transactional.id", format!("vajra-sink-{topic}-{sink_partition}"));
                cfg.set("enable.idempotence", "true");
                cfg.set("transaction.timeout.ms", "120000");
            }
            for (k, v) in &extra {
                cfg.set(k.as_str(), v.as_str());
            }
            let producer: ThreadedProducer<_> = match cfg.create() {
                Ok(p) => p,
                Err(e) => { yield Err(exec_datafusion_err!("kafka sink: create producer: {e}")); return; }
            };
            let ck = checkpoint_location
                .as_deref()
                .and_then(|l| CheckpointStore::from_location(l).ok());
            let group = group_id.clone().unwrap_or_default();
            // EO: a consumer carrying the shared group.id supplies the ConsumerGroupMetadata for
            // `send_offsets_to_transaction` (rdkafka has no group-id-only constructor). The source
            // uses manual assign (no group membership), so the group id alone drives the offset
            // commit; the stable transactional.id provides the fencing.
            let cgm = if exactly_once {
                let mc: BaseConsumer = match ClientConfig::new()
                    .set("bootstrap.servers", &bootstrap)
                    .set("group.id", &group)
                    .create()
                {
                    Ok(c) => c,
                    Err(e) => { yield Err(exec_datafusion_err!("kafka sink: group consumer: {e}")); return; }
                };
                match mc.group_metadata() {
                    Some(m) => Some(m),
                    None => { yield Err(exec_datafusion_err!("kafka sink: no group metadata for `{group}`")); return; }
                }
            } else {
                None
            };
            if exactly_once {
                if let Err(e) = producer.init_transactions(Timeout::After(Duration::from_secs(30))) {
                    yield Err(exec_datafusion_err!("kafka sink: init_transactions: {e}")); return;
                }
                if let Err(e) = producer.begin_transaction() {
                    yield Err(exec_datafusion_err!("kafka sink: begin_transaction: {e}")); return;
                }
            }

            let raw = match input.execute(0, context) {
                Ok(s) => s,
                Err(e) => { yield Err(e); return; }
            };
            let mut decoded = match DecodedFlowEventStream::try_new(raw) {
                Ok(s) => s,
                Err(e) => { yield Err(e); return; }
            };
            let data_schema = decoded.schema();
            let vi = match resolve_value_idx(&data_schema, value_col.as_deref()) {
                Ok(i) => i,
                Err(e) => { yield Err(e); return; }
            };
            let ki = match key_col.as_deref() {
                Some(name) => match data_schema.index_of(name) {
                    Ok(i) => Some(i),
                    Err(_) => { yield Err(exec_datafusion_err!("kafka sink: key column `{name}` not found")); return; }
                },
                None => data_schema.index_of("key").ok(),
            };

            while let Some(item) = decoded.next().await {
                match item {
                    Ok(FlowEvent::Data { batch, .. }) => {
                        if batch.num_rows() == 0 { continue; }
                        // Cast value/key columns to Utf8 and produce their bytes.
                        let vals = match cast(batch.column(vi), &DataType::Utf8) {
                            Ok(a) => a,
                            Err(e) => { yield Err(exec_datafusion_err!("kafka sink: cast value to string: {e}")); return; }
                        };
                        let varr = vals.as_string::<i32>();
                        let keys = match ki {
                            Some(i) => match cast(batch.column(i), &DataType::Utf8) {
                                Ok(a) => Some(a),
                                Err(e) => { yield Err(exec_datafusion_err!("kafka sink: cast key to string: {e}")); return; }
                            },
                            None => None,
                        };
                        let karr = keys.as_ref().map(|k| k.as_string::<i32>());
                        for r in 0..batch.num_rows() {
                            if varr.is_null(r) { continue; } // Spark drops null-value rows
                            let payload = varr.value(r).as_bytes();
                            let key_bytes: Option<&[u8]> = match karr {
                                Some(k) if !k.is_null(r) => Some(k.value(r).as_bytes()),
                                _ => None, // null key → round-robin partitioning (Spark semantics)
                            };
                            // Send; retry on a full queue (back-pressure). `map_err(|(e,_)| e)`
                            // unifies the with-key / without-key `BaseRecord` types.
                            loop {
                                let base = BaseRecord::to(&topic).payload(payload);
                                let res = match key_bytes {
                                    Some(kb) => producer.send(base.key(kb)).map_err(|(e, _)| e),
                                    None => producer.send(base).map_err(|(e, _)| e),
                                };
                                match res {
                                    Ok(()) => break,
                                    Err(KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)) => {
                                        tokio::time::sleep(Duration::from_millis(5)).await;
                                    }
                                    Err(e) => { yield Err(exec_datafusion_err!("kafka sink: produce: {e}")); return; }
                                }
                            }
                        }
                    }
                    // Epoch boundary.
                    Ok(FlowEvent::Marker(FlowMarker::Checkpoint { id })) => {
                        if exactly_once {
                            // Commit this epoch's records AND the source's consumed offsets in ONE
                            // Kafka transaction (read-process-write EO): atomic, so recovery from the
                            // group's committed offsets never duplicates or loses.
                            let tpl = match &ck {
                                Some(ck) => match read_staged_epoch_offsets(ck, id).await {
                                    Ok(t) => t,
                                    Err(e) => { yield Err(e); return; }
                                },
                                None => TopicPartitionList::new(),
                            };
                            if tpl.count() > 0 {
                                if let Some(cgm) = &cgm {
                                    if let Err(e) = producer.send_offsets_to_transaction(&tpl, cgm, Timeout::After(Duration::from_secs(30))) {
                                        yield Err(exec_datafusion_err!("kafka sink: send_offsets_to_transaction: {e}")); return;
                                    }
                                }
                            }
                            if let Err(e) = producer.commit_transaction(Timeout::After(Duration::from_secs(60))) {
                                yield Err(exec_datafusion_err!("kafka sink: commit_transaction: {e}")); return;
                            }
                            if let Err(e) = producer.begin_transaction() {
                                yield Err(exec_datafusion_err!("kafka sink: begin_transaction: {e}")); return;
                            }
                        } else if let Err(e) = producer.flush(Timeout::After(Duration::from_secs(60))) {
                            // at-least-once: flush so the epoch's records are durable before commit.
                            yield Err(exec_datafusion_err!("kafka sink: flush: {e}")); return;
                        }
                        yield Ok(RecordBatch::new_empty(empty_out.clone()));
                    }
                    // Terminal (micro-batch availableNow).
                    Ok(FlowEvent::Marker(FlowMarker::EndOfData)) => {
                        if exactly_once {
                            if let Err(e) = producer.commit_transaction(Timeout::After(Duration::from_secs(60))) {
                                yield Err(exec_datafusion_err!("kafka sink: commit_transaction (eod): {e}")); return;
                            }
                        } else if let Err(e) = producer.flush(Timeout::After(Duration::from_secs(60))) {
                            yield Err(exec_datafusion_err!("kafka sink: flush: {e}")); return;
                        }
                        yield Ok(RecordBatch::new_empty(empty_out.clone()));
                    }
                    Ok(FlowEvent::Marker(_)) => {}
                    Err(e) => { yield Err(e); return; }
                }
            }
            // Drain anything still queued at stream end.
            let _ = producer.flush(Timeout::After(Duration::from_secs(60)));
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(empty, out)))
    }
}
