//! `FlowEventToDataExec` — adapts a streaming flow-event input into a plain data
//! `RecordBatch` stream, so a normal (batch) file writer can durably persist a stream.
//!
//! Each input batch is either a **marker** batch (the `_marker` column has non-null
//! entries, carrying watermark/latency/checkpoint markers with null data — skipped) or a
//! **data** batch (markers null) whose flow-event fields (`_marker`, `_retracted`) are
//! stripped, yielding the original data columns. Retraction-aware output is a follow-up;
//! for append-only sources this writes every data row.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, BinaryArray, RecordBatch};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{internal_err, plan_err, DataFusionError, Result};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use object_store::path::Path as StorePath;
use sail_common_datafusion::streaming::checkpoint::CheckpointStore;
use sail_common_datafusion::streaming::event::encoding::DecodedFlowEventStream;
use sail_common_datafusion::streaming::event::marker::FlowMarker;
use sail_common_datafusion::streaming::event::stream::FlowEventStream;
use sail_common_datafusion::streaming::event::FlowEvent;
use sail_common_datafusion::streaming::event::schema::{
    try_from_flow_event_schema, MARKER_FIELD_NAME, RETRACTED_FIELD_NAME,
};

#[derive(Debug)]
pub struct FlowEventToDataExec {
    input: Arc<dyn ExecutionPlan>,
    /// Decoded data schema (input flow-event schema minus marker/retracted).
    data_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl FlowEventToDataExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let data_schema = Arc::new(try_from_flow_event_schema(&input.schema())?);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(data_schema.clone()),
            input.properties().output_partitioning().clone(),
            EmissionType::Incremental,
            input.properties().boundedness,
        ));
        Ok(Self {
            input,
            data_schema,
            properties,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for FlowEventToDataExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "FlowEventToDataExec")
    }
}

impl ExecutionPlan for FlowEventToDataExec {
    fn name(&self) -> &str {
        "FlowEventToDataExec"
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
            return plan_err!("FlowEventToDataExec requires exactly one child");
        }
        Ok(Arc::new(FlowEventToDataExec::try_new(children.remove(0))?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut input_stream = self.input.execute(partition, context)?;
        let data_schema = self.data_schema.clone();
        let keep: Vec<usize> = self
            .input
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| f.name() != MARKER_FIELD_NAME && f.name() != RETRACTED_FIELD_NAME)
            .map(|(i, _)| i)
            .collect();
        let out = async_stream::stream! {
            while let Some(item) = input_stream.next().await {
                let batch = match item {
                    Ok(b) => b,
                    Err(e) => { yield Err(e); return; }
                };
                // Skip marker batches (the `_marker` column has non-null entries).
                if let Ok(idx) = batch.schema().index_of(MARKER_FIELD_NAME) {
                    if let Some(m) = batch.column(idx).as_any().downcast_ref::<BinaryArray>() {
                        if m.null_count() < m.len() {
                            continue;
                        }
                    }
                }
                match batch.project(&keep) {
                    Ok(data) if data.num_rows() > 0 => yield Ok(data),
                    Ok(_) => {}
                    Err(e) => { yield Err(e.into()); return; }
                }
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(data_schema, out)))
    }
}

/// Adapts a file-writer plan into a streaming sink: drains the writer's output (which
/// triggers the durable file writes) and emits empty-schema batches, satisfying the
/// streaming-query sink contract (a sink produces no data rows). Used so a normal
/// (bounded) file writer can back a streaming write — durable for `availableNow` /
/// `once` triggers (the input terminates so the writer finalizes its files).
#[derive(Debug)]
pub struct EmptySinkAdapterExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl EmptySinkAdapterExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let empty = Arc::new(datafusion::arrow::datatypes::Schema::empty());
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(empty),
            input.properties().output_partitioning().clone(),
            EmissionType::Both,
            input.properties().boundedness,
        ));
        Self { input, properties }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for EmptySinkAdapterExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "EmptySinkAdapterExec")
    }
}

impl ExecutionPlan for EmptySinkAdapterExec {
    fn name(&self) -> &str {
        "EmptySinkAdapterExec"
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
            return plan_err!("EmptySinkAdapterExec requires exactly one child");
        }
        Ok(Arc::new(EmptySinkAdapterExec::new(children.remove(0))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut input_stream = self.input.execute(partition, context)?;
        let empty = Arc::new(datafusion::arrow::datatypes::Schema::empty());
        let empty_out = empty.clone();
        let out = async_stream::stream! {
            while let Some(item) = input_stream.next().await {
                match item {
                    Ok(_) => yield Ok(RecordBatch::new_empty(empty_out.clone())),
                    Err(e) => { yield Err(e); return; }
                }
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(empty, out)))
    }
}

/// Exposes a single partition `index` of a multi-partition input as its only (partition 0)
/// output. Used to fan a multi-partition streaming source into N independent single-partition
/// write pipelines (one file per source partition) — see docs/design/streaming-parallelism.md.
#[derive(Debug)]
pub struct PartitionSelectExec {
    input: Arc<dyn ExecutionPlan>,
    index: usize,
    properties: Arc<PlanProperties>,
}

impl PartitionSelectExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, index: usize) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(1),
            input.properties().emission_type,
            input.properties().boundedness,
        ));
        Self {
            input,
            index,
            properties,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    pub fn index(&self) -> usize {
        self.index
    }
}

impl DisplayAs for PartitionSelectExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "PartitionSelectExec: index={}", self.index)
    }
}

impl ExecutionPlan for PartitionSelectExec {
    fn name(&self) -> &str {
        "PartitionSelectExec"
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
            return plan_err!("PartitionSelectExec requires exactly one child");
        }
        Ok(Arc::new(PartitionSelectExec::new(
            children.remove(0),
            self.index,
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("PartitionSelectExec: invalid partition {partition}");
        }
        // Map our single output partition to the selected input partition.
        self.input.execute(self.index, context)
    }
}

/// Drives N independent (single-partition) child sink pipelines **concurrently** — one per
/// source partition — and presents one empty-schema completion stream to the streaming
/// driver. This is the parallel streaming file sink: it sidesteps DataFusion `DataSinkExec`'s
/// single-partition requirement by giving each child exactly one partition, so N files are
/// written in parallel (one per source partition). Completes only after **all** children
/// finish (all-N `EndOfData`), so the driver's exactly-once offset/state commit is unaffected.
#[derive(Debug)]
pub struct ParallelStreamSinkExec {
    children: Vec<Arc<dyn ExecutionPlan>>,
    properties: Arc<PlanProperties>,
}

impl ParallelStreamSinkExec {
    pub fn new(children: Vec<Arc<dyn ExecutionPlan>>) -> Self {
        let empty = Arc::new(Schema::empty());
        let boundedness = children
            .first()
            .map(|c| c.properties().boundedness)
            .unwrap_or(Boundedness::Bounded);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(empty),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            boundedness,
        ));
        Self {
            children,
            properties,
        }
    }
}

impl DisplayAs for ParallelStreamSinkExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "ParallelStreamSinkExec: partitions={}", self.children.len())
    }
}

impl ExecutionPlan for ParallelStreamSinkExec {
    fn name(&self) -> &str {
        "ParallelStreamSinkExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ParallelStreamSinkExec::new(children)))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("ParallelStreamSinkExec: invalid partition {partition}");
        }
        // Start every child sink (each single-partition) and drain it on its own task so the
        // N writers run on separate cores. Each child emits a count row when its file is
        // durable; we discard those and emit a single empty batch once ALL children finish.
        let mut handles = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let mut stream = child.execute(0, context.clone())?;
            handles.push(tokio::spawn(async move {
                while let Some(item) = stream.next().await {
                    item?;
                }
                Ok::<(), DataFusionError>(())
            }));
        }
        let empty = Arc::new(Schema::empty());
        let empty_out = empty.clone();
        let out = async_stream::stream! {
            let mut futs: FuturesUnordered<_> = handles.into_iter().collect();
            while let Some(joined) = futs.next().await {
                match joined {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => { yield Err(e); return; }
                    Err(e) => {
                        yield Err(DataFusionError::Execution(format!(
                            "ParallelStreamSinkExec writer task panicked: {e}"
                        )));
                        return;
                    }
                }
            }
            yield Ok(RecordBatch::new_empty(empty_out.clone()));
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(empty, out)))
    }
}

/// Sink-side exactly-once commit wrapper. Its child is the streaming file write pipeline,
/// configured to write its data into the per-batch subdirectory `<base>/<batch_id>/`. On
/// execute it:
///   1. cleans `<base>/<batch_id>/` (idempotent retry: removes orphan files from a crashed
///      earlier attempt of this same batch);
///   2. drains the child to completion (all writes durable);
///   3. lists the per-batch subdirectory and atomically writes the `_spark_metadata/<batch_id>`
///      commit log — the commit point that makes the batch's output visible to readers.
///
/// A crash before step 3 leaves the batch uncommitted (no metadata file); on restart the source
/// replays it, step 1 wipes the partial output, and the metadata write is idempotent. This is
/// Vajra's `FileStreamSink.addBatch`. See `crate::streaming_sink_log`.
#[derive(Debug)]
pub struct StreamingSinkCommitExec {
    input: Arc<dyn ExecutionPlan>,
    object_store_url: ObjectStoreUrl,
    base: StorePath,
    batch_id: u64,
    properties: Arc<PlanProperties>,
}

impl StreamingSinkCommitExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        object_store_url: ObjectStoreUrl,
        base: StorePath,
        batch_id: u64,
    ) -> Self {
        let empty = Arc::new(Schema::empty());
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(empty),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            input.properties().boundedness,
        ));
        Self {
            input,
            object_store_url,
            base,
            batch_id,
            properties,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }
    pub fn base(&self) -> &StorePath {
        &self.base
    }
    pub fn batch_id(&self) -> u64 {
        self.batch_id
    }
}

impl DisplayAs for StreamingSinkCommitExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "StreamingSinkCommitExec: batch_id={}", self.batch_id)
    }
}

impl ExecutionPlan for StreamingSinkCommitExec {
    fn name(&self) -> &str {
        "StreamingSinkCommitExec"
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
            return plan_err!("StreamingSinkCommitExec requires exactly one child");
        }
        Ok(Arc::new(StreamingSinkCommitExec::new(
            children.remove(0),
            self.object_store_url.clone(),
            self.base.clone(),
            self.batch_id,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("StreamingSinkCommitExec: invalid partition {partition}");
        }
        let store = context.runtime_env().object_store(&self.object_store_url)?;
        let input = Arc::clone(&self.input);
        let base = self.base.clone();
        let batch_id = self.batch_id;
        let ctx = context.clone();
        let empty = Arc::new(Schema::empty());
        let empty_out = empty.clone();
        let out = async_stream::stream! {
            // 1. Clean the per-batch subdir before any write (idempotent retry).
            if let Err(e) = crate::streaming_sink_log::clean_batch_dir(&store, &base, batch_id).await {
                yield Err(e.into());
                return;
            }
            // 2. Drain the child write pipeline to completion (writes become durable).
            let mut input_stream = match input.execute(0, ctx) {
                Ok(s) => s,
                Err(e) => { yield Err(e); return; }
            };
            while let Some(item) = input_stream.next().await {
                if let Err(e) = item { yield Err(e); return; }
            }
            // 3. List what the batch wrote and atomically commit the metadata log.
            let metas = match crate::streaming_sink_log::list_batch_files(&store, &base, batch_id).await {
                Ok(m) => m,
                Err(e) => { yield Err(e.into()); return; }
            };
            if let Err(e) = crate::streaming_sink_log::commit_batch(&store, &base, batch_id, &metas).await {
                yield Err(e.into());
                return;
            }
            yield Ok(RecordBatch::new_empty(empty_out.clone()));
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(empty, out)))
    }
}

/// Realtime (`Trigger.Continuous`) durable file sink: time-slices the continuous decoded data
/// stream into **epochs** (every `commit_interval`) and commits each epoch durably — writing the
/// epoch's batches to `<out>/<epoch>/part-0.parquet` and committing `_spark_metadata/<epoch>` via
/// the same commit log the micro-batch file sink uses (`crate::streaming_sink_log`).
///
/// Unlike the micro-batch sink (one bounded write that finalizes on stream end), this runs inside a
/// single long-lived pipeline and finalizes per epoch on a timer (DataFusion's writer only
/// finalizes on stream-end, so we write each epoch with the Arrow `ArrowWriter` directly). Readers
/// honoring `_spark_metadata` see only committed epochs; an in-flight (uncommitted) epoch's files
/// are invisible. See docs/design/streaming-realtime-mode.md (F1b).
#[derive(Debug)]
pub struct RealtimeFileSinkExec {
    /// Flow-event input (NOT decoded): the sink reads `Checkpoint{epoch}` barriers in-band to
    /// delimit each epoch's data — that is what ties source offsets to sink files exactly-once.
    input: Arc<dyn ExecutionPlan>,
    object_store_url: ObjectStoreUrl,
    base: StorePath,
    /// Streaming `checkpointLocation` — the sink reads the source's per-epoch staged offsets and
    /// writes the single atomic committed record (`realtime/committed`). Required for EO recovery.
    checkpoint_location: Option<String>,
    properties: Arc<PlanProperties>,
}

impl RealtimeFileSinkExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        object_store_url: ObjectStoreUrl,
        base: StorePath,
        checkpoint_location: Option<String>,
    ) -> Self {
        let empty = Arc::new(Schema::empty());
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(empty),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            input.properties().boundedness,
        ));
        Self {
            input,
            object_store_url,
            base,
            checkpoint_location,
            properties,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }
    pub fn base(&self) -> &StorePath {
        &self.base
    }
    pub fn checkpoint_location(&self) -> Option<&str> {
        self.checkpoint_location.as_deref()
    }
}

impl DisplayAs for RealtimeFileSinkExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "RealtimeFileSinkExec: base={}", self.base)
    }
}

/// The single atomic committed record (`realtime/committed`) — the source-of-truth for realtime EO
/// recovery: the latest committed epoch + the per-(topic,partition) offsets included in it. Written
/// as ONE object `put` (object stores have no multi-object txn → one atomic object = no torn commit).
#[derive(serde::Serialize, serde::Deserialize, Default)]
struct RealtimeCommitted {
    epoch: u64,
    offsets: std::collections::BTreeMap<String, i64>,
}

impl ExecutionPlan for RealtimeFileSinkExec {
    fn name(&self) -> &str {
        "RealtimeFileSinkExec"
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
            return plan_err!("RealtimeFileSinkExec requires exactly one child");
        }
        Ok(Arc::new(RealtimeFileSinkExec::new(
            children.remove(0),
            self.object_store_url.clone(),
            self.base.clone(),
            self.checkpoint_location.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("RealtimeFileSinkExec: invalid partition {partition}");
        }
        let store = context.runtime_env().object_store(&self.object_store_url)?;
        let input = Arc::clone(&self.input);
        let base = self.base.clone();
        let checkpoint_location = self.checkpoint_location.clone();
        let ctx = context.clone();
        let empty = Arc::new(Schema::empty());
        let empty_out = empty.clone();

        // Commit epoch `epoch` exactly-once (Flink 2PC collapsed to an object-store-atomic commit):
        //  1. write the epoch's buffered data to `<base>/<epoch>/part-0.parquet` (pre-commit),
        //  2. commit `_spark_metadata/<epoch>` so readers see the files (idempotent overwrite),
        //  3. atomic `put` of `realtime/committed` = {epoch, offsets} read from the source's
        //     `sources/0/staged-epoch-<epoch>` — THE commit point (single source of truth).
        // Crash before (3): nothing committed → source re-reads from the last committed offset →
        // the epoch is redone identically (no dup, no loss). See docs/design/streaming-realtime-mode.md.
        async fn commit_epoch(
            store: &Arc<dyn object_store::ObjectStore>,
            base: &StorePath,
            ck: &Option<CheckpointStore>,
            epoch: u64,
            schema: &SchemaRef,
            batches: &[RecordBatch],
        ) -> Result<()> {
            if !batches.is_empty() {
                crate::streaming_sink_log::clean_batch_dir(store, base, epoch)
                    .await
                    .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))?;
                let mut buf: Vec<u8> = Vec::new();
                {
                    let mut w = datafusion::parquet::arrow::ArrowWriter::try_new(
                        &mut buf,
                        schema.clone(),
                        None,
                    )
                    .map_err(|e| DataFusionError::ParquetError(Box::new(e)))?;
                    for b in batches {
                        w.write(b)
                            .map_err(|e| DataFusionError::ParquetError(Box::new(e)))?;
                    }
                    w.close()
                        .map_err(|e| DataFusionError::ParquetError(Box::new(e)))?;
                }
                let part = base.clone().join(epoch.to_string()).join("part-0.parquet");
                use object_store::ObjectStoreExt;
                store
                    .put(&part, bytes::Bytes::from(buf).into())
                    .await
                    .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))?;
                let metas = crate::streaming_sink_log::list_batch_files(store, base, epoch)
                    .await
                    .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))?;
                crate::streaming_sink_log::commit_batch(store, base, epoch, &metas)
                    .await
                    .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))?;
            }
            // The atomic commit point: read the source's pre-committed offsets for this epoch and
            // write the single committed record. After this returns, the epoch is durably committed.
            if let Some(ck) = ck {
                let offsets: std::collections::BTreeMap<String, i64> = match ck
                    .get(&format!("sources/0/staged-epoch-{epoch}"))
                    .await
                    .ok()
                    .flatten()
                {
                    Some(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
                    None => std::collections::BTreeMap::new(),
                };
                let rec = RealtimeCommitted { epoch, offsets };
                if let Ok(body) = serde_json::to_vec(&rec) {
                    ck.put("realtime/committed", bytes::Bytes::from(body))
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                }
            }
            Ok(())
        }

        let out = async_stream::stream! {
            let raw = match input.execute(0, ctx) {
                Ok(s) => s,
                Err(e) => { yield Err(e); return; }
            };
            let mut decoded = match DecodedFlowEventStream::try_new(raw) {
                Ok(s) => s,
                Err(e) => { yield Err(e); return; }
            };
            let data_schema = decoded.schema();
            let ck = checkpoint_location
                .as_deref()
                .and_then(|l| CheckpointStore::from_location(l).ok());
            let mut buffer: Vec<RecordBatch> = Vec::new();
            while let Some(item) = decoded.next().await {
                match item {
                    Ok(FlowEvent::Data { batch, .. }) => {
                        // Append-only realtime (stateless): retractions don't occur. Buffer rows.
                        if batch.num_rows() > 0 {
                            buffer.push(batch);
                        }
                    }
                    Ok(FlowEvent::Marker(FlowMarker::Checkpoint { id })) => {
                        // Epoch boundary: durably commit this epoch's data + offsets atomically.
                        if let Err(e) =
                            commit_epoch(&store, &base, &ck, id, &data_schema, &buffer).await
                        {
                            yield Err(e); return;
                        }
                        buffer.clear();
                        yield Ok(RecordBatch::new_empty(empty_out.clone()));
                    }
                    // Watermark / latency markers don't bound a durable commit here.
                    Ok(FlowEvent::Marker(_)) => {}
                    Err(e) => { yield Err(e); return; }
                }
            }
            // Stream end: any trailing buffered rows are NOT committed — they belong to an
            // un-checkpointed epoch and are re-read from the last committed offset on restart
            // (exactly-once: no partial/uncommitted epoch is ever made visible).
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(empty, out)))
    }
}
