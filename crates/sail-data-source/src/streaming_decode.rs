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
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{plan_err, Result};
use futures::StreamExt;
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
