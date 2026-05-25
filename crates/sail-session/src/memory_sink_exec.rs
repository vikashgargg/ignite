use std::any::Any;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_stream::stream;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{exec_datafusion_err, plan_err, Result};
use futures::StreamExt;
use sail_common_datafusion::streaming::event::schema::{
    MARKER_FIELD_NAME, RETRACTED_FIELD_NAME,
};
use sail_plan::memory_buffer::BufferHandle;

/// Physical sink node for `df.writeStream.format("memory").queryName(name)`.
///
/// For each micro-batch this node strips the flow-event fields (`_marker`,
/// `_retracted`) from the incoming batch and appends the data rows to the
/// shared `BufferHandle`. The output stream is empty (zero rows, empty schema)
/// — the node is a sink.
#[derive(Debug)]
pub struct MemorySinkExec {
    input: Arc<dyn ExecutionPlan>,
    buffer: BufferHandle,
    properties: Arc<PlanProperties>,
}

impl MemorySinkExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, buffer: BufferHandle) -> Self {
        let empty_schema = Arc::new(datafusion::arrow::datatypes::Schema::empty());
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(empty_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        ));
        Self {
            input,
            buffer,
            properties,
        }
    }
}

impl DisplayAs for MemorySinkExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "MemorySink")
    }
}

impl ExecutionPlan for MemorySinkExec {
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
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!("MemorySinkExec requires exactly one child");
        }
        Ok(Arc::new(Self {
            input: children.remove(0),
            buffer: Arc::clone(&self.buffer),
            properties: Arc::clone(&self.properties),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return plan_err!("MemorySinkExec only supports a single partition");
        }

        let mut input_stream = self.input.execute(0, context)?;
        let buffer = Arc::clone(&self.buffer);
        let empty_schema = Arc::new(datafusion::arrow::datatypes::Schema::empty());

        let output = stream! {
            while let Some(batch_result) = input_stream.next().await {
                let batch = match batch_result {
                    Ok(b) => b,
                    Err(e) => { yield Err(e); return; }
                };

                let push_result = strip_flow_event_fields(batch).and_then(|data_batch| {
                    buffer.lock().map_err(|_| {
                        exec_datafusion_err!("memory sink buffer lock is poisoned")
                    }).map(|mut g| g.push(data_batch))
                });
                if let Err(e) = push_result {
                    yield Err(e);
                    return;
                }

                yield Ok(RecordBatch::new_empty(Arc::clone(&empty_schema)));
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(datafusion::arrow::datatypes::Schema::empty()),
            output,
        )))
    }
}

fn strip_flow_event_fields(batch: RecordBatch) -> Result<RecordBatch> {
    let keep_indices: Vec<usize> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| {
            f.name() != MARKER_FIELD_NAME && f.name() != RETRACTED_FIELD_NAME
        })
        .map(|(i, _)| i)
        .collect();

    batch
        .project(&keep_indices)
        .map_err(datafusion_common::DataFusionError::from)
}
