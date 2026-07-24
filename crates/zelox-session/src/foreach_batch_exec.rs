use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_pyarrow::ToPyArrow;
use async_stream::stream;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{exec_datafusion_err, plan_err, Result};
use futures::StreamExt;
use pyo3::prelude::PyAnyMethods;
use pyo3::Python;
use zelox_python_udf::cereal::pyspark_udf::PySparkUdfPayload;

/// Physical execution node for `df.writeStream.foreachBatch(func)`.
///
/// For each micro-batch this node calls `func(arrow_table, epoch_id)` where
/// `arrow_table` is a PyArrow `Table` built from the current `RecordBatch`.
/// The output stream is empty (zero rows, empty schema) — the node is a sink.
#[derive(Debug)]
pub struct ForeachBatchSinkExec {
    input: Arc<dyn ExecutionPlan>,
    /// Pre-built payload bytes for `PySparkUdfPayload::load`.
    payload: Vec<u8>,
    epoch_counter: Arc<AtomicI64>,
    properties: Arc<PlanProperties>,
}

impl ForeachBatchSinkExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, command: Vec<u8>, eval_type: i32) -> Result<Self> {
        // Build the payload expected by PySparkUdfPayload::load:
        // 4-byte big-endian eval_type followed by the raw command bytes.
        let mut payload = Vec::with_capacity(4 + command.len());
        payload.extend(eval_type.to_be_bytes());
        payload.extend_from_slice(&command);

        let schema = Arc::new(datafusion::arrow::datatypes::Schema::empty());
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Both,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        ));
        Ok(Self {
            input,
            payload,
            epoch_counter: Arc::new(AtomicI64::new(0)),
            properties,
        })
    }
}

impl DisplayAs for ForeachBatchSinkExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "ForeachBatchSink")
    }
}

impl ExecutionPlan for ForeachBatchSinkExec {
    fn name(&self) -> &str {
        Self::static_name()
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
            return plan_err!("ForeachBatchSinkExec requires exactly one child");
        }
        Ok(Arc::new(Self {
            input: children.remove(0),
            payload: self.payload.clone(),
            epoch_counter: Arc::clone(&self.epoch_counter),
            properties: Arc::clone(&self.properties),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return plan_err!("ForeachBatchSinkExec only supports a single partition");
        }

        let mut input_stream = self.input.execute(0, context)?;
        let payload = self.payload.clone();
        let epoch_counter = Arc::clone(&self.epoch_counter);
        let empty_schema = Arc::new(datafusion::arrow::datatypes::Schema::empty());

        let output = stream! {
            while let Some(batch_result) = input_stream.next().await {
                let batch = match batch_result {
                    Ok(b) => b,
                    Err(e) => { yield Err(e); return; }
                };

                let epoch_id = epoch_counter.fetch_add(1, Ordering::SeqCst);

                if let Err(e) = call_foreach_batch(&payload, batch, epoch_id) {
                    yield Err(e);
                    return;
                }

                // Emit an empty batch to signal this micro-batch was processed.
                yield Ok(RecordBatch::new_empty(Arc::clone(&empty_schema)));
            }
        };

        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::new(datafusion::arrow::datatypes::Schema::empty()),
            output,
        )))
    }
}

fn call_foreach_batch(payload: &[u8], batch: RecordBatch, epoch_id: i64) -> Result<()> {
    Python::attach(|py| {
        let func = PySparkUdfPayload::load(py, payload).map_err(|e| {
            exec_datafusion_err!("foreachBatch: failed to load Python function: {e}")
        })?;

        let arrow_table = batch.to_pyarrow(py).map_err(|e| {
            exec_datafusion_err!("foreachBatch: failed to convert batch to PyArrow: {e}")
        })?;

        func.call1((arrow_table, epoch_id))
            .map_err(|e| exec_datafusion_err!("foreachBatch: Python function error: {e}"))?;

        Ok(())
    })
}
