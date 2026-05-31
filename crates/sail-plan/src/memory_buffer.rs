use std::any::Any;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::TableType;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{plan_err, Result};
use datafusion_expr::{Expr, TableProviderFilterPushDown};
use futures::stream;

/// Shared handle to the in-memory batch buffer.
/// Cloning the handle gives another reference to the same buffer.
pub type BufferHandle = Arc<Mutex<Vec<RecordBatch>>>;

fn lock_buffer(buf: &Mutex<Vec<RecordBatch>>) -> Result<MutexGuard<'_, Vec<RecordBatch>>> {
    buf.lock().map_err(|_| {
        datafusion_common::DataFusionError::Internal(
            "memory sink buffer lock is poisoned".to_string(),
        )
    })
}

/// In-memory streaming sink table.
///
/// Registered in the DataFusion catalog under `query_name` so that
/// `spark.table(name)` can read accumulated results.
/// The shared `BufferHandle` is also held by `MemorySinkExec` which
/// appends micro-batch data during streaming execution.
#[derive(Debug)]
pub struct MemoryStreamBuffer {
    schema: SchemaRef,
    buffer: BufferHandle,
}

impl MemoryStreamBuffer {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            buffer: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns a clone of the handle so `MemorySinkExec` can share the buffer.
    pub fn buffer_handle(&self) -> BufferHandle {
        Arc::clone(&self.buffer)
    }
}

#[async_trait]
impl TableProvider for MemoryStreamBuffer {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let guard = lock_buffer(&self.buffer)?;
        let batches = guard.clone();
        drop(guard);

        let (schema, projected_batches) = match projection {
            Some(proj) => {
                let schema = Arc::new(self.schema.project(proj)?);
                let projected: Vec<RecordBatch> = batches
                    .into_iter()
                    .map(|b| {
                        b.project(proj)
                            .map_err(datafusion_common::DataFusionError::from)
                    })
                    .collect::<Result<_>>()?;
                (schema, projected)
            }
            None => (self.schema.clone(), batches),
        };

        Ok(Arc::new(MemoryBufferScanExec::new(
            projected_batches,
            schema,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

// ---------------------------------------------------------------------------
// Minimal read-only execution plan for MemoryStreamBuffer::scan
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct MemoryBufferScanExec {
    batches: Vec<RecordBatch>,
    properties: Arc<PlanProperties>,
}

impl MemoryBufferScanExec {
    fn new(batches: Vec<RecordBatch>, schema: SchemaRef) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            batches,
            properties,
        }
    }
}

impl DisplayAs for MemoryBufferScanExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "MemoryBufferScan: {} batches", self.batches.len())
    }
}

impl ExecutionPlan for MemoryBufferScanExec {
    fn name(&self) -> &str {
        "MemoryBufferScanExec"
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
            return plan_err!("MemoryBufferScanExec has no children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return plan_err!("MemoryBufferScanExec only supports a single partition");
        }
        let schema = self.properties.eq_properties.schema().clone();
        let batches = self.batches.clone();
        let output = stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
    }
}
