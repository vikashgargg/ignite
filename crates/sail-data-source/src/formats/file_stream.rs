//! Streaming file source — the streaming counterpart of the batch listing table.
//!
//! Reuses the batch file reader (parquet/CSV/JSON) for the actual I/O and wraps its output
//! as a flow-event stream, so `spark.readStream.format("parquet").load(dir)` works.
//!
//! MVP scope: processes the files available at query start, then emits `EndOfData` and ends —
//! suited to `trigger(availableNow=True)` and one-shot file ETL. Continuous new-file polling
//! and a processed-files metadata log (Spark `FileStreamSource` semantics, for exactly-once
//! across runs) are tracked follow-ups; see docs/benchmarks/REAL_WORLD_HEAD_TO_HEAD.md.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::{plan_err, Result};
use futures::StreamExt;
use sail_common_datafusion::streaming::event::encoding::EncodedFlowEventStream;
use sail_common_datafusion::streaming::event::marker::FlowMarker;
use sail_common_datafusion::streaming::event::schema::to_flow_event_schema;
use sail_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;
use sail_common_datafusion::streaming::event::FlowEvent;
use sail_common_datafusion::streaming::source::StreamSource;

/// A streaming source backed by files in a directory.
#[derive(Debug)]
pub struct FileStreamSource {
    /// The batch listing table that does the actual file I/O.
    inner: Arc<dyn TableProvider>,
    data_schema: SchemaRef,
}

impl FileStreamSource {
    pub fn new(inner: Arc<dyn TableProvider>, data_schema: SchemaRef) -> Self {
        Self { inner, data_schema }
    }
}

#[async_trait::async_trait]
impl StreamSource for FileStreamSource {
    fn data_schema(&self) -> SchemaRef {
        Arc::clone(&self.data_schema)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        bounded: bool,
        _checkpoint_location: Option<&str>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !bounded {
            // MVP: only the available files are processed (then the stream ends). A
            // continuous trigger would expect new-file polling, which isn't implemented yet.
            log::warn!(
                "streaming file source: processing currently-available files once; \
                 continuous new-file polling is not yet implemented — use trigger(availableNow=True)"
            );
        }
        // Reuse the batch file reader for the parquet/CSV/JSON I/O. `FileSourceExec` reads
        // all of its partitions itself and presents a single flow-event stream.
        let data_plan = self.inner.scan(state, projection, filters, limit).await?;
        Ok(Arc::new(FileSourceExec::try_new(data_plan)?))
    }
}

/// Wraps a (single-partition) batch file-scan plan as a flow-event source: each data batch
/// becomes an append-only `FlowEvent::Data`, followed by an `EndOfData` marker.
#[derive(Debug)]
pub struct FileSourceExec {
    input: Arc<dyn ExecutionPlan>,
    data_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl FileSourceExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let data_schema = input.schema();
        let output_schema = Arc::new(to_flow_event_schema(&data_schema));
        // Preserve the batch reader's partitioning so file reading stays parallel across
        // cores (DataFusion `ListingTable` enumerates file/row-group splits into
        // `target_partitions` partitions — the streaming equivalent of Flink's
        // `SplitEnumerator` fanning splits to parallel source readers, and Spark's
        // file-task parallelism). Each partition emits its own `EndOfData`.
        let partitioning = Partitioning::UnknownPartitioning(
            input.properties().output_partitioning().partition_count().max(1),
        );
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            partitioning,
            EmissionType::Both,
            // The available file set is finite; the stream ends after `EndOfData`.
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            data_schema,
            properties,
        })
    }
}

impl DisplayAs for FileSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FileSourceExec")
    }
}

impl ExecutionPlan for FileSourceExec {
    fn name(&self) -> &str {
        "FileSourceExec"
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match <[_; 1]>::try_from(children) {
            Ok([input]) => Ok(Arc::new(FileSourceExec::try_new(input)?)),
            Err(_) => plan_err!("{} requires exactly one child", self.name()),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Each partition reads its own file-group split (parallel I/O) and emits that
        // split's data batches followed by its own `EndOfData`. The streaming framework
        // terminates a bounded query only after every partition's `EndOfData` (same
        // contract as the multi-partition rate source).
        let data_stream = self.input.execute(partition, context)?;
        let events = data_stream
            .map(|r| r.map(FlowEvent::append_only_data))
            .chain(futures::stream::once(async {
                Ok(FlowEvent::Marker(FlowMarker::EndOfData))
            }));
        let stream = Box::pin(FlowEventStreamAdapter::new(
            Arc::clone(&self.data_schema),
            events,
        ));
        Ok(Box::pin(EncodedFlowEventStream::new(stream)))
    }
}
