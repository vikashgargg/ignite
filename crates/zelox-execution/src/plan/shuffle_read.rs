use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{exec_datafusion_err, internal_err, Result};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::future::try_join_all;
use futures::{StreamExt, TryStreamExt};
use log::warn;
use zelox_common_datafusion::streaming::event::schema::MARKER_FIELD_NAME;
use zelox_physical_plan::streaming::exchange::merge_flow_event_streams;

use crate::plan::ListListDisplay;
use crate::stream::merge::MergedRecordBatchStream;
use crate::stream::reader::{TaskReadLocation, TaskStreamReader};

#[derive(Debug, Clone)]
pub struct ShuffleReadExec {
    /// For each output partition, a list of locations to read from.
    locations: Vec<Vec<TaskReadLocation>>,
    properties: Arc<PlanProperties>,
    reader: Arc<dyn TaskStreamReader>,
}

impl ShuffleReadExec {
    pub fn new(
        locations: Vec<Vec<TaskReadLocation>>,
        reader: Arc<dyn TaskStreamReader>,
        properties: Arc<PlanProperties>,
    ) -> Self {
        Self {
            locations,
            properties,
            reader,
        }
    }
}

impl DisplayAs for ShuffleReadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ShuffleReadExec: partitioning={}, locations={}",
            self.properties.output_partitioning(),
            ListListDisplay(&self.locations)
        )
    }
}

impl ExecutionPlan for ShuffleReadExec {
    fn name(&self) -> &str {
        "ShuffleReadExec"
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
            return internal_err!("ShuffleReadExec does not accept children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let locations = self
            .locations
            .get(partition)
            .ok_or_else(|| {
                exec_datafusion_err!("read locations for partition {partition} not found")
            })?
            .clone();
        if locations.is_empty() {
            warn!("empty read locations for partition {partition}");
        }
        let reader = self.reader.clone();
        let output_schema = self.schema();
        // Continuous (unbounded) streaming shuffles get Flink `withIdleness` in the N→M merge; bounded
        // (batch / availableNow) shuffles keep the exact watermark MIN (sub-channels END when drained).
        let realtime = matches!(self.properties.boundedness, Boundedness::Unbounded { .. });
        let output =
            futures::stream::once(async move {
                shuffle_read(reader, &locations, output_schema, realtime).await
            })
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}

async fn shuffle_read(
    reader: Arc<dyn TaskStreamReader>,
    locations: &[TaskReadLocation],
    schema: SchemaRef,
    realtime: bool,
) -> Result<SendableRecordBatchStream> {
    let futures = locations
        .iter()
        .map(|location| reader.open(location, schema.clone()));
    let streams = try_join_all(futures).await?;
    // VAJ-BF2 T-BF2.3b: a flow-event (streaming) shuffle carries the `_marker` column — its N producer
    // sub-streams (one per upstream task, hash-routed with broadcast markers) MUST be merged with the
    // marker-aware N→M receiver (MIN-merge distinct-source watermarks + align Chandy-Lamport barriers),
    // not the batch `select_all` naive interleave which would mis-align barriers and skip the MIN. Batch
    // shuffles (no marker column) keep `select_all`. Only flow-event shuffles created by the distributed
    // streaming stage-boundary cut (ZELOX_DISTRIBUTED_STREAM) reach this branch, so batch is untouched.
    if schema.index_of(MARKER_FIELD_NAME).is_ok() {
        let mapped: Vec<SendableRecordBatchStream> = streams
            .into_iter()
            .map(|s| {
                let s = s.map(|item| item.map_err(|e| DataFusionError::External(Box::new(e))));
                Box::pin(RecordBatchStreamAdapter::new(schema.clone(), s))
                    as SendableRecordBatchStream
            })
            .collect();
        Ok(merge_flow_event_streams(mapped, schema, realtime))
    } else {
        Ok(Box::pin(MergedRecordBatchStream::new(schema, streams)))
    }
}
