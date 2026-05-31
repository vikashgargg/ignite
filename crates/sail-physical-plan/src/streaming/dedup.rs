use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{BooleanBuilder, RecordBatch};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{internal_err, plan_datafusion_err, plan_err, Result, ScalarValue};
use futures::{stream, StreamExt};
use sail_common_datafusion::streaming::event::encoding::{
    DecodedFlowEventStream, EncodedFlowEventStream,
};
use sail_common_datafusion::streaming::event::schema::to_flow_event_schema;
use sail_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;
use sail_common_datafusion::streaming::event::FlowEvent;

/// Stateful streaming deduplication physical operator.
///
/// Tracks all key tuples seen across micro-batches in an in-memory `HashSet`.
/// For each incoming data batch, emits only rows whose key has not been
/// seen before and adds those keys to the set. Markers pass through unchanged.
#[derive(Debug)]
pub struct StreamDeduplicateExec {
    input: Arc<dyn ExecutionPlan>,
    key_cols: Vec<String>,
    data_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl StreamDeduplicateExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        key_cols: Vec<String>,
        data_schema: SchemaRef,
    ) -> Result<Self> {
        let flow_schema = Arc::new(to_flow_event_schema(&data_schema));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(flow_schema),
            datafusion::physical_expr::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: true,
            },
        ));
        Ok(Self {
            input,
            key_cols,
            data_schema,
            properties,
        })
    }
}

impl DisplayAs for StreamDeduplicateExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "StreamDeduplicateExec: keys=[{}]",
            self.key_cols.join(", ")
        )
    }
}

impl ExecutionPlan for StreamDeduplicateExec {
    fn name(&self) -> &str {
        "StreamDeduplicateExec"
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
        let (Some(child), true) = (children.pop(), children.is_empty()) else {
            return plan_err!("{} expects exactly one child", self.name());
        };
        Ok(Arc::new(StreamDeduplicateExec::try_new(
            child,
            self.key_cols.clone(),
            self.data_schema.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("StreamDeduplicateExec: invalid partition {partition}");
        }

        // Pre-compute key column indices from the data schema.
        let key_indices: Vec<usize> = self
            .key_cols
            .iter()
            .map(|name| {
                self.data_schema
                    .index_of(name.as_str())
                    .map_err(|_| plan_datafusion_err!("dedup key '{}' not found in schema", name))
            })
            .collect::<Result<_>>()?;

        let data_schema = self.data_schema.clone();
        let input_stream =
            DecodedFlowEventStream::try_new(self.input.execute(partition, context)?)?;

        type Seen = HashSet<Vec<ScalarValue>>;
        let init: (DecodedFlowEventStream, Seen) = (input_stream, HashSet::new());

        let event_stream = stream::unfold(init, move |(mut input, mut seen)| {
            let key_indices = key_indices.clone();
            async move {
                loop {
                    match input.next().await {
                        None => return None,
                        Some(Err(e)) => return Some((Err(e), (input, seen))),
                        Some(Ok(FlowEvent::Data { batch, .. })) => {
                            let filtered = match filter_new_rows(&batch, &key_indices, &mut seen) {
                                Err(e) => return Some((Err(e), (input, seen))),
                                Ok(b) if b.num_rows() == 0 => continue,
                                Ok(b) => b,
                            };
                            let len = filtered.num_rows();
                            let retracted = {
                                let mut b = BooleanBuilder::with_capacity(len);
                                b.append_n(len, false);
                                b.finish()
                            };
                            return Some((
                                Ok(FlowEvent::Data {
                                    batch: filtered,
                                    retracted,
                                }),
                                (input, seen),
                            ));
                        }
                        Some(Ok(other)) => {
                            return Some((Ok(other), (input, seen)));
                        }
                    }
                }
            }
        });

        let flow_stream = Box::pin(FlowEventStreamAdapter::new(data_schema, event_stream));
        Ok(Box::pin(EncodedFlowEventStream::new(flow_stream)))
    }
}

/// Returns a filtered `RecordBatch` containing only rows whose key tuple
/// has not been seen before. Newly-seen keys are inserted into `seen`.
fn filter_new_rows(
    batch: &RecordBatch,
    key_indices: &[usize],
    seen: &mut HashSet<Vec<ScalarValue>>,
) -> Result<RecordBatch> {
    let mut keep = BooleanBuilder::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let key: Vec<ScalarValue> = key_indices
            .iter()
            .map(|&col_idx| ScalarValue::try_from_array(batch.column(col_idx), row_idx))
            .collect::<Result<_>>()?;
        keep.append_value(seen.insert(key));
    }
    Ok(compute::filter_record_batch(batch, &keep.finish())?)
}
