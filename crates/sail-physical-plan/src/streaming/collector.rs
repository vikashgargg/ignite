use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{arrow_datafusion_err, internal_err, plan_err, Result};
use futures::{stream, StreamExt};
use sail_common_datafusion::streaming::event::encoding::DecodedFlowEventStream;
use sail_common_datafusion::streaming::event::schema::try_from_flow_event_schema;
use sail_common_datafusion::streaming::event::FlowEvent;

/// A physical plan node that collects a stream of retractable data batches
/// into final data batches.
/// The input schema must be a flow event schema, while the output schema
/// is the corresponding data schema.
#[derive(Debug)]
pub struct StreamCollectorExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl StreamCollectorExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        if input.properties().boundedness != Boundedness::Bounded {
            return plan_err!("stream collector requires bounded input");
        }
        let schema = Arc::new(try_from_flow_event_schema(&input.schema())?);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            // We emit data at the end since we need to handle retractions.
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self { input, properties })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for StreamCollectorExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "{}", Self::static_name())
    }
}

impl ExecutionPlan for StreamCollectorExec {
    fn name(&self) -> &str {
        Self::static_name()
    }


    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
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
        Ok(Arc::new(StreamCollectorExec::try_new(child)?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("invalid partition for {}: {partition}", self.name());
        }

        if self.input.output_partitioning().partition_count() != 1 {
            return internal_err!("{} requires a single input partition", self.name());
        }
        let in_stream = self.input.execute(partition, context)?;
        let out_schema = self.schema();
        // Bounded collect: materialize the changelog into a final table. Each retraction row
        // (`retracted = true`) exactly equals a previously-inserted row (the operator emits the
        // prior value verbatim), so we net by full-row identity — no group-key knowledge needed.
        // Insert (`false`) ⇒ +1, retract (`true`) ⇒ −1; surviving rows (count > 0) are the
        // converged result. Append-only input has no retracts, so this is just a full collect.
        // Emission is `Final` (declared above), so buffering to end is the intended contract.
        let collect_schema = out_schema.clone();
        let fut = async move {
            let mut decoded = DecodedFlowEventStream::try_new(in_stream)?;
            // row-identity -> (net count, one representative single-row batch to re-emit)
            let mut table: HashMap<OwnedRow, (i64, RecordBatch)> = HashMap::new();
            let mut order: Vec<OwnedRow> = vec![]; // stable output order (first-seen)
            let mut converter: Option<RowConverter> = None;
            while let Some(event) = decoded.next().await {
                match event {
                    Ok(FlowEvent::Marker(_)) => {}
                    Ok(FlowEvent::Data { batch, retracted }) => {
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        let conv = match &mut converter {
                            Some(c) => c,
                            None => {
                                let c = RowConverter::new(
                                    batch
                                        .columns()
                                        .iter()
                                        .map(|c| SortField::new(c.data_type().clone()))
                                        .collect(),
                                )
                                .map_err(|e| arrow_datafusion_err!(e))?;
                                converter.get_or_insert(c)
                            }
                        };
                        let rows = conv
                            .convert_columns(batch.columns())
                            .map_err(|e| arrow_datafusion_err!(e))?;
                        for i in 0..batch.num_rows() {
                            let key = rows.row(i).owned();
                            let delta = if retracted.value(i) { -1 } else { 1 };
                            let entry = table.entry(key.clone()).or_insert_with(|| {
                                order.push(key.clone());
                                (0, batch.slice(i, 1))
                            });
                            entry.0 += delta;
                        }
                    }
                    Err(e) => return Err(e),
                }
            }
            // Emit surviving rows (net count > 0) in first-seen order.
            let mut survivors: Vec<RecordBatch> = vec![];
            for key in &order {
                if let Some((count, row)) = table.get(key) {
                    for _ in 0..(*count).max(0) {
                        survivors.push(row.clone());
                    }
                }
            }
            if survivors.is_empty() {
                Ok(RecordBatch::new_empty(collect_schema))
            } else {
                concat_batches(&collect_schema, &survivors).map_err(|e| arrow_datafusion_err!(e))
            }
        };
        // Yield the single materialized batch (Final emission).
        let stream = stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// StreamCollectorExec changelog-netting test: a bounded changelog stream
// (insert / retract+insert / standalone insert) must materialize to the
// converged table (docs/STREAMING_ARCHITECTURE.md: output ≡ batch on a snapshot).
// ---------------------------------------------------------------------------
#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{BooleanArray, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
    use futures::{stream, TryStreamExt};
    use sail_common_datafusion::streaming::event::encoding::EncodedFlowEventStream;
    use sail_common_datafusion::streaming::event::marker::FlowMarker;
    use sail_common_datafusion::streaming::event::schema::to_flow_event_schema;
    use sail_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;
    use sail_common_datafusion::streaming::event::FlowEvent;

    use super::*;

    #[derive(Debug)]
    struct Src {
        events: Vec<FlowEvent>,
        data_schema: SchemaRef,
        properties: Arc<PlanProperties>,
    }
    impl Src {
        fn new(events: Vec<FlowEvent>, data_schema: SchemaRef) -> Self {
            let flow = Arc::new(to_flow_event_schema(&data_schema));
            let properties = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(flow),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Both,
                Boundedness::Bounded,
            ));
            Self { events, data_schema, properties }
        }
    }
    impl DisplayAs for Src {
        fn fmt_as(&self, _: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "Src")
        }
    }
    impl ExecutionPlan for Src {
        fn name(&self) -> &str { "Src" }
        fn properties(&self) -> &Arc<PlanProperties> { &self.properties }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }
        fn with_new_children(self: Arc<Self>, _: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> { Ok(self) }
        fn execute(&self, _p: usize, _c: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
            let s = stream::iter(self.events.clone().into_iter().map(Ok));
            let flow = Box::pin(FlowEventStreamAdapter::new(self.data_schema.clone(), s));
            Ok(Box::pin(EncodedFlowEventStream::new(flow)))
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("count", DataType::Int64, false),
        ]))
    }
    fn row(s: &SchemaRef, k: i64, c: i64, retracted: bool) -> FlowEvent {
        let batch = RecordBatch::try_new(
            s.clone(),
            vec![Arc::new(Int64Array::from(vec![k])), Arc::new(Int64Array::from(vec![c]))],
        )
        .unwrap();
        FlowEvent::Data { batch, retracted: BooleanArray::from(vec![retracted]) }
    }

    #[tokio::test]
    async fn collector_materializes_changelog_to_converged_table() {
        let s = schema();
        let events = vec![
            row(&s, 1, 5, false),                                  // insert k1=5
            row(&s, 2, 3, false),                                  // insert k2=3 (never changes)
            row(&s, 1, 5, true),                                   // retract k1=5
            row(&s, 1, 7, false),                                  // insert k1=7 (converged)
            FlowEvent::Marker(FlowMarker::EndOfData),
        ];
        let src = Arc::new(Src::new(events, s.clone()));
        let collector = Arc::new(StreamCollectorExec::try_new(src).unwrap());
        let out: Vec<RecordBatch> = collector
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        // Net result: k1=7 (5 retracted), k2=3.
        let mut pairs: Vec<(i64, i64)> = vec![];
        for b in &out {
            let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let c = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                pairs.push((k.value(i), c.value(i)));
            }
        }
        pairs.sort_unstable();
        assert_eq!(pairs, vec![(1, 7), (2, 3)], "collector nets changelog to converged table");
    }
}
