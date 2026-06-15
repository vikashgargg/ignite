use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{exec_datafusion_err, plan_err, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::UnKnownColumn;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    internal_err, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties,
};
use futures::future::try_join_all;
use futures::StreamExt;

use sail_common_datafusion::streaming::event::schema::MARKER_FIELD_NAME;

use crate::plan::ListListDisplay;
use crate::stream::writer::{TaskStreamSinkState, TaskStreamWriter, TaskWriteLocation};

/// Is this a flow-event **marker** batch (the `_marker` column has any non-null entry)? Marker
/// batches are broadcast to every shuffle partition; data batches are hash-routed. A non-streaming
/// (batch) shuffle has no `_marker` column, so this is always false there.
fn is_marker_batch(batch: &RecordBatch) -> bool {
    use datafusion::arrow::array::{Array, BinaryArray};
    if let Ok(idx) = batch.schema().index_of(MARKER_FIELD_NAME) {
        if let Some(m) = batch.column(idx).as_any().downcast_ref::<BinaryArray>() {
            return m.null_count() < m.len();
        }
    }
    false
}

#[derive(Debug, Clone)]
pub struct ShuffleWriteExec {
    plan: Arc<dyn ExecutionPlan>,
    /// The partitioning scheme for the shuffle output.
    /// The partition count for the shuffle output can be different from the
    /// partition count of the input plan.
    shuffle_partitioning: Partitioning,
    /// For each input partition, a list of locations to write to.
    locations: Vec<Vec<TaskWriteLocation>>,
    properties: Arc<PlanProperties>,
    writer: Arc<dyn TaskStreamWriter>,
}

impl ShuffleWriteExec {
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        locations: Vec<Vec<TaskWriteLocation>>,
        writer: Arc<dyn TaskStreamWriter>,
        partitioning: Partitioning,
    ) -> Self {
        let partitioning = match partitioning {
            Partitioning::Hash(expr, n) if expr.is_empty() => Partitioning::UnknownPartitioning(n),
            Partitioning::Hash(expr, n) => {
                // https://github.com/apache/arrow-datafusion/issues/5184
                Partitioning::Hash(
                    expr.into_iter()
                        .filter(|e| e.as_any().downcast_ref::<UnKnownColumn>().is_none())
                        .collect(),
                    n,
                )
            }
            _ => partitioning,
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::empty())),
            // The shuffle write plan has the same number of partitions as the input plan.
            // For each partition that are executed, the data is further partitioned according to
            // the shuffle partitioning, resulting in multiple output streams.
            // These output streams are written to locations managed by the worker,
            // while the return value of `.execute()` is always an empty stream.
            Partitioning::UnknownPartitioning(plan.output_partitioning().partition_count()),
            EmissionType::Final,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        ));
        Self {
            plan,
            shuffle_partitioning: partitioning,
            locations,
            properties,
            writer,
        }
    }
}

impl DisplayAs for ShuffleWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ShuffleWriteExec: partitioning={}, locations={}",
            self.shuffle_partitioning,
            ListListDisplay(&self.locations),
        )
    }
}

impl ExecutionPlan for ShuffleWriteExec {
    fn name(&self) -> &str {
        "ShuffleWriteExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let child = children.pop();
        match (child, children.is_empty()) {
            (Some(plan), true) => Ok(Arc::new(Self {
                plan,
                ..self.as_ref().clone()
            })),
            _ => plan_err!("ShuffleWriteExec should have one child"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let locations = self
            .locations
            .get(partition)
            .ok_or_else(|| {
                exec_datafusion_err!("write locations for partition {partition} not found")
            })?
            .clone();
        let writer = self.writer.clone();
        if self.shuffle_partitioning.partition_count() != locations.len() {
            return internal_err!(
                "partition count mismatch: shuffle partitioning has {} partitions, but {} locations were provided",
                self.shuffle_partitioning.partition_count(),
                locations.len()
            );
        }
        let stream = self.plan.execute(partition, context)?;
        // TODO: Revisit this
        let shuffle_partitioning = match &self.shuffle_partitioning {
            Partitioning::UnknownPartitioning(size) => Partitioning::RoundRobinBatch(*size),
            shuffle_partitioning => shuffle_partitioning.clone(),
        };
        // TODO: Support metrics in batch partitioner
        let num_input_partitions = self
            .plan
            .properties()
            .output_partitioning()
            .partition_count();
        let partitioner = BatchPartitioner::try_new(
            shuffle_partitioning,
            Default::default(),
            partition,
            num_input_partitions,
        )?;
        let empty = RecordBatch::new_empty(self.schema());
        let output = futures::stream::once(async move {
            shuffle_write(writer, stream, &locations, partitioner).await?;
            Ok(empty)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}

async fn shuffle_write(
    writer: Arc<dyn TaskStreamWriter>,
    mut stream: SendableRecordBatchStream,
    locations: &[TaskWriteLocation],
    mut partitioner: BatchPartitioner,
) -> Result<()> {
    let schema = stream.schema();
    let mut partition_sinks = {
        let futures = locations
            .iter()
            .map(|location| writer.open(location, schema.clone()));
        try_join_all(futures)
            .await?
            .into_iter()
            .map(Some)
            .collect::<Vec<_>>()
    };
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if is_marker_batch(&batch) {
            // Flow-event markers (watermark / checkpoint barrier / latency / EndOfData) are
            // CONTROL events every downstream partition must observe — **broadcast** them to all
            // sinks, never hash-route (a marker batch has null data in the key columns, so hashing
            // would misroute it). Cross-node counterpart of `StreamExchangeExec`'s marker broadcast,
            // required for distributed barrier alignment (F3). For batch shuffles `is_marker_batch`
            // is always false, so this branch is a no-op there.
            let mut active = 0;
            for sink_slot in partition_sinks.iter_mut() {
                let Some(sink) = sink_slot.as_mut() else {
                    continue;
                };
                active += 1;
                match sink.write(Ok(batch.clone())).await {
                    TaskStreamSinkState::Ok => {}
                    TaskStreamSinkState::Error(e) => return Err(e),
                    TaskStreamSinkState::Closed => {
                        *sink_slot = None;
                        active -= 1;
                    }
                }
            }
            if active == 0 {
                break;
            }
            continue;
        }
        let mut partitions: Vec<Option<RecordBatch>> = vec![None; partition_sinks.len()];
        partitioner.partition(batch, |p, batch| {
            partitions[p] = Some(batch);
            Ok(())
        })?;
        let mut active = 0;
        for p in 0..partitions.len() {
            let Some(sink) = partition_sinks[p].as_mut() else {
                continue;
            };
            // We should update the number of active sinks here,
            // even if the current batch does not have data for this partition.
            active += 1;
            if let Some(batch) = partitions[p].take() {
                match sink.write(Ok(batch)).await {
                    TaskStreamSinkState::Ok => {}
                    TaskStreamSinkState::Error(e) => {
                        return Err(e);
                    }
                    TaskStreamSinkState::Closed => {
                        partition_sinks[p] = None;
                        // This sink is closed when writing this batch,
                        // so we should not consider it active anymore.
                        active -= 1;
                    }
                }
            }
        }
        if active == 0 {
            break;
        }
    }
    // TODO: Ensure the sinks are cleaned up properly when an error causes an early return
    //   of this function. We need to consider this for sinks that handle remote data.
    let futures = partition_sinks
        .into_iter()
        .filter_map(|s| s.map(|x| x.close()));
    try_join_all(futures).await?;
    Ok(())
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;

    use datafusion::arrow::array::{
        new_null_array, ArrayRef, BinaryArray, BooleanArray, Int64Array,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::metrics::Time;
    use sail_common_datafusion::streaming::event::marker::FlowMarker;
    use sail_common_datafusion::streaming::event::schema::{
        MARKER_FIELD_NAME, RETRACTED_FIELD_NAME,
    };

    use super::*;
    use crate::id::{JobId, TaskStreamKey};
    use crate::stream::error::TaskStreamResult;
    use crate::stream::writer::{LocalStreamStorage, TaskStreamSink};

    fn flow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(MARKER_FIELD_NAME, DataType::Binary, true),
            Field::new(RETRACTED_FIELD_NAME, DataType::Boolean, false),
            Field::new("k", DataType::Int64, true),
        ]))
    }

    fn data_batch(ks: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            flow_schema(),
            vec![
                Arc::new(BinaryArray::from(vec![None::<&[u8]>; ks.len()])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![false; ks.len()])) as ArrayRef,
                Arc::new(Int64Array::from(ks.to_vec())) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn marker_batch() -> RecordBatch {
        let bytes = FlowMarker::Checkpoint { id: 7 }.encode().unwrap();
        RecordBatch::try_new(
            flow_schema(),
            vec![
                Arc::new(BinaryArray::from(vec![Some(bytes.as_slice())])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
                new_null_array(&DataType::Int64, 1),
            ],
        )
        .unwrap()
    }

    #[derive(Debug)]
    struct MockWriter {
        recorders: Vec<Arc<StdMutex<Vec<RecordBatch>>>>,
        next: AtomicUsize,
    }

    struct MockSink {
        rec: Arc<StdMutex<Vec<RecordBatch>>>,
    }

    #[tonic::async_trait]
    impl TaskStreamSink for MockSink {
        async fn write(&mut self, batch: TaskStreamResult<RecordBatch>) -> TaskStreamSinkState {
            if let Ok(b) = batch {
                self.rec.lock().unwrap().push(b);
            }
            TaskStreamSinkState::Ok
        }
        async fn close(self: Box<Self>) -> Result<()> {
            Ok(())
        }
    }

    #[tonic::async_trait]
    impl TaskStreamWriter for MockWriter {
        async fn open(
            &self,
            _location: &TaskWriteLocation,
            _schema: Arc<Schema>,
        ) -> Result<Box<dyn TaskStreamSink>> {
            let i = self.next.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(MockSink {
                rec: self.recorders[i].clone(),
            }))
        }
    }

    fn is_marker(b: &RecordBatch) -> bool {
        super::is_marker_batch(b)
    }

    #[tokio::test]
    async fn marker_batches_broadcast_to_all_partitions_data_hash_routed() {
        let n = 3;
        let recorders: Vec<Arc<StdMutex<Vec<RecordBatch>>>> =
            (0..n).map(|_| Arc::new(StdMutex::new(vec![]))).collect();
        let writer = Arc::new(MockWriter {
            recorders: recorders.clone(),
            next: AtomicUsize::new(0),
        });
        let locations: Vec<TaskWriteLocation> = (0..n)
            .map(|p| TaskWriteLocation::Local {
                storage: LocalStreamStorage::Memory { replicas: 1 },
                key: TaskStreamKey {
                    job_id: JobId::from(1u64),
                    stage: 0,
                    partition: p,
                    attempt: 0,
                    channel: 0,
                },
            })
            .collect();
        let partitioner = BatchPartitioner::try_new(
            Partitioning::Hash(vec![Arc::new(Column::new("k", 2))], n),
            Time::default(),
            0,
            1,
        )
        .unwrap();

        // Source: a data batch (30 distinct keys) then a Checkpoint marker, then end.
        let ks: Vec<i64> = (0..30).collect();
        let items: Vec<Result<RecordBatch>> = vec![Ok(data_batch(&ks)), Ok(marker_batch())];
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            flow_schema(),
            futures::stream::iter(items),
        ));

        shuffle_write(writer, stream, &locations, partitioner)
            .await
            .unwrap();

        // Every partition must have received exactly one marker (broadcast).
        for (p, rec) in recorders.iter().enumerate() {
            let batches = rec.lock().unwrap();
            let markers = batches.iter().filter(|b| is_marker(b)).count();
            assert_eq!(markers, 1, "partition {p} must get exactly one broadcast marker");
        }
        // All 30 data rows are preserved across partitions (hash-routed, none lost/duplicated).
        let total_data: usize = recorders
            .iter()
            .flat_map(|r| r.lock().unwrap().iter().filter(|b| !is_marker(b)).map(|b| b.num_rows()).collect::<Vec<_>>())
            .sum();
        assert_eq!(total_data, 30, "all data rows hash-routed exactly once");
    }
}
