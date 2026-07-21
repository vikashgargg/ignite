use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{arrow_datafusion_err, plan_err, Result};
use futures::{Stream, StreamExt};
use zelox_common_datafusion::streaming::checkpoint::CheckpointStore;
use zelox_common_datafusion::streaming::event::encoding::EncodedFlowEventStream;
use zelox_common_datafusion::streaming::event::marker::FlowMarker;
use zelox_common_datafusion::streaming::event::schema::to_flow_event_schema;
use zelox_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;
use zelox_common_datafusion::streaming::event::FlowEvent;
use zelox_common_datafusion::streaming::source::StreamSource;

use crate::options::gen::RateReadOptions;

#[derive(Debug, Clone)]
pub struct RateStreamSource {
    options: RateReadOptions,
    schema: SchemaRef,
}

impl RateStreamSource {
    pub fn try_new(options: RateReadOptions, schema: SchemaRef) -> Result<Self> {
        Self::validate_schema(&schema)?;
        Ok(Self { options, schema })
    }

    fn validate_schema(schema: &Schema) -> Result<()> {
        match schema.fields.iter().as_slice() {
            [t, v] => {
                if !matches!(
                    t.data_type(),
                    DataType::Timestamp(TimeUnit::Microsecond, Some(_tz))
                ) {
                    plan_err!("invalid timestamp type for rate table")
                } else if !matches!(v.data_type(), DataType::Int64) {
                    plan_err!("invalid value type for rate table")
                } else {
                    Ok(())
                }
            }
            _ => {
                plan_err!("invalid schema for rate table")
            }
        }
    }
}

#[async_trait]
impl StreamSource for RateStreamSource {
    fn data_schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
        bounded: bool,
        checkpoint_location: Option<&str>,
        _realtime_interval_ms: Option<u64>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields.len()).collect());
        // Restore: resume from the last committed offset (exactly-once recovery), so a
        // restart replays from where the previous run committed rather than from 0.
        let mut options = self.options.clone();
        if let Some(loc) = checkpoint_location {
            if let Ok(ck) = CheckpointStore::from_location(loc) {
                if let Some(committed) = read_committed_offset(&ck).await {
                    options.start_offset = committed;
                }
            }
        }
        Ok(Arc::new(RateSourceExec::try_new(
            options,
            Arc::clone(&self.schema),
            projection,
            bounded,
            checkpoint_location.map(str::to_string),
        )?))
    }
}

/// Read the last durably-committed row offset, if any (single-object `sources/0/committed`).
pub async fn read_committed_offset(ck: &CheckpointStore) -> Option<usize> {
    let bytes = ck.get("sources/0/committed").await.ok().flatten()?;
    String::from_utf8_lossy(&bytes).trim().parse::<usize>().ok()
}

/// Stage (write-ahead) the end offset reached by the current batch as a single object. The runner
/// promotes `sources/0/staged` → `committed` only after the batch's output is durable — see
/// `StreamingQuery::run` and docs/design/streaming-exactly-once.md.
async fn write_staged_offset(ck: &CheckpointStore, offset: usize) {
    let _ = ck
        .put("sources/0/staged", bytes::Bytes::from(offset.to_string()))
        .await;
}

#[derive(Debug)]
pub struct RateSourceExec {
    options: RateReadOptions,
    time_zone: Arc<str>,
    original_schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Vec<usize>,
    /// Trigger `availableNow`/`once`: emit one batch of available rows then stop.
    bounded: bool,
    /// Streaming `checkpointLocation`, when set — to stage the offset reached for
    /// exactly-once recovery (the runner commits it after the output is durable).
    checkpoint_location: Option<String>,
    properties: Arc<PlanProperties>,
}

impl RateSourceExec {
    /// Creates a new execution plan for the rate source.
    /// The schema should be the original schema before projection.
    pub fn try_new(
        options: RateReadOptions,
        schema: SchemaRef,
        projection: Vec<usize>,
        bounded: bool,
        checkpoint_location: Option<String>,
    ) -> Result<Self> {
        let time_zone = Self::infer_time_zone(&schema)?;
        let projected_schema = Arc::new(schema.project(&projection)?);
        let output_schema = Arc::new(to_flow_event_schema(&projected_schema));
        let boundedness = if bounded {
            Boundedness::Bounded
        } else {
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            }
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(options.num_partitions),
            EmissionType::Both,
            boundedness,
        ));
        Ok(Self {
            options,
            time_zone,
            original_schema: schema,
            projected_schema,
            projection,
            bounded,
            checkpoint_location,
            properties,
        })
    }

    pub fn options(&self) -> &RateReadOptions {
        &self.options
    }

    pub fn original_schema(&self) -> &SchemaRef {
        &self.original_schema
    }

    pub fn projection(&self) -> &[usize] {
        &self.projection
    }

    pub fn bounded(&self) -> bool {
        self.bounded
    }

    fn infer_time_zone(schema: &Schema) -> Result<Arc<str>> {
        match schema.fields.iter().as_slice() {
            [t, _] => {
                if let DataType::Timestamp(_, Some(tz)) = t.data_type() {
                    Ok(Arc::clone(tz))
                } else {
                    plan_err!("invalid timestamp type for rate table schema")
                }
            }
            _ => {
                plan_err!("invalid schema for rate table")
            }
        }
    }
}

impl DisplayAs for RateSourceExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "{}: rows_per_second={}, num_partitions={}",
            self.name(),
            self.options.rows_per_second,
            self.options.num_partitions
        )
    }
}

impl ExecutionPlan for RateSourceExec {
    fn name(&self) -> &str {
        Self::static_name()
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
            plan_err!("{} cannot have children", self.name())
        } else {
            Ok(self)
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.options.num_partitions {
            return plan_err!(
                "invalid partition {} for {} with {} partition(s)",
                partition,
                self.name(),
                self.options.num_partitions
            );
        }
        if self.bounded {
            // Trigger availableNow/once: emit one batch of the currently-available
            // rows, then EndOfData, then end the stream so the query terminates
            // instead of running continuously.
            let rows = (self.options.rows_per_second / self.options.num_partitions).max(1);
            // Partition p emits disjoint, globally-contiguous values: start at p, stride by N.
            let mut generator = BatchGenerator::try_new(
                Arc::clone(&self.time_zone),
                &self.projection,
                self.projected_schema.clone(),
                self.options.start_offset + partition,
                self.options.num_partitions,
            )?;
            // Write-ahead (in the async stream): stage the end offset this batch reaches. The
            // runner promotes it to committed only after the output is durable (exactly-once).
            // Single-partition only — multi-partition offset recovery needs the checkpoint
            // coordinator (docs/design/streaming-parallelism.md, Phase 3).
            let ck = if self.options.num_partitions == 1 {
                self.checkpoint_location
                    .as_deref()
                    .and_then(|l| CheckpointStore::from_location(l).ok())
            } else {
                None
            };
            let staged_offset = self.options.start_offset + rows;
            let data = generator.generate(rows).map(FlowEvent::append_only_data);
            let events = async_stream::stream! {
                if let Some(ck) = &ck {
                    write_staged_offset(ck, staged_offset).await;
                }
                yield data;
                yield Ok(FlowEvent::Marker(FlowMarker::EndOfData));
            };
            let stream = Box::pin(FlowEventStreamAdapter::new(
                self.projected_schema.clone(),
                events,
            ));
            return Ok(Box::pin(EncodedFlowEventStream::new(stream)));
        }
        // TODO: consider token bucket algorithm for data generation with a more stable rate
        // TODO: make the data generation algorithm configurable
        let output: Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>> =
            if self.options.rows_per_second == 0 {
                let output = futures::stream::unfold((), |()| async move {
                    tokio::time::sleep(Duration::MAX).await;
                    None
                });
                Box::pin(output)
            } else {
                let rows_per_second =
                    (self.options.rows_per_second / self.options.num_partitions).max(1);
                // We generate at most 1000 batches per second
                // since the sleep function only has millisecond accuracy.
                let batches_per_second = rows_per_second.min(1_000);
                let batch_size = rows_per_second / batches_per_second;
                let interval = Duration::from_secs(1) / (batches_per_second as u32);
                // Partition p emits disjoint, globally-contiguous values: start at p, stride by N.
                let generator = BatchGenerator::try_new(
                    Arc::clone(&self.time_zone),
                    &self.projection,
                    self.projected_schema.clone(),
                    self.options.start_offset + partition,
                    self.options.num_partitions,
                )?;
                let output = futures::stream::unfold(generator, move |mut generator| async move {
                    // The interval does not take into account the time it takes to generate data,
                    // but the sleep itself is inaccurate anyway.
                    tokio::time::sleep(interval).await;
                    let result = generator.generate(batch_size);
                    Some((result, generator))
                });
                Box::pin(output)
            };
        // Emit a LatencyTracker marker before each data batch so downstream operators
        // (and sinks) can measure end-to-end latency as now() - emission timestamp.
        let output = output.flat_map(|x| {
            futures::stream::iter(vec![
                Ok(FlowEvent::Marker(FlowMarker::LatencyTracker {
                    source: "rate".to_string(),
                    id: 0,
                    timestamp: chrono::Utc::now(),
                })),
                x.map(FlowEvent::append_only_data),
            ])
        });
        let stream = Box::pin(FlowEventStreamAdapter::new(
            self.projected_schema.clone(),
            output,
        ));
        Ok(Box::pin(EncodedFlowEventStream::new(stream)))
    }
}

/// The action for generating each column in the record batch.
enum BatchGeneratorAction {
    /// Generates a timestamp array.
    Timestamp,
    /// Generates a value array.
    Value,
    /// Copies a previously generated array.
    Copy(usize),
}

struct BatchGenerator {
    offset: usize,
    /// Step between successive values. For an N-partition source each partition strides by
    /// N starting at its index, so partitions emit disjoint, globally-contiguous values
    /// (Spark rate-source semantics): partition p → p, p+N, p+2N, … `stride == 1` for the
    /// single-partition case (unchanged behavior).
    stride: usize,
    projected_schema: SchemaRef,
    time_zone: Arc<str>,
    actions: Vec<BatchGeneratorAction>,
}

impl BatchGenerator {
    fn try_new(
        time_zone: Arc<str>,
        projection: &[usize],
        projected_schema: SchemaRef,
        start_offset: usize,
        stride: usize,
    ) -> Result<Self> {
        let mut actions = vec![];
        let mut timestamp_index = None;
        let mut value_index = None;
        for i in projection {
            match i {
                0 => {
                    if let Some(j) = timestamp_index {
                        actions.push(BatchGeneratorAction::Copy(j));
                    } else {
                        timestamp_index = Some(actions.len());
                        actions.push(BatchGeneratorAction::Timestamp);
                    }
                }
                1 => {
                    if let Some(j) = value_index {
                        actions.push(BatchGeneratorAction::Copy(j));
                    } else {
                        value_index = Some(actions.len());
                        actions.push(BatchGeneratorAction::Value);
                    }
                }
                _ => {
                    return plan_err!("invalid projection index {i} for rate source table");
                }
            }
        }
        Ok(Self {
            offset: start_offset,
            stride: stride.max(1),
            projected_schema,
            time_zone,
            actions,
        })
    }

    fn generate(&mut self, batch_size: usize) -> Result<RecordBatch> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.actions.len());
        for action in &self.actions {
            match action {
                BatchGeneratorAction::Timestamp => {
                    let ts = chrono::Utc::now().timestamp_micros();
                    let array = TimestampMicrosecondArray::from(vec![ts; batch_size])
                        .with_timezone(Arc::clone(&self.time_zone));
                    columns.push(Arc::new(array) as _);
                }
                BatchGeneratorAction::Value => {
                    let values = (0..batch_size)
                        .map(|i| (self.offset + i * self.stride) as i64)
                        .collect::<Vec<_>>();
                    let array = Int64Array::from(values);
                    columns.push(Arc::new(array) as _);
                }
                BatchGeneratorAction::Copy(index) => {
                    columns.push(columns[*index].clone());
                }
            }
        }
        self.offset += batch_size * self.stride;
        RecordBatch::try_new(self.projected_schema.clone(), columns)
            .map_err(|e| arrow_datafusion_err!(e))
    }
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::execution_plan::Boundedness;
    use datafusion::physical_plan::ExecutionPlan;
    use futures::TryStreamExt;

    use super::RateSourceExec;
    use crate::options::gen::RateReadOptions;

    fn rate_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn options() -> RateReadOptions {
        RateReadOptions {
            rows_per_second: 5,
            num_partitions: 1,
            start_offset: 0,
        }
    }

    // Trigger availableNow/once: the bounded rate source must end its stream so the
    // streaming query terminates. An unbounded source would hang this collect.
    #[tokio::test]
    async fn bounded_rate_source_terminates() {
        let exec =
            RateSourceExec::try_new(options(), rate_schema(), vec![0, 1], true, None).unwrap();
        assert!(matches!(
            exec.properties().boundedness,
            Boundedness::Bounded
        ));
        let stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        // The outer unwrap panics (failing the test) if the source does not terminate.
        let batches = tokio::time::timeout(Duration::from_secs(10), stream.try_collect::<Vec<_>>())
            .await
            .unwrap()
            .unwrap();
        assert!(!batches.is_empty(), "bounded rate source produced no data");
    }

    // Default (continuous) rate source stays unbounded.
    #[tokio::test]
    async fn unbounded_rate_source_is_unbounded() {
        let exec =
            RateSourceExec::try_new(options(), rate_schema(), vec![0, 1], false, None).unwrap();
        assert!(matches!(
            exec.properties().boundedness,
            Boundedness::Unbounded { .. }
        ));
    }
}
