use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanBuilder, RecordBatch};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{internal_err, plan_datafusion_err, plan_err, Result, ScalarValue};
use futures::{stream, StreamExt};
use sail_common_datafusion::streaming::event::encoding::{
    DecodedFlowEventStream, EncodedFlowEventStream,
};
use sail_common_datafusion::streaming::event::marker::FlowMarker;
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
    /// Event-time column for watermark-bounded eviction (`dropDuplicatesWithinWatermark` /
    /// `dropDuplicates` with a watermark). When set, late rows (event-time < watermark) are
    /// dropped and seen-keys older than the watermark are evicted → bounded state. `None` =
    /// unbounded (Spark `dropDuplicates()` with no watermark).
    event_time_col: Option<String>,
    data_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl StreamDeduplicateExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        key_cols: Vec<String>,
        event_time_col: Option<String>,
        data_schema: SchemaRef,
    ) -> Result<Self> {
        let flow_schema = Arc::new(to_flow_event_schema(&data_schema));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(flow_schema),
            datafusion::physical_expr::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            // Matches Spark `dropDuplicates()` (no watermark): the query runs, accumulating
            // the seen-keys set — the unbounded-state risk is the user's, exactly as in
            // Spark. `requires_infinite_memory: true` would make DataFusion's sanity checker
            // refuse to execute it. Watermark-bounded eviction (`dropDuplicatesWithinWatermark`,
            // evict keys below the watermark) is the follow-up for guaranteed-bounded state.
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        ));
        Ok(Self {
            input,
            key_cols,
            event_time_col,
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
            self.event_time_col.clone(),
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
                // Streaming data schemas are unqualified; a key may carry a relation
                // qualifier (`?table?.#0`) — match the unqualified suffix as a fallback.
                self.data_schema
                    .index_of(name.as_str())
                    .or_else(|_| {
                        let unq = name.rsplit('.').next().unwrap_or(name.as_str());
                        self.data_schema.index_of(unq)
                    })
                    .map_err(|_| plan_datafusion_err!("dedup key '{}' not found in schema", name))
            })
            .collect::<Result<_>>()?;

        // Event-time column index for watermark-bounded eviction, if configured.
        let event_time_idx: Option<usize> = match &self.event_time_col {
            Some(name) => Some(
                self.data_schema
                    .index_of(name.as_str())
                    .or_else(|_| {
                        let unq = name.rsplit('.').next().unwrap_or(name.as_str());
                        self.data_schema.index_of(unq)
                    })
                    .map_err(|_| {
                        plan_datafusion_err!("dedup event-time column '{name}' not found in schema")
                    })?,
            ),
            None => None,
        };

        let data_schema = self.data_schema.clone();
        let input_stream =
            DecodedFlowEventStream::try_new(self.input.execute(partition, context)?)?;

        // `seen`: key tuple → latest event-time (micros) of that key, for watermark eviction.
        // `watermark`: latest watermark (micros), only meaningful when `event_time_idx` is set.
        type Seen = HashMap<Vec<ScalarValue>, i64>;
        let init: (DecodedFlowEventStream, Seen, Option<i64>) =
            (input_stream, Seen::new(), None);

        let event_stream = stream::unfold(init, move |(mut input, mut seen, mut watermark)| {
            let key_indices = key_indices.clone();
            async move {
                loop {
                    match input.next().await {
                        None => return None,
                        Some(Err(e)) => return Some((Err(e), (input, seen, watermark))),
                        Some(Ok(FlowEvent::Data { batch, .. })) => {
                            let filtered = match filter_new_rows(
                                &batch,
                                &key_indices,
                                event_time_idx,
                                watermark,
                                &mut seen,
                            ) {
                                Err(e) => return Some((Err(e), (input, seen, watermark))),
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
                                (input, seen, watermark),
                            ));
                        }
                        // Watermark advance: evict seen keys older than the watermark → bounded
                        // state (`dropDuplicatesWithinWatermark` / dropDuplicates+watermark).
                        Some(Ok(FlowEvent::Marker(FlowMarker::Watermark { source, timestamp })))
                            if event_time_idx.is_some() =>
                        {
                            let wm = timestamp.timestamp_micros();
                            let wm = watermark.map_or(wm, |c| c.max(wm));
                            watermark = Some(wm);
                            seen.retain(|_, &mut et| et >= wm);
                            return Some((
                                Ok(FlowEvent::Marker(FlowMarker::Watermark { source, timestamp })),
                                (input, seen, watermark),
                            ));
                        }
                        Some(Ok(other)) => {
                            return Some((Ok(other), (input, seen, watermark)));
                        }
                    }
                }
            }
        });

        let flow_stream = Box::pin(FlowEventStreamAdapter::new(data_schema, event_stream));
        Ok(Box::pin(EncodedFlowEventStream::new(flow_stream)))
    }
}

/// Filter a batch to first-seen rows. With a configured event-time column + watermark:
/// rows older than the watermark are dropped as late, and each kept/duplicate key records its
/// latest event-time (for watermark eviction). Without it, behaves as unbounded distinct.
fn filter_new_rows(
    batch: &RecordBatch,
    key_indices: &[usize],
    event_time_idx: Option<usize>,
    watermark: Option<i64>,
    seen: &mut HashMap<Vec<ScalarValue>, i64>,
) -> Result<RecordBatch> {
    let mut keep = BooleanBuilder::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let event_time = match event_time_idx {
            Some(idx) => event_time_micros(batch.column(idx), row_idx)?,
            None => None,
        };
        // Drop late rows (event-time < watermark) so an evicted key can't slip through.
        if let (Some(et), Some(wm)) = (event_time, watermark) {
            if et < wm {
                keep.append_value(false);
                continue;
            }
        }
        let key: Vec<ScalarValue> = key_indices
            .iter()
            .map(|&col_idx| ScalarValue::try_from_array(batch.column(col_idx), row_idx))
            .collect::<Result<_>>()?;
        let et_store = event_time.unwrap_or(i64::MIN);
        match seen.get_mut(&key) {
            Some(slot) => {
                // Duplicate: drop, but extend retention to its latest event-time.
                *slot = (*slot).max(et_store);
                keep.append_value(false);
            }
            None => {
                seen.insert(key, et_store);
                keep.append_value(true);
            }
        }
    }
    Ok(compute::filter_record_batch(batch, &keep.finish())?)
}

/// Extract a timestamp array element as microseconds, if it is a timestamp.
fn event_time_micros(array: &dyn Array, row: usize) -> Result<Option<i64>> {
    Ok(match ScalarValue::try_from_array(array, row)? {
        ScalarValue::TimestampMicrosecond(v, _) => v,
        ScalarValue::TimestampMillisecond(v, _) => v.map(|x| x.saturating_mul(1000)),
        ScalarValue::TimestampNanosecond(v, _) => v.map(|x| x / 1000),
        ScalarValue::TimestampSecond(v, _) => v.map(|x| x.saturating_mul(1_000_000)),
        _ => None,
    })
}
