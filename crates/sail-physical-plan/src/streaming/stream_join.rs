//! Stateful stream–stream equi-join (`StreamJoinExec`).
//!
//! Both inputs are unbounded flow-event streams. The operator buffers rows from each
//! side and, when a batch arrives on one side, joins it against the **accumulated**
//! batches of the other side (via DataFusion's `HashJoinExec`). Each matching pair is
//! emitted exactly once: when side A's batch arrives it is joined against all prior B
//! rows; when side B's batch arrives it is joined against all prior A rows — so a pair
//! is produced exactly when the second of the two rows arrives.
//!
//! The operator's watermark is the **minimum** of the two inputs' watermarks (Flink
//! semantics), forwarded downstream as it advances.
//!
//! Scope (first version): inner equi-join, append-only. Outer joins and watermark-based
//! state eviction are documented follow-ups (see docs/design/streaming-stream-join.md).

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use chrono::DateTime;
use datafusion::arrow::array::{BooleanBuilder, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::NullEquality;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExprRef};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{internal_err, JoinType, Result};
use futures::{stream, StreamExt};
use sail_common_datafusion::streaming::event::encoding::{
    DecodedFlowEventStream, EncodedFlowEventStream,
};
use sail_common_datafusion::streaming::event::marker::FlowMarker;
use sail_common_datafusion::streaming::event::schema::to_flow_event_schema;
use sail_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;
use sail_common_datafusion::streaming::event::FlowEvent;

use crate::streaming::window_accum::StaticBatchExec;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Left,
    Right,
}

#[derive(Debug)]
pub struct StreamJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    /// Equi-join key pairs `(left_key, right_key)` against the decoded data schemas.
    on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    join_type: JoinType,
    left_data_schema: SchemaRef,
    right_data_schema: SchemaRef,
    /// Join output data schema (left ++ right columns).
    output_data_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl StreamJoinExec {
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
        join_type: JoinType,
        left_data_schema: SchemaRef,
        right_data_schema: SchemaRef,
    ) -> Result<Self> {
        // Compute the join output data schema with a trial join over empty inputs.
        let output_data_schema = {
            let trial = HashJoinExec::try_new(
                Arc::new(EmptyExec::new(left_data_schema.clone())),
                Arc::new(EmptyExec::new(right_data_schema.clone())),
                on.clone(),
                None,
                &join_type,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )?;
            trial.schema()
        };
        let flow_schema = Arc::new(to_flow_event_schema(&output_data_schema));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(flow_schema),
            datafusion::physical_expr::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        ));
        Ok(Self {
            left,
            right,
            on,
            join_type,
            left_data_schema,
            right_data_schema,
            output_data_schema,
            properties,
        })
    }
}

impl DisplayAs for StreamJoinExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "StreamJoinExec: type={:?}, on={:?}", self.join_type, self.on)
    }
}

/// Join `left_batches ⋈ right_batches` on the equi-keys, returning output batches.
async fn run_join(
    left_batches: Vec<RecordBatch>,
    right_batches: Vec<RecordBatch>,
    on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    join_type: JoinType,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    ctx: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    let left = Arc::new(StaticBatchExec::new(left_batches, left_schema));
    let right = Arc::new(StaticBatchExec::new(right_batches, right_schema));
    let join = HashJoinExec::try_new(
        left,
        right,
        on,
        None,
        &join_type,
        None,
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNothing,
        false,
    )?;
    let mut s = join.execute(0, ctx)?;
    let mut out = vec![];
    while let Some(b) = s.next().await {
        let b = b?;
        if b.num_rows() > 0 {
            out.push(b);
        }
    }
    Ok(out)
}

/// Build a `FlowEvent::Data` (append-only, all `retracted = false`).
fn data_event(batch: RecordBatch) -> FlowEvent {
    let len = batch.num_rows();
    let mut b = BooleanBuilder::with_capacity(len);
    b.append_n(len, false);
    FlowEvent::Data {
        batch,
        retracted: b.finish(),
    }
}

type JoinState = (
    futures::stream::SelectAll<
        std::pin::Pin<Box<dyn stream::Stream<Item = (Side, Result<FlowEvent>)> + Send>>,
    >,
    Vec<RecordBatch>, // left buffered
    Vec<RecordBatch>, // right buffered
    Option<i64>,      // left watermark (micros)
    Option<i64>,      // right watermark (micros)
    Option<i64>,      // last emitted (min) watermark
    VecDeque<FlowEvent>,
    Arc<TaskContext>,
);

impl ExecutionPlan for StreamJoinExec {
    fn name(&self) -> &str {
        "StreamJoinExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [left, right]: [Arc<dyn ExecutionPlan>; 2] = children
            .try_into()
            .map_err(|_| datafusion_common::DataFusionError::Internal(
                "StreamJoinExec requires exactly two children".to_string(),
            ))?;
        Ok(Arc::new(StreamJoinExec::try_new(
            left,
            right,
            self.on.clone(),
            self.join_type,
            self.left_data_schema.clone(),
            self.right_data_schema.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("StreamJoinExec: invalid partition {partition}");
        }
        let left_stream = DecodedFlowEventStream::try_new(self.left.execute(0, context.clone())?)?;
        let right_stream = DecodedFlowEventStream::try_new(self.right.execute(0, context.clone())?)?;
        // Tag each side and merge into one stream.
        let left_tagged: std::pin::Pin<Box<dyn stream::Stream<Item = (Side, Result<FlowEvent>)> + Send>> =
            Box::pin(left_stream.map(|e| (Side::Left, e)));
        let right_tagged: std::pin::Pin<Box<dyn stream::Stream<Item = (Side, Result<FlowEvent>)> + Send>> =
            Box::pin(right_stream.map(|e| (Side::Right, e)));
        let merged = stream::select_all(vec![left_tagged, right_tagged]);

        let on = self.on.clone();
        let join_type = self.join_type;
        let left_schema = self.left_data_schema.clone();
        let right_schema = self.right_data_schema.clone();

        let init: JoinState = (
            merged,
            vec![],
            vec![],
            None,
            None,
            None,
            VecDeque::new(),
            context,
        );

        let event_stream = stream::unfold(init, move |state| {
            let (mut merged, mut left_buf, mut right_buf, mut lwm, mut rwm, mut last_wm, mut buf, ctx) =
                state;
            let on = on.clone();
            let left_schema = left_schema.clone();
            let right_schema = right_schema.clone();
            async move {
                loop {
                    if let Some(ev) = buf.pop_front() {
                        return Some((
                            Ok(ev),
                            (merged, left_buf, right_buf, lwm, rwm, last_wm, buf, ctx),
                        ));
                    }
                    match merged.next().await {
                        None => return None,
                        Some((_, Err(e))) => {
                            return Some((
                                Err(e),
                                (merged, left_buf, right_buf, lwm, rwm, last_wm, buf, ctx),
                            ))
                        }
                        Some((side, Ok(FlowEvent::Data { batch, .. }))) => {
                            if batch.num_rows() == 0 {
                                continue;
                            }
                            let res = match side {
                                Side::Left => {
                                    run_join(
                                        vec![batch.clone()],
                                        right_buf.clone(),
                                        on.clone(),
                                        join_type,
                                        left_schema.clone(),
                                        right_schema.clone(),
                                        ctx.clone(),
                                    )
                                    .await
                                }
                                Side::Right => {
                                    run_join(
                                        left_buf.clone(),
                                        vec![batch.clone()],
                                        on.clone(),
                                        join_type,
                                        left_schema.clone(),
                                        right_schema.clone(),
                                        ctx.clone(),
                                    )
                                    .await
                                }
                            };
                            match res {
                                Err(e) => {
                                    return Some((
                                        Err(e),
                                        (merged, left_buf, right_buf, lwm, rwm, last_wm, buf, ctx),
                                    ))
                                }
                                Ok(out) => {
                                    for b in out {
                                        buf.push_back(data_event(b));
                                    }
                                }
                            }
                            match side {
                                Side::Left => left_buf.push(batch),
                                Side::Right => right_buf.push(batch),
                            }
                        }
                        Some((side, Ok(FlowEvent::Marker(FlowMarker::Watermark { timestamp, .. })))) => {
                            let ts = timestamp.timestamp_micros();
                            match side {
                                Side::Left => lwm = Some(lwm.map_or(ts, |c| c.max(ts))),
                                Side::Right => rwm = Some(rwm.map_or(ts, |c| c.max(ts))),
                            }
                            // Operator watermark = min of both inputs (only once both seen).
                            if let (Some(l), Some(r)) = (lwm, rwm) {
                                let m = l.min(r);
                                if last_wm.is_none_or(|prev| m > prev) {
                                    last_wm = Some(m);
                                    if let Some(t) = DateTime::from_timestamp_micros(m) {
                                        buf.push_back(FlowEvent::Marker(FlowMarker::Watermark {
                                            source: "stream-join".to_string(),
                                            timestamp: t,
                                        }));
                                    }
                                }
                            }
                        }
                        Some((_, Ok(other))) => {
                            buf.push_back(other);
                        }
                    }
                }
            }
        });

        let flow_stream =
            Box::pin(FlowEventStreamAdapter::new(self.output_data_schema.clone(), event_stream));
        Ok(Box::pin(EncodedFlowEventStream::new(flow_stream)))
    }
}
