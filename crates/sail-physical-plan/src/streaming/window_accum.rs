use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, BooleanArray, BooleanBuilder, RecordBatch, StructArray, TimestampMicrosecondArray,
};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{internal_err, plan_datafusion_err, plan_err, Result};
use futures::{stream, StreamExt};
use sail_common_datafusion::streaming::event::encoding::{
    DecodedFlowEventStream, EncodedFlowEventStream,
};
use sail_common_datafusion::streaming::event::marker::FlowMarker;
use sail_common_datafusion::streaming::event::schema::to_flow_event_schema;
use sail_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;
use sail_common_datafusion::streaming::event::FlowEvent;

// ---------------------------------------------------------------------------
// StaticBatchExec — feeds Vec<RecordBatch> into AggregateExec
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct StaticBatchExec {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl StaticBatchExec {
    fn new(batches: Vec<RecordBatch>, schema: SchemaRef) -> Self {
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            datafusion::physical_expr::Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            batches,
            schema,
            properties: props,
        }
    }
}

impl DisplayAs for StaticBatchExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "StaticBatchExec")
    }
}

impl ExecutionPlan for StaticBatchExec {
    fn name(&self) -> &str {
        "StaticBatchExec"
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
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(
        &self,
        _partition: usize,
        _ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema.clone();
        let batches = self.batches.clone();
        let s = stream::iter(
            batches
                .into_iter()
                .map(Ok::<_, datafusion_common::DataFusionError>),
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s)))
    }
}

// ---------------------------------------------------------------------------
// Per-stream state (lives for the lifetime of one execute() call)
// ---------------------------------------------------------------------------

struct AccumState {
    pending_rows: Vec<RecordBatch>,
    watermark_micros: Option<i64>,
    event_time_col_idx: usize,
    delay_micros: i64,
}

impl AccumState {
    fn new(event_time_col_idx: usize, delay_micros: i64) -> Self {
        Self {
            pending_rows: vec![],
            watermark_micros: None,
            event_time_col_idx,
            delay_micros,
        }
    }

    fn push(&mut self, batch: RecordBatch) {
        if batch.num_rows() == 0 {
            return;
        }
        let col = batch.column(self.event_time_col_idx);
        let max_ts: Option<i64> = match col.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, _) => col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .and_then(compute::max),
            _ => None,
        };
        if let Some(ts) = max_ts {
            let wm = ts - self.delay_micros;
            self.watermark_micros = Some(self.watermark_micros.map_or(wm, |c| c.max(wm)));
        }
        self.pending_rows.push(batch);
    }
}

// ---------------------------------------------------------------------------
// WindowAccumExec
// ---------------------------------------------------------------------------

/// Stateful event-time window aggregation for Spark Structured Streaming.
///
/// Wraps the data-only (flow-event stripped) streaming input and:
/// 1. Buffers incoming data rows across micro-batches.
/// 2. Tracks the watermark as `max(event_time) - delay_micros`.
/// 3. At each `Checkpoint` marker: re-aggregates all pending rows, emits
///    windows whose `end ≤ watermark`, then passes through the checkpoint.
#[derive(Debug)]
pub struct WindowAccumExec {
    input: Arc<dyn ExecutionPlan>,
    group_exprs: Arc<PhysicalGroupBy>,
    aggr_exprs: Vec<Arc<AggregateFunctionExpr>>,
    data_input_schema: SchemaRef,
    agg_output_schema: SchemaRef,
    event_time_col: String,
    delay_micros: i64,
    properties: Arc<PlanProperties>,
}

impl WindowAccumExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        group_exprs: PhysicalGroupBy,
        aggr_exprs: Vec<Arc<AggregateFunctionExpr>>,
        data_input_schema: SchemaRef,
        event_time_col: String,
        delay_micros: i64,
    ) -> Result<Self> {
        let agg_output_schema = {
            let empty = Arc::new(EmptyExec::new(data_input_schema.clone()));
            let trial = AggregateExec::try_new(
                AggregateMode::Single,
                group_exprs.clone(),
                aggr_exprs.clone(),
                vec![None; aggr_exprs.len()],
                empty,
                data_input_schema.clone(),
            )?;
            trial.schema()
        };
        let flow_schema = Arc::new(to_flow_event_schema(&agg_output_schema));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(flow_schema),
            datafusion::physical_expr::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        ));
        Ok(Self {
            input,
            group_exprs: Arc::new(group_exprs),
            aggr_exprs,
            data_input_schema,
            agg_output_schema,
            event_time_col,
            delay_micros,
            properties,
        })
    }

    fn event_time_col_idx(&self) -> Option<usize> {
        self.data_input_schema
            .fields()
            .iter()
            .position(|f| f.name().eq_ignore_ascii_case(&self.event_time_col))
    }
}

impl DisplayAs for WindowAccumExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "WindowAccumExec: eventTime={}, delay={}µs",
            self.event_time_col, self.delay_micros
        )
    }
}

impl ExecutionPlan for WindowAccumExec {
    fn name(&self) -> &str {
        "WindowAccumExec"
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
        Ok(Arc::new(WindowAccumExec::try_new(
            child,
            (*self.group_exprs).clone(),
            self.aggr_exprs.clone(),
            self.data_input_schema.clone(),
            self.event_time_col.clone(),
            self.delay_micros,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("WindowAccumExec: invalid partition {partition}");
        }
        let col_idx = self.event_time_col_idx().ok_or_else(|| {
            let names: Vec<_> = self
                .data_input_schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            plan_datafusion_err!(
                "event-time column '{}' not found in input schema {names:?}",
                self.event_time_col
            )
        })?;

        let group_exprs = Arc::clone(&self.group_exprs);
        let aggr_exprs = self.aggr_exprs.clone();
        let data_schema = self.data_input_schema.clone();
        let agg_schema = self.agg_output_schema.clone();
        let input_stream =
            DecodedFlowEventStream::try_new(self.input.execute(partition, context.clone())?)?;

        // State carried through the unfold loop:
        // (input_stream, accum_state, output_buffer, context)
        type UnfoldState = (
            DecodedFlowEventStream,
            AccumState,
            VecDeque<FlowEvent>,
            Arc<TaskContext>,
        );
        let init: UnfoldState = (
            input_stream,
            AccumState::new(col_idx, self.delay_micros),
            VecDeque::new(),
            context,
        );

        let event_stream = stream::unfold(init, move |(mut input, mut acc, mut buf, ctx)| {
            let group_exprs = Arc::clone(&group_exprs);
            let aggr_exprs = aggr_exprs.clone();
            let data_schema = data_schema.clone();
            async move {
                loop {
                    // First drain the output buffer.
                    if let Some(ev) = buf.pop_front() {
                        return Some((Ok(ev), (input, acc, buf, ctx)));
                    }
                    // Then read from input.
                    match input.next().await {
                        None => return None,
                        Some(Err(e)) => return Some((Err(e), (input, acc, buf, ctx))),
                        Some(Ok(FlowEvent::Data { batch, .. })) => {
                            acc.push(batch);
                            // No output yet; loop to read next event.
                        }
                        Some(Ok(FlowEvent::Marker(FlowMarker::Checkpoint { id }))) => {
                            // Re-aggregate all pending rows.
                            let batches = acc.pending_rows.clone();
                            let wm = acc.watermark_micros;
                            match run_aggregate(
                                batches,
                                &group_exprs,
                                &aggr_exprs,
                                &data_schema,
                                ctx.clone(),
                            )
                            .await
                            {
                                Err(e) => return Some((Err(e), (input, acc, buf, ctx))),
                                Ok(agg_batches) => {
                                    for agg_batch in agg_batches {
                                        if let Some(mask) = window_end_mask(&agg_batch, wm) {
                                            match compute::filter_record_batch(&agg_batch, &mask) {
                                                Ok(filtered) if filtered.num_rows() > 0 => {
                                                    let len = filtered.num_rows();
                                                    let retracted = {
                                                        let mut b =
                                                            BooleanBuilder::with_capacity(len);
                                                        b.append_n(len, false);
                                                        b.finish()
                                                    };
                                                    buf.push_back(FlowEvent::Data {
                                                        batch: filtered,
                                                        retracted,
                                                    });
                                                }
                                                _ => {}
                                            }
                                        }
                                    }
                                }
                            }
                            buf.push_back(FlowEvent::Marker(FlowMarker::Checkpoint { id }));
                        }
                        Some(Ok(other)) => {
                            buf.push_back(other);
                        }
                    }
                }
            }
        });

        let flow_stream = Box::pin(FlowEventStreamAdapter::new(agg_schema, event_stream));
        Ok(Box::pin(EncodedFlowEventStream::new(flow_stream)))
    }
}

/// Run the GROUP BY aggregate on `batches` and return all output batches.
async fn run_aggregate(
    batches: Vec<RecordBatch>,
    group_exprs: &PhysicalGroupBy,
    aggr_exprs: &[Arc<AggregateFunctionExpr>],
    data_schema: &SchemaRef,
    context: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(vec![]);
    }
    let static_input = Arc::new(StaticBatchExec::new(batches, data_schema.clone()));
    let agg = AggregateExec::try_new(
        AggregateMode::Single,
        group_exprs.clone(),
        aggr_exprs.to_vec(),
        vec![None; aggr_exprs.len()],
        static_input,
        data_schema.clone(),
    )?;
    let mut stream = agg.execute(0, context)?;
    let mut out = vec![];
    while let Some(batch) = stream.next().await {
        out.push(batch?);
    }
    Ok(out)
}

/// Build a mask: true for rows where `window.end ≤ watermark_micros`.
/// Returns `None` if there's no "window" column or watermark is not yet set.
fn window_end_mask(batch: &RecordBatch, watermark_micros: Option<i64>) -> Option<BooleanArray> {
    let wm = watermark_micros?;
    let idx = batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name().eq_ignore_ascii_case("window"))?;
    let struct_arr = batch.column(idx).as_any().downcast_ref::<StructArray>()?;
    let end_col = struct_arr.column_by_name("end")?;
    let end_ts = end_col
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()?;
    let mut b = BooleanBuilder::with_capacity(batch.num_rows());
    for i in 0..end_ts.len() {
        b.append_value(!end_ts.is_null(i) && end_ts.value(i) <= wm);
    }
    Some(b.finish())
}
