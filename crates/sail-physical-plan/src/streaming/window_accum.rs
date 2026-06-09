use std::any::Any;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, BooleanArray, BooleanBuilder, RecordBatch, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
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
use datafusion_common::{internal_err, plan_err, Result};
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
pub(crate) struct StaticBatchExec {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl StaticBatchExec {
    pub(crate) fn new(batches: Vec<RecordBatch>, schema: SchemaRef) -> Self {
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
    /// Latest watermark, received via `FlowMarker::Watermark` (delay already applied
    /// upstream by `WatermarkExec`). Decoupled from any raw column surviving query
    /// optimization — see docs/design/streaming-watermark.md.
    watermark_micros: Option<i64>,
    /// Window ends already emitted, so a window is emitted exactly once (and late
    /// rows that would re-open a closed window are not re-emitted).
    emitted_ends: HashSet<i64>,
}

impl AccumState {
    fn new() -> Self {
        Self {
            pending_rows: vec![],
            watermark_micros: None,
            emitted_ends: HashSet::new(),
        }
    }

    fn push(&mut self, batch: RecordBatch) {
        if batch.num_rows() > 0 {
            self.pending_rows.push(batch);
        }
    }

    /// Advance the watermark (monotonic).
    fn set_watermark(&mut self, micros: i64) {
        self.watermark_micros = Some(self.watermark_micros.map_or(micros, |c| c.max(micros)));
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

        let group_exprs = Arc::clone(&self.group_exprs);
        let aggr_exprs = self.aggr_exprs.clone();
        let data_schema = self.data_input_schema.clone();
        let agg_schema = self.agg_output_schema.clone();
        let in_stream = self.input.execute(partition, context.clone())?;
        let input_stream = DecodedFlowEventStream::try_new(in_stream).map_err(|e| {
            let names: Vec<_> = self
                .input
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            datafusion_common::exec_datafusion_err!("WindowAccumExec decode (input {names:?}): {e}")
        })?;

        // State carried through the unfold loop:
        // (input_stream, accum_state, output_buffer, context)
        type UnfoldState = (
            DecodedFlowEventStream,
            AccumState,
            VecDeque<FlowEvent>,
            Arc<TaskContext>,
        );
        let init: UnfoldState = (input_stream, AccumState::new(), VecDeque::new(), context);

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
                        Some(Ok(FlowEvent::Marker(FlowMarker::Watermark { source, timestamp }))) => {
                            // Watermark advanced: emit windows that have now closed
                            // (end ≤ watermark), exactly once, then drop their rows.
                            acc.set_watermark(timestamp.timestamp_micros());
                            let wm = acc.watermark_micros;
                            let batches = acc.pending_rows.clone();
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
                                        if let Some(mask) =
                                            window_emit_mask(&agg_batch, wm, &mut acc.emitted_ends)
                                        {
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
                            // Bound state: keep only rows whose window is still open.
                            acc.pending_rows =
                                retain_open_window_rows(std::mem::take(&mut acc.pending_rows), wm);
                            buf.push_back(FlowEvent::Marker(FlowMarker::Watermark {
                                source,
                                timestamp,
                            }));
                        }
                        Some(Ok(other)) => {
                            // Watermark drives eviction now; other markers (Checkpoint,
                            // EndOfData, LatencyTracker) pass through.
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

/// Find the time-window struct column (a `Struct` with an `end` Timestamp(µs) field)
/// and return its per-row `end` values. Works for both the aggregate **output** (the
/// `window` group column) and the aggregate **input** (the optimizer's CSE-renamed
/// `__common_expr_N` window column), so it doesn't depend on a specific column name.
fn window_end_micros(batch: &RecordBatch) -> Option<Vec<Option<i64>>> {
    for col in batch.columns() {
        let Some(struct_arr) = col.as_any().downcast_ref::<StructArray>() else {
            continue;
        };
        let Some(end_col) = struct_arr.column_by_name("end") else {
            continue;
        };
        // The window struct's `end` may be any timestamp precision (the window UDF
        // emits nanoseconds, the source is microseconds); normalize to microseconds
        // so it compares with the watermark (also microseconds).
        let DataType::Timestamp(unit, _) = end_col.data_type() else {
            continue;
        };
        let read = |raw: Option<i64>| -> Option<i64> {
            raw.map(|v| match unit {
                TimeUnit::Second => v.saturating_mul(1_000_000),
                TimeUnit::Millisecond => v.saturating_mul(1_000),
                TimeUnit::Microsecond => v,
                TimeUnit::Nanosecond => v / 1_000,
            })
        };
        let any = end_col.as_any();
        let out: Vec<Option<i64>> = match unit {
            TimeUnit::Second => {
                let a = any.downcast_ref::<TimestampSecondArray>()?;
                (0..a.len())
                    .map(|i| read((!a.is_null(i)).then(|| a.value(i))))
                    .collect()
            }
            TimeUnit::Millisecond => {
                let a = any.downcast_ref::<TimestampMillisecondArray>()?;
                (0..a.len())
                    .map(|i| read((!a.is_null(i)).then(|| a.value(i))))
                    .collect()
            }
            TimeUnit::Microsecond => {
                let a = any.downcast_ref::<TimestampMicrosecondArray>()?;
                (0..a.len())
                    .map(|i| read((!a.is_null(i)).then(|| a.value(i))))
                    .collect()
            }
            TimeUnit::Nanosecond => {
                let a = any.downcast_ref::<TimestampNanosecondArray>()?;
                (0..a.len())
                    .map(|i| read((!a.is_null(i)).then(|| a.value(i))))
                    .collect()
            }
        };
        return Some(out);
    }
    None
}

/// Mask for aggregate-output rows whose window has closed (`end ≤ watermark`) and has
/// not been emitted before. Records newly-emitted window ends so each window is emitted
/// exactly once (append mode). `None` if no window column or no watermark yet.
fn window_emit_mask(
    batch: &RecordBatch,
    watermark_micros: Option<i64>,
    emitted: &mut HashSet<i64>,
) -> Option<BooleanArray> {
    let wm = watermark_micros?;
    let ends = window_end_micros(batch)?;
    let mut b = BooleanBuilder::with_capacity(ends.len());
    for end in ends {
        // `insert` returns true only for a not-yet-emitted window end.
        let emit = end.is_some_and(|e| e <= wm && emitted.insert(e));
        b.append_value(emit);
    }
    Some(b.finish())
}

/// Keep only rows whose window is still open (`end > watermark`); drop closed-window
/// rows so pending state stays bounded. Keeps everything if the watermark or window
/// column can't be determined.
fn retain_open_window_rows(
    batches: Vec<RecordBatch>,
    watermark_micros: Option<i64>,
) -> Vec<RecordBatch> {
    let Some(wm) = watermark_micros else {
        return batches;
    };
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        let Some(ends) = window_end_micros(&batch) else {
            out.push(batch);
            continue;
        };
        let mut b = BooleanBuilder::with_capacity(ends.len());
        for end in ends {
            // Keep rows whose window is still open (end > watermark) or has no end.
            b.append_value(end.is_none_or(|e| e > wm));
        }
        match compute::filter_record_batch(&batch, &b.finish()) {
            Ok(filtered) if filtered.num_rows() > 0 => out.push(filtered),
            Ok(_) => {}                 // all rows closed → drop batch
            Err(_) => out.push(batch),  // on error, keep (safe)
        }
    }
    out
}
