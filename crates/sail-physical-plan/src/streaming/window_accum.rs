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
use sail_common_datafusion::streaming::checkpoint::CheckpointStore;
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
    /// Watermark at the last `Final` merge. The `Partial` pre-aggregation runs per batch
    /// (incremental), but the `Final` merge + emit only needs to run when windows can
    /// close — we throttle it to `FINAL_THROTTLE_MICROS` of watermark advance (Flink
    /// emits on trigger, not per element), cutting per-batch aggregate overhead.
    last_final_wm: Option<i64>,
    /// Whether committed state has been restored yet (restore happens async on the first poll,
    /// since `execute()` is sync but the checkpoint store is async).
    restored: bool,
}

/// Re-run the `Final` merge + emit at most once per this much watermark advance.
/// Bounds emit latency to this (windowed agg already carries a watermark delay).
const FINAL_THROTTLE_MICROS: i64 = 200_000; // 200 ms

impl AccumState {
    fn new() -> Self {
        Self {
            pending_rows: vec![],
            watermark_micros: None,
            emitted_ends: HashSet::new(),
            last_final_wm: None,
            restored: false,
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
    /// `Partial`-mode aggregate output schema (group cols + partial state cols). Each
    /// incoming batch is pre-aggregated to this; partials are merged with `Final` mode
    /// only when a window closes (incremental aggregation — store one partial per
    /// window, never re-aggregate raw rows; see docs/design/streaming-watermark.md).
    partial_schema: SchemaRef,
    /// `Final`-mode group-by: column refs into the partial schema's group columns.
    final_group_by: Arc<PhysicalGroupBy>,
    event_time_col: String,
    delay_micros: i64,
    /// Streaming `checkpointLocation`, when set — snapshot the open-window partial state
    /// on `EndOfData` and restore it on the next run (stateful exactly-once recovery).
    checkpoint_location: Option<String>,
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
        checkpoint_location: Option<String>,
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
        // Partial-mode schema (group cols + partial state cols) for incremental pre-agg.
        let partial_schema = {
            let empty = Arc::new(EmptyExec::new(data_input_schema.clone()));
            let trial = AggregateExec::try_new(
                AggregateMode::Partial,
                group_exprs.clone(),
                aggr_exprs.clone(),
                vec![None; aggr_exprs.len()],
                empty,
                data_input_schema.clone(),
            )?;
            trial.schema()
        };
        // Final-mode group-by references the group columns (which lead the partial
        // schema) by position, since Final consumes partial state, not raw data.
        let num_group_cols = group_exprs.expr().len();
        let final_group_by = PhysicalGroupBy::new_single(
            (0..num_group_cols)
                .map(|i| {
                    let name = partial_schema.field(i).name().clone();
                    (
                        Arc::new(datafusion::physical_plan::expressions::Column::new(
                            &name, i,
                        ))
                            as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                        name,
                    )
                })
                .collect(),
        );
        let flow_schema = Arc::new(to_flow_event_schema(&agg_output_schema));
        // One independent instance per input partition: each owns a disjoint key subset
        // (via the upstream keyed StreamExchangeExec) and closes its windows on the
        // broadcast watermark. Pass the input partition count through.
        let n_partitions = input.properties().output_partitioning().partition_count();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(flow_schema),
            datafusion::physical_expr::Partitioning::UnknownPartitioning(n_partitions),
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
            partial_schema,
            final_group_by: Arc::new(final_group_by),
            event_time_col,
            delay_micros,
            checkpoint_location,
            properties,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    pub fn group_exprs(&self) -> &PhysicalGroupBy {
        &self.group_exprs
    }
    pub fn aggr_exprs(&self) -> &[Arc<AggregateFunctionExpr>] {
        &self.aggr_exprs
    }
    pub fn data_input_schema(&self) -> &SchemaRef {
        &self.data_input_schema
    }
    pub fn event_time_col(&self) -> &str {
        &self.event_time_col
    }
    pub fn delay_micros(&self) -> i64 {
        self.delay_micros
    }
    pub fn checkpoint_location(&self) -> Option<&str> {
        self.checkpoint_location.as_deref()
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
            self.checkpoint_location.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let n = self.properties.output_partitioning().partition_count();
        if partition >= n {
            return internal_err!("WindowAccumExec: invalid partition {partition} (have {n})");
        }

        let group_exprs = Arc::clone(&self.group_exprs);
        let aggr_exprs = self.aggr_exprs.clone();
        let data_schema = self.data_input_schema.clone();
        let agg_schema = self.agg_output_schema.clone();
        let partial_schema = self.partial_schema.clone();
        let final_group_by = Arc::clone(&self.final_group_by);
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
        // Restore operator state (open-window partials + watermark + emitted ends) from
        // the last committed snapshot, for stateful exactly-once recovery across runs.
        // Per-partition state id so each parallel instance snapshots/restores independently.
        let state_op_id = format!("window-{partition}");
        let acc = AccumState::new();
        // Build the checkpoint store synchronously (no I/O); the actual restore is async and runs
        // on the first poll below (execute() is sync). Per-partition state id so each parallel
        // instance snapshots/restores independently.
        let ck = self
            .checkpoint_location
            .as_deref()
            .and_then(|l| CheckpointStore::from_location(l).ok());
        let init: UnfoldState = (input_stream, acc, VecDeque::new(), context);

        let event_stream = stream::unfold(init, move |(mut input, mut acc, mut buf, ctx)| {
            let group_exprs = Arc::clone(&group_exprs);
            let aggr_exprs = aggr_exprs.clone();
            let data_schema = data_schema.clone();
            let partial_schema = partial_schema.clone();
            let final_group_by = Arc::clone(&final_group_by);
            let ck = ck.clone();
            let state_op_id = state_op_id.clone();
            async move {
                // Restore committed state on the first poll (open-window partials + watermark +
                // emitted ends), for stateful exactly-once recovery across runs.
                if !acc.restored {
                    if let Some(ck) = &ck {
                        let (batches, meta) =
                            crate::streaming::state_io::restore_state(ck, &state_op_id).await;
                        acc.pending_rows = batches;
                        if let Some((wm, ends)) = meta.split_first() {
                            acc.watermark_micros = (*wm != i64::MIN).then_some(*wm);
                            acc.emitted_ends = ends.iter().copied().collect();
                        }
                    }
                    acc.restored = true;
                }
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
                            // Incremental pre-aggregation: reduce the batch to partial
                            // state (one row per window-group) immediately, instead of
                            // buffering raw rows. Keeps state O(#open windows), not O(#rows).
                            if batch.num_rows() > 0 {
                                match run_partial_aggregate(
                                    vec![batch],
                                    &group_exprs,
                                    &aggr_exprs,
                                    &data_schema,
                                    ctx.clone(),
                                )
                                .await
                                {
                                    Err(e) => return Some((Err(e), (input, acc, buf, ctx))),
                                    Ok(mut partials) => acc.pending_rows.append(&mut partials),
                                }
                            }
                            // No output yet; loop to read next event.
                        }
                        Some(Ok(FlowEvent::Marker(FlowMarker::Watermark {
                            source,
                            timestamp,
                        }))) => {
                            acc.set_watermark(timestamp.timestamp_micros());
                            let wm = acc.watermark_micros;
                            // Throttle the Final merge/emit (Partial pre-agg already ran per
                            // batch): only when the watermark advanced past the threshold —
                            // windows still emit exactly once, within the threshold of close.
                            let should_final = match (wm, acc.last_final_wm) {
                                (Some(w), Some(last)) => w - last >= FINAL_THROTTLE_MICROS,
                                (Some(_), None) => true,
                                _ => false,
                            };
                            if should_final {
                                acc.last_final_wm = wm;
                                if let Err(e) = finalize_and_emit(
                                    &mut acc,
                                    &final_group_by,
                                    &aggr_exprs,
                                    &partial_schema,
                                    wm,
                                    &mut buf,
                                    ctx.clone(),
                                )
                                .await
                                {
                                    return Some((Err(e), (input, acc, buf, ctx)));
                                }
                            }
                            buf.push_back(FlowEvent::Marker(FlowMarker::Watermark {
                                source,
                                timestamp,
                            }));
                        }
                        Some(Ok(FlowEvent::Marker(FlowMarker::EndOfData))) => {
                            if let Some(ck) = &ck {
                                // Checkpointed run (availableNow/once): SNAPSHOT the open-window
                                // partial state (write-ahead) so windows spanning runs complete
                                // correctly — the runner commits it after the output is durable.
                                // (Do NOT flush; open windows carry over to the next run.)
                                let mut meta = vec![acc.watermark_micros.unwrap_or(i64::MIN)];
                                meta.extend(acc.emitted_ends.iter().copied());
                                crate::streaming::state_io::stage_state(
                                    ck,
                                    &state_op_id,
                                    &partial_schema,
                                    &acc.pending_rows,
                                    &meta,
                                )
                                .await;
                            } else {
                                // No checkpoint: flush all remaining windows (terminal).
                                if let Err(e) = finalize_and_emit(
                                    &mut acc,
                                    &final_group_by,
                                    &aggr_exprs,
                                    &partial_schema,
                                    Some(i64::MAX),
                                    &mut buf,
                                    ctx.clone(),
                                )
                                .await
                                {
                                    return Some((Err(e), (input, acc, buf, ctx)));
                                }
                            }
                            buf.push_back(FlowEvent::Marker(FlowMarker::EndOfData));
                        }
                        Some(Ok(other)) => {
                            // Watermark drives eviction; other markers (Checkpoint,
                            // LatencyTracker) pass through.
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

/// Merge accumulated partial states (`Final` mode), emit windows that have closed
/// (`end ≤ emit_wm`) exactly once, and drop their partials to bound state.
async fn finalize_and_emit(
    acc: &mut AccumState,
    final_group_by: &PhysicalGroupBy,
    aggr_exprs: &[Arc<AggregateFunctionExpr>],
    partial_schema: &SchemaRef,
    emit_wm: Option<i64>,
    buf: &mut VecDeque<FlowEvent>,
    context: Arc<TaskContext>,
) -> Result<()> {
    let partials = acc.pending_rows.clone();
    let agg_batches = run_final_aggregate(
        partials,
        final_group_by,
        aggr_exprs,
        partial_schema,
        context,
    )
    .await?;
    for agg_batch in agg_batches {
        if let Some(mask) = window_emit_mask(&agg_batch, emit_wm, &mut acc.emitted_ends) {
            if let Ok(filtered) = compute::filter_record_batch(&agg_batch, &mask) {
                if filtered.num_rows() > 0 {
                    let len = filtered.num_rows();
                    let mut b = BooleanBuilder::with_capacity(len);
                    b.append_n(len, false);
                    buf.push_back(FlowEvent::Data {
                        batch: filtered,
                        retracted: b.finish(),
                    });
                }
            }
        }
    }
    // Bound state: keep only partials whose window is still open.
    acc.pending_rows = retain_open_window_rows(std::mem::take(&mut acc.pending_rows), emit_wm);
    Ok(())
}

/// `Partial`-mode pre-aggregation of `batches` → partial state rows (one per
/// window-group). Run per incoming batch so we never buffer raw rows.
async fn run_partial_aggregate(
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
        AggregateMode::Partial,
        group_exprs.clone(),
        aggr_exprs.to_vec(),
        vec![None; aggr_exprs.len()],
        static_input,
        data_schema.clone(),
    )?;
    let mut stream = agg.execute(0, context)?;
    let mut out = vec![];
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if batch.num_rows() > 0 {
            out.push(batch);
        }
    }
    Ok(out)
}

/// `Final`-mode merge of accumulated partial states → final aggregate results.
/// `final_group_by` references the group columns of `partial_schema` by position.
async fn run_final_aggregate(
    partials: Vec<RecordBatch>,
    final_group_by: &PhysicalGroupBy,
    aggr_exprs: &[Arc<AggregateFunctionExpr>],
    partial_schema: &SchemaRef,
    context: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    if partials.is_empty() {
        return Ok(vec![]);
    }
    let static_input = Arc::new(StaticBatchExec::new(partials, partial_schema.clone()));
    let agg = AggregateExec::try_new(
        AggregateMode::Final,
        final_group_by.clone(),
        aggr_exprs.to_vec(),
        vec![None; aggr_exprs.len()],
        static_input,
        partial_schema.clone(),
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
    // A window is emitted exactly once *across finalize calls* — but a keyed window has
    // MANY rows per window end (one per group key), and ALL of them must emit together the
    // first time the window closes. So test membership without mutating, then record the
    // newly-closed ends afterward. (Using `emitted.insert(e)` in the loop would suppress
    // every group row after the first for a given window end — collapsing keyed windows.)
    let mut b = BooleanBuilder::with_capacity(ends.len());
    for end in &ends {
        let emit = end.is_some_and(|e| e <= wm && !emitted.contains(&e));
        b.append_value(emit);
    }
    for end in ends.into_iter().flatten() {
        if end <= wm {
            emitted.insert(end);
        }
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
            Ok(_) => {}                // all rows closed → drop batch
            Err(_) => out.push(batch), // on error, keep (safe)
        }
    }
    out
}
