use std::any::Any;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, BooleanArray, BooleanBuilder, RecordBatch, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use datafusion::arrow::compute;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{DiskManager, SendableRecordBatchStream, TaskContext};
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
// SpillSourceExec — LAZY input for the Final merge (F5.2). Yields the in-memory
// `pending` partials, then reads each SPILLED chunk back from the checkpoint store
// ONE AT A TIME (a chunk is materialized only while it flows through, then dropped).
// So the merge's input never holds the whole (possibly ≫ RAM) state — peak input ≈
// one chunk + the in-memory pending (both ≤ the spill budget). The Final AggregateExec
// fed from this runs under a bounded MemoryPool (see `bounded_agg_context`) so it spills
// its OWN hash table too — together they bound the finalize PEAK at large cardinality.
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct SpillSourceExec {
    pending: Vec<RecordBatch>,
    spilled: Vec<u64>,
    ck: Option<CheckpointStore>,
    op_id: String,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl SpillSourceExec {
    pub(crate) fn new(
        pending: Vec<RecordBatch>,
        spilled: Vec<u64>,
        ck: Option<CheckpointStore>,
        op_id: String,
        schema: SchemaRef,
    ) -> Self {
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            datafusion::physical_expr::Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            pending,
            spilled,
            ck,
            op_id,
            schema,
            properties: props,
        }
    }
}

impl DisplayAs for SpillSourceExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "SpillSourceExec(spilled={})", self.spilled.len())
    }
}

impl ExecutionPlan for SpillSourceExec {
    fn name(&self) -> &str {
        "SpillSourceExec"
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
        let pending = self.pending.clone(); // Arc-backed; in-memory, bounded by the spill budget
        let spilled = self.spilled.clone();
        let ck = self.ck.clone();
        let op_id = self.op_id.clone();
        // In-memory partials first.
        let pending_stream = stream::iter(
            pending
                .into_iter()
                .map(Ok::<_, datafusion_common::DataFusionError>),
        );
        // Then each spilled chunk, read back lazily one at a time (the inner future is polled
        // only when the outer stream advances, so only one chunk is resident at a time).
        let spill_stream = stream::iter(spilled).flat_map(move |idx| {
            let ck = ck.clone();
            let op_id = op_id.clone();
            stream::once(async move {
                match &ck {
                    Some(ck) => crate::streaming::state_io::read_spill(ck, &op_id, idx).await,
                    None => vec![],
                }
            })
            .flat_map(|batches| {
                stream::iter(
                    batches
                        .into_iter()
                        .map(Ok::<_, datafusion_common::DataFusionError>),
                )
            })
        });
        let s = pending_stream.chain(spill_stream);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s)))
    }
}

/// Build a TaskContext for the Final merge whose RuntimeEnv has a **bounded** memory pool
/// (`FairSpillPool`) + an enabled DiskManager, so DataFusion's grouped-hash `AggregateExec`
/// spills its hash table to disk under pressure instead of OOM-ing (REFERENCES §5). All other
/// session state (config, UDFs) is inherited from the operator's context. F5.2.
fn bounded_agg_context(base: &TaskContext, budget_bytes: usize) -> Result<Arc<TaskContext>> {
    let mut dm: DiskManagerBuilder = DiskManager::builder();
    dm.set_mode(DiskManagerMode::OsTmpDirectory);
    // Generous on-disk ceiling: spilling must never be blocked by this cap (state ≫ RAM is the
    // whole point); the MemoryPool — not the disk cap — governs when we spill.
    dm.set_max_temp_directory_size(1024 * 1024 * 1024 * 1024); // 1 TiB
    let runtime = RuntimeEnvBuilder::default()
        .with_memory_pool(Arc::new(FairSpillPool::new(budget_bytes.max(1024 * 1024))))
        .with_disk_manager_builder(dm)
        .build()
        .map(Arc::new)?;
    Ok(Arc::new(TaskContext::new(
        base.task_id(),
        base.session_id(),
        base.session_config().clone(),
        base.scalar_functions().clone(),
        base.aggregate_functions().clone(),
        base.window_functions().clone(),
        runtime,
    )))
}

/// Output semantics of the window operator (Spark `outputMode`, extended with Flink-style
/// allowed-lateness for zero-loss late-data convergence — see
/// docs/design/streaming-update-retraction-mode.md).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WindowOutputMode {
    /// Emit each window exactly once when it closes (`watermark ≥ end`); drop late data.
    /// Spark Structured Streaming append / RisingWave emit-on-window-close. Default.
    Append,
    /// Changelog: each finalize emits windows whose aggregate value changed as a
    /// retraction of the prior value (`retracted = true`) followed by the new value
    /// (`retracted = false`). State is retained until `watermark > end + allowed_lateness`,
    /// so late records arriving within the lateness bound UPDATE the already-emitted result
    /// instead of being dropped (Flink emit-on-update + allowedLateness; differential-dataflow
    /// retract+insert). Zero data loss within the bound — beyond Spark append / Flink SQL.
    Update,
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
    /// Update (changelog) mode only: the last value emitted per group key, so a later finalize
    /// can RETRACT the stale value before emitting the updated one. Value = (window end micros,
    /// full-row key for change detection, the emitted single-row batch to retract). Entries are
    /// dropped once `end + allowed_lateness ≤ watermark` (the window can no longer change).
    last_emitted: HashMap<OwnedRow, (i64, OwnedRow, RecordBatch)>,
    /// Continuous (realtime) EO only: the committed epoch restored on startup (F3-c). Set when
    /// `realtime/committed` exists; lets recovery restore exactly that epoch's per-epoch state.
    last_committed_epoch: Option<u64>,
    /// F5 spillable state: approx in-memory bytes of `pending_rows`, and the indices of partial-state
    /// chunks SPILLED to the checkpoint store (Arrow-IPC) when `pending_rows` exceeds the memory
    /// budget — bounding accumulation RAM. Spills are read back + folded into the full state at each
    /// finalize/snapshot (so EO is unchanged — the durable snapshot is always the full flattened
    /// state). See docs/design/streaming-spillable-state-f5.md.
    pending_bytes: usize,
    spilled: Vec<u64>,
    next_spill: u64,
    /// F5.2: an in-flight `Final` merge driven INCREMENTALLY (one output batch per poll) so the
    /// finalize never materializes the whole result in `buf` — peak output is bounded to a couple
    /// of in-flight events. While `Some`, the unfold loop drives this before reading new input
    /// (so a finalize completes before the next marker — preserves barrier order).
    active_merge: Option<SendableRecordBatchStream>,
    /// Append-mode: window ends emitted DURING the active merge, applied to `emitted_ends` only when
    /// the merge completes — so every output batch of one finalize sees the SAME pre-finalize
    /// snapshot of `emitted_ends` (this is the invariant whose violation caused the 64K-cap bug).
    merge_newly_emitted: HashSet<i64>,
    /// The emit watermark of the active merge (the close threshold for this finalize).
    merge_emit_wm: Option<i64>,
    /// The marker (Watermark / EndOfData) to emit AFTER the active merge's output drains, so window
    /// output precedes the marker.
    after_merge_marker: Option<FlowEvent>,
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
            last_emitted: HashMap::new(),
            last_committed_epoch: None,
            pending_bytes: 0,
            spilled: vec![],
            next_spill: 0,
            active_merge: None,
            merge_newly_emitted: HashSet::new(),
            merge_emit_wm: None,
            after_merge_marker: None,
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
    /// Output semantics (Spark `outputMode`). Defaults to `Append` (today's behavior); set via
    /// [`WindowAccumExec::with_output_mode`].
    output_mode: WindowOutputMode,
    /// Update mode only: keep a closed window's state this long past its end so late-but-in-bound
    /// records update the result (retract + re-emit) instead of being dropped. Micros; 0 = none.
    allowed_lateness_micros: i64,
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
            output_mode: WindowOutputMode::Append,
            allowed_lateness_micros: 0,
            properties,
        })
    }

    /// Set the output mode + allowed lateness (changelog/update; see [`WindowOutputMode`]).
    /// Default is `Append` with no lateness — identical to the prior behavior.
    pub fn with_output_mode(mut self, mode: WindowOutputMode, allowed_lateness_micros: i64) -> Self {
        self.output_mode = mode;
        self.allowed_lateness_micros = allowed_lateness_micros.max(0);
        self
    }

    pub fn output_mode(&self) -> WindowOutputMode {
        self.output_mode
    }

    pub fn allowed_lateness_micros(&self) -> i64 {
        self.allowed_lateness_micros
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
        Ok(Arc::new(
            WindowAccumExec::try_new(
                child,
                (*self.group_exprs).clone(),
                self.aggr_exprs.clone(),
                self.data_input_schema.clone(),
                self.event_time_col.clone(),
                self.delay_micros,
                self.checkpoint_location.clone(),
            )?
            .with_output_mode(self.output_mode, self.allowed_lateness_micros),
        ))
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
        let output_mode = self.output_mode;
        let allowed_lateness = self.allowed_lateness_micros;
        let num_group_cols = self.final_group_by.expr().len();
        // F5: spill `pending_rows` to the checkpoint store when it exceeds this budget (bounds
        // accumulation RAM). Default 128 MiB; override via SAIL_STREAMING_STATE_BUDGET_BYTES.
        let state_budget_bytes: usize = std::env::var("SAIL_STREAMING_STATE_BUDGET_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(128 * 1024 * 1024);
        // F5.3: compaction (PartialReduce of accumulated partials) is on by default; kill-switch for
        // A/B and safety.
        let compact_enabled = std::env::var("VAJRA_F5_NO_COMPACT").is_err();
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
            let output_mode = output_mode;
            let allowed_lateness = allowed_lateness;
            let num_group_cols = num_group_cols;
            let state_budget_bytes = state_budget_bytes;
            let compact_enabled = compact_enabled;
            async move {
                // Restore committed state on the first poll (open-window partials + watermark +
                // emitted ends), for stateful exactly-once recovery across runs.
                if !acc.restored {
                    if let Some(ck) = &ck {
                        // Continuous (realtime) EO restores the COMMITTED epoch's state (F3-c) —
                        // the same epoch the source seeks offsets for (consistent global snapshot).
                        // Micro-batch (no realtime/committed) restores the staged→committed blob.
                        let (batches, meta) = match crate::streaming::state_io::committed_epoch(ck)
                            .await
                        {
                            Some(epoch) => {
                                acc.last_committed_epoch = Some(epoch);
                                crate::streaming::state_io::restore_epoch_state(
                                    ck,
                                    &state_op_id,
                                    epoch,
                                )
                                .await
                            }
                            None => crate::streaming::state_io::restore_state(ck, &state_op_id).await,
                        };
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
                    // F5.2: if a finalize merge is in flight, drive it ONE output batch at a time
                    // (incremental emit — the result never fully materializes in `buf`). Complete it
                    // before reading new input, so a finalize finishes before the next marker.
                    if acc.active_merge.is_some() {
                        // Pull one batch, releasing the &mut borrow of `acc.active_merge` before we
                        // touch other `acc` fields below.
                        let next = match acc.active_merge.as_mut() {
                            Some(m) => m.next().await,
                            None => None,
                        };
                        match next {
                            Some(Err(e)) => return Some((Err(e), (input, acc, buf, ctx))),
                            Some(Ok(agg_batch)) => {
                                if let Err(e) = consume_merge_batch(
                                    &mut acc,
                                    &agg_batch,
                                    output_mode,
                                    allowed_lateness,
                                    num_group_cols,
                                    &mut buf,
                                ) {
                                    return Some((Err(e), (input, acc, buf, ctx)));
                                }
                                continue; // loop back to drain produced events from `buf`
                            }
                            None => {
                                // Merge complete: apply the deferred emitted-ends snapshot (append),
                                // rebuild the RETAINED open-window state (lazily, re-spilling over
                                // budget), then emit the deferred marker.
                                if output_mode == WindowOutputMode::Append {
                                    let newly = std::mem::take(&mut acc.merge_newly_emitted);
                                    acc.emitted_ends.extend(newly);
                                }
                                let retain_wm = match output_mode {
                                    WindowOutputMode::Append => acc.merge_emit_wm,
                                    WindowOutputMode::Update => acc
                                        .merge_emit_wm
                                        .map(|w| w.saturating_sub(allowed_lateness)),
                                };
                                if let Err(e) = rebuild_retained_state(
                                    &mut acc,
                                    retain_wm,
                                    ck.as_ref(),
                                    &state_op_id,
                                    &partial_schema,
                                    state_budget_bytes,
                                    &final_group_by,
                                    &aggr_exprs,
                                    ctx.clone(),
                                    compact_enabled,
                                )
                                .await
                                {
                                    return Some((Err(e), (input, acc, buf, ctx)));
                                }
                                acc.active_merge = None;
                                if let Some(marker) = acc.after_merge_marker.take() {
                                    buf.push_back(marker);
                                }
                                continue;
                            }
                        }
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
                                    Ok(mut partials) => {
                                        for b in &partials {
                                            acc.pending_bytes += b.get_array_memory_size();
                                        }
                                        acc.pending_rows.append(&mut partials);
                                    }
                                }
                                // F5.3: over budget → first COMPACT (PartialReduce) to collapse the
                                // duplicate (window,key) partials that accumulate one-per-batch, so
                                // in-memory state trends to O(distinct open groups) (Flink keeps one
                                // accumulator per key) — cuts both RAM and spill volume. Only worth it
                                // when there are multiple batches to merge.
                                if compact_enabled
                                    && acc.pending_bytes > state_budget_bytes
                                    && acc.pending_rows.len() > 1
                                {
                                    match compact_partials(
                                        std::mem::take(&mut acc.pending_rows),
                                        &final_group_by,
                                        &aggr_exprs,
                                        &partial_schema,
                                        ctx.clone(),
                                    )
                                    .await
                                    {
                                        Err(e) => return Some((Err(e), (input, acc, buf, ctx))),
                                        Ok(compacted) => {
                                            acc.pending_bytes = compacted
                                                .iter()
                                                .map(|b| b.get_array_memory_size())
                                                .sum();
                                            acc.pending_rows = compacted;
                                        }
                                    }
                                }
                                // F5: STILL over budget after compaction → spill the in-memory
                                // partials to the checkpoint store (Arrow-IPC chunk), evicting them
                                // from RAM. Folded back into the full state at the next
                                // finalize/snapshot (EO unchanged).
                                if acc.pending_bytes > state_budget_bytes && !acc.pending_rows.is_empty() {
                                    if let Some(ck) = &ck {
                                        let idx = acc.next_spill;
                                        if crate::streaming::state_io::write_spill(
                                            ck, &state_op_id, idx, &partial_schema, &acc.pending_rows,
                                        )
                                        .await
                                        {
                                            if std::env::var("VAJRA_F5_DEBUG").is_ok() {
                                                eprintln!("F5_SPILL p{partition} idx={idx}");
                                            }
                                            acc.spilled.push(idx);
                                            acc.next_spill += 1;
                                            acc.pending_rows.clear();
                                            acc.pending_bytes = 0;
                                        }
                                    }
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
                                // F5.2: start a RESUMABLE merge; its output (then this watermark
                                // marker) is emitted incrementally by the active-merge driver above.
                                match begin_finalize(
                                    &acc,
                                    &final_group_by,
                                    &aggr_exprs,
                                    &partial_schema,
                                    ck.as_ref(),
                                    &state_op_id,
                                    ctx.clone(),
                                    state_budget_bytes,
                                ) {
                                    Err(e) => return Some((Err(e), (input, acc, buf, ctx))),
                                    Ok(stream) => {
                                        acc.active_merge = Some(stream);
                                        acc.merge_emit_wm = wm;
                                        acc.merge_newly_emitted.clear();
                                        acc.after_merge_marker =
                                            Some(FlowEvent::Marker(FlowMarker::Watermark {
                                                source,
                                                timestamp,
                                            }));
                                    }
                                }
                            } else {
                                buf.push_back(FlowEvent::Marker(FlowMarker::Watermark {
                                    source,
                                    timestamp,
                                }));
                            }
                        }
                        Some(Ok(FlowEvent::Marker(FlowMarker::EndOfData))) => {
                            if let Some(ck) = &ck {
                                // Checkpointed run (availableNow/once): SNAPSHOT the open-window
                                // partial state (write-ahead) so windows spanning runs complete
                                // correctly — the runner commits it after the output is durable.
                                // (Do NOT flush; open windows carry over to the next run.)
                                let mut meta = vec![acc.watermark_micros.unwrap_or(i64::MIN)];
                                meta.extend(acc.emitted_ends.iter().copied());
                                // F5: snapshot the FULL state (in-memory + spilled folded in) so the
                                // committed blob is complete — recovery is unchanged by spilling.
                                let full = gather_partials(
                                    &acc.pending_rows, &acc.spilled, Some(ck), &state_op_id,
                                )
                                .await;
                                crate::streaming::state_io::stage_state(
                                    ck,
                                    &state_op_id,
                                    &partial_schema,
                                    &full,
                                    &meta,
                                )
                                .await;
                                buf.push_back(FlowEvent::Marker(FlowMarker::EndOfData));
                            } else {
                                // No checkpoint: flush ALL remaining windows (terminal), resumably —
                                // emit_wm = i64::MAX closes every window. The EndOfData marker is
                                // emitted after the output drains.
                                match begin_finalize(
                                    &acc,
                                    &final_group_by,
                                    &aggr_exprs,
                                    &partial_schema,
                                    ck.as_ref(),
                                    &state_op_id,
                                    ctx.clone(),
                                    state_budget_bytes,
                                ) {
                                    Err(e) => return Some((Err(e), (input, acc, buf, ctx))),
                                    Ok(stream) => {
                                        acc.active_merge = Some(stream);
                                        acc.merge_emit_wm = Some(i64::MAX);
                                        acc.merge_newly_emitted.clear();
                                        acc.after_merge_marker =
                                            Some(FlowEvent::Marker(FlowMarker::EndOfData));
                                    }
                                }
                            }
                        }
                        Some(Ok(FlowEvent::Marker(FlowMarker::Checkpoint { id }))) => {
                            // F3-c (continuous/realtime EO): WRITE-AHEAD this operator's keyed state
                            // for the epoch BEFORE forwarding the barrier to the realtime sink (which
                            // then atomically commits realtime/committed=id). On restart we restore
                            // exactly the committed epoch's state — the same epoch the source seeks
                            // offsets for ⇒ consistent global snapshot (Chandy-Lamport), exactly-once
                            // across a crash for stateful continuous queries.
                            if let Some(ck) = &ck {
                                let mut meta = vec![acc.watermark_micros.unwrap_or(i64::MIN)];
                                meta.extend(acc.emitted_ends.iter().copied());
                                // F5: epoch snapshot captures the FULL state (memory + spilled). Do
                                // NOT delete the spills here — the operator keeps running and they
                                // remain live (only finalize, which consumes them, GCs them).
                                let full = gather_partials(
                                    &acc.pending_rows, &acc.spilled, Some(ck), &state_op_id,
                                )
                                .await;
                                crate::streaming::state_io::stage_epoch_state(
                                    ck,
                                    &state_op_id,
                                    id,
                                    &partial_schema,
                                    &full,
                                    &meta,
                                )
                                .await;
                                // GC a small trailing window (never at/after the just-staged epoch).
                                if id >= 2 {
                                    crate::streaming::state_io::gc_epoch_state(
                                        ck,
                                        &state_op_id,
                                        id - 2,
                                    )
                                    .await;
                                }
                                acc.last_committed_epoch = Some(id);
                            }
                            buf.push_back(FlowEvent::Marker(FlowMarker::Checkpoint { id }));
                        }
                        Some(Ok(other)) => {
                            // Watermark drives eviction; other markers (LatencyTracker) pass through.
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

/// Merge accumulated partial states (`Final` mode) and emit results per the output mode:
/// - `Append`: emit each window once when it closes (`end ≤ emit_wm`), then drop its partials.
/// - `Update`: emit a changelog (retract stale value + insert new value) for every window whose
///   aggregate changed; retain state until `end + allowed_lateness ≤ emit_wm` so late-but-in-bound
///   data updates the result instead of being dropped (zero-loss convergence).
// Full partial state: in-memory `pending_rows` plus spilled chunks read back (F5), so spilling is
// transparent to correctness/exactly-once. Used by every finalize and snapshot.
async fn gather_partials(
    pending: &[RecordBatch],
    spilled: &[u64],
    ck: Option<&CheckpointStore>,
    op_id: &str,
) -> Vec<RecordBatch> {
    let mut out = pending.to_vec();
    if let Some(ck) = ck {
        for &idx in spilled {
            out.extend(crate::streaming::state_io::read_spill(ck, op_id, idx).await);
        }
    }
    out
}

/// F5.2: start a `Final` merge as a STREAM over a LAZY [`SpillSourceExec`] input (so the
/// possibly-≫RAM partial state is never fully materialized — spilled chunks are read one at a
/// time) run under a bounded-pool [`bounded_agg_context`] (so DataFusion spills its hash table).
/// The caller stores the returned stream in `acc.active_merge` and drains it incrementally.
fn begin_finalize(
    acc: &AccumState,
    final_group_by: &PhysicalGroupBy,
    aggr_exprs: &[Arc<AggregateFunctionExpr>],
    partial_schema: &SchemaRef,
    ck: Option<&CheckpointStore>,
    state_op_id: &str,
    context: Arc<TaskContext>,
    state_budget_bytes: usize,
) -> Result<SendableRecordBatchStream> {
    let input = Arc::new(SpillSourceExec::new(
        acc.pending_rows.clone(), // Arc-backed; bounded by the spill budget
        acc.spilled.clone(),
        ck.cloned(),
        state_op_id.to_string(),
        partial_schema.clone(),
    ));
    let merge_ctx = bounded_agg_context(&context, state_budget_bytes)?;
    let agg = AggregateExec::try_new(
        AggregateMode::Final,
        final_group_by.clone(),
        aggr_exprs.to_vec(),
        vec![None; aggr_exprs.len()],
        input,
        partial_schema.clone(),
    )?;
    agg.execute(0, merge_ctx)
}

/// Emit ONE output batch of the in-flight finalize merge (called once per `active_merge` poll).
/// Append: filter closed windows against the PRE-finalize `emitted_ends` snapshot (so every batch
/// of one finalize emits — the 64K-cap invariant) and record newly-closed ends in
/// `merge_newly_emitted` (applied to `emitted_ends` only when the merge completes). Update: emit a
/// changelog delta for this batch.
fn consume_merge_batch(
    acc: &mut AccumState,
    agg_batch: &RecordBatch,
    output_mode: WindowOutputMode,
    allowed_lateness: i64,
    num_group_cols: usize,
    buf: &mut VecDeque<FlowEvent>,
) -> Result<()> {
    match output_mode {
        WindowOutputMode::Append => {
            if let Some(mask) = window_emit_mask(agg_batch, acc.merge_emit_wm, &acc.emitted_ends) {
                if let Ok(filtered) = compute::filter_record_batch(agg_batch, &mask) {
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
            if let Some(wm) = acc.merge_emit_wm {
                mark_emitted_ends(agg_batch, wm, &mut acc.merge_newly_emitted);
            }
            Ok(())
        }
        WindowOutputMode::Update => {
            let wm = acc.merge_emit_wm;
            emit_changelog(
                acc,
                std::slice::from_ref(agg_batch),
                wm,
                allowed_lateness,
                num_group_cols,
                buf,
            )
        }
    }
}

/// F5.2: after a finalize merge completes, rebuild the RETAINED open-window partial state, bounded.
/// Re-scans the prior state (in-memory `pending_rows` then each spilled chunk, ONE at a time),
/// keeps only still-open windows (`retain_open_window_rows`), and re-accumulates them — re-spilling
/// to FRESH indices when over budget — then GCs the consumed spills. Peak ≈ one chunk + budget, so
/// the retained state can itself be ≫ RAM (continuous queries with many open windows).
#[expect(clippy::too_many_arguments)]
async fn rebuild_retained_state(
    acc: &mut AccumState,
    retain_wm: Option<i64>,
    ck: Option<&CheckpointStore>,
    state_op_id: &str,
    partial_schema: &SchemaRef,
    state_budget_bytes: usize,
    final_group_by: &PhysicalGroupBy,
    aggr_exprs: &[Arc<AggregateFunctionExpr>],
    context: Arc<TaskContext>,
    compact_enabled: bool,
) -> Result<()> {
    let old_pending = std::mem::take(&mut acc.pending_rows);
    let old_spilled = std::mem::take(&mut acc.spilled);
    acc.pending_bytes = 0;
    let mut new_pending: Vec<RecordBatch> = vec![];
    let mut new_bytes = 0usize;

    // Absorb a chunk of partials: keep open windows, accumulate, spill over budget to a fresh index.
    macro_rules! absorb {
        ($chunk:expr) => {{
            for kept in retain_open_window_rows($chunk, retain_wm) {
                new_bytes += kept.get_array_memory_size();
                new_pending.push(kept);
            }
            if new_bytes > state_budget_bytes && !new_pending.is_empty() {
                if let Some(ck) = ck {
                    let idx = acc.next_spill;
                    if crate::streaming::state_io::write_spill(
                        ck,
                        state_op_id,
                        idx,
                        partial_schema,
                        &new_pending,
                    )
                    .await
                    {
                        acc.spilled.push(idx);
                        acc.next_spill += 1;
                        new_pending.clear();
                        new_bytes = 0;
                    }
                }
            }
        }};
    }

    for batch in old_pending {
        absorb!(vec![batch]);
    }
    if let Some(ck) = ck {
        for &idx in &old_spilled {
            let chunk = crate::streaming::state_io::read_spill(ck, state_op_id, idx).await;
            absorb!(chunk);
        }
        // GC the consumed OLD spills (their retained rows are now in new_pending / fresh spills).
        for &idx in &old_spilled {
            crate::streaming::state_io::delete_spill(ck, state_op_id, idx).await;
        }
    }
    // F5.3: compact the carried-forward open-window remainder so long-lived windows (large windows /
    // update-mode within allowed-lateness) keep ONE partial per (window,key) instead of piling up
    // across finalizes. A compaction error is a genuine fault — propagate it (never silently drop
    // state).
    if compact_enabled && new_pending.len() > 1 {
        new_pending =
            compact_partials(new_pending, final_group_by, aggr_exprs, partial_schema, context)
                .await?;
        new_bytes = new_pending.iter().map(|b| b.get_array_memory_size()).sum();
    }
    acc.pending_rows = new_pending;
    acc.pending_bytes = new_bytes;
    Ok(())
}

/// Build a `BooleanArray` of `len` copies of `v` (the per-row `retracted` flag).
fn retracted_flags(len: usize, v: bool) -> BooleanArray {
    let mut b = BooleanBuilder::with_capacity(len);
    b.append_n(len, v);
    b.finish()
}

/// Update-mode changelog emit: for each current aggregate row, compare against the last value
/// emitted for that group key. New → insert; changed → retract(old) + insert(new); unchanged →
/// skip. Retracts and inserts are each coalesced into one batch (retracts first, so a sink sees a
/// consistent delete-then-insert). Finalized windows (`end + lateness ≤ wm`) are dropped from the
/// tracking map — they can no longer change.
fn emit_changelog(
    acc: &mut AccumState,
    agg_batches: &[RecordBatch],
    emit_wm: Option<i64>,
    allowed_lateness: i64,
    num_group_cols: usize,
    buf: &mut VecDeque<FlowEvent>,
) -> Result<()> {
    let Some(wm) = emit_wm else { return Ok(()) };
    let mut retracts: Vec<RecordBatch> = vec![];
    let mut inserts: Vec<RecordBatch> = vec![];
    for agg_batch in agg_batches {
        if agg_batch.num_rows() == 0 {
            continue;
        }
        let group_cols: Vec<_> = (0..num_group_cols)
            .map(|i| Arc::clone(agg_batch.column(i)))
            .collect();
        let all_cols: Vec<_> = agg_batch.columns().to_vec();
        let group_conv = RowConverter::new(
            group_cols
                .iter()
                .map(|c| SortField::new(c.data_type().clone()))
                .collect(),
        )
        .map_err(|e| datafusion_common::arrow_datafusion_err!(e))?;
        let full_conv = RowConverter::new(
            all_cols
                .iter()
                .map(|c| SortField::new(c.data_type().clone()))
                .collect(),
        )
        .map_err(|e| datafusion_common::arrow_datafusion_err!(e))?;
        let group_rows = group_conv
            .convert_columns(&group_cols)
            .map_err(|e| datafusion_common::arrow_datafusion_err!(e))?;
        let full_rows = full_conv
            .convert_columns(&all_cols)
            .map_err(|e| datafusion_common::arrow_datafusion_err!(e))?;
        let ends =
            window_end_micros(agg_batch).unwrap_or_else(|| vec![None; agg_batch.num_rows()]);
        for i in 0..agg_batch.num_rows() {
            let gkey = group_rows.row(i).owned();
            let fkey = full_rows.row(i).owned();
            let end = ends.get(i).copied().flatten().unwrap_or(i64::MAX);
            let row = agg_batch.slice(i, 1);
            match acc.last_emitted.get(&gkey) {
                Some((_, prev_fkey, _)) if *prev_fkey == fkey => {} // unchanged → skip
                Some((_, _, prev)) => {
                    retracts.push(prev.clone());
                    inserts.push(row.clone());
                    acc.last_emitted.insert(gkey, (end, fkey, row));
                }
                None => {
                    inserts.push(row.clone());
                    acc.last_emitted.insert(gkey, (end, fkey, row));
                }
            }
        }
    }
    if let Some(first) = retracts.first() {
        let schema = first.schema();
        let batch =
            concat_batches(&schema, &retracts).map_err(|e| datafusion_common::arrow_datafusion_err!(e))?;
        let n = batch.num_rows();
        buf.push_back(FlowEvent::Data {
            batch,
            retracted: retracted_flags(n, true),
        });
    }
    if let Some(first) = inserts.first() {
        let schema = first.schema();
        let batch =
            concat_batches(&schema, &inserts).map_err(|e| datafusion_common::arrow_datafusion_err!(e))?;
        let n = batch.num_rows();
        buf.push_back(FlowEvent::Data {
            batch,
            retracted: retracted_flags(n, false),
        });
    }
    // Stop tracking windows that can no longer change (`end + lateness ≤ wm`).
    acc.last_emitted
        .retain(|_, (end, _, _)| end.saturating_add(allowed_lateness) > wm);
    Ok(())
}

/// F5.3 compaction: merge many partial-state rows into ONE per `(window, key)` group WITHOUT
/// finalizing, via DataFusion's `AggregateMode::PartialReduce` (input = intermediate accumulator
/// state, output = intermediate accumulator state — the tree-reduce merge step; see the pinned
/// `datafusion-physical-plan` `AggregateMode` docs). Input and output schema are both
/// `partial_schema`, so the compacted partials are a drop-in replacement that the `Final` merge
/// still consumes identically. Collapses the per-batch partial pile-up so open-window state trends
/// to O(distinct groups) (like a Flink keyed accumulator) instead of O(batches × groups).
async fn compact_partials(
    partials: Vec<RecordBatch>,
    final_group_by: &PhysicalGroupBy,
    aggr_exprs: &[Arc<AggregateFunctionExpr>],
    partial_schema: &SchemaRef,
    context: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    if partials.len() <= 1 {
        return Ok(partials);
    }
    let input = Arc::new(StaticBatchExec::new(partials, partial_schema.clone()));
    let agg = AggregateExec::try_new(
        AggregateMode::PartialReduce,
        final_group_by.clone(),
        aggr_exprs.to_vec(),
        vec![None; aggr_exprs.len()],
        input,
        partial_schema.clone(),
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

/// Mask for aggregate-output rows whose window has closed (`end ≤ watermark`) and has not been
/// emitted in a PRIOR finalize. PURE — reads `emitted` but never mutates it. `None` if no window
/// column or no watermark yet.
///
/// Mutating `emitted` here was a correctness bug: a single closed window with > `batch_size`
/// (8192) groups is emitted by the final aggregate as MULTIPLE Arrow batches; inserting the
/// window's `end` after the first batch made `window_emit_mask` suppress every subsequent batch of
/// the SAME window in the same finalize (measured: 8 partitions × 8192 = 65536-key cap, silent
/// loss past 64k keys). The caller now marks ends emitted ONCE, after processing all batches of the
/// finalize (see `mark_emitted_ends`), so all batches of a window emit together and only a LATER
/// finalize is suppressed.
fn window_emit_mask(
    batch: &RecordBatch,
    watermark_micros: Option<i64>,
    emitted: &HashSet<i64>,
) -> Option<BooleanArray> {
    let wm = watermark_micros?;
    let ends = window_end_micros(batch)?;
    let mut b = BooleanBuilder::with_capacity(ends.len());
    for end in &ends {
        let emit = end.is_some_and(|e| e <= wm && !emitted.contains(&e));
        b.append_value(emit);
    }
    Some(b.finish())
}

/// Record every closed-window end (`end ≤ wm`) in `batch` as emitted, so a LATER finalize doesn't
/// re-emit the window. Called once after ALL batches of a finalize are emitted (see the bug note on
/// `window_emit_mask`).
fn mark_emitted_ends(batch: &RecordBatch, wm: i64, emitted: &mut HashSet<i64>) {
    if let Some(ends) = window_end_micros(batch) {
        for end in ends.into_iter().flatten() {
            if end <= wm {
                emitted.insert(end);
            }
        }
    }
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

// ---------------------------------------------------------------------------
// Update-mode (changelog) unit tests — prove zero-loss late-data convergence.
// These exercise emit_changelog directly with hand-built aggregate batches
// (window struct + key + count), simulating late data that changes a window's
// count across finalize rounds. The full append pipeline is EKS-validated.
// ---------------------------------------------------------------------------
#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod update_mode_tests {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use datafusion::arrow::array::{Int64Array, StructArray, TimestampMicrosecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};

    use super::*;

    // One-row aggregate batch: window [start,end), key k, count c (all micros).
    fn agg_row(start: i64, end: i64, k: i64, c: i64) -> RecordBatch {
        let win_fields = Fields::from(vec![
            Field::new("start", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("end", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        ]);
        let win = StructArray::from(vec![
            (
                Arc::new(Field::new("start", DataType::Timestamp(TimeUnit::Microsecond, None), false)),
                Arc::new(TimestampMicrosecondArray::from(vec![start])) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("end", DataType::Timestamp(TimeUnit::Microsecond, None), false)),
                Arc::new(TimestampMicrosecondArray::from(vec![end])) as Arc<dyn Array>,
            ),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("window", DataType::Struct(win_fields), false),
            Field::new("k", DataType::Int64, false),
            Field::new("count", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(win),
                Arc::new(Int64Array::from(vec![k])),
                Arc::new(Int64Array::from(vec![c])),
            ],
        )
        .unwrap()
    }

    // Extract (retracted_flag, count) pairs from the buffered changelog events.
    fn drain(buf: &mut VecDeque<FlowEvent>) -> Vec<(bool, i64)> {
        let mut out = vec![];
        while let Some(ev) = buf.pop_front() {
            if let FlowEvent::Data { batch, retracted } = ev {
                let cnt = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    out.push((retracted.value(i), cnt.value(i)));
                }
            }
        }
        out
    }

    #[test]
    fn late_data_converges_via_retract_insert() {
        let mut acc = AccumState::new();
        let mut buf = VecDeque::new();
        let lateness = 5_000_000; // 5s
        let win = (0, 10_000_000); // [0s, 10s)

        // Round 1 @wm=8s: window still open, count=5 → one INSERT, no retract.
        emit_changelog(&mut acc, &[agg_row(win.0, win.1, 1, 5)], Some(8_000_000), lateness, 2, &mut buf).unwrap();
        assert_eq!(drain(&mut buf), vec![(false, 5)], "first emit is a plain insert");

        // Round 2 @wm=12s (window CLOSED at 10s, but within lateness): late data lifted
        // count 5→7. Append mode would have dropped this; update mode must RETRACT 5 + INSERT 7.
        emit_changelog(&mut acc, &[agg_row(win.0, win.1, 1, 7)], Some(12_000_000), lateness, 2, &mut buf).unwrap();
        assert_eq!(
            drain(&mut buf),
            vec![(true, 5), (false, 7)],
            "late-in-bound update retracts stale 5 and inserts converged 7 (zero loss)"
        );

        // Round 3 @wm=12s, no change → no output (idempotent).
        emit_changelog(&mut acc, &[agg_row(win.0, win.1, 1, 7)], Some(12_000_000), lateness, 2, &mut buf).unwrap();
        assert_eq!(drain(&mut buf), vec![], "unchanged window emits nothing");

        // Round 4 @wm=20s (> end+lateness=15s): window finalized → dropped from tracking.
        assert!(acc.last_emitted.len() == 1);
        emit_changelog(&mut acc, &[agg_row(win.0, win.1, 1, 7)], Some(20_000_000), lateness, 2, &mut buf).unwrap();
        assert!(acc.last_emitted.is_empty(), "finalized window evicted from changelog state");
    }

    #[test]
    fn independent_keys_tracked_separately() {
        let mut acc = AccumState::new();
        let mut buf = VecDeque::new();
        let win = (0, 10_000_000);
        // Two keys in the same window.
        emit_changelog(
            &mut acc,
            &[agg_row(win.0, win.1, 1, 3), agg_row(win.0, win.1, 2, 4)],
            Some(5_000_000),
            0,
            2,
            &mut buf,
        )
        .unwrap();
        assert_eq!(drain(&mut buf), vec![(false, 3), (false, 4)], "both keys inserted");
        // Only key 2 changes.
        emit_changelog(
            &mut acc,
            &[agg_row(win.0, win.1, 1, 3), agg_row(win.0, win.1, 2, 9)],
            Some(6_000_000),
            0,
            2,
            &mut buf,
        )
        .unwrap();
        assert_eq!(drain(&mut buf), vec![(true, 4), (false, 9)], "only changed key emits retract+insert");
    }
}

// ---------------------------------------------------------------------------
// End-to-end operator test: drive the real WindowAccumExec on an OUT-OF-ORDER
// watermarked stream and prove the north-star contract (docs/STREAMING_ARCHITECTURE.md):
// update mode converges to the batch-truth count (zero loss); append drops late data.
// Window struct is pre-attached (the spark_window UDF that computes it is upstream and
// out of scope here) so we exercise the stateful operator: watermark eviction, changelog
// emit, and allowed-lateness retention.
// ---------------------------------------------------------------------------
#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod update_mode_e2e_tests {
    use std::any::Any;
    use std::sync::Arc;

    use chrono::DateTime;
    use datafusion::arrow::array::{Int64Array, StructArray, TimestampMicrosecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
    use datafusion::physical_plan::aggregates::PhysicalGroupBy;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
    use futures::stream;
    use sail_common_datafusion::streaming::event::encoding::{
        DecodedFlowEventStream, EncodedFlowEventStream,
    };
    use sail_common_datafusion::streaming::event::marker::FlowMarker;
    use sail_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;

    use super::*;

    // Synthetic flow-event source: yields a fixed Vec<FlowEvent> as an encoded flow-event stream
    // (same shape KafkaSourceExec produces), one bounded partition.
    #[derive(Debug)]
    struct FlowEventSource {
        events: Vec<FlowEvent>,
        data_schema: SchemaRef,
        properties: Arc<PlanProperties>,
    }
    impl FlowEventSource {
        fn new(events: Vec<FlowEvent>, data_schema: SchemaRef) -> Self {
            let flow_schema = Arc::new(to_flow_event_schema(&data_schema));
            let properties = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(flow_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Both,
                Boundedness::Bounded,
            ));
            Self { events, data_schema, properties }
        }
    }
    impl DisplayAs for FlowEventSource {
        fn fmt_as(&self, _: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "FlowEventSource")
        }
    }
    impl ExecutionPlan for FlowEventSource {
        fn name(&self) -> &str { "FlowEventSource" }
        fn as_any(&self) -> &dyn Any { self }
        fn properties(&self) -> &Arc<PlanProperties> { &self.properties }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }
        fn with_new_children(self: Arc<Self>, _: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> { Ok(self) }
        fn execute(&self, _p: usize, _c: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
            let events = self.events.clone();
            let s = stream::iter(events.into_iter().map(Ok));
            let flow = Box::pin(FlowEventStreamAdapter::new(self.data_schema.clone(), s));
            Ok(Box::pin(EncodedFlowEventStream::new(flow)))
        }
    }

    fn data_schema() -> SchemaRef {
        let win = DataType::Struct(Fields::from(vec![
            Field::new("start", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("end", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        ]));
        Arc::new(Schema::new(vec![
            Field::new("window", win, false),
            Field::new("k", DataType::Int64, false),
        ]))
    }

    // n rows all in window [start,end) for key k.
    fn rows(schema: &SchemaRef, start: i64, end: i64, k: i64, n: usize) -> FlowEvent {
        let win = StructArray::from(vec![
            (
                Arc::new(Field::new("start", DataType::Timestamp(TimeUnit::Microsecond, None), false)),
                Arc::new(TimestampMicrosecondArray::from(vec![start; n])) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("end", DataType::Timestamp(TimeUnit::Microsecond, None), false)),
                Arc::new(TimestampMicrosecondArray::from(vec![end; n])) as Arc<dyn Array>,
            ),
        ]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(win), Arc::new(Int64Array::from(vec![k; n]))],
        )
        .unwrap();
        FlowEvent::append_only_data(batch)
    }

    // n DISTINCT keys (k=0..n-1) in one window [start,end).
    fn rows_distinct(schema: &SchemaRef, start: i64, end: i64, n: usize) -> FlowEvent {
        let win = StructArray::from(vec![
            (
                Arc::new(Field::new("start", DataType::Timestamp(TimeUnit::Microsecond, None), false)),
                Arc::new(TimestampMicrosecondArray::from(vec![start; n])) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("end", DataType::Timestamp(TimeUnit::Microsecond, None), false)),
                Arc::new(TimestampMicrosecondArray::from(vec![end; n])) as Arc<dyn Array>,
            ),
        ]);
        let ks: Vec<i64> = (0..n as i64).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(win), Arc::new(Int64Array::from(ks))],
        )
        .unwrap();
        FlowEvent::append_only_data(batch)
    }

    fn watermark(micros: i64) -> FlowEvent {
        FlowEvent::Marker(FlowMarker::Watermark {
            source: "test".to_string(),
            timestamp: DateTime::from_timestamp_micros(micros).unwrap(),
        })
    }

    // Regression: a closed window with > batch_size (8192) distinct keys is emitted by the final
    // aggregate as MULTIPLE Arrow batches; all must emit. The bug capped each window at one batch
    // (8192) — manifesting as a 65536 = 8 partitions × 8192 silent key cap. Here, 1 partition,
    // 20000 keys in one window → must emit all 20000.
    #[tokio::test]
    async fn append_emits_all_keys_above_batch_size_no_cap() {
        let s = data_schema();
        let n = 20000usize; // > 8192
        let events = vec![
            rows_distinct(&s, 0, 10_000_000, n),
            watermark(12_000_000), // close the window
            FlowEvent::Marker(FlowMarker::EndOfData),
        ];
        let exec = window_exec(events, WindowOutputMode::Append, 0);
        let result = run_and_net(exec).await;
        assert_eq!(result.len(), n, "all {n} keys must emit (regression: was capped at 8192/partition)");
    }

    fn window_exec(events: Vec<FlowEvent>, mode: WindowOutputMode, lateness: i64) -> Arc<WindowAccumExec> {
        let ds = data_schema();
        let src = Arc::new(FlowEventSource::new(events, ds.clone()));
        let group = PhysicalGroupBy::new_single(vec![
            (Arc::new(Column::new("window", 0)) as _, "window".to_string()),
            (Arc::new(Column::new("k", 1)) as _, "k".to_string()),
        ]);
        let count = AggregateExprBuilder::new(count_udaf(), vec![Arc::new(Column::new("k", 1))])
            .schema(ds.clone())
            .alias("count")
            .build()
            .unwrap();
        Arc::new(
            WindowAccumExec::try_new(src, group, vec![Arc::new(count)], ds, "k".to_string(), 0, None)
                .unwrap()
                .with_output_mode(mode, lateness),
        )
    }

    // Net the emitted changelog into the final per-key count (collector semantics): a retract row
    // cancels a prior insert with the same count value; the surviving count is the materialized result.
    async fn run_and_net(exec: Arc<WindowAccumExec>) -> Vec<i64> {
        let stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        let mut decoded = DecodedFlowEventStream::try_new(stream).unwrap();
        let mut live: Vec<i64> = vec![]; // surviving count values
        while let Some(ev) = decoded.next().await {
            if let FlowEvent::Data { batch, retracted } = ev.unwrap() {
                let cnt = batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..batch.num_rows() {
                    let c = cnt.value(i);
                    if retracted.value(i) {
                        if let Some(pos) = live.iter().position(|x| *x == c) {
                            live.remove(pos);
                        }
                    } else {
                        live.push(c);
                    }
                }
            }
        }
        live.sort_unstable();
        live
    }

    // 5 in-order rows for window W=[0,10s); late 2 rows arrive AFTER W closes (wm past end) but
    // within allowed lateness. Batch truth = 7.
    fn out_of_order_stream() -> Vec<FlowEvent> {
        let s = data_schema();
        let (w0, w1) = (0i64, 10_000_000i64); // [0s, 10s)
        vec![
            rows(&s, w0, w1, 1, 5),       // 5 on-time rows
            watermark(8_000_000),          // wm=8s, window open
            watermark(12_000_000),         // wm=12s, window CLOSED (end 10s)
            rows(&s, w0, w1, 1, 2),       // 2 LATE rows for the closed window
            watermark(13_000_000),         // wm=13s, still within end+lateness for update(L=5s)
            watermark(20_000_000),         // wm=20s, past end+lateness -> evict
            FlowEvent::Marker(FlowMarker::EndOfData),
        ]
    }

    #[tokio::test]
    async fn update_mode_converges_zero_loss() {
        let exec = window_exec(out_of_order_stream(), WindowOutputMode::Update, 5_000_000);
        let result = run_and_net(exec).await;
        assert_eq!(result, vec![7], "update mode converges to batch truth (5+2), zero loss");
    }

    #[tokio::test]
    async fn append_mode_drops_late() {
        let exec = window_exec(out_of_order_stream(), WindowOutputMode::Append, 0);
        let result = run_and_net(exec).await;
        assert_eq!(result, vec![5], "append drops the 2 late rows (window already closed)");
    }
}
