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
use sail_common_datafusion::streaming::checkpoint::CheckpointStore;
use sail_common_datafusion::streaming::event::encoding::{
    DecodedFlowEventStream, EncodedFlowEventStream,
};
use sail_common_datafusion::streaming::event::marker::FlowMarker;
use sail_common_datafusion::streaming::event::schema::to_flow_event_schema;
use sail_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;
use sail_common_datafusion::streaming::event::FlowEvent;

use crate::streaming::window_accum::{SpillSourceExec, StaticBatchExec};

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
    /// Residual (interval) filter applied to matched pairs, against the output schema.
    filter: Option<PhysicalExprRef>,
    /// Interval bounds `(lower_micros, upper_micros)` for state eviction (Flink rule).
    interval_bounds: Option<(i64, i64)>,
    /// Event-time column index in each side's data schema (for eviction).
    left_ts_idx: Option<usize>,
    right_ts_idx: Option<usize>,
    /// Streaming `checkpointLocation`, when set — snapshot the buffered join state on
    /// `EndOfData` and restore it on the next run (stateful exactly-once recovery).
    checkpoint_location: Option<String>,
    properties: Arc<PlanProperties>,
}

impl StreamJoinExec {
    #[expect(clippy::too_many_arguments)]
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
        join_type: JoinType,
        left_data_schema: SchemaRef,
        right_data_schema: SchemaRef,
        filter: Option<PhysicalExprRef>,
        interval_bounds: Option<(i64, i64)>,
        left_ts_idx: Option<usize>,
        right_ts_idx: Option<usize>,
        checkpoint_location: Option<String>,
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
            filter,
            interval_bounds,
            left_ts_idx,
            right_ts_idx,
            checkpoint_location,
            properties,
        })
    }

    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }
    pub fn on(&self) -> &[(PhysicalExprRef, PhysicalExprRef)] {
        &self.on
    }
    pub fn join_type(&self) -> JoinType {
        self.join_type
    }
    pub fn left_data_schema(&self) -> &SchemaRef {
        &self.left_data_schema
    }
    pub fn right_data_schema(&self) -> &SchemaRef {
        &self.right_data_schema
    }
    pub fn filter(&self) -> Option<&PhysicalExprRef> {
        self.filter.as_ref()
    }
    pub fn interval_bounds(&self) -> Option<(i64, i64)> {
        self.interval_bounds
    }
    pub fn left_ts_idx(&self) -> Option<usize> {
        self.left_ts_idx
    }
    pub fn right_ts_idx(&self) -> Option<usize> {
        self.right_ts_idx
    }
    pub fn checkpoint_location(&self) -> Option<&str> {
        self.checkpoint_location.as_deref()
    }
}

impl DisplayAs for StreamJoinExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "StreamJoinExec: type={:?}, on={:?}",
            self.join_type, self.on
        )
    }
}

/// Apply the residual (interval) filter to a join-output batch.
fn apply_filter(batch: RecordBatch, filter: &PhysicalExprRef) -> Result<RecordBatch> {
    let n = batch.num_rows();
    let value = filter.evaluate(&batch)?;
    let array = value.into_array(n)?;
    let mask = array
        .as_any()
        .downcast_ref::<datafusion::arrow::array::BooleanArray>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "stream join filter did not evaluate to a boolean".to_string(),
            )
        })?;
    Ok(datafusion::arrow::compute::filter_record_batch(
        &batch, mask,
    )?)
}

/// Evict buffered rows whose event-time (`ts_idx`, microseconds) is `< threshold`;
/// they can no longer match (Flink interval-join cleanup). Rows with a null/uncomparable
/// timestamp are kept (conservative).
fn evict_older_than(batches: Vec<RecordBatch>, ts_idx: usize, threshold: i64) -> Vec<RecordBatch> {
    use datafusion::arrow::array::{Array, BooleanBuilder, TimestampMicrosecondArray};
    let mut out = Vec::with_capacity(batches.len());
    for b in batches {
        let Some(ts) = b
            .column(ts_idx)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
        else {
            out.push(b);
            continue;
        };
        let mut mask = BooleanBuilder::with_capacity(ts.len());
        for i in 0..ts.len() {
            mask.append_value(ts.is_null(i) || ts.value(i) >= threshold);
        }
        match datafusion::arrow::compute::filter_record_batch(&b, &mask.finish()) {
            Ok(f) if f.num_rows() > 0 => out.push(f),
            Ok(_) => {}
            Err(_) => out.push(b),
        }
    }
    out
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

type MergedStream = futures::stream::SelectAll<
    std::pin::Pin<Box<dyn stream::Stream<Item = (Side, Result<FlowEvent>)> + Send>>,
>;

/// F5-join: per-side buffered state, with spill (Flink-class large join state). Each side's buffer
/// is bounded in RAM by a byte budget; over budget it spills cold batches to the checkpoint store
/// (Arrow-IPC, same `state_io` primitive as the window operator) and tracks the spilled chunk
/// indices. The join PROBES the other side as a lazy stream (in-RAM + spilled) so join memory is
/// bounded regardless of buffer size — the hash is built on the small INCOMING batch.
struct JoinAccum {
    left_buf: Vec<RecordBatch>,
    right_buf: Vec<RecordBatch>,
    left_spilled: Vec<u64>,
    right_spilled: Vec<u64>,
    left_bytes: usize,
    right_bytes: usize,
    next_left: u64,
    next_right: u64,
    lwm: Option<i64>,
    rwm: Option<i64>,
    last_wm: Option<i64>,
    restored: bool,
}

impl JoinAccum {
    fn new() -> Self {
        Self {
            left_buf: vec![],
            right_buf: vec![],
            left_spilled: vec![],
            right_spilled: vec![],
            left_bytes: 0,
            right_bytes: 0,
            next_left: 0,
            next_right: 0,
            lwm: None,
            rwm: None,
            last_wm: None,
            restored: false,
        }
    }
}

const JOIN_LEFT_OP: &str = "join-0-left";
const JOIN_RIGHT_OP: &str = "join-0-right";
/// Single snapshot epoch for the join's incremental checkpoint (the join takes one EndOfData
/// snapshot; the manifest references its already-persisted spill chunks). See state_io inc-ckpt.
const JOIN_SNAPSHOT_EPOCH: u64 = 1;

/// Per-side spill budget (bytes); over this a side's buffer spills cold batches to the checkpoint
/// store. Shared with the window operator's budget env. Default 128 MiB.
fn join_state_budget() -> usize {
    std::env::var("SAIL_STREAMING_STATE_BUDGET_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(128 * 1024 * 1024)
}

/// Spill a side's in-memory buffer to the checkpoint store when it exceeds the budget, evicting it
/// from RAM (Flink RocksDB-style offload). No-op without a checkpoint store (state stays in RAM —
/// the unbounded-by-design case, as in Flink without state TTL/backend).
async fn spill_side_over_budget(
    buf: &mut Vec<RecordBatch>,
    bytes: &mut usize,
    spilled: &mut Vec<u64>,
    next: &mut u64,
    budget: usize,
    ck: Option<&CheckpointStore>,
    op_id: &str,
    schema: &SchemaRef,
) {
    if *bytes > budget && !buf.is_empty() {
        if let Some(ck) = ck {
            let idx = *next;
            if crate::streaming::state_io::write_spill(ck, op_id, idx, schema, buf).await {
                if std::env::var("VAJRA_F5_DEBUG").is_ok() {
                    eprintln!("F5_JOIN_SPILL {op_id} idx={idx}");
                }
                spilled.push(idx);
                *next += 1;
                buf.clear();
                *bytes = 0;
            }
        }
    }
}

/// Full buffered state for a side: in-memory batches plus spilled chunks read back (so spilling is
/// transparent to the join result + the snapshot). Used at snapshot and at interval eviction.
async fn gather_side(
    buf: &[RecordBatch],
    spilled: &[u64],
    ck: Option<&CheckpointStore>,
    op_id: &str,
) -> Vec<RecordBatch> {
    let mut out = buf.to_vec();
    if let Some(ck) = ck {
        for &idx in spilled {
            out.extend(crate::streaming::state_io::read_spill(ck, op_id, idx).await);
        }
    }
    out
}

/// Interval-eviction for a spillable side: gather the full state (in-RAM + spilled), drop rows
/// older than `threshold`, then re-accumulate (re-spilling over budget into fresh chunks) and GC
/// the consumed spills. Only invoked when interval bounds are set — in which case the live state is
/// bounded by the interval window, so the gather is cheap.
#[expect(clippy::too_many_arguments)]
async fn evict_respill_side(
    buf: &mut Vec<RecordBatch>,
    bytes: &mut usize,
    spilled: &mut Vec<u64>,
    next: &mut u64,
    budget: usize,
    ck: Option<&CheckpointStore>,
    op_id: &str,
    schema: &SchemaRef,
    ts_idx: usize,
    threshold: i64,
) {
    let full = gather_side(buf, spilled, ck, op_id).await;
    if let Some(ck) = ck {
        for &idx in spilled.iter() {
            crate::streaming::state_io::delete_spill(ck, op_id, idx).await;
        }
    }
    spilled.clear();
    let kept = evict_older_than(full, ts_idx, threshold);
    *buf = vec![];
    *bytes = 0;
    for b in kept {
        *bytes += b.get_array_memory_size();
        buf.push(b);
        spill_side_over_budget(buf, bytes, spilled, next, budget, ck, op_id, schema).await;
    }
}

/// Reorder a `right ++ left` join-output batch into `left ++ right` column order (used when the
/// INCOMING batch is on the RIGHT: we build the hash on it and probe the left buffer, then restore
/// the canonical left-then-right output schema). `left_cols`/`right_cols` are the column counts.
fn reorder_right_left_to_left_right(
    batch: &RecordBatch,
    left_cols: usize,
    right_cols: usize,
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let mut cols = Vec::with_capacity(left_cols + right_cols);
    // batch = [right(0..right_cols), left(right_cols..right_cols+left_cols)]
    for i in 0..left_cols {
        cols.push(Arc::clone(batch.column(right_cols + i)));
    }
    for i in 0..right_cols {
        cols.push(Arc::clone(batch.column(i)));
    }
    Ok(RecordBatch::try_new(output_schema.clone(), cols)?)
}

/// Join the INCOMING batch against the OTHER side's full buffer (in-RAM + spilled), with the hash
/// built on the small incoming batch and the (possibly ≫ RAM) buffer streamed lazily via
/// `SpillSourceExec` as the probe — so join memory is bounded by the incoming batch, not the buffer
/// (Flink builds the join hash incrementally per side; we build per incoming batch). Inner-join only
/// (planner-enforced), which is symmetric, so a right-side arrival swaps keys + reorders output.
#[expect(clippy::too_many_arguments)]
async fn join_incoming_against_buffer(
    incoming: RecordBatch,
    incoming_side: Side,
    other_buf: Vec<RecordBatch>,
    other_spilled: Vec<u64>,
    ck: Option<CheckpointStore>,
    on: &[(PhysicalExprRef, PhysicalExprRef)],
    join_type: JoinType,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    output_schema: SchemaRef,
    ctx: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    match incoming_side {
        Side::Left => {
            // build = incoming LEFT; probe = streamed RIGHT buffer; output is already left ++ right.
            let build = Arc::new(StaticBatchExec::new(vec![incoming], left_schema));
            let probe = Arc::new(SpillSourceExec::new(
                other_buf,
                other_spilled,
                ck,
                JOIN_RIGHT_OP.to_string(),
                right_schema,
            ));
            run_hash_join(build, probe, on.to_vec(), join_type, ctx).await
        }
        Side::Right => {
            // build = incoming RIGHT; probe = streamed LEFT buffer; swap keys; reorder output back.
            let left_cols = left_schema.fields().len();
            let right_cols = right_schema.fields().len();
            let build = Arc::new(StaticBatchExec::new(vec![incoming], right_schema));
            let probe = Arc::new(SpillSourceExec::new(
                other_buf,
                other_spilled,
                ck,
                JOIN_LEFT_OP.to_string(),
                left_schema,
            ));
            let swapped: Vec<_> = on.iter().map(|(l, r)| (r.clone(), l.clone())).collect();
            let out = run_hash_join(build, probe, swapped, join_type, ctx).await?;
            out.into_iter()
                .map(|b| reorder_right_left_to_left_right(&b, left_cols, right_cols, &output_schema))
                .collect()
        }
    }
}

/// Run a `HashJoinExec` (build = left input collected, probe = right input streamed) and collect
/// non-empty output batches.
async fn run_hash_join(
    build: Arc<dyn ExecutionPlan>,
    probe: Arc<dyn ExecutionPlan>,
    on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    join_type: JoinType,
    ctx: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    let join = HashJoinExec::try_new(
        build,
        probe,
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
        let [left, right]: [Arc<dyn ExecutionPlan>; 2] = children.try_into().map_err(|_| {
            datafusion_common::DataFusionError::Internal(
                "StreamJoinExec requires exactly two children".to_string(),
            )
        })?;
        Ok(Arc::new(StreamJoinExec::try_new(
            left,
            right,
            self.on.clone(),
            self.join_type,
            self.left_data_schema.clone(),
            self.right_data_schema.clone(),
            self.filter.clone(),
            self.interval_bounds,
            self.left_ts_idx,
            self.right_ts_idx,
            self.checkpoint_location.clone(),
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
        let right_stream =
            DecodedFlowEventStream::try_new(self.right.execute(0, context.clone())?)?;
        // Tag each side and merge into one stream.
        let left_tagged: std::pin::Pin<
            Box<dyn stream::Stream<Item = (Side, Result<FlowEvent>)> + Send>,
        > = Box::pin(left_stream.map(|e| (Side::Left, e)));
        let right_tagged: std::pin::Pin<
            Box<dyn stream::Stream<Item = (Side, Result<FlowEvent>)> + Send>,
        > = Box::pin(right_stream.map(|e| (Side::Right, e)));
        let merged = stream::select_all(vec![left_tagged, right_tagged]);

        let on = self.on.clone();
        let join_type = self.join_type;
        let left_schema = self.left_data_schema.clone();
        let right_schema = self.right_data_schema.clone();
        let output_schema = self.output_data_schema.clone();
        let filter = self.filter.clone();
        let interval_bounds = self.interval_bounds;
        let left_ts_idx = self.left_ts_idx;
        let right_ts_idx = self.right_ts_idx;
        let budget = join_state_budget();
        // Build the checkpoint store synchronously (no I/O); committed join state is restored
        // async on the first poll (execute() is sync). Buffers start empty + restored=false.
        let ck = self
            .checkpoint_location
            .as_deref()
            .and_then(|l| CheckpointStore::from_location(l).ok());
        type UnfoldState = (MergedStream, JoinAccum, VecDeque<FlowEvent>, Arc<TaskContext>);
        let init: UnfoldState = (merged, JoinAccum::new(), VecDeque::new(), context);

        let event_stream = stream::unfold(init, move |(mut merged, mut acc, mut buf, ctx)| {
            let on = on.clone();
            let left_schema = left_schema.clone();
            let right_schema = right_schema.clone();
            let output_schema = output_schema.clone();
            let filter = filter.clone();
            let ck = ck.clone();
            async move {
                // Restore committed join state (+ watermarks) on the first poll. The snapshot is the
                // FULL gathered state (in-RAM + previously-spilled), so it loads back as in-RAM
                // buffers; they re-spill over budget as the join proceeds (EO unchanged by spilling).
                if !acc.restored {
                    if let Some(ck) = &ck {
                        // inc-ckpt.2b (gated VAJRA_INC_CKPT): restore the incremental snapshot (residual
                        // + chunks referenced by the manifest); else the legacy full snapshot. Both
                        // load back as in-RAM buffers (they re-spill over budget as the join proceeds).
                        let inc = std::env::var("VAJRA_INC_CKPT").is_ok();
                        let (lb, meta) = if inc {
                            crate::streaming::state_io::restore_epoch_incremental(
                                ck,
                                JOIN_LEFT_OP,
                                JOIN_SNAPSHOT_EPOCH,
                            )
                            .await
                        } else {
                            crate::streaming::state_io::restore_state(ck, JOIN_LEFT_OP).await
                        };
                        let (rb, _) = if inc {
                            crate::streaming::state_io::restore_epoch_incremental(
                                ck,
                                JOIN_RIGHT_OP,
                                JOIN_SNAPSHOT_EPOCH,
                            )
                            .await
                        } else {
                            crate::streaming::state_io::restore_state(ck, JOIN_RIGHT_OP).await
                        };
                        acc.left_bytes = lb.iter().map(|b| b.get_array_memory_size()).sum();
                        acc.right_bytes = rb.iter().map(|b| b.get_array_memory_size()).sum();
                        acc.left_buf = lb;
                        acc.right_buf = rb;
                        if let [l, r, last] = meta[..] {
                            acc.lwm = (l != i64::MIN).then_some(l);
                            acc.rwm = (r != i64::MIN).then_some(r);
                            acc.last_wm = (last != i64::MIN).then_some(last);
                        }
                    }
                    acc.restored = true;
                }
                loop {
                    if let Some(ev) = buf.pop_front() {
                        return Some((Ok(ev), (merged, acc, buf, ctx)));
                    }
                    match merged.next().await {
                        None => return None,
                        Some((_, Err(e))) => return Some((Err(e), (merged, acc, buf, ctx))),
                        Some((side, Ok(FlowEvent::Data { batch, .. }))) => {
                            if batch.num_rows() == 0 {
                                continue;
                            }
                            // Join the incoming batch against the OTHER side's buffer (in-RAM +
                            // spilled), streamed as the probe — hash built on the small incoming
                            // batch, so join memory is bounded by the batch, not the buffer.
                            let (other_buf, other_spilled) = match side {
                                Side::Left => (acc.right_buf.clone(), acc.right_spilled.clone()),
                                Side::Right => (acc.left_buf.clone(), acc.left_spilled.clone()),
                            };
                            let res = join_incoming_against_buffer(
                                batch.clone(),
                                side,
                                other_buf,
                                other_spilled,
                                ck.clone(),
                                &on,
                                join_type,
                                left_schema.clone(),
                                right_schema.clone(),
                                output_schema.clone(),
                                ctx.clone(),
                            )
                            .await;
                            match res {
                                Err(e) => return Some((Err(e), (merged, acc, buf, ctx))),
                                Ok(out) => {
                                    for b in out {
                                        // Residual (interval) time-range filter on matches.
                                        let b = match &filter {
                                            Some(f) => match apply_filter(b, f) {
                                                Ok(fb) => fb,
                                                Err(e) => {
                                                    return Some((Err(e), (merged, acc, buf, ctx)))
                                                }
                                            },
                                            None => b,
                                        };
                                        if b.num_rows() > 0 {
                                            buf.push_back(data_event(b));
                                        }
                                    }
                                }
                            }
                            // Append the incoming batch to its OWN side's buffer; spill over budget.
                            let sz = batch.get_array_memory_size();
                            match side {
                                Side::Left => {
                                    acc.left_bytes += sz;
                                    acc.left_buf.push(batch);
                                    spill_side_over_budget(
                                        &mut acc.left_buf,
                                        &mut acc.left_bytes,
                                        &mut acc.left_spilled,
                                        &mut acc.next_left,
                                        budget,
                                        ck.as_ref(),
                                        JOIN_LEFT_OP,
                                        &left_schema,
                                    )
                                    .await;
                                }
                                Side::Right => {
                                    acc.right_bytes += sz;
                                    acc.right_buf.push(batch);
                                    spill_side_over_budget(
                                        &mut acc.right_buf,
                                        &mut acc.right_bytes,
                                        &mut acc.right_spilled,
                                        &mut acc.next_right,
                                        budget,
                                        ck.as_ref(),
                                        JOIN_RIGHT_OP,
                                        &right_schema,
                                    )
                                    .await;
                                }
                            }
                        }
                        Some((
                            side,
                            Ok(FlowEvent::Marker(FlowMarker::Watermark { timestamp, .. })),
                        )) => {
                            let ts = timestamp.timestamp_micros();
                            match side {
                                Side::Left => acc.lwm = Some(acc.lwm.map_or(ts, |c| c.max(ts))),
                                Side::Right => acc.rwm = Some(acc.rwm.map_or(ts, |c| c.max(ts))),
                            }
                            // Operator watermark = min of both inputs (only once both seen).
                            if let (Some(l), Some(r)) = (acc.lwm, acc.rwm) {
                                let m = l.min(r);
                                if acc.last_wm.is_none_or(|prev| m > prev) {
                                    acc.last_wm = Some(m);
                                    if let Some(t) = DateTime::from_timestamp_micros(m) {
                                        buf.push_back(FlowEvent::Marker(FlowMarker::Watermark {
                                            source: "stream-join".to_string(),
                                            timestamp: t,
                                        }));
                                    }
                                }
                            }
                            // Interval-join eviction (Flink rule), spill-aware: gather (in-RAM +
                            // spilled) -> drop rows that can no longer match -> re-spill. Only with
                            // bounds + event-time columns; otherwise state is unbounded-by-design.
                            if let Some((lower, upper)) = interval_bounds {
                                if let (Some(rw), Some(idx)) = (acc.rwm, left_ts_idx) {
                                    evict_respill_side(
                                        &mut acc.left_buf,
                                        &mut acc.left_bytes,
                                        &mut acc.left_spilled,
                                        &mut acc.next_left,
                                        budget,
                                        ck.as_ref(),
                                        JOIN_LEFT_OP,
                                        &left_schema,
                                        idx,
                                        rw - upper,
                                    )
                                    .await;
                                }
                                if let (Some(lw), Some(idx)) = (acc.lwm, right_ts_idx) {
                                    evict_respill_side(
                                        &mut acc.right_buf,
                                        &mut acc.right_bytes,
                                        &mut acc.right_spilled,
                                        &mut acc.next_right,
                                        budget,
                                        ck.as_ref(),
                                        JOIN_RIGHT_OP,
                                        &right_schema,
                                        idx,
                                        lw + lower,
                                    )
                                    .await;
                                }
                            }
                        }
                        Some((_, Ok(FlowEvent::Marker(FlowMarker::EndOfData)))) => {
                            // Snapshot the FULL buffered join state (in-RAM + spilled folded in) so
                            // it survives a restart; the runner commits it after the output is
                            // durable. Spilling is transparent to recovery.
                            if let Some(ck) = &ck {
                                let meta = vec![
                                    acc.lwm.unwrap_or(i64::MIN),
                                    acc.rwm.unwrap_or(i64::MIN),
                                    acc.last_wm.unwrap_or(i64::MIN),
                                ];
                                if std::env::var("VAJRA_INC_CKPT").is_ok() {
                                    // inc-ckpt.2b (O(delta)): write only the in-RAM residual + a
                                    // manifest referencing the already-persisted spill chunks — no
                                    // re-gather/re-write of the spilled bulk (it was written off the
                                    // barrier path during spill). Restore folds residual ++ chunks back.
                                    crate::streaming::state_io::stage_epoch_incremental(
                                        ck,
                                        JOIN_LEFT_OP,
                                        JOIN_SNAPSHOT_EPOCH,
                                        &left_schema,
                                        &acc.left_buf,
                                        &acc.left_spilled,
                                        &meta,
                                    )
                                    .await;
                                    crate::streaming::state_io::stage_epoch_incremental(
                                        ck,
                                        JOIN_RIGHT_OP,
                                        JOIN_SNAPSHOT_EPOCH,
                                        &right_schema,
                                        &acc.right_buf,
                                        &acc.right_spilled,
                                        &[],
                                    )
                                    .await;
                                } else {
                                    let lfull = gather_side(
                                        &acc.left_buf,
                                        &acc.left_spilled,
                                        Some(ck),
                                        JOIN_LEFT_OP,
                                    )
                                    .await;
                                    let rfull = gather_side(
                                        &acc.right_buf,
                                        &acc.right_spilled,
                                        Some(ck),
                                        JOIN_RIGHT_OP,
                                    )
                                    .await;
                                    crate::streaming::state_io::stage_state(
                                        ck,
                                        JOIN_LEFT_OP,
                                        &left_schema,
                                        &lfull,
                                        &meta,
                                    )
                                    .await;
                                    crate::streaming::state_io::stage_state(
                                        ck,
                                        JOIN_RIGHT_OP,
                                        &right_schema,
                                        &rfull,
                                        &[],
                                    )
                                    .await;
                                }
                            }
                            buf.push_back(FlowEvent::Marker(FlowMarker::EndOfData));
                        }
                        Some((_, Ok(other))) => {
                            buf.push_back(other);
                        }
                    }
                }
            }
        });

        let flow_stream = Box::pin(FlowEventStreamAdapter::new(
            self.output_data_schema.clone(),
            event_stream,
        ));
        Ok(Box::pin(EncodedFlowEventStream::new(flow_stream)))
    }
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::any::Any;

    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::Partitioning;

    use super::*;

    // --- reorder_right_left_to_left_right: pure-function correctness (right-arrival path) ---
    #[test]
    fn reorder_right_left_to_left_right_swaps_column_blocks() {
        // build = right side [rk, rv]; probe = left side [lk, lv]; HashJoin output = right ++ left.
        let schema = Arc::new(Schema::new(vec![
            Field::new("rk", DataType::Int64, false),
            Field::new("rv", DataType::Int64, false),
            Field::new("lk", DataType::Int64, false),
            Field::new("lv", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])), // rk
                Arc::new(Int64Array::from(vec![100])), // rv
                Arc::new(Int64Array::from(vec![1])), // lk
                Arc::new(Int64Array::from(vec![10])), // lv
            ],
        )
        .unwrap();
        // canonical left ++ right output: [lk, lv, rk, rv]
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("lk", DataType::Int64, false),
            Field::new("lv", DataType::Int64, false),
            Field::new("rk", DataType::Int64, false),
            Field::new("rv", DataType::Int64, false),
        ]));
        let reordered =
            reorder_right_left_to_left_right(&batch, 2, 2, &(out_schema as SchemaRef)).unwrap();
        let col = |i: usize| {
            reordered
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!((col(0), col(1), col(2), col(3)), (1, 10, 1, 100), "lk,lv,rk,rv");
    }

    // --- full inner-join correctness over the streaming-probe path ---
    #[derive(Debug)]
    struct OneShotSource {
        events: Vec<FlowEvent>,
        data_schema: SchemaRef,
        props: Arc<PlanProperties>,
    }
    impl OneShotSource {
        fn new(events: Vec<FlowEvent>, data_schema: SchemaRef) -> Self {
            let flow = Arc::new(to_flow_event_schema(&data_schema));
            let props = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(flow),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Both,
                Boundedness::Bounded,
            ));
            Self { events, data_schema, props }
        }
    }
    impl DisplayAs for OneShotSource {
        fn fmt_as(&self, _: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "OneShotSource")
        }
    }
    impl ExecutionPlan for OneShotSource {
        fn name(&self) -> &str { "OneShotSource" }
        fn as_any(&self) -> &dyn Any { self }
        fn properties(&self) -> &Arc<PlanProperties> { &self.props }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }
        fn with_new_children(self: Arc<Self>, _: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> { Ok(self) }
        fn execute(&self, _p: usize, _c: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
            let s = stream::iter(self.events.clone().into_iter().map(Ok));
            let flow = Box::pin(FlowEventStreamAdapter::new(self.data_schema.clone(), s));
            Ok(Box::pin(EncodedFlowEventStream::new(flow)))
        }
    }

    fn kv_schema(kname: &str, vname: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(kname, DataType::Int64, false),
            Field::new(vname, DataType::Int64, false),
        ]))
    }
    fn kv_batch(schema: &SchemaRef, ks: Vec<i64>, vs: Vec<i64>) -> FlowEvent {
        let b = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(ks)), Arc::new(Int64Array::from(vs))],
        )
        .unwrap();
        FlowEvent::append_only_data(b)
    }

    #[tokio::test]
    async fn inner_join_streaming_probe_emits_all_pairs() {
        let ls = kv_schema("k", "lv");
        let rs = kv_schema("k", "rv");
        let left = Arc::new(OneShotSource::new(
            vec![kv_batch(&ls, vec![1, 2], vec![10, 20]), FlowEvent::Marker(FlowMarker::EndOfData)],
            ls.clone(),
        ));
        let right = Arc::new(OneShotSource::new(
            vec![kv_batch(&rs, vec![1, 2, 1], vec![100, 200, 101]), FlowEvent::Marker(FlowMarker::EndOfData)],
            rs.clone(),
        ));
        let on: Vec<(PhysicalExprRef, PhysicalExprRef)> =
            vec![(Arc::new(Column::new("k", 0)), Arc::new(Column::new("k", 0)))];
        let exec = Arc::new(
            StreamJoinExec::try_new(left, right, on, JoinType::Inner, ls, rs, None, None, None, None, None)
                .unwrap(),
        );
        let mut decoded =
            DecodedFlowEventStream::try_new(exec.execute(0, Arc::new(TaskContext::default())).unwrap())
                .unwrap();
        // collect output rows as (lk, lv, rk, rv)
        let mut rows: Vec<(i64, i64, i64, i64)> = vec![];
        while let Some(ev) = decoded.next().await {
            if let FlowEvent::Data { batch, .. } = ev.unwrap() {
                let c = |i: usize| {
                    batch.column(i).as_any().downcast_ref::<Int64Array>().unwrap().clone()
                };
                let (a, b, cc, d) = (c(0), c(1), c(2), c(3));
                for i in 0..batch.num_rows() {
                    rows.push((a.value(i), b.value(i), cc.value(i), d.value(i)));
                }
            }
        }
        rows.sort_unstable();
        // left {(1,10),(2,20)} INNER right {(1,100),(2,200),(1,101)} on k:
        // k=1 -> (1,10)x(1,100),(1,101); k=2 -> (2,20)x(2,200)
        assert_eq!(
            rows,
            vec![(1, 10, 1, 100), (1, 10, 1, 101), (2, 20, 2, 200)],
            "inner join emits every matching pair exactly once, columns left++right"
        );
    }

    // Spill→probe path (race-free, no env): the right buffer is entirely SPILLED to the checkpoint
    // store; a left batch must still join correctly by streaming the spilled chunks as the probe.
    #[tokio::test]
    async fn join_probes_spilled_buffer() {
        let ls = kv_schema("k", "lv");
        let rs = kv_schema("k", "rv");
        let ctx = Arc::new(TaskContext::default());
        let dir = std::env::temp_dir().join(format!("f5join-{}", std::process::id()));
        let ck = CheckpointStore::from_location(dir.to_str().unwrap()).unwrap();

        // Spill the right side as two chunks (simulates over-budget eviction to disk).
        let r0 = match kv_batch(&rs, vec![1], vec![100]) {
            FlowEvent::Data { batch, .. } => batch,
            _ => unreachable!(),
        };
        let r1 = match kv_batch(&rs, vec![2], vec![200]) {
            FlowEvent::Data { batch, .. } => batch,
            _ => unreachable!(),
        };
        assert!(crate::streaming::state_io::write_spill(&ck, JOIN_RIGHT_OP, 0, &rs, &[r0]).await);
        assert!(crate::streaming::state_io::write_spill(&ck, JOIN_RIGHT_OP, 1, &rs, &[r1]).await);

        let incoming = match kv_batch(&ls, vec![1, 2], vec![10, 20]) {
            FlowEvent::Data { batch, .. } => batch,
            _ => unreachable!(),
        };
        let on: Vec<(PhysicalExprRef, PhysicalExprRef)> =
            vec![(Arc::new(Column::new("k", 0)), Arc::new(Column::new("k", 0)))];
        let out_schema = {
            let trial = HashJoinExec::try_new(
                Arc::new(EmptyExec::new(ls.clone())),
                Arc::new(EmptyExec::new(rs.clone())),
                on.clone(),
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap();
            trial.schema()
        };
        // Left arrives; right buffer is fully spilled (in-mem empty, spilled=[0,1]).
        let out = join_incoming_against_buffer(
            incoming,
            Side::Left,
            vec![],
            vec![0, 1],
            Some(ck.clone()),
            &on,
            JoinType::Inner,
            ls,
            rs,
            out_schema,
            ctx,
        )
        .await
        .unwrap();
        let mut rows: Vec<(i64, i64, i64, i64)> = vec![];
        for batch in &out {
            let c = |i: usize| batch.column(i).as_any().downcast_ref::<Int64Array>().unwrap().clone();
            let (a, b, cc, d) = (c(0), c(1), c(2), c(3));
            for i in 0..batch.num_rows() {
                rows.push((a.value(i), b.value(i), cc.value(i), d.value(i)));
            }
        }
        rows.sort_unstable();
        crate::streaming::state_io::delete_spill(&ck, JOIN_RIGHT_OP, 0).await;
        crate::streaming::state_io::delete_spill(&ck, JOIN_RIGHT_OP, 1).await;
        assert_eq!(
            rows,
            vec![(1, 10, 1, 100), (2, 20, 2, 200)],
            "join must read the SPILLED right buffer back through the streamed probe"
        );
    }
}
