use std::fmt::Formatter;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion_common::DFSchemaRef;
use datafusion_expr::{Expr, JoinType, LogicalPlan, UserDefinedLogicalNodeCore};

/// Logical node for a **stateful stream–stream equi-join**.
///
/// Both inputs are unbounded flow-event streams. Physical planning maps this to
/// `StreamJoinExec`, which buffers rows from both sides keyed by the join key and
/// emits each match exactly once as rows arrive on either side, advancing its
/// watermark as the min of the two inputs' watermarks.
#[derive(Clone, Debug)]
pub struct StreamJoinNode {
    left: Arc<LogicalPlan>,
    right: Arc<LogicalPlan>,
    /// Equi-join key pairs `(left_expr, right_expr)`.
    pub on: Vec<(Expr, Expr)>,
    /// Optional residual (non-equi) filter applied to matched pairs — e.g. the
    /// interval-join time-range condition.
    pub filter: Option<Expr>,
    pub join_type: JoinType,
    /// Event-time columns (resolved names) per side, present when that side has a
    /// `withWatermark`. Used together with `interval_bounds` to evict state.
    pub left_event_time: Option<String>,
    pub right_event_time: Option<String>,
    /// Interval-join bounds `(lower_micros, upper_micros)` extracted from the time-range
    /// filter: a match requires `right.ts ∈ [left.ts + lower, left.ts + upper]`. When set
    /// (with both event-time columns), `StreamJoinExec` evicts state per the Flink rule.
    pub interval_bounds: Option<(i64, i64)>,
    /// Streaming `checkpointLocation`, when set — for join-buffer state snapshot/restore
    /// (stateful exactly-once recovery; see docs/design/streaming-exactly-once.md).
    pub checkpoint_location: Option<String>,
    /// Flow-event output schema (marker/retracted + the join's data columns).
    schema: DFSchemaRef,
}

impl StreamJoinNode {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        left: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        on: Vec<(Expr, Expr)>,
        filter: Option<Expr>,
        join_type: JoinType,
        left_event_time: Option<String>,
        right_event_time: Option<String>,
        interval_bounds: Option<(i64, i64)>,
        checkpoint_location: Option<String>,
        schema: DFSchemaRef,
    ) -> Self {
        Self {
            left,
            right,
            on,
            filter,
            join_type,
            left_event_time,
            right_event_time,
            interval_bounds,
            checkpoint_location,
            schema,
        }
    }

    pub fn left(&self) -> &Arc<LogicalPlan> {
        &self.left
    }
    pub fn right(&self) -> &Arc<LogicalPlan> {
        &self.right
    }
}

impl PartialEq for StreamJoinNode {
    fn eq(&self, other: &Self) -> bool {
        self.left == other.left
            && self.right == other.right
            && self.on == other.on
            && self.filter == other.filter
            && self.join_type == other.join_type
            && self.left_event_time == other.left_event_time
            && self.right_event_time == other.right_event_time
            && self.interval_bounds == other.interval_bounds
            && self.checkpoint_location == other.checkpoint_location
            && self.schema == other.schema
    }
}

impl Eq for StreamJoinNode {}

impl Hash for StreamJoinNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.left.hash(state);
        self.right.hash(state);
        self.filter.hash(state);
        self.join_type.hash(state);
        self.left_event_time.hash(state);
        self.right_event_time.hash(state);
        self.interval_bounds.hash(state);
        self.checkpoint_location.hash(state);
        self.schema.hash(state);
    }
}

impl PartialOrd for StreamJoinNode {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for StreamJoinNode {
    fn name(&self) -> &str {
        "StreamJoin"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.left.as_ref(), self.right.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        // Surface the equi-key and filter expressions so the optimizer treats them as
        // referenced (prevents column pruning of the join keys).
        let mut exprs: Vec<Expr> = self
            .on
            .iter()
            .flat_map(|(l, r)| [l.clone(), r.clone()])
            .collect();
        if let Some(f) = &self.filter {
            exprs.push(f.clone());
        }
        exprs
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "StreamJoin: type={:?}, on={:?}", self.join_type, self.on)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        let [left, right]: [LogicalPlan; 2] = inputs.try_into().map_err(|_| {
            datafusion_common::DataFusionError::Internal(
                "StreamJoin requires exactly two inputs".to_string(),
            )
        })?;
        // Reconstruct on/filter from the flattened expressions.
        let mut iter = exprs.into_iter();
        let mut on = Vec::with_capacity(self.on.len());
        for _ in 0..self.on.len() {
            match (iter.next(), iter.next()) {
                (Some(l), Some(r)) => on.push((l, r)),
                _ => {
                    return Err(datafusion_common::DataFusionError::Internal(
                        "StreamJoin expression count mismatch".to_string(),
                    ))
                }
            }
        }
        let filter = iter.next();
        Ok(Self {
            left: Arc::new(left),
            right: Arc::new(right),
            on,
            filter,
            join_type: self.join_type,
            left_event_time: self.left_event_time.clone(),
            right_event_time: self.right_event_time.clone(),
            interval_bounds: self.interval_bounds,
            checkpoint_location: self.checkpoint_location.clone(),
            schema: self.schema.clone(),
        })
    }
}
