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
    /// Optional residual (non-equi) filter applied to matched pairs.
    pub filter: Option<Expr>,
    pub join_type: JoinType,
    /// Flow-event output schema (marker/retracted + the join's data columns).
    schema: DFSchemaRef,
}

impl StreamJoinNode {
    pub fn new(
        left: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        on: Vec<(Expr, Expr)>,
        filter: Option<Expr>,
        join_type: JoinType,
        schema: DFSchemaRef,
    ) -> Self {
        Self {
            left,
            right,
            on,
            filter,
            join_type,
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
            schema: self.schema.clone(),
        })
    }
}
