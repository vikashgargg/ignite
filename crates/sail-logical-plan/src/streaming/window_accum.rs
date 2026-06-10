use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion_common::{plan_err, DFSchemaRef, Result};
use datafusion_expr::{Expr, UserDefinedLogicalNodeCore};
use educe::Educe;

/// A logical plan node for stateful event-time window aggregation in streaming.
///
/// The input plan produces data-only rows (no flow-event columns).
/// At each checkpoint boundary, the executor re-aggregates all pending input rows,
/// emits windows whose end timestamp ≤ the current watermark, and evicts
/// rows that can no longer affect future windows.
///
/// `output_schema` is the flow-event schema wrapping the aggregate output.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct WindowAccumNode {
    /// Data-only input (filter-retracted + projected, no _marker/_retracted cols).
    input: Arc<LogicalPlan>,
    /// GROUP BY expressions (includes the window() alias).
    pub group_exprs: Vec<Expr>,
    /// Aggregate function expressions.
    pub aggr_exprs: Vec<Expr>,
    /// Name of the event-time column in the data-only input schema.
    pub event_time_col: String,
    /// Watermark delay in microseconds.
    pub delay_micros: i64,
    /// Output schema in flow-event format (data cols + _marker + _retracted).
    #[educe(PartialOrd(ignore))]
    output_schema: DFSchemaRef,
    /// Schema of just the data columns (aggregate output schema).
    #[educe(PartialOrd(ignore))]
    pub data_schema: DFSchemaRef,
    /// Streaming `checkpointLocation`, when set — for operator-state snapshot/restore
    /// (stateful exactly-once recovery; see docs/design/streaming-exactly-once.md).
    pub checkpoint_location: Option<String>,
}

impl WindowAccumNode {
    pub fn new(
        input: LogicalPlan,
        group_exprs: Vec<Expr>,
        aggr_exprs: Vec<Expr>,
        event_time_col: String,
        delay_micros: i64,
        output_schema: DFSchemaRef,
        data_schema: DFSchemaRef,
        checkpoint_location: Option<String>,
    ) -> Self {
        Self {
            input: Arc::new(input),
            group_exprs,
            aggr_exprs,
            event_time_col,
            delay_micros,
            output_schema,
            data_schema,
            checkpoint_location,
        }
    }

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
    }
}

impl UserDefinedLogicalNodeCore for WindowAccumNode {
    fn name(&self) -> &str {
        "WindowAccum"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut exprs = self.group_exprs.clone();
        exprs.extend(self.aggr_exprs.clone());
        exprs
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "WindowAccum: eventTime={}, delay={}µs",
            self.event_time_col, self.delay_micros
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if inputs.len() != 1 {
            return plan_err!("{} requires exactly one input", self.name());
        }
        let n_group = self.group_exprs.len();
        let n_aggr = self.aggr_exprs.len();
        if exprs.len() != n_group + n_aggr {
            return plan_err!(
                "{} requires {} expressions, got {}",
                self.name(),
                n_group + n_aggr,
                exprs.len()
            );
        }
        let mut exprs = exprs;
        let aggr_exprs = exprs.split_off(n_group);
        let group_exprs = exprs;
        let Some(input) = inputs.pop() else {
            return plan_err!("{} requires exactly one input", self.name());
        };
        Ok(Self::new(
            input,
            group_exprs,
            aggr_exprs,
            self.event_time_col.clone(),
            self.delay_micros,
            self.output_schema.clone(),
            self.data_schema.clone(),
            self.checkpoint_location.clone(),
        ))
    }
}
