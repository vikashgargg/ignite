use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion_common::{plan_err, DFSchemaRef, Result};
use datafusion_expr::{Expr, UserDefinedLogicalNodeCore};
use educe::Educe;

/// A logical plan node that carries event-time watermark metadata.
/// It is a transparent passthrough for the input plan's schema.
/// The streaming rewriter uses this to enable stateful window aggregation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct WatermarkNode {
    input: Arc<LogicalPlan>,
    /// The name of the event-time column in the input schema.
    pub event_time_col: String,
    /// The watermark delay in microseconds (max_event_time - delay = watermark).
    pub delay_micros: i64,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl WatermarkNode {
    pub fn new(input: LogicalPlan, event_time_col: String, delay_micros: i64) -> Self {
        let schema = input.schema().clone();
        Self {
            input: Arc::new(input),
            event_time_col,
            delay_micros,
            schema,
        }
    }

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
    }
}

impl UserDefinedLogicalNodeCore for WatermarkNode {
    fn name(&self) -> &str {
        "Watermark"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Watermark: eventTime={}, delay={}µs",
            self.event_time_col, self.delay_micros
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if !exprs.is_empty() {
            return plan_err!("{} does not take expressions", self.name());
        }
        if inputs.len() != 1 {
            return plan_err!("{} requires exactly one input", self.name());
        }
        let Some(input) = inputs.pop() else {
            return plan_err!("{} requires exactly one input", self.name());
        };
        Ok(Self::new(
            input,
            self.event_time_col.clone(),
            self.delay_micros,
        ))
    }
}
