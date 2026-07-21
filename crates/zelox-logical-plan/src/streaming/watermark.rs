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
    /// Per-partition watermark (Flink): source-partition column name + total partition count. When
    /// set, the watermark = MIN over partitions (withheld until all N seen) — fixes premature window
    /// close when one realtime source instance reads N out-of-order partitions. None = global max.
    pub partition_col: Option<String>,
    pub num_partitions: usize,
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
            partition_col: None,
            num_partitions: 1,
            schema,
        }
    }

    pub fn with_partition_watermark(mut self, partition_col: String, num_partitions: usize) -> Self {
        self.partition_col = Some(partition_col);
        self.num_partitions = num_partitions.max(1);
        self
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
        let mut node = Self::new(input, self.event_time_col.clone(), self.delay_micros);
        node.partition_col = self.partition_col.clone();
        node.num_partitions = self.num_partitions;
        Ok(node)
    }
}
