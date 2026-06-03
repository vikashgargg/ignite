use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion_common::{plan_err, DFSchemaRef, Result};
use datafusion_expr::{Expr, UserDefinedLogicalNodeCore};
use educe::Educe;

/// Stateful streaming deduplication node.
///
/// Tracks all key tuples seen across micro-batches; emits only rows whose
/// key tuple has not been seen before. Equivalent to Spark's
/// `df.dropDuplicates(cols)` in streaming mode.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct StreamDeduplicateNode {
    input: Arc<LogicalPlan>,
    /// Column names used as the deduplication key.
    pub key_cols: Vec<String>,
    /// Output schema — same as input (flow-event schema, preserved unchanged).
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl StreamDeduplicateNode {
    pub fn new(input: Arc<LogicalPlan>, key_cols: Vec<String>) -> Self {
        let schema = input.schema().clone();
        Self {
            input,
            key_cols,
            schema,
        }
    }

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
    }
}

impl UserDefinedLogicalNodeCore for StreamDeduplicateNode {
    fn name(&self) -> &str {
        "StreamDeduplicate"
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
        write!(f, "StreamDeduplicate: keys=[{}]", self.key_cols.join(", "))
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
        Ok(Self::new(Arc::new(input), self.key_cols.clone()))
    }
}
