use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::utils::items::ItemTaker;

/// Logical sink node for `df.writeStream.format("memory").queryName(name)`.
///
/// The node wraps the streaming input and holds the name under which the
/// accumulated results are registered as a queryable DataFusion table.
/// Physical planning converts it into `MemorySinkExec` which appends each
/// micro-batch to a shared in-memory buffer.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct MemorySinkNode {
    input: Arc<LogicalPlan>,
    /// Table name registered in the session catalog (from `queryName(...)`).
    pub query_name: String,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl MemorySinkNode {
    pub fn new(input: Arc<LogicalPlan>, query_name: String) -> Self {
        Self {
            input,
            query_name,
            schema: Arc::new(DFSchema::empty()),
        }
    }

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
    }
}

impl UserDefinedLogicalNodeCore for MemorySinkNode {
    fn name(&self) -> &str {
        "MemorySink"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "MemorySink: query_name={}", self.query_name)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        exprs.zero()?;
        Ok(Self {
            input: Arc::new(inputs.one()?),
            query_name: self.query_name.clone(),
            schema: self.schema.clone(),
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
