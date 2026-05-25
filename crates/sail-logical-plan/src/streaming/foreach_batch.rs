use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use sail_common_datafusion::utils::items::ItemTaker;

/// Logical sink node for `df.writeStream.foreachBatch(func)`.
///
/// The node wraps the streaming input and holds the serialized Python function.
/// Physical planning converts it into `ForeachBatchSinkExec` which calls
/// `func(arrow_table, epoch_id)` for each micro-batch.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct ForeachBatchSinkNode {
    input: Arc<LogicalPlan>,
    /// CloudPickle-serialized Python function bytes.
    pub command: Vec<u8>,
    /// PySpark UDF eval type (as i32 from the protobuf).
    pub eval_type: i32,
    /// Python version string (e.g. "3.11").
    pub python_version: String,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
}

impl ForeachBatchSinkNode {
    pub fn new(
        input: Arc<LogicalPlan>,
        command: Vec<u8>,
        eval_type: i32,
        python_version: String,
    ) -> Self {
        Self {
            input,
            command,
            eval_type,
            python_version,
            schema: Arc::new(DFSchema::empty()),
        }
    }

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
    }
}

impl UserDefinedLogicalNodeCore for ForeachBatchSinkNode {
    fn name(&self) -> &str {
        "ForeachBatchSink"
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
        write!(f, "ForeachBatchSink: eval_type={}", self.eval_type)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        exprs.zero()?;
        Ok(Self {
            input: Arc::new(inputs.one()?),
            command: self.command.clone(),
            eval_type: self.eval_type,
            python_version: self.python_version.clone(),
            schema: self.schema.clone(),
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
