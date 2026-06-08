use std::fmt::Formatter;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::RecordBatch;
use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use sail_common_datafusion::utils::items::ItemTaker;

/// Shared in-memory buffer handle written by the streaming memory sink and read
/// by the queryable view registered under `queryName(...)`. This is the same
/// type as `sail_plan::memory_buffer::BufferHandle`, kept structural here to
/// avoid a crate dependency cycle.
pub type MemorySinkBuffer = Arc<Mutex<Vec<RecordBatch>>>;

/// Logical sink node for `df.writeStream.format("memory").queryName(name)`.
///
/// The node wraps the streaming input and carries the shared buffer handle that
/// physical planning hands to `MemorySinkExec`. The same handle backs the
/// queryable temporary view registered under `query_name`, so results written by
/// the sink are visible to `SELECT ... FROM query_name` — without any dependency
/// on DataFusion's default catalog (which Vajra does not populate).
#[derive(Clone, Debug)]
pub struct MemorySinkNode {
    input: Arc<LogicalPlan>,
    /// Name the results are queryable under (from `queryName(...)`).
    pub query_name: String,
    schema: DFSchemaRef,
    buffer: MemorySinkBuffer,
}

impl PartialEq for MemorySinkNode {
    fn eq(&self, other: &Self) -> bool {
        // The buffer handle is identity state, not part of plan equality.
        self.input == other.input
            && self.query_name == other.query_name
            && self.schema == other.schema
    }
}

impl Eq for MemorySinkNode {}

impl Hash for MemorySinkNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.query_name.hash(state);
        self.schema.hash(state);
    }
}

impl PartialOrd for MemorySinkNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Order by query_name only (schema/buffer are not meaningfully ordered).
        self.query_name.partial_cmp(&other.query_name)
    }
}

impl MemorySinkNode {
    pub fn new(input: Arc<LogicalPlan>, query_name: String, buffer: MemorySinkBuffer) -> Self {
        Self {
            input,
            query_name,
            schema: Arc::new(DFSchema::empty()),
            buffer,
        }
    }

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
    }

    pub fn buffer(&self) -> &MemorySinkBuffer {
        &self.buffer
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
            buffer: Arc::clone(&self.buffer),
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
