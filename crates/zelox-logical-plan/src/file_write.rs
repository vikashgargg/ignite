use std::fmt::Formatter;
use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef};
use datafusion_expr::expr::Sort;
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use educe::Educe;
use zelox_common_datafusion::catalog::CatalogPartitionField;
use zelox_common_datafusion::datasource::{BucketBy, OptionLayer, SinkMode};
use zelox_common_datafusion::utils::items::ItemTaker;

#[derive(Clone, Debug, Eq, PartialEq, Hash, PartialOrd)]
pub struct FileWriteOptions {
    pub format: String,
    pub mode: SinkMode,
    pub partition_by: Vec<CatalogPartitionField>,
    pub sort_by: Vec<Sort>,
    pub bucket_by: Option<BucketBy>,
    pub options: Vec<OptionLayer>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Educe)]
#[educe(PartialOrd)]
pub struct FileWriteNode {
    input: Arc<LogicalPlan>,
    options: FileWriteOptions,
    #[educe(PartialOrd(ignore))]
    schema: DFSchemaRef,
    /// The declared (catalog) schema of the write target, when writing to a
    /// known table. Carries the table's column nullability, which the inserted
    /// data plan does not preserve (DataFusion marks VALUES/literal projections
    /// as non-nullable). Table formats use this to record the correct column
    /// nullability in the table metadata. `None` for ad-hoc data-source writes.
    #[educe(PartialOrd(ignore))]
    declared_schema: Option<DFSchemaRef>,
}

impl FileWriteNode {
    pub fn new(input: Arc<LogicalPlan>, options: FileWriteOptions) -> Self {
        Self {
            input,
            options,
            schema: Arc::new(DFSchema::empty()),
            declared_schema: None,
        }
    }

    pub fn with_declared_schema(mut self, declared_schema: Option<DFSchemaRef>) -> Self {
        self.declared_schema = declared_schema;
        self
    }

    pub fn options(&self) -> &FileWriteOptions {
        &self.options
    }

    pub fn declared_schema(&self) -> Option<&DFSchemaRef> {
        self.declared_schema.as_ref()
    }
}

impl UserDefinedLogicalNodeCore for FileWriteNode {
    fn name(&self) -> &str {
        "FileWrite"
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
        write!(f, "FileWrite: options={:?}", self.options)?;
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        exprs.zero()?;
        Ok(Self {
            input: Arc::new(inputs.one()?),
            options: self.options.clone(),
            schema: self.schema.clone(),
            declared_schema: self.declared_schema.clone(),
        })
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![(0..self.input.schema().fields().len()).collect()])
    }
}
