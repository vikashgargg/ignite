#[expect(clippy::disallowed_types)]
use datafusion_expr::{EmptyRelation, LogicalPlan, SetVariable, Statement};

use crate::error::PlanResult;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_set_variable(
        &self,
        variable: String,
        value: String,
    ) -> PlanResult<LogicalPlan> {
        let variable = if variable.eq_ignore_ascii_case("timezone")
            || variable.eq_ignore_ascii_case("time.zone")
        {
            "datafusion.execution.time_zone".to_string()
        } else {
            variable
        };
        // For spark.* keys, DataFusion's config system would error on unknown namespaces.
        // We silently accept and ignore these (Spark stores them for user-level access).
        if variable.starts_with("spark.") {
            return Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: std::sync::Arc::new(datafusion_common::DFSchema::empty()),
            }));
        }
        let statement = Statement::SetVariable(SetVariable { variable, value });

        Ok(LogicalPlan::Statement(statement))
    }
}
