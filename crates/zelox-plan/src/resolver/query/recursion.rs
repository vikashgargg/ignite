use std::sync::Arc;

use datafusion::datasource::cte_worktable::CteWorkTable;
use datafusion::datasource::provider_as_source;
use datafusion_common::TableReference;
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Projection};
use zelox_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_recursive_query_plan(
        &self,
        cte_name: &str,
        plan: spec::QueryPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        // Peel off a wrapping TableAlias (e.g. `WITH RECURSIVE t(a, b) AS (...)`)
        // so we can match directly on the SetOperation inside.
        let (inner_plan, column_aliases) = match plan {
            spec::QueryPlan {
                node:
                    spec::QueryNode::TableAlias {
                        input,
                        name: _,
                        columns,
                    },
                ..
            } => (*input, columns),
            other => (other, vec![]),
        };

        match inner_plan {
            spec::QueryPlan {
                node:
                    spec::QueryNode::SetOperation(spec::SetOperation {
                        left,
                        right,
                        set_op_type: spec::SetOpType::Union,
                        is_all,
                        ..
                    }),
                ..
            } => {
                // Step 1: resolve the static (base) term
                let static_plan = self.resolve_query_plan(*left, state).await?;

                // Step 1b: apply column aliases to the static plan now, so the work
                // table's schema matches what the recursive term will reference.
                let static_plan = if !column_aliases.is_empty() {
                    let schema = static_plan.schema();
                    if column_aliases.len() != schema.fields().len() {
                        return Err(PlanError::invalid(format!(
                            "recursive CTE column aliases ({}) do not match static term columns ({})",
                            column_aliases.len(),
                            schema.fields().len()
                        )));
                    }
                    let expr: Vec<Expr> = schema
                        .columns()
                        .into_iter()
                        .zip(column_aliases)
                        .map(|(col, alias)| {
                            Expr::Column(col).alias(state.register_field_name(alias))
                        })
                        .collect();
                    LogicalPlan::Projection(Projection::try_new(expr, Arc::new(static_plan))?)
                } else {
                    static_plan
                };

                // Step 2: create a CteWorkTable placeholder with the (aliased) static schema
                let schema = Arc::clone(static_plan.schema().inner());
                let work_table = Arc::new(CteWorkTable::new(cte_name, schema));
                let work_table_scan = LogicalPlanBuilder::scan(
                    cte_name.to_string(),
                    provider_as_source(work_table),
                    None,
                )?
                .build()?;

                // Step 3: register placeholder so the recursive term can resolve self-references
                let ref_key = TableReference::bare(cte_name);
                state.insert_cte(ref_key, work_table_scan);

                // Step 4: resolve the recursive term (uses the placeholder above)
                let recursive_plan = self.resolve_query_plan(*right, state).await?;

                // Step 5: build the RecursiveQuery logical plan
                let is_distinct = !is_all;
                Ok(LogicalPlanBuilder::from(static_plan)
                    .to_recursive_query(cte_name.to_string(), recursive_plan, is_distinct)?
                    .build()?)
            }
            other => self.resolve_query_plan(other, state).await,
        }
    }
}
