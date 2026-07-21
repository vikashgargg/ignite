use datafusion_expr::{col, when, Expr, LogicalPlan, LogicalPlanBuilder};
use zelox_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::command::write::{WriteColumnMatch, WriteMode, WritePlanBuilder, WriteTarget};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Resolves `UPDATE table SET col = expr [, ...] [WHERE condition]` using a
    /// Copy-on-Write overwrite: scan all rows, project each column through a
    /// `CASE WHEN condition THEN new_value ELSE original_value END` expression for
    /// updated columns, then write the result back with `WriteMode::Truncate`.
    pub(super) async fn resolve_command_update(
        &self,
        table: spec::ObjectName,
        _table_alias: Option<spec::Identifier>,
        assignments: Vec<(spec::ObjectName, spec::Expr)>,
        condition: Option<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        // Build a full scan of the target table.
        // The raw scan has opaque DataFusion field IDs (#0, #1, …) in its schema.
        // The PlanResolverState maps those IDs to the real column names — resolve
        // the WHERE condition and SET expressions BEFORE renaming, while the state
        // still knows about those opaque IDs.
        let table_plan = self
            .resolve_merge_table_plan_for_update(table.clone(), state)
            .await?;
        let schema = table_plan.schema().clone();

        // Resolve the WHERE condition (if any) against the opaque schema.
        let condition_expr: Option<Expr> = if let Some(cond) = condition {
            Some(self.resolve_expression(cond, &schema, state).await?)
        } else {
            None
        };

        // Resolve SET assignments into (column_name_lowercase, Expr) pairs.
        // The column names from the spec are the user-visible names (e.g. "name").
        let mut assignment_exprs: Vec<(String, Expr)> = Vec::with_capacity(assignments.len());
        for (col_name, value_expr) in assignments {
            let col_name = col_name
                .parts()
                .last()
                .map(|s| s.as_ref().to_lowercase())
                .ok_or_else(|| PlanError::invalid("UPDATE assignment has empty column name"))?;
            let value = self.resolve_expression(value_expr, &schema, state).await?;
            assignment_exprs.push((col_name, value));
        }

        // Resolve the human-readable column names from state (opaque ID → real name).
        let column_names = PlanResolver::get_field_names(&schema, state)?;

        // Build projection using opaque IDs (col("#0") etc.), then alias each
        // column to its real name so the output schema carries "id", "name", …
        // For updated columns wrap in CASE WHEN to apply the change conditionally.
        let projections: Vec<Expr> = schema
            .fields()
            .iter()
            .zip(column_names.iter())
            .map(|(field, real_name)| -> PlanResult<Expr> {
                let real_name_lower = real_name.to_lowercase();
                // col("#0") etc. — the opaque reference DataFusion knows about.
                let original = col(field.name().as_str());

                let value = match assignment_exprs.iter().find(|(c, _)| c == &real_name_lower) {
                    None => original,
                    Some((_, new_value)) => match &condition_expr {
                        None => new_value.clone(),
                        Some(cond) => when(cond.clone(), new_value.clone())
                            .otherwise(original)
                            .map_err(PlanError::from)?,
                    },
                };
                Ok(value.alias(real_name.as_str()))
            })
            .collect::<PlanResult<_>>()?;

        let projected = LogicalPlanBuilder::from(table_plan)
            .project(projections)?
            .build()?;

        // Write back with Truncate (full overwrite), matching columns by name.
        let builder = WritePlanBuilder::new()
            .with_mode(WriteMode::Truncate)
            .with_target(WriteTarget::Table {
                table,
                column_match: WriteColumnMatch::ByName,
            });
        self.resolve_write_with_builder(projected, builder, state)
            .await
    }

    /// Creates a scan plan for the named table — shared with the merge resolver pattern.
    async fn resolve_merge_table_plan_for_update(
        &self,
        name: spec::ObjectName,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let read = spec::ReadNamedTable {
            name,
            temporal: None,
            sample: None,
            options: vec![],
        };
        let plan = spec::QueryPlan::new(spec::QueryNode::Read {
            read_type: spec::ReadType::NamedTable(Box::new(read)),
            is_streaming: false,
        });
        self.resolve_query_plan(plan, state).await
    }
}
