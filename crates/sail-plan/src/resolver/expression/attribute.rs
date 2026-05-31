use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use datafusion_common::scalar::ScalarStructBuilder;
use datafusion_common::{Column, DFSchemaRef, TableReference};
use datafusion_expr::expr::{Alias, ScalarFunction};
use datafusion_expr::{col, expr, lit};
use datafusion_functions::core::get_field;
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

fn metadata_column_schema() -> Fields {
    Fields::from(vec![
        Field::new("file_path", DataType::Utf8, false),
        Field::new("file_name", DataType::Utf8, false),
        Field::new("file_size", DataType::Int64, false),
        Field::new("file_block_start", DataType::Int64, false),
        Field::new("file_block_length", DataType::Int64, false),
        Field::new(
            "file_modification_time",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ])
}

impl PlanResolver<'_> {
    pub(super) fn resolve_expression_attribute(
        &self,
        name: spec::ObjectName,
        plan_id: Option<i64>,
        is_metadata_column: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        if is_metadata_column {
            return Ok(Self::metadata_column_expr());
        }
        if let Some((name, expr)) =
            self.resolve_aggregate_field(&name, state.get_grouping_for_having())?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        if let Some((name, expr)) =
            self.resolve_aggregate_field(&name, state.get_projections_for_having())?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        if let Some((name, expr)) =
            self.resolve_field_or_nested_field(&name, plan_id, schema, state)?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        if let Some((name, expr)) =
            self.resolve_aggregate_field(&name, state.get_projections_for_grouping())?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        if let Some((name, expr)) =
            self.resolve_aggregate_alias_field(&name, state.get_grouping_for_select())?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        if let Some((name, expr)) =
            self.resolve_lateral_alias_field(&name, state.get_lateral_aliases())?
        {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        if let Some((name, expr)) = self.resolve_hidden_field(&name, plan_id, schema, state)? {
            return Ok(NamedExpr::new(vec![name], expr));
        }
        // _metadata is a Spark hidden column with schema {file_path, file_name, ...}.
        // Return a null struct for _metadata or a field extraction for _metadata.field_name.
        if !name.parts().is_empty() && name.parts()[0].as_ref() == "_metadata" {
            let meta_expr = Self::metadata_column_expr();
            if name.parts().len() == 1 {
                return Ok(meta_expr);
            }
            // _metadata.field_name — extract the sub-field from the null struct
            let field_name = name.parts()[1].as_ref().to_string();
            let get_field_expr = expr::Expr::ScalarFunction(ScalarFunction::new_udf(
                get_field(),
                vec![meta_expr.expr, lit(field_name.clone())],
            ));
            return Ok(NamedExpr::new(vec![field_name], get_field_expr));
        }
        let Some(outer_schema) = state.get_outer_query_schema().cloned() else {
            return Err(PlanError::AnalysisError(format!(
                // Spark tests expect the error message to start with: "attribute {name:?} is missing"
                "attribute {name:?} is missing from the schema: cannot resolve attribute"
            )));
        };
        match self.resolve_outer_field(&name, &outer_schema, state)? {
            Some((name, expr)) => Ok(NamedExpr::new(vec![name], expr)),
            None => Err(PlanError::AnalysisError(format!(
                // Spark tests expect the error message to start with: "attribute {name:?} is missing"
                "attribute {name:?} is missing from the schema: cannot resolve attribute or outer attribute"
            ))),
        }
    }

    fn metadata_column_expr() -> NamedExpr {
        let fields = metadata_column_schema();
        let null_struct = ScalarStructBuilder::new_null(fields);
        let expr = expr::Expr::Literal(null_struct, None);
        NamedExpr::new(vec!["_metadata".to_string()], expr)
    }

    fn resolve_field_or_nested_field(
        &self,
        name: &spec::ObjectName,
        plan_id: Option<i64>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<(String, expr::Expr)>> {
        let candidates = Self::generate_qualified_nested_field_candidates(name.parts());
        let mut candidates = schema
            .iter()
            .flat_map(|(qualifier, field)| {
                let Ok(info) = state.get_field_info(field.name()) else {
                    return vec![];
                };
                if info.is_hidden() {
                    return vec![];
                }
                candidates
                    .iter()
                    .filter_map(|(q, name, inner)| {
                        if qualifier_matches(q.as_ref(), qualifier)
                            && info.matches(name.as_ref(), plan_id)
                        {
                            let expr = Self::resolve_potentially_nested_field(
                                col((qualifier, field)),
                                field.data_type(),
                                inner,
                            )?;
                            let name = inner.last().unwrap_or(name).as_ref().to_string();
                            Some((name, expr))
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .collect::<Vec<_>>();
        if candidates.len() > 1 {
            return Err(PlanError::AnalysisError(format!(
                "ambiguous attribute: {name:?}"
            )));
        }
        Ok(candidates.pop())
    }

    fn resolve_aggregate_field(
        &self,
        name: &spec::ObjectName,
        expressions: &[NamedExpr],
    ) -> PlanResult<Option<(String, expr::Expr)>> {
        let [name] = name.parts() else {
            return Ok(None);
        };
        let mut candidates = expressions
            .iter()
            .filter_map(|expr| {
                let NamedExpr {
                    name: agg, expr, ..
                } = expr;
                match agg.as_slice() {
                    [agg] if agg.eq_ignore_ascii_case(name.as_ref()) => {
                        Some((name.as_ref().to_string(), expr.clone()))
                    }
                    _ => None,
                }
            })
            .collect::<Vec<_>>();
        if candidates.len() > 1 {
            return Err(PlanError::AnalysisError(format!(
                "ambiguous aggregate expression: {name:?}"
            )));
        }
        Ok(candidates.pop())
    }

    /// Resolves a name against pre-resolved GROUP BY expressions by matching the expression's
    /// alias. Handles both single-part names (e.g. `window`) and multi-part names for struct
    /// field access (e.g. `window.start`). Used when SELECT is resolved with GROUP BY in scope.
    fn resolve_aggregate_alias_field(
        &self,
        name: &spec::ObjectName,
        expressions: &[NamedExpr],
    ) -> PlanResult<Option<(String, expr::Expr)>> {
        let parts = name.parts();
        if parts.is_empty() || expressions.is_empty() {
            return Ok(None);
        }
        let first = parts[0].as_ref();
        let mut candidates = expressions
            .iter()
            .filter_map(|named| {
                let alias_name = match &named.expr {
                    expr::Expr::Alias(Alias { name, .. }) => name.as_str(),
                    _ => return None,
                };
                if alias_name.eq_ignore_ascii_case(first) {
                    Some(named.expr.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if candidates.len() > 1 {
            return Err(PlanError::AnalysisError(format!(
                "ambiguous group-by expression: {name:?}"
            )));
        }
        let Some(base_expr) = candidates.pop() else {
            return Ok(None);
        };
        if parts.len() == 1 {
            return Ok(Some((first.to_string(), base_expr)));
        }
        let mut result_expr = base_expr;
        for field in &parts[1..] {
            result_expr = expr::Expr::ScalarFunction(ScalarFunction::new_udf(
                get_field(),
                vec![result_expr, lit(field.as_ref().to_string())],
            ));
        }
        let output_name = parts
            .last()
            .map(|x| x.as_ref().to_string())
            .unwrap_or_default();
        Ok(Some((output_name, result_expr)))
    }

    /// Resolves a single-part name against lateral column aliases accumulated in the current SELECT.
    /// Allows later expressions in the same SELECT to reference aliases defined earlier.
    fn resolve_lateral_alias_field(
        &self,
        name: &spec::ObjectName,
        aliases: &[NamedExpr],
    ) -> PlanResult<Option<(String, expr::Expr)>> {
        let [attr_name] = name.parts() else {
            return Ok(None);
        };
        if aliases.is_empty() {
            return Ok(None);
        }
        let mut candidates = aliases
            .iter()
            .filter_map(|named| {
                if named.name.len() == 1 && named.name[0].eq_ignore_ascii_case(attr_name.as_ref()) {
                    Some((attr_name.as_ref().to_string(), named.expr.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if candidates.len() > 1 {
            return Err(PlanError::AnalysisError(format!(
                "ambiguous lateral alias: {attr_name:?}"
            )));
        }
        Ok(candidates.pop())
    }

    fn resolve_hidden_field(
        &self,
        name: &spec::ObjectName,
        plan_id: Option<i64>,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<(String, expr::Expr)>> {
        let [name] = name.parts() else {
            return Ok(None);
        };
        let mut candidates = schema
            .iter()
            .filter_map(|(qualifier, field)| {
                if qualifier.is_some() {
                    return None;
                }
                let Ok(info) = state.get_field_info(field.name()) else {
                    return None;
                };
                if !info.is_hidden() {
                    return None;
                }
                if info.matches(name.as_ref(), plan_id) {
                    Some((
                        name.as_ref().to_string(),
                        expr::Expr::Column(Column::new_unqualified(field.name())),
                    ))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if candidates.len() > 1 {
            return Err(PlanError::AnalysisError(format!(
                "ambiguous attribute: {name:?}"
            )));
        }
        Ok(candidates.pop())
    }

    fn resolve_outer_field(
        &self,
        name: &spec::ObjectName,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Option<(String, expr::Expr)>> {
        let candidates = Self::generate_qualified_field_candidates(name.parts());
        let mut candidates = schema
            .iter()
            .flat_map(|(qualifier, field)| {
                let Ok(info) = state.get_field_info(field.name()) else {
                    return vec![];
                };
                if info.is_hidden() {
                    return vec![];
                }
                candidates
                    .iter()
                    .filter(|(q, name)| {
                        qualifier_matches(q.as_ref(), qualifier)
                            && info.matches(name.as_ref(), None)
                    })
                    .map(|(_, name)| {
                        (
                            name.as_ref().to_string(),
                            expr::Expr::OuterReferenceColumn(
                                field.clone(),
                                Column::new(qualifier.cloned(), field.name()),
                            ),
                        )
                    })
                    .collect()
            })
            .collect::<Vec<_>>();
        if candidates.len() > 1 {
            return Err(PlanError::AnalysisError(format!(
                "ambiguous outer attribute: {name:?}"
            )));
        }
        Ok(candidates.pop())
    }

    fn resolve_potentially_nested_field<T: AsRef<str>>(
        expr: expr::Expr,
        data_type: &DataType,
        inner: &[T],
    ) -> Option<expr::Expr> {
        match inner {
            [] => Some(expr),
            [name, remaining @ ..] => match data_type {
                DataType::Struct(fields) => fields
                    .iter()
                    .find(|x| x.name().eq_ignore_ascii_case(name.as_ref()))
                    .and_then(|field| {
                        let args = vec![expr, lit(field.name().to_string())];
                        let expr =
                            expr::Expr::ScalarFunction(ScalarFunction::new_udf(get_field(), args));
                        Self::resolve_potentially_nested_field(expr, field.data_type(), remaining)
                    }),
                _ => None,
            },
        }
    }

    fn generate_qualified_field_candidates<T: AsRef<str>>(
        name: &[T],
    ) -> Vec<(Option<TableReference>, &T)> {
        match name {
            [n1] => vec![(None, n1)],
            [n1, n2] => vec![(Some(TableReference::bare(n1.as_ref())), n2)],
            [n1, n2, n3] => vec![(Some(TableReference::partial(n1.as_ref(), n2.as_ref())), n3)],
            [n1, n2, n3, n4] => vec![(
                Some(TableReference::full(n1.as_ref(), n2.as_ref(), n3.as_ref())),
                n4,
            )],
            _ => vec![],
        }
    }

    fn generate_qualified_nested_field_candidates<T: AsRef<str>>(
        name: &[T],
    ) -> Vec<(Option<TableReference>, &T, &[T])> {
        let mut out = vec![];
        if let [n1, x @ ..] = name {
            out.push((None, n1, x));
        }
        if let [n1, n2, x @ ..] = name {
            out.push((Some(TableReference::bare(n1.as_ref())), n2, x));
        }
        if let [n1, n2, n3, x @ ..] = name {
            out.push((
                Some(TableReference::partial(n1.as_ref(), n2.as_ref())),
                n3,
                x,
            ));
        }
        if let [n1, n2, n3, n4, x @ ..] = name {
            out.push((
                Some(TableReference::full(n1.as_ref(), n2.as_ref(), n3.as_ref())),
                n4,
                x,
            ));
        }
        out
    }
}

/// Returns whether the qualifier matches the target qualifier.
/// Identifiers are case-insensitive.
/// Note that the match is not symmetric, so please ensure the arguments are in the correct order.
pub(super) fn qualifier_matches(
    qualifier: Option<&TableReference>,
    target: Option<&TableReference>,
) -> bool {
    let table_matches = |table: &str| {
        target
            .map(|x| x.table())
            .is_some_and(|x| x.eq_ignore_ascii_case(table))
    };
    let schema_matches = |schema: &str| {
        target
            .and_then(|x| x.schema())
            .is_some_and(|x| x.eq_ignore_ascii_case(schema))
    };
    let catalog_matches = |catalog: &str| {
        target
            .and_then(|x| x.catalog())
            .is_some_and(|x| x.eq_ignore_ascii_case(catalog))
    };
    match qualifier {
        Some(TableReference::Bare { table }) => table_matches(table),
        Some(TableReference::Partial { schema, table }) => {
            schema_matches(schema) && table_matches(table)
        }
        Some(TableReference::Full {
            catalog,
            schema,
            table,
        }) => catalog_matches(catalog) && schema_matches(schema) && table_matches(table),
        None => true,
    }
}
