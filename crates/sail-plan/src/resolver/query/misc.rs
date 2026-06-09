use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::MemTable;
use datafusion_common::{DFSchema, DFSchemaRef, ParamValues};
use datafusion_expr::{EmptyRelation, Extension, LogicalPlan, UNNAMED_TABLE};
use log::warn;
use sail_common::spec;
use sail_common_datafusion::array::record_batch::{
    cast_record_batch_positionally, read_record_batches,
};
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_logical_plan::range::RangeNode;
use sail_logical_plan::streaming::watermark::WatermarkNode;

use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Resolves a query plan that produces an empty relation.
    /// When `produce_one_row` is true, it can be used for literal projection with no input.
    pub(super) fn resolve_query_empty(&self, produce_one_row: bool) -> PlanResult<LogicalPlan> {
        Ok(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row,
            schema: DFSchemaRef::new(DFSchema::empty()),
        }))
    }

    pub(super) async fn resolve_query_range(
        &self,
        range: spec::Range,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let spec::Range {
            start,
            end,
            step,
            num_partitions,
        } = range;
        let start = start.unwrap_or(0);
        // TODO: use parallelism in Spark configuration as the default
        let num_partitions = num_partitions.unwrap_or(1);
        if num_partitions < 1 {
            return Err(PlanError::invalid(format!(
                "invalid number of partitions: {num_partitions}"
            )));
        }
        let alias = state.register_field_name("id");
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(RangeNode::try_new(alias, start, end, step, num_partitions)?),
        }))
    }

    pub(super) async fn resolve_query_with_parameters(
        &self,
        input: spec::QueryPlan,
        positional: Vec<spec::Expr>,
        named: Vec<(String, spec::Expr)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let evaluator = LiteralEvaluator::new();
        let schema = Arc::new(DFSchema::empty());
        // Evaluate named arguments eagerly so that IDENTIFIER(:col) expressions
        // inside the query body can substitute their placeholder values at plan-resolution
        // time (before `with_param_values` is applied to the resolved plan).
        let named_params = {
            let mut params = HashMap::new();
            for (name, arg) in named {
                let expr = self.resolve_expression(arg, &schema, state).await?;
                let param = evaluator
                    .evaluate(&expr)
                    .map_err(|e| PlanError::invalid(e.to_string()))?;
                params.insert(name, param);
            }
            params
        };
        // Evaluate positional arguments eagerly for the same reason.
        let positional_params = {
            let mut params = vec![];
            for arg in positional {
                let expr = self.resolve_expression(arg, &schema, state).await?;
                let param = evaluator
                    .evaluate(&expr)
                    .map_err(|e| PlanError::invalid(e.to_string()))?;
                params.push(param);
            }
            params
        };
        // Enter a scope that makes both named and positional parameter values
        // available for IDENTIFIER clause evaluation inside the query body.
        let mut scope =
            state.enter_param_values_scope(named_params.clone(), positional_params.clone());
        let state = scope.state();
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let input = if !positional_params.is_empty() {
            input.with_param_values(ParamValues::from(positional_params))?
        } else {
            input
        };
        if !named_params.is_empty() {
            Ok(input.with_param_values(ParamValues::from(named_params))?)
        } else {
            Ok(input)
        }
    }

    pub(super) async fn resolve_query_local_relation(
        &self,
        data: Option<Vec<u8>>,
        schema: Option<spec::Schema>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let batches = if let Some(data) = data {
            read_record_batches(&data)?
        } else {
            vec![]
        };
        let (schema, batches) = if let Some(schema) = schema {
            let schema = Arc::new(self.resolve_schema(schema, state)?);
            let batches = batches
                .into_iter()
                .map(|b| Ok(cast_record_batch_positionally(b, schema.clone())?))
                .collect::<PlanResult<_>>()?;
            (schema, batches)
        } else if let [batch, ..] = batches.as_slice() {
            (batch.schema(), batches)
        } else {
            return Err(PlanError::invalid("missing schema for local relation"));
        };
        let table_provider = Arc::new(MemTable::try_new(schema, vec![batches])?);
        self.resolve_table_provider_with_rename(
            table_provider,
            UNNAMED_TABLE,
            None,
            vec![],
            None,
            state,
        )
    }

    pub(super) async fn resolve_query_hint(
        &self,
        input: spec::QueryPlan,
        _name: String,
        _parameters: Vec<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        // TODO: Implement
        warn!("Hint operation is not yet supported and is a no-op");
        self.resolve_query_plan_with_hidden_fields(input, state)
            .await
    }

    pub(super) async fn resolve_query_collect_metrics(
        &self,
        input: spec::QueryPlan,
        _name: String,
        _metrics: Vec<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        // Metrics collection is informational — pass through the child plan unchanged.
        warn!("collect_metrics is not yet implemented and will be treated as a no-op");
        self.resolve_query_plan_with_hidden_fields(input, state)
            .await
    }

    pub(super) async fn resolve_query_parse(
        &self,
        parse: spec::Parse,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        warn!("df.parse() is not yet implemented; passing through input unchanged");
        self.resolve_query_plan(*parse.input, state).await
    }

    pub(super) async fn resolve_query_with_watermark(
        &self,
        watermark: spec::WithWatermark,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(*watermark.input, state).await?;
        let delay_micros = parse_spark_duration_to_micros(&watermark.delay_threshold).unwrap_or(0);
        // Resolve the event-time column to its internal (resolved) field name, so the
        // physical WatermarkExec can find it in the optimized schema (the resolver
        // renames columns to internal ids like `#0`).
        let event_time_col = self
            .resolve_columns(
                input.schema(),
                std::slice::from_ref(&watermark.event_time),
                state,
            )?
            .into_iter()
            .next()
            .map(|c| c.name)
            .ok_or_else(|| {
                PlanError::invalid(format!(
                    "withWatermark: event-time column `{}` not found",
                    watermark.event_time
                ))
            })?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(WatermarkNode::new(input, event_time_col, delay_micros)),
        }))
    }
}

/// Parse a Spark duration string like "10 minutes", "30 seconds" into microseconds.
pub(crate) fn parse_spark_duration_to_micros(s: &str) -> Option<i64> {
    let s = s.trim();
    let (num_str, rest) = s.split_once(char::is_whitespace)?;
    let n: i64 = num_str.parse().ok()?;
    let unit = rest.trim().trim_end_matches('s');
    let micros = match unit {
        "microsecond" => n,
        "millisecond" => n * 1_000,
        "second" => n * 1_000_000,
        "minute" => n * 60_000_000,
        "hour" => n * 3_600_000_000,
        "day" => n * 86_400_000_000,
        "week" => n * 7 * 86_400_000_000,
        _ => return None,
    };
    Some(micros)
}
