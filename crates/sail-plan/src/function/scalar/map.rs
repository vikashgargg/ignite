use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::functions_nested::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{cast, expr, lit, ExprSchemable};
use datafusion_spark::function::map::map_from_arrays::MapFromArrays;
use datafusion_spark::function::map::map_from_entries::MapFromEntries;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::map::str_to_map::StrToMap;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn map(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    use crate::function::common::ScalarFunctionBuilder as F;

    if !input.arguments.len().is_multiple_of(2) {
        return Err(PlanError::InvalidArgument(format!(
            "map(k1, v1, k2, v2, ...): expect number of args to be multiple of 2, got {}",
            input.arguments.len()
        )));
    }

    let schema = input.function_context.schema;
    let (keys, values): (Vec<_>, Vec<_>) = input
        .arguments
        .chunks(2)
        .map(|key_value| (key_value[0].clone(), key_value[1].clone()))
        .unzip();
    let value_contains_null = values.iter().try_fold(false, |nullable, value| {
        Ok::<_, PlanError>(nullable || value.nullable(schema.as_ref())?)
    })?;

    let keys = expr_fn::make_array(keys);
    let values = expr_fn::make_array(values);
    // Carry the true value nullability from construction (via the values *list* — a
    // List<primitive> cast, which DataFusion 54 still supports) rather than forcing the value
    // nullable and then tightening the built Map back with a `cast` — DataFusion 54 no longer
    // supports nullability-only casts on `Map`/`List<Struct>` in `simplify_expressions`.
    let values = cast_list_value_nullability(values, schema, value_contains_null)?;
    F::udf(MapFromArrays::new())(ScalarFunctionInput {
        arguments: vec![keys, values],
        function_context: input.function_context,
    })
}

fn map_from_arrays(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    use crate::function::common::ScalarFunctionBuilder as F;

    let schema = input.function_context.schema;
    let (keys, values) = input.arguments.two()?;
    let value_contains_null = match values.get_type(schema.as_ref())? {
        DataType::List(field) | DataType::LargeList(field) => field.is_nullable(),
        _ => true,
    };
    let values = cast_list_value_nullability(values, schema, value_contains_null)?;
    F::udf(MapFromArrays::new())(ScalarFunctionInput {
        arguments: vec![keys, values],
        function_context: input.function_context,
    })
}

fn map_from_entries(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    use crate::function::common::ScalarFunctionBuilder as F;

    // `map_from_entries` preserves the entries' value nullability into the resulting Map, so we
    // pass the entries through unchanged rather than force-nullable + tighten via a cast that
    // DataFusion 54 no longer supports on List<Struct>/Map.
    let entries = input.arguments.one()?;
    F::udf(MapFromEntries::new())(ScalarFunctionInput {
        arguments: vec![entries],
        function_context: input.function_context,
    })
}

fn map_entries(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    // The input map already carries its true value nullability; `map_entries` preserves it into
    // the resulting List<Struct<key,value>>. We no longer force-nullable + tighten via a cast
    // (DataFusion 54 forbids nullability-only casts on List<Struct>).
    let map = input.arguments.one()?;
    Ok(expr_fn::map_entries(map))
}

fn map_values(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let map = input.arguments.one()?;
    Ok(expr_fn::map_values(map))
}

fn cast_list_value_nullability(
    expr: expr::Expr,
    schema: &datafusion_common::DFSchemaRef,
    nullable: bool,
) -> PlanResult<expr::Expr> {
    let data_type = expr.get_type(schema.as_ref())?;
    let target_type = match data_type {
        DataType::List(field) if field.is_nullable() != nullable => {
            DataType::List(with_nullable(&field, nullable))
        }
        DataType::LargeList(field) if field.is_nullable() != nullable => {
            DataType::LargeList(with_nullable(&field, nullable))
        }
        _ => return Ok(expr),
    };
    Ok(cast(expr, target_type))
}

fn with_nullable(field: &FieldRef, nullable: bool) -> FieldRef {
    Arc::new(field.as_ref().clone().with_nullable(nullable))
}

fn map_concat(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    use datafusion_expr::Expr;

    use crate::function::common::ScalarFunctionBuilder as F;

    // If any input is NULL, return NULL
    // This is done by creating a CASE expression that checks each input for NULL
    if input.arguments.is_empty() {
        return Ok(lit(ScalarValue::Null));
    }

    // Build the result expression
    let (keys, values) = input
        .arguments
        .iter()
        .map(|map| {
            (
                expr_fn::map_keys(map.clone()),
                expr_fn::map_values(map.clone()),
            )
        })
        .unzip();

    let keys = expr_fn::array_concat(keys);
    let values = expr_fn::array_concat(values);
    let result = F::udf(MapFromArrays::new())(ScalarFunctionInput {
        arguments: vec![keys, values],
        function_context: input.function_context,
    })?;

    // Wrap the result with CASE to handle NULLs:
    // CASE WHEN arg1 IS NULL OR arg2 IS NULL OR ... THEN NULL ELSE result END
    // We already checked that arguments is not empty, so reduce will always return Some
    if let Some(null_check) = input
        .arguments
        .iter()
        .map(|arg| arg.clone().is_null())
        .reduce(|a, b| a.or(b))
    {
        Ok(Expr::Case(expr::Case {
            expr: None,
            when_then_expr: vec![(Box::new(null_check), Box::new(lit(ScalarValue::Null)))],
            else_expr: Some(Box::new(result)),
        }))
    } else {
        // This should never happen because we checked arguments is not empty
        Ok(result)
    }
}

fn map_contains_key(map: expr::Expr, key: expr::Expr) -> expr::Expr {
    expr_fn::array_has(expr_fn::map_keys(map), key)
}

fn str_to_map(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    use crate::function::common::ScalarFunctionBuilder as F;

    let (strs, delims) = input.arguments.at_least_one()?;

    let pair_delims = delims.first().cloned().unwrap_or(lit(","));
    let key_value_delims = delims.get(1).cloned().unwrap_or(lit(":"));

    F::udf(StrToMap::new())(ScalarFunctionInput {
        arguments: vec![strs, pair_delims, key_value_delims],
        function_context: input.function_context,
    })
}

pub(super) fn list_built_in_map_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("map", F::custom(map)),
        ("map_concat", F::custom(map_concat)),
        ("map_contains_key", F::binary(map_contains_key)),
        ("map_entries", F::custom(map_entries)),
        ("map_from_arrays", F::custom(map_from_arrays)),
        ("map_from_entries", F::custom(map_from_entries)),
        ("map_keys", F::unary(expr_fn::map_keys)),
        ("map_values", F::custom(map_values)),
        ("str_to_map", F::custom(str_to_map)),
    ]
}
