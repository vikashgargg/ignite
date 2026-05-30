use datafusion::arrow::datatypes::DataType;
use datafusion::functions::expr_fn;
use datafusion::functions::regex::expr_fn as regex_fn;
use datafusion::functions::regex::regexpcount::RegexpCountFunc;
use datafusion::functions::regex::regexpinstr::RegexpInstrFunc;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{cast, expr, lit, try_cast, when, BinaryExpr, ExprSchemable, Operator};
use datafusion_functions_nested::expr_fn::array_element;
use datafusion_spark::function::string::elt::SparkElt;
use datafusion_spark::function::string::expr_fn as string_fn;
use datafusion_spark::function::string::format_string::FormatStringFunc;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::string::format_number::FormatNumber;
use sail_function::scalar::string::levenshtein::Levenshtein;
use sail_function::scalar::string::make_valid_utf8::MakeValidUtf8;
use sail_function::scalar::string::randstr::Randstr;
use sail_function::scalar::string::soundex::Soundex;
use sail_function::scalar::string::spark_base64::{SparkBase64, SparkUnbase64};
use sail_function::scalar::string::spark_concat_ws::SparkConcatWs;
use sail_function::scalar::string::spark_encode_decode::{SparkDecode, SparkEncode};
use sail_function::scalar::string::spark_mask::SparkMask;
use sail_function::scalar::string::spark_quote::SparkQuote;
use sail_function::scalar::string::spark_regexp_extract_all::SparkRegexpExtractAll;
use sail_function::scalar::string::spark_sentences::SparkSentences;
use sail_function::scalar::string::spark_split::SparkSplit;
use sail_function::scalar::string::spark_binary_pad::{SparkBinaryLpad, SparkBinaryRpad};
use sail_function::scalar::string::spark_to_binary::{SparkToBinary, SparkTryToBinary};
use sail_function::scalar::string::spark_to_char::SparkToCharNumber;
use sail_function::scalar::string::spark_to_number::SparkToNumber;
use datafusion_expr::ScalarUDF;
use datafusion_spark::function::math::hex::SparkHex;
use sail_function::scalar::datetime::spark_to_chrono_fmt::SparkToChronoFmt;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn regexp_replace(string: expr::Expr, pattern: expr::Expr, replacement: expr::Expr) -> expr::Expr {
    regex_fn::regexp_replace(string, pattern, replacement, Some(lit("g")))
}

fn regexp_extract(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput { mut arguments, .. } = input;
    // regexp_extract(str, pattern, idx) - idx defaults to 1
    let idx = if arguments.len() == 3 {
        arguments
            .pop()
            .ok_or_else(|| PlanError::invalid("regexp_extract requires 2 or 3 arguments"))?
    } else {
        lit(1i64)
    };
    let (string, pattern) = arguments
        .two()
        .map_err(|_| PlanError::invalid("regexp_extract requires 2 or 3 arguments"))?;
    // Wrap pattern with an outer capture group so idx=0 (entire match) works.
    // After wrapping, regexp_match returns [entire_match, group1, group2, ...].
    let wrapped_pattern = expr_fn::concat_ws(lit(""), vec![lit("("), pattern, lit(")")]);
    let matches = regex_fn::regexp_match(string, wrapped_pattern, None);
    // array_element is 1-indexed; +1 accounts for the outer group we added.
    let element = array_element(matches, idx + lit(1i64));
    // Spark returns "" instead of NULL when no match.
    Ok(expr_fn::coalesce(vec![element, lit("")]))
}

fn regexp_substr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let (string, pattern) = input
        .arguments
        .two()
        .map_err(|_| PlanError::invalid("regexp_substr requires 2 arguments"))?;
    let wrapped_pattern = expr_fn::concat_ws(lit(""), vec![lit("("), pattern, lit(")")]);
    let matches = regex_fn::regexp_match(string, wrapped_pattern, None);
    Ok(array_element(matches, lit(1i64)))
}

fn substr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        mut arguments,
        function_context,
    } = input;
    let length_opt = (arguments.len() == 3).then(|| arguments.pop()).flatten();
    let (string, position) = arguments
        .two()
        .map_err(|_| PlanError::invalid("substr requires 2 or 3 arguments"))?;
    let string = cast_to_logical_string_or_try(string, function_context.schema, false)?;
    // Spark uses 1-based indexing, but treats pos=0 the same as pos=1 (start of string).
    // For negative positions, Spark counts from the end of the string.
    // DataFusion follows the SQL standard where pos=0 reduces the effective length by 1,
    // and pos<0 reduces even more. We convert Spark's semantics to DataFusion's:
    // - pos > 0: use as-is (1-based from start)
    // - pos = 0: use 1 (same behavior as pos=1 in Spark)
    // - pos < 0: use greatest(char_length(str) + pos + 1, 1) (absolute position from end)
    // For literal positive positions (the common case), we skip the CASE WHEN to keep plans clean.
    let position = match &position {
        expr::Expr::Literal(ScalarValue::Int64(Some(n)), _) if *n > 0 => position,
        expr::Expr::Literal(ScalarValue::Int32(Some(n)), _) if *n > 0 => position,
        expr::Expr::Literal(ScalarValue::Int64(Some(0)), _)
        | expr::Expr::Literal(ScalarValue::Int32(Some(0)), _) => lit(1i64),
        _ => when(position.clone().gt(lit(0i64)), position.clone())
            .when(position.clone().eq(lit(0i64)), lit(1i64))
            .otherwise(expr_fn::greatest(vec![
                cast(expr_fn::char_length(string.clone()), DataType::Int64)
                    + position.clone()
                    + lit(1i64),
                lit(1i64),
            ]))?,
    };
    let substr_res = match length_opt {
        Some(length) => expr_fn::substring(string, position, length),
        None => expr_fn::substr(string, position),
    };
    // TODO: Spark client throws "UNEXPECTED EXCEPTION: ArrowInvalid('Unrecognized type: 24')"
    //  when the return type is Utf8View.
    Ok(cast(substr_res, DataType::Utf8))
}

fn overlay(mut args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
    if args.len() == 4
        && matches!(
            args[3],
            expr::Expr::Literal(ScalarValue::Int64(Some(-1)), _)
                | expr::Expr::Literal(ScalarValue::Int32(Some(-1)), _)
        )
    {
        args.pop();
    }
    Ok(expr_fn::overlay(args))
}

fn position(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        mut arguments,
        function_context,
    } = input;
    let start_opt = (arguments.len() == 3).then(|| arguments.pop()).flatten();
    let (substr, str) = arguments
        .into_iter()
        .map(|expr| cast_to_logical_string_or_try(expr, function_context.schema, false))
        .collect::<PlanResult<Vec<_>>>()?
        .two()
        .map_err(|_| PlanError::invalid("position requires 2 or 3 arguments"))?;
    Ok(match start_opt {
        Some(start) => {
            let str_from_pos = expr_fn::substr(str, start.clone());
            let pos = expr_fn::strpos(str_from_pos, substr);
            when(start.clone().lt_eq(lit(0)), lit(0))
                .when(pos.clone().eq(lit(0)), lit(0))
                .when(pos.clone().gt(lit(0)), start + pos - lit(1))
                .end()?
        }
        None => expr_fn::strpos(str, substr),
    })
}

fn space(n: expr::Expr) -> expr::Expr {
    expr_fn::repeat(lit(" "), n)
}

fn replace(mut args: Vec<expr::Expr>) -> PlanResult<expr::Expr> {
    let replacement = (args.len() == 3)
        .then(|| args.pop())
        .flatten()
        .unwrap_or_else(|| lit(""));
    let (str, substr) = args
        .two()
        .map_err(|_| PlanError::invalid("replace requires 2 or 3 arguments"))?;
    Ok(expr_fn::replace(str, substr, replacement))
}

fn lower(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    Ok(expr_fn::lower(validate_utf8(input)?))
}

fn upper(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    Ok(expr_fn::upper(validate_utf8(input)?))
}

fn startswith(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    in_str_str_out_bool(expr_fn::starts_with)(input)
}

fn endswith(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    in_str_str_out_bool(expr_fn::ends_with)(input)
}

fn contains(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    in_str_str_out_bool(expr_fn::contains)(input)
}

fn bit_length(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    in_str_out_i32(expr_fn::bit_length)(input)
}

fn octet_length(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    in_str_out_i32(expr_fn::octet_length)(input)
}

fn ascii(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    in_str_out_i32(expr_fn::ascii)(input)
}

fn cast_to_logical_string_or_try(
    arg: expr::Expr,
    schema: &DFSchema,
    is_try: bool,
) -> PlanResult<expr::Expr> {
    let data_type = match arg.get_type(schema)? {
        DataType::LargeBinary | DataType::LargeUtf8 => DataType::LargeUtf8,
        DataType::Utf8View => DataType::Utf8View,
        _ => DataType::Utf8,
    };
    Ok(if is_try {
        try_cast(arg, data_type)
    } else {
        cast(arg, data_type)
    })
}

fn validate_utf8_or_try(input: ScalarFunctionInput, is_try: bool) -> PlanResult<expr::Expr> {
    cast_to_logical_string_or_try(
        input.arguments.one()?,
        input.function_context.schema,
        is_try,
    )
}

fn validate_utf8(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    validate_utf8_or_try(input, false)
}

fn try_validate_utf8(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    validate_utf8_or_try(input, true)
}

fn is_valid_utf8(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    Ok(try_validate_utf8(input)?.is_not_null())
}

fn in_str_str_out_bool(
    func: impl Fn(expr::Expr, expr::Expr) -> expr::Expr,
) -> impl Fn(ScalarFunctionInput) -> PlanResult<expr::Expr> {
    move |input: ScalarFunctionInput| {
        let (arg1, arg2) = input
            .arguments
            .into_iter()
            .map(|expr| cast_to_logical_string_or_try(expr, input.function_context.schema, false))
            .collect::<PlanResult<Vec<_>>>()?
            .two()?;
        Ok(func(arg1, arg2))
    }
}

fn in_str_out_i32(
    func: impl Fn(expr::Expr) -> expr::Expr,
) -> impl Fn(ScalarFunctionInput) -> PlanResult<expr::Expr> {
    move |input: ScalarFunctionInput| Ok(cast(func(validate_utf8(input)?), DataType::Int32))
}

fn rev_args(
    func: impl Fn(Vec<expr::Expr>) -> expr::Expr,
) -> impl Fn(Vec<expr::Expr>) -> expr::Expr {
    move |args: Vec<expr::Expr>| func(args.into_iter().rev().collect())
}

// Oracle-style DECODE(expr, s1, r1 [, s2, r2 ...] [, default])
// Uses NULL-safe equality (IS NOT DISTINCT FROM) so NULL matches NULL.
// Falls back to charset decode (binary, charset) when called with exactly 2 args.
fn decode_dispatch(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    use crate::function::common::ScalarFunctionBuilder as F;
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    if arguments.len() == 2 {
        // charset decode(binary, charset)
        let udf = F::udf(SparkDecode::new());
        return udf(ScalarFunctionInput {
            arguments,
            function_context,
        });
    }
    if arguments.len() < 3 {
        return Err(PlanError::invalid(
            "decode requires at least 3 arguments for Oracle-style DECODE",
        ));
    }
    let mut iter = arguments.into_iter();
    let subject = iter.next().unwrap();
    let remaining: Vec<expr::Expr> = iter.collect();
    // Even total remaining → odd pairs → last is default; odd total → even pairs → NULL default
    let has_default = remaining.len() % 2 == 1;
    let pairs_end = if has_default {
        remaining.len() - 1
    } else {
        remaining.len()
    };
    let default_expr = if has_default {
        remaining[remaining.len() - 1].clone()
    } else {
        lit(ScalarValue::Null)
    };
    let pairs: Vec<_> = remaining[..pairs_end].chunks(2).collect();
    if pairs.is_empty() {
        return Ok(default_expr);
    }
    let mut builder = {
        let search = pairs[0][0].clone();
        let result = pairs[0][1].clone();
        let cond = expr::Expr::BinaryExpr(BinaryExpr {
            left: Box::new(subject.clone()),
            op: Operator::IsNotDistinctFrom,
            right: Box::new(search),
        });
        when(cond, result)
    };
    for chunk in &pairs[1..] {
        let search = chunk[0].clone();
        let result = chunk[1].clone();
        let cond = expr::Expr::BinaryExpr(BinaryExpr {
            left: Box::new(subject.clone()),
            op: Operator::IsNotDistinctFrom,
            right: Box::new(search),
        });
        builder = builder.when(cond, result);
    }
    builder
        .otherwise(default_expr)
        .map_err(|e| PlanError::internal(e.to_string()))
}

fn is_binary_type(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView | DataType::FixedSizeBinary(_)
    )
}

// lpad dispatch: binary first arg → SparkBinaryLpad, string first arg → DataFusion lpad.
fn lpad_dispatch(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    use crate::function::common::ScalarFunctionBuilder as F;
    let first_type = input
        .arguments
        .first()
        .and_then(|e| e.get_type(input.function_context.schema).ok());
    if first_type.as_ref().map(is_binary_type).unwrap_or(false) {
        F::udf(SparkBinaryLpad::new())(input)
    } else {
        F::var_arg(expr_fn::lpad)(input)
    }
}

// rpad dispatch: binary first arg → SparkBinaryRpad, string first arg → DataFusion rpad.
fn rpad_dispatch(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    use crate::function::common::ScalarFunctionBuilder as F;
    let first_type = input
        .arguments
        .first()
        .and_then(|e| e.get_type(input.function_context.schema).ok());
    if first_type.as_ref().map(is_binary_type).unwrap_or(false) {
        F::udf(SparkBinaryRpad::new())(input)
    } else {
        F::var_arg(expr_fn::rpad)(input)
    }
}

fn is_numeric_type(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

// to_char / to_varchar dispatch:
// - binary arg → base64/hex/decode based on format literal
// - numeric arg → Oracle number formatting via SparkToCharNumber
// - date/timestamp/other → DataFusion to_char with Spark-to-chrono format conversion
fn to_char_dispatch(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    use crate::function::common::ScalarFunctionBuilder as F;
    let schema = input.function_context.schema;
    let first_type = input
        .arguments
        .first()
        .and_then(|e| e.get_type(schema).ok());

    if first_type.as_ref().map(is_binary_type).unwrap_or(false) {
        if input.arguments.len() != 2 {
            return Err(PlanError::invalid("to_char requires 2 arguments for binary input"));
        }
        let (bytes, format) = input.arguments.two()?;
        let fmt_lower = match &format {
            expr::Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Some(s.to_lowercase()),
            _ => None,
        };
        return match fmt_lower.as_deref() {
            Some("base64") => Ok(ScalarUDF::from(SparkBase64::new()).call(vec![bytes])),
            Some("hex") => {
                let hex_expr = expr::Expr::ScalarFunction(expr::ScalarFunction {
                    func: std::sync::Arc::new(ScalarUDF::from(SparkHex::new())),
                    args: vec![bytes],
                });
                Ok(hex_expr)
            }
            _ => Ok(ScalarUDF::from(SparkDecode::new()).call(vec![bytes, format])),
        };
    }

    if first_type.as_ref().map(is_numeric_type).unwrap_or(false) {
        return F::udf(SparkToCharNumber::new())(input);
    }

    // Date/timestamp/string: use DataFusion's to_char with Spark format → chrono conversion
    if input.arguments.len() != 2 {
        return Err(PlanError::invalid("to_char requires 2 arguments"));
    }
    let (val_expr, format) = input.arguments.two()?;
    let chrono_fmt = ScalarUDF::from(SparkToChronoFmt::new()).call(vec![format]);
    Ok(expr_fn::to_char(val_expr, chrono_fmt))
}

// btrim dispatch: cast binary args to Utf8 before applying string btrim.
fn btrim_dispatch(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    use crate::function::common::ScalarFunctionBuilder as F;
    let schema = input.function_context.schema;
    let args: Vec<expr::Expr> = input
        .arguments
        .into_iter()
        .map(|e| {
            if e.get_type(schema).ok().as_ref().map(is_binary_type).unwrap_or(false) {
                cast(e, DataType::Utf8)
            } else {
                e
            }
        })
        .collect();
    F::var_arg(expr_fn::btrim)(ScalarFunctionInput {
        arguments: args,
        function_context: input.function_context,
    })
}

pub(super) fn list_built_in_string_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("ascii", F::custom(ascii)),
        ("base64", F::udf(SparkBase64::new())),
        ("bit_length", F::custom(bit_length)),
        ("btrim", F::custom(btrim_dispatch)),
        ("char", F::unary(expr_fn::chr)),
        ("char_length", F::unary(expr_fn::char_length)),
        ("character_length", F::unary(expr_fn::char_length)),
        ("chr", F::unary(expr_fn::chr)),
        ("collate", F::unknown("collate")),
        ("concat_ws", F::udf(SparkConcatWs::new())),
        ("contains", F::custom(contains)),
        ("decode", F::custom(decode_dispatch)),
        ("elt", F::udf(SparkElt::new())),
        ("encode", F::udf(SparkEncode::new())),
        ("endswith", F::custom(endswith)),
        ("find_in_set", F::binary(expr_fn::find_in_set)),
        ("format_number", F::udf(FormatNumber::new())),
        ("format_string", F::udf(FormatStringFunc::new())),
        ("initcap", F::unary(expr_fn::initcap)),
        ("instr", F::binary(expr_fn::instr)),
        ("is_valid_utf8", F::custom(is_valid_utf8)),
        ("lcase", F::custom(lower)),
        ("left", F::binary(expr_fn::left)),
        ("len", F::unary(expr_fn::length)),
        ("length", F::unary(expr_fn::length)),
        ("levenshtein", F::udf(Levenshtein::new())),
        ("locate", F::custom(position)),
        ("lower", F::custom(lower)),
        ("lpad", F::custom(lpad_dispatch)),
        ("ltrim", F::var_arg(rev_args(expr_fn::ltrim))),
        ("luhn_check", F::unary(string_fn::luhn_check)),
        ("make_valid_utf8", F::udf(MakeValidUtf8::new())),
        ("mask", F::udf(SparkMask::new())),
        ("octet_length", F::custom(octet_length)),
        ("overlay", F::var_arg(overlay)),
        ("position", F::custom(position)),
        ("printf", F::udf(FormatStringFunc::new())),
        ("quote", F::udf(SparkQuote::new())),
        ("randstr", F::udf(Randstr::new())),
        ("regexp_count", F::udf(RegexpCountFunc::new())),
        ("regexp_extract", F::custom(regexp_extract)),
        ("regexp_extract_all", F::udf(SparkRegexpExtractAll::new())),
        ("regexp_instr", F::udf(RegexpInstrFunc::new())),
        ("regexp_replace", F::ternary(regexp_replace)),
        ("regexp_substr", F::custom(regexp_substr)),
        ("repeat", F::binary(expr_fn::repeat)),
        ("replace", F::var_arg(replace)),
        ("right", F::binary(expr_fn::right)),
        ("rpad", F::custom(rpad_dispatch)),
        ("rtrim", F::var_arg(rev_args(expr_fn::rtrim))),
        ("sentences", F::udf(SparkSentences::new())),
        ("soundex", F::udf(Soundex::new())),
        ("space", F::unary(space)),
        ("split", F::udf(SparkSplit::new())),
        ("split_part", F::ternary(expr_fn::split_part)),
        ("startswith", F::custom(startswith)),
        ("substr", F::custom(substr)),
        ("substring", F::custom(substr)),
        ("substring_index", F::ternary(expr_fn::substr_index)),
        ("to_binary", F::udf(SparkToBinary::new())),
        ("to_char", F::custom(to_char_dispatch)),
        ("to_number", F::udf(SparkToNumber::new(false))),
        ("to_varchar", F::custom(to_char_dispatch)),
        ("translate", F::ternary(expr_fn::translate)),
        ("trim", F::var_arg(rev_args(expr_fn::trim))),
        ("try_to_binary", F::udf(SparkTryToBinary::new())),
        ("try_to_number", F::udf(SparkToNumber::new(true))),
        ("try_validate_utf8", F::custom(try_validate_utf8)),
        ("ucase", F::custom(upper)),
        ("unbase64", F::udf(SparkUnbase64::new())),
        ("upper", F::custom(upper)),
        ("validate_utf8", F::custom(validate_utf8)),
        ("strpos", F::binary(expr_fn::strpos)),
    ]
}
