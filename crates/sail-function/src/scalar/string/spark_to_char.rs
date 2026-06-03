use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use super::format_number::{cast_arrow_array_to_f64, scalar_to_f64};

/// Oracle-style number-to-string formatting for Spark's `to_char`/`to_varchar`.
/// Supports format elements: 9, 0, G, D, ., ,, $, S
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkToCharNumber {
    signature: Signature,
}

impl Default for SparkToCharNumber {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkToCharNumber {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkToCharNumber {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_to_char_number"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args.len() != 2 {
            return datafusion_common::exec_err!(
                "spark_to_char_number requires 2 arguments, got {}",
                args.len()
            );
        }

        match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(fmt))) => {
                let fmt = fmt.clone();
                format_with_scalar_fmt(&args[0], |v| oracle_format_number(v, &fmt))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            ColumnarValue::Array(arr) => {
                let fmts = datafusion::arrow::compute::cast(arr, &DataType::Utf8)?;
                let fmt_arr = fmts.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "Failed to cast format to StringArray".to_string(),
                    )
                })?;
                match &args[0] {
                    ColumnarValue::Array(num_arr) => {
                        let f64_arr = cast_arrow_array_to_f64(num_arr)?;
                        let result: StringArray = f64_arr
                            .iter()
                            .zip(fmt_arr.iter())
                            .map(|(v_opt, f_opt)| match (v_opt, f_opt) {
                                (Some(v), Some(f)) => oracle_format_number(v, f),
                                _ => None,
                            })
                            .collect();
                        Ok(ColumnarValue::Array(Arc::new(result)))
                    }
                    ColumnarValue::Scalar(s) => {
                        let value = scalar_to_f64(s)?;
                        let result: StringArray = fmt_arr
                            .iter()
                            .map(|f_opt| match (value, f_opt) {
                                (Some(v), Some(f)) => oracle_format_number(v, f),
                                _ => None,
                            })
                            .collect();
                        Ok(ColumnarValue::Array(Arc::new(result)))
                    }
                }
            }
            _ => datafusion_common::exec_err!(
                "spark_to_char_number second argument must be a string format"
            ),
        }
    }
}

fn format_with_scalar_fmt(
    number: &ColumnarValue,
    fmt: impl Fn(f64) -> Option<String>,
) -> Result<ColumnarValue> {
    match number {
        ColumnarValue::Scalar(scalar) => {
            let value = scalar_to_f64(scalar)?;
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                value.and_then(fmt),
            )))
        }
        ColumnarValue::Array(arr) => {
            let f64_arr = cast_arrow_array_to_f64(arr)?;
            let result: StringArray = f64_arr.iter().map(|opt| opt.and_then(&fmt)).collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

/// Format a numeric value using an Oracle-style number format string.
/// Supported format elements: 9, 0, G (or ,), D (or .), $ (literal), S (trailing sign)
pub fn oracle_format_number(value: f64, fmt: &str) -> Option<String> {
    let negative = value < 0.0;
    let abs_val = value.abs();
    let fmt_upper = fmt.to_uppercase();

    // Parse format string
    let mut has_dollar = false;
    let mut int_slots: Vec<char> = Vec::new(); // '9' or '0' for each integer digit position
    let mut group_trigger_positions: Vec<usize> = Vec::new(); // digit-slot count at each G/,
    let mut has_dec_sep = false;
    let mut dec_slots: Vec<char> = Vec::new();
    let mut trailing_sign = false;

    let chars: Vec<char> = fmt_upper.chars().collect();
    let mut i = 0;
    let mut parsing_int = true;

    while i < chars.len() {
        match chars[i] {
            '$' if i == 0 || (i == 1 && parsing_int) => {
                has_dollar = true;
                i += 1;
            }
            '9' | '0' => {
                if parsing_int {
                    int_slots.push(chars[i]);
                } else {
                    dec_slots.push(chars[i]);
                }
                i += 1;
            }
            'G' | ',' if parsing_int => {
                group_trigger_positions.push(int_slots.len());
                i += 1;
            }
            'D' | '.' if parsing_int => {
                has_dec_sep = true;
                parsing_int = false;
                i += 1;
            }
            'S' if i == chars.len() - 1 => {
                trailing_sign = true;
                i += 1;
            }
            'M' if i + 1 < chars.len() && chars[i + 1] == 'I' => {
                trailing_sign = true;
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    let n = int_slots.len();
    let dec_len = dec_slots.len();

    // Compute rounded integer and decimal parts together to handle carry
    let (rounded_int, dec_str) = if dec_len > 0 {
        let factor = 10u64.pow(dec_len as u32);
        let total = (abs_val * factor as f64).round() as u64;
        let di = total % factor;
        let ri = total / factor;
        (ri, format!("{:0>width$}", di, width = dec_len))
    } else {
        (abs_val.round() as u64, String::new())
    };

    let int_str = format!("{}", rounded_int);

    // Overflow: more digits than format slots
    if int_str.len() > n {
        return None;
    }

    // Build padded integer digit string (left-pad with fill char)
    let pad_count = n - int_str.len();
    let mut padded: Vec<char> = Vec::with_capacity(n);
    for &slot in &int_slots[..pad_count] {
        padded.push(if slot == '0' { '0' } else { ' ' });
    }
    padded.extend(int_str.chars());

    // Group separator positions from right = n - (digit-slot count at G)
    // E.g. "99G999" → group_trigger_positions=[2], n=5 → from_right=3
    let group_from_right: Vec<usize> = group_trigger_positions
        .iter()
        .map(|&left| n - left)
        .collect();

    // Build integer string with group separators
    let mut int_result = String::new();
    for (idx, &c) in padded.iter().enumerate() {
        int_result.push(c);
        let right_pos = n - idx - 1;
        if right_pos > 0 && group_from_right.contains(&right_pos) {
            int_result.push(',');
        }
    }

    // Assemble result
    let mut result = String::new();
    if has_dollar {
        result.push('$');
    }
    result.push_str(&int_result);
    if has_dec_sep {
        result.push('.');
        result.push_str(&dec_str);
    }
    if trailing_sign {
        result.push(if negative { '-' } else { '+' });
    }

    // Spark trims leading spaces
    Some(result.trim_start().to_string())
}
