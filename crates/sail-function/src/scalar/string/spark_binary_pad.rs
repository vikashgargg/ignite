use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryBuilder, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::as_int64_array;
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::signature::{Signature, Volatility};

// Spark binary lpad: lpad(binary, len [, fill_binary])
// Pads the binary value on the left to `len` bytes using `fill_binary` (default: zero byte).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkBinaryLpad {
    signature: Signature,
}

impl Default for SparkBinaryLpad {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBinaryLpad {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkBinaryLpad {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_binary_lpad"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 || arg_types.len() > 3 {
            return exec_err!(
                "spark_binary_lpad requires 2 or 3 arguments, got {}",
                arg_types.len()
            );
        }
        let mut out = vec![DataType::Binary, DataType::Int64];
        if arg_types.len() == 3 {
            out.push(DataType::Binary);
        }
        Ok(out)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        // Resolve scalars to single-row arrays for uniform handling.
        let batch_size = args
            .iter()
            .find_map(|a| {
                if let ColumnarValue::Array(arr) = a {
                    Some(arr.len())
                } else {
                    None
                }
            })
            .unwrap_or(1);

        let arrays: Vec<ArrayRef> = args
            .iter()
            .map(|a| a.clone().into_array(batch_size))
            .collect::<Result<_>>()?;

        let binary_arr = arrays[0].as_binary::<i32>();
        let len_arr = as_int64_array(&arrays[1])?;
        let fill_opt = arrays.get(2);

        let mut builder = BinaryBuilder::with_capacity(batch_size, batch_size * 4);

        for i in 0..batch_size {
            if binary_arr.is_null(i) || len_arr.is_null(i) {
                builder.append_null();
                continue;
            }
            let src = binary_arr.value(i);
            let target_len = len_arr.value(i);
            if target_len < 0 {
                builder.append_value(&[] as &[u8]);
                continue;
            }
            let target_len = target_len as usize;
            let fill_null = fill_opt.map(|a| a.is_null(i)).unwrap_or(false);
            if fill_null {
                builder.append_null();
                continue;
            }

            let result = binary_lpad(
                src,
                target_len,
                fill_opt.map(|a| a.as_binary::<i32>().value(i)),
            );
            builder.append_value(&result);
        }

        let result = Arc::new(builder.finish()) as ArrayRef;
        if batch_size == 1 && args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_))) {
            let scalar = ScalarValue::try_from_array(&result, 0)?;
            Ok(ColumnarValue::Scalar(scalar))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

fn binary_lpad(src: &[u8], target_len: usize, fill: Option<&[u8]>) -> Vec<u8> {
    if src.len() >= target_len {
        return src[src.len() - target_len..].to_vec();
    }
    let pad_len = target_len - src.len();
    let fill = fill.unwrap_or(&[0u8]);
    if fill.is_empty() {
        return src.to_vec();
    }
    let mut result = Vec::with_capacity(target_len);
    let mut written = 0;
    while written < pad_len {
        let take = (pad_len - written).min(fill.len());
        result.extend_from_slice(&fill[..take]);
        written += take;
    }
    result.extend_from_slice(src);
    result
}

// Spark binary rpad: rpad(binary, len [, fill_binary])
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkBinaryRpad {
    signature: Signature,
}

impl Default for SparkBinaryRpad {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBinaryRpad {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkBinaryRpad {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_binary_rpad"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 || arg_types.len() > 3 {
            return exec_err!(
                "spark_binary_rpad requires 2 or 3 arguments, got {}",
                arg_types.len()
            );
        }
        let mut out = vec![DataType::Binary, DataType::Int64];
        if arg_types.len() == 3 {
            out.push(DataType::Binary);
        }
        Ok(out)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let batch_size = args
            .iter()
            .find_map(|a| {
                if let ColumnarValue::Array(arr) = a {
                    Some(arr.len())
                } else {
                    None
                }
            })
            .unwrap_or(1);

        let arrays: Vec<ArrayRef> = args
            .iter()
            .map(|a| a.clone().into_array(batch_size))
            .collect::<Result<_>>()?;

        let binary_arr = arrays[0].as_binary::<i32>();
        let len_arr = as_int64_array(&arrays[1])?;
        let fill_opt = arrays.get(2);

        let mut builder = BinaryBuilder::with_capacity(batch_size, batch_size * 4);

        for i in 0..batch_size {
            if binary_arr.is_null(i) || len_arr.is_null(i) {
                builder.append_null();
                continue;
            }
            let src = binary_arr.value(i);
            let target_len = len_arr.value(i);
            if target_len < 0 {
                builder.append_value(&[] as &[u8]);
                continue;
            }
            let target_len = target_len as usize;
            let fill_null = fill_opt.map(|a| a.is_null(i)).unwrap_or(false);
            if fill_null {
                builder.append_null();
                continue;
            }

            let result = binary_rpad(
                src,
                target_len,
                fill_opt.map(|a| a.as_binary::<i32>().value(i)),
            );
            builder.append_value(&result);
        }

        let result = Arc::new(builder.finish()) as ArrayRef;
        if batch_size == 1 && args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_))) {
            let scalar = ScalarValue::try_from_array(&result, 0)?;
            Ok(ColumnarValue::Scalar(scalar))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

fn binary_rpad(src: &[u8], target_len: usize, fill: Option<&[u8]>) -> Vec<u8> {
    if src.len() >= target_len {
        return src[..target_len].to_vec();
    }
    let pad_len = target_len - src.len();
    let fill = fill.unwrap_or(&[0u8]);
    if fill.is_empty() {
        return src.to_vec();
    }
    let mut result = Vec::with_capacity(target_len);
    result.extend_from_slice(src);
    let mut written = 0;
    while written < pad_len {
        let take = (pad_len - written).min(fill.len());
        result.extend_from_slice(&fill[..take]);
        written += take;
    }
    result
}
