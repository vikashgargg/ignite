use std::sync::Arc;

use chrono::{NaiveTime, Timelike};
use datafusion::arrow::array::Time64MicrosecondArray;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use zelox_common_datafusion::utils::items::ItemTaker;

/// `try_to_time(str, chrono_format)` — parses time with a Chrono format, returns
/// `Time64(Microsecond)` or `NULL` on error.  The format string must already be a
/// Chrono strftime format (i.e. the Spark → Chrono conversion happens at plan time).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTryToTimeWithFmt {
    signature: Signature,
}

impl Default for SparkTryToTimeWithFmt {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryToTimeWithFmt {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }

    fn parse_micros(value: &str, fmt: &str) -> Option<i64> {
        NaiveTime::parse_from_str(value, fmt).ok().map(|t| {
            let secs = t.num_seconds_from_midnight() as i64;
            let nanos = t.nanosecond() as i64;
            secs * 1_000_000 + nanos / 1_000
        })
    }
}

impl ScalarUDFImpl for SparkTryToTimeWithFmt {

    fn name(&self) -> &str {
        "spark_try_to_time_with_fmt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Time64(TimeUnit::Microsecond))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let (value_cv, fmt_cv) = args.two()?;

        let fmt = match &fmt_cv {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::Time64Microsecond(None)));
            }
            _ => return exec_err!("spark_try_to_time_with_fmt: format must be a Utf8 scalar"),
        };

        match value_cv {
            ColumnarValue::Scalar(ScalarValue::Utf8(s)) => {
                let micros = s.as_deref().and_then(|v| Self::parse_micros(v, &fmt));
                Ok(ColumnarValue::Scalar(ScalarValue::Time64Microsecond(
                    micros,
                )))
            }
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(s)) => {
                let micros = s.as_deref().and_then(|v| Self::parse_micros(v, &fmt));
                Ok(ColumnarValue::Scalar(ScalarValue::Time64Microsecond(
                    micros,
                )))
            }
            ColumnarValue::Array(array) => {
                let result: Time64MicrosecondArray = match array.data_type() {
                    DataType::Utf8 => as_string_array(&array)?
                        .iter()
                        .map(|x| x.and_then(|v| Self::parse_micros(v, &fmt)))
                        .collect(),
                    DataType::LargeUtf8 => as_large_string_array(&array)?
                        .iter()
                        .map(|x| x.and_then(|v| Self::parse_micros(v, &fmt)))
                        .collect(),
                    DataType::Utf8View => as_string_view_array(&array)?
                        .iter()
                        .map(|x| x.and_then(|v| Self::parse_micros(v, &fmt)))
                        .collect(),
                    _ => return exec_err!("expected string array for spark_try_to_time_with_fmt"),
                };
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            _ => exec_err!("spark_try_to_time_with_fmt: value must be a string"),
        }
    }
}
