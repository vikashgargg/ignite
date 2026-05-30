use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions::datetime::to_date::ToDateFunc;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTryToDate {
    signature: Signature,
}

impl Default for SparkTryToDate {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTryToDate {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkTryToDate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "spark_try_to_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let result = ToDateFunc::new().invoke_with_args(args);
        match result {
            Ok(v) => Ok(v),
            Err(_) => Ok(ColumnarValue::Scalar(ScalarValue::Date32(None))),
        }
    }
}
