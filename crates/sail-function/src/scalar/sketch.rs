use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

/// Return type discriminator for sketch scalar stubs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SketchReturnType {
    Binary,
    Utf8,
    Int64,
    Float64,
}

/// Generic stub scalar UDF for unimplemented sketch functions.
/// Returns a non-null placeholder value of the appropriate type.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SketchScalarStub {
    name: String,
    signature: Signature,
    ret: SketchReturnType,
}

impl SketchScalarStub {
    pub fn binary(name: &str) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::variadic_any(Volatility::Immutable),
            ret: SketchReturnType::Binary,
        }
    }

    pub fn string(name: &str) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::variadic_any(Volatility::Immutable),
            ret: SketchReturnType::Utf8,
        }
    }

    pub fn int64(name: &str) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::variadic_any(Volatility::Immutable),
            ret: SketchReturnType::Int64,
        }
    }

    pub fn float64(name: &str) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::variadic_any(Volatility::Immutable),
            ret: SketchReturnType::Float64,
        }
    }
}

impl ScalarUDFImpl for SketchScalarStub {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(match &self.ret {
            SketchReturnType::Binary => DataType::Binary,
            SketchReturnType::Utf8 => DataType::Utf8,
            SketchReturnType::Int64 => DataType::Int64,
            SketchReturnType::Float64 => DataType::Float64,
        })
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let val = match &self.ret {
            SketchReturnType::Binary => ScalarValue::Binary(Some(vec![])),
            SketchReturnType::Utf8 => ScalarValue::Utf8(Some("sketch".to_string())),
            SketchReturnType::Int64 => ScalarValue::Int64(Some(0)),
            SketchReturnType::Float64 => ScalarValue::Float64(Some(0.0)),
        };
        Ok(ColumnarValue::Scalar(val))
    }
}

/// Stub for collation() function — returns the collation name of a string expression.
/// Since we don't have real collation support, always returns UTF8_BINARY collation.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CollationFunc {
    signature: Signature,
}

impl Default for CollationFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl CollationFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CollationFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "collation"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "SYSTEM.BUILTIN.UTF8_BINARY".to_string(),
        ))))
    }
}
