use std::any::Any;

use datafusion::arrow::array::{Array, ArrayRef, BinaryArray, Float64Builder};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{plan_datafusion_err, Result, ScalarValue};
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

// ---------------------------------------------------------------------------
// ThetaSketchEstimate — decode serialized sketch bytes → f64 cardinality
// ---------------------------------------------------------------------------

const THETA_MAGIC: u32 = 0x5448_4554;
const THETA_K: usize = 4096;

fn estimate_from_bytes(bytes: &[u8]) -> Result<f64> {
    if bytes.len() < 16 {
        return Err(plan_datafusion_err!("theta_sketch_estimate: buffer too short"));
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if magic != THETA_MAGIC {
        // Treat unknown format as 0 (stub / legacy data)
        return Ok(0.0);
    }
    let count = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let theta = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    if count < THETA_K {
        return Ok(count as f64);
    }
    let theta_f = theta as f64 / u64::MAX as f64;
    if theta_f <= 0.0 {
        return Ok(count as f64);
    }
    Ok(count as f64 / theta_f)
}

/// `theta_sketch_estimate(binary) → float64`
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ThetaSketchEstimateFunc {
    signature: Signature,
}

impl Default for ThetaSketchEstimateFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ThetaSketchEstimateFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ThetaSketchEstimateFunc {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "theta_sketch_estimate" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg = args.args.into_iter().next()
            .ok_or_else(|| plan_datafusion_err!("theta_sketch_estimate: missing argument"))?;
        match arg {
            ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes))) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(estimate_from_bytes(&bytes)?))))
            }
            ColumnarValue::Scalar(_) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))))
            }
            ColumnarValue::Array(arr) => {
                let bins = arr.as_any().downcast_ref::<BinaryArray>()
                    .ok_or_else(|| plan_datafusion_err!("theta_sketch_estimate: expected Binary array"))?;
                let mut b = Float64Builder::with_capacity(bins.len());
                for i in 0..bins.len() {
                    if bins.is_null(i) {
                        b.append_null();
                    } else {
                        b.append_value(estimate_from_bytes(bins.value(i))?);
                    }
                }
                Ok(ColumnarValue::Array(std::sync::Arc::new(b.finish()) as ArrayRef))
            }
        }
    }
}

/// `hll_sketch_estimate(binary) → float64` — same logic, different magic acceptance
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HllSketchEstimateFunc {
    signature: Signature,
}

impl Default for HllSketchEstimateFunc {
    fn default() -> Self { Self::new() }
}

impl HllSketchEstimateFunc {
    pub fn new() -> Self {
        Self { signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable) }
    }
}

impl ScalarUDFImpl for HllSketchEstimateFunc {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "hll_sketch_estimate" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // HLL sketches from hll_sketch_agg use the same theta serialisation internally
        ThetaSketchEstimateFunc::new().invoke_with_args(args)
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
