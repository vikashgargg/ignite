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
    // Empty / stub sketch → 0
    if bytes.is_empty() {
        return Ok(0.0);
    }
    if bytes.len() < 16 {
        return Ok(0.0);
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if magic != THETA_MAGIC {
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

/// Deserialise a theta sketch from bytes into a sorted Vec<u64> of hashes + theta.
fn decode_sketch(bytes: &[u8]) -> (Vec<u64>, u64) {
    if bytes.len() < 16 {
        return (vec![], u64::MAX);
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if magic != THETA_MAGIC {
        return (vec![], u64::MAX);
    }
    let count = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let theta = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let mut hashes = Vec::with_capacity(count);
    for i in 0..count {
        let off = 16 + i * 8;
        if off + 8 > bytes.len() {
            break;
        }
        hashes.push(u64::from_le_bytes(bytes[off..off + 8].try_into().unwrap()));
    }
    (hashes, theta)
}

/// Encode a (hashes, theta) pair back to the theta sketch binary format.
fn encode_sketch(mut hashes: Vec<u64>, theta: u64) -> Vec<u8> {
    hashes.sort_unstable();
    hashes.dedup();
    let count = hashes.len();
    let mut out = Vec::with_capacity(16 + count * 8);
    out.extend_from_slice(&THETA_MAGIC.to_le_bytes());
    out.extend_from_slice(&(count as u32).to_le_bytes());
    out.extend_from_slice(&theta.to_le_bytes());
    for h in &hashes {
        out.extend_from_slice(&h.to_le_bytes());
    }
    out
}

/// Merge two binary sketch arrays using a combining function.
fn merge_two_sketches(
    args: ScalarFunctionArgs,
    combine: impl Fn(Vec<u64>, u64, Vec<u64>, u64) -> (Vec<u64>, u64),
) -> Result<ColumnarValue> {
    let mut it = args.args.into_iter();
    let (a, b) = match (it.next(), it.next()) {
        (Some(a), Some(b)) => (a, b),
        _ => return Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![])))),
    };
    let get_bytes = |cv: ColumnarValue| -> Vec<u8> {
        match cv {
            ColumnarValue::Scalar(ScalarValue::Binary(Some(v))) => v,
            _ => vec![],
        }
    };
    let bytes_a = get_bytes(a);
    let bytes_b = get_bytes(b);
    let (ha, ta) = decode_sketch(&bytes_a);
    let (hb, tb) = decode_sketch(&bytes_b);
    let (merged, theta) = combine(ha, ta, hb, tb);
    Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(
        encode_sketch(merged, theta),
    ))))
}

/// `theta_union(s1, s2) → Binary` — union of two theta sketches.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ThetaUnionFunc {
    signature: Signature,
}
impl ThetaUnionFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Binary, DataType::Binary],
                Volatility::Immutable,
            ),
        }
    }
}
impl Default for ThetaUnionFunc {
    fn default() -> Self {
        Self::new()
    }
}
impl ScalarUDFImpl for ThetaUnionFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "theta_union"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        merge_two_sketches(args, |mut ha, ta, hb, tb| {
            let theta = ta.min(tb);
            ha.extend(hb.into_iter().filter(|h| *h < theta));
            ha.retain(|h| *h < theta);
            if ha.len() > THETA_K {
                ha.sort_unstable();
                ha.dedup();
                ha.truncate(THETA_K);
                let new_theta = ha.iter().copied().max().unwrap_or(u64::MAX);
                return (ha, new_theta);
            }
            (ha, theta)
        })
    }
}

/// `theta_intersection(s1, s2) → Binary` — intersection of two theta sketches.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ThetaIntersectionFunc {
    signature: Signature,
}
impl ThetaIntersectionFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Binary, DataType::Binary],
                Volatility::Immutable,
            ),
        }
    }
}
impl Default for ThetaIntersectionFunc {
    fn default() -> Self {
        Self::new()
    }
}
impl ScalarUDFImpl for ThetaIntersectionFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "theta_intersection"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        merge_two_sketches(args, |ha, ta, hb, tb| {
            let theta = ta.min(tb);
            let set_b: std::collections::HashSet<u64> = hb.into_iter().collect();
            let intersected: Vec<u64> = ha
                .into_iter()
                .filter(|h| set_b.contains(h) && *h < theta)
                .collect();
            (intersected, theta)
        })
    }
}

/// `theta_difference(s1, s2) → Binary` — set difference A \ B of two theta sketches.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ThetaDifferenceFunc {
    signature: Signature,
}
impl ThetaDifferenceFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Binary, DataType::Binary],
                Volatility::Immutable,
            ),
        }
    }
}
impl Default for ThetaDifferenceFunc {
    fn default() -> Self {
        Self::new()
    }
}
impl ScalarUDFImpl for ThetaDifferenceFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "theta_difference"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        merge_two_sketches(args, |ha, ta, hb, tb| {
            let theta = ta.min(tb);
            let set_b: std::collections::HashSet<u64> = hb.into_iter().collect();
            let diff: Vec<u64> = ha
                .into_iter()
                .filter(|h| !set_b.contains(h) && *h < theta)
                .collect();
            (diff, theta)
        })
    }
}

/// `hll_union(s1, s2) → Binary` — alias for theta_union (same format).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HllUnionFunc {
    inner: ThetaUnionFunc,
}
impl HllUnionFunc {
    pub fn new() -> Self {
        Self {
            inner: ThetaUnionFunc::new(),
        }
    }
}
impl Default for HllUnionFunc {
    fn default() -> Self {
        Self::new()
    }
}
impl ScalarUDFImpl for HllUnionFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "hll_union"
    }
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        self.inner.return_type(args)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.inner.invoke_with_args(args)
    }
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
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "theta_sketch_estimate"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg = args
            .args
            .into_iter()
            .next()
            .ok_or_else(|| plan_datafusion_err!("theta_sketch_estimate: missing argument"))?;
        match arg {
            ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes))) => Ok(ColumnarValue::Scalar(
                ScalarValue::Float64(Some(estimate_from_bytes(&bytes)?)),
            )),
            ColumnarValue::Scalar(_) => Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0)))),
            ColumnarValue::Array(arr) => {
                let bins = arr.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
                    plan_datafusion_err!("theta_sketch_estimate: expected Binary array")
                })?;
                let mut b = Float64Builder::with_capacity(bins.len());
                for i in 0..bins.len() {
                    if bins.is_null(i) {
                        b.append_null();
                    } else {
                        b.append_value(estimate_from_bytes(bins.value(i))?);
                    }
                }
                Ok(ColumnarValue::Array(
                    std::sync::Arc::new(b.finish()) as ArrayRef
                ))
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
    fn default() -> Self {
        Self::new()
    }
}

impl HllSketchEstimateFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for HllSketchEstimateFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "hll_sketch_estimate"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

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
