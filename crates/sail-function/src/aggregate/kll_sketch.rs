use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, Float32Array, Float64Array, Int64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{plan_datafusion_err, Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};

use crate::byte_utils::{read_f64_le, read_u32_le};

// ---------------------------------------------------------------------------
// KLL sketch — simplified Karnin-Lang-Liberty approximate quantile sketch
// ---------------------------------------------------------------------------
//
// Maintains a Vec<f64> of sampled values with max capacity K.
// When full, sort + keep every other element (compaction step) — cuts size by ~2×.
// For quantile(rank): sort retained values, return value at floor(rank * (n-1)).
//
// Binary wire format (little-endian):
//   [4 bytes] magic = 0x4B4C4C53 ("KLLS")
//   [4 bytes] u32  count of retained f64 values
//   [count × 8 bytes] f64 values

const KLL_K: usize = 200;
const KLL_MAGIC: u32 = 0x4B4C_4C53; // "KLLS"

#[derive(Debug, Clone)]
struct KllSketch {
    values: Vec<f64>,
}

impl KllSketch {
    fn new() -> Self {
        Self { values: Vec::new() }
    }

    fn add(&mut self, v: f64) {
        self.values.push(v);
        if self.values.len() >= KLL_K * 2 {
            self.compact();
        }
    }

    fn compact(&mut self) {
        self.values.sort_by(|a, b| a.total_cmp(b));
        // Keep every other element (even indices)
        let retained: Vec<f64> = self
            .values
            .iter()
            .enumerate()
            .filter(|(i, _)| i % 2 == 0)
            .map(|(_, v)| *v)
            .collect();
        self.values = retained;
    }

    fn merge(&mut self, other: &KllSketch) {
        self.values.extend_from_slice(&other.values);
        if self.values.len() >= KLL_K * 2 {
            self.compact();
        }
    }

    fn quantile(&mut self, rank: f64) -> Option<f64> {
        if self.values.is_empty() {
            return None;
        }
        self.values.sort_by(|a, b| a.total_cmp(b));
        let n = self.values.len();
        let idx = ((rank * (n - 1) as f64).floor() as usize).min(n - 1);
        Some(self.values[idx])
    }

    fn serialize(&self) -> Vec<u8> {
        let count = self.values.len() as u32;
        let mut buf = Vec::with_capacity(8 + count as usize * 8);
        buf.extend_from_slice(&KLL_MAGIC.to_le_bytes());
        buf.extend_from_slice(&count.to_le_bytes());
        for v in &self.values {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        buf
    }

    fn deserialize(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 8 {
            return Err(plan_datafusion_err!("kll_sketch: buffer too short"));
        }
        let magic = read_u32_le(bytes, 0);
        if magic != KLL_MAGIC {
            return Err(plan_datafusion_err!("kll_sketch: bad magic {:08x}", magic));
        }
        let count = read_u32_le(bytes, 4) as usize;
        if bytes.len() < 8 + count * 8 {
            return Err(plan_datafusion_err!("kll_sketch: truncated value list"));
        }
        let mut values = Vec::with_capacity(count);
        for i in 0..count {
            let off = 8 + i * 8;
            let v = read_f64_le(bytes, off);
            values.push(v);
        }
        Ok(Self { values })
    }
}

// ---------------------------------------------------------------------------
// Macro to define the three KLL aggregate UDAFs
// ---------------------------------------------------------------------------

macro_rules! kll_agg_udaf {
    ($name:ident, $fn_name:literal, $input_type:expr, $convert:expr) => {
        #[derive(Debug, PartialEq, Eq, Hash)]
        pub struct $name {
            signature: Signature,
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $name {
            pub fn new() -> Self {
                Self {
                    // Accept (col) or (col, k) — second arg is optional capacity, ignored
                    signature: Signature::one_of(
                        vec![
                            datafusion_expr::TypeSignature::Exact(vec![$input_type]),
                            datafusion_expr::TypeSignature::Exact(vec![
                                $input_type,
                                DataType::Int32,
                            ]),
                            datafusion_expr::TypeSignature::Exact(vec![
                                $input_type,
                                DataType::Int64,
                            ]),
                        ],
                        Volatility::Immutable,
                    ),
                }
            }
        }

        impl AggregateUDFImpl for $name {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                $fn_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok(DataType::Binary)
            }

            fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
                Ok(vec![Arc::new(Field::new(
                    "kll_state",
                    DataType::Binary,
                    true,
                ))])
            }

            fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
                Ok(Box::new(KllAccumulator::new($convert)))
            }
        }
    };
}

kll_agg_udaf!(
    KllSketchAggBigint,
    "kll_sketch_agg_bigint",
    DataType::Int64,
    |v: f64| v as i64
);
kll_agg_udaf!(
    KllSketchAggDouble,
    "kll_sketch_agg_double",
    DataType::Float64,
    |v: f64| v
);
kll_agg_udaf!(
    KllSketchAggFloat,
    "kll_sketch_agg_float",
    DataType::Float32,
    |v: f64| v as f32
);

// ---------------------------------------------------------------------------
// Generic KLL accumulator
// ---------------------------------------------------------------------------

/// Type-erased converter: sketch f64 → type-specific f64 for reconstruction.
/// We store as f64 internally; on output we just serialize the binary blob.
type Converter<T> = fn(f64) -> T;

/// Accumulator that fills a KLL sketch from a typed column and serializes to Binary.
struct KllAccumulator<T: 'static> {
    sketch: KllSketch,
    _converter: Converter<T>,
}

impl<T: 'static> Debug for KllAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KllAccumulator")
            .field("sketch_len", &self.sketch.values.len())
            .finish()
    }
}

impl<T: 'static> KllAccumulator<T> {
    fn new(converter: Converter<T>) -> Self {
        Self {
            sketch: KllSketch::new(),
            _converter: converter,
        }
    }
}

impl Accumulator for KllAccumulator<i64> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let col = values[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| plan_datafusion_err!("kll_sketch_agg_bigint: expected Int64"))?;
        for v in col.iter().flatten() {
            self.sketch.add(v as f64);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        merge_binary_states(&mut self.sketch, states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.sketch.serialize())))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.sketch.serialize()))])
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.sketch.values.len() * 8
    }
}

impl Accumulator for KllAccumulator<f64> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let col = values[0]
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| plan_datafusion_err!("kll_sketch_agg_double: expected Float64"))?;
        for v in col.iter().flatten() {
            self.sketch.add(v);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        merge_binary_states(&mut self.sketch, states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.sketch.serialize())))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.sketch.serialize()))])
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.sketch.values.len() * 8
    }
}

impl Accumulator for KllAccumulator<f32> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let col = values[0]
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| plan_datafusion_err!("kll_sketch_agg_float: expected Float32"))?;
        for v in col.iter().flatten() {
            self.sketch.add(v as f64);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        merge_binary_states(&mut self.sketch, states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.sketch.serialize())))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.sketch.serialize()))])
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.sketch.values.len() * 8
    }
}

fn merge_binary_states(sketch: &mut KllSketch, states: &[ArrayRef]) -> Result<()> {
    let Some(col) = states.first() else {
        return Ok(());
    };
    let bins = col
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| plan_datafusion_err!("kll_sketch merge: expected Binary"))?;
    for row in 0..bins.len() {
        if bins.is_null(row) {
            continue;
        }
        let other = KllSketch::deserialize(bins.value(row))?;
        sketch.merge(&other);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Scalar UDFs: kll_sketch_get_quantile_{bigint,double,float}
// ---------------------------------------------------------------------------

use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

/// `kll_sketch_get_quantile_bigint(binary, float64) → int64`
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct KllSketchGetQuantileBigint {
    signature: Signature,
}

impl Default for KllSketchGetQuantileBigint {
    fn default() -> Self {
        Self::new()
    }
}

impl KllSketchGetQuantileBigint {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Binary, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for KllSketchGetQuantileBigint {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "kll_sketch_get_quantile_bigint"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        kll_quantile_scalar(&args, |v| ScalarValue::Int64(Some(v as i64)))
    }
}

/// `kll_sketch_get_quantile_double(binary, float64) → float64`
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct KllSketchGetQuantileDouble {
    signature: Signature,
}

impl Default for KllSketchGetQuantileDouble {
    fn default() -> Self {
        Self::new()
    }
}

impl KllSketchGetQuantileDouble {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Binary, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for KllSketchGetQuantileDouble {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "kll_sketch_get_quantile_double"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        kll_quantile_scalar(&args, |v| ScalarValue::Float64(Some(v)))
    }
}

/// `kll_sketch_get_quantile_float(binary, float64) → float32`
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct KllSketchGetQuantileFloat {
    signature: Signature,
}

impl Default for KllSketchGetQuantileFloat {
    fn default() -> Self {
        Self::new()
    }
}

impl KllSketchGetQuantileFloat {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Binary, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for KllSketchGetQuantileFloat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "kll_sketch_get_quantile_float"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        kll_quantile_scalar(&args, |v| ScalarValue::Float32(Some(v as f32)))
    }
}

/// Shared implementation for all kll_sketch_get_quantile_* scalar UDFs.
fn kll_quantile_scalar(
    args: &ScalarFunctionArgs,
    make_scalar: impl Fn(f64) -> ScalarValue,
) -> Result<ColumnarValue> {
    if args.args.len() != 2 {
        return Err(plan_datafusion_err!(
            "kll_sketch_get_quantile: expected 2 arguments"
        ));
    }

    let sketch_arg = &args.args[0];
    let rank_arg = &args.args[1];

    let rank = match rank_arg {
        ColumnarValue::Scalar(ScalarValue::Float64(Some(r))) => *r,
        ColumnarValue::Scalar(ScalarValue::Float32(Some(r))) => *r as f64,
        _ => 0.5_f64,
    };

    match sketch_arg {
        ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes))) => {
            let mut sketch = KllSketch::deserialize(bytes)?;
            let result = sketch.quantile(rank);
            Ok(ColumnarValue::Scalar(match result {
                Some(v) => make_scalar(v),
                None => ScalarValue::Null,
            }))
        }
        ColumnarValue::Scalar(_) => Ok(ColumnarValue::Scalar(ScalarValue::Null)),
        ColumnarValue::Array(arr) => {
            let bins = arr
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| plan_datafusion_err!("kll_sketch_get_quantile: expected Binary"))?;
            let mut results: Vec<ScalarValue> = Vec::with_capacity(bins.len());
            for i in 0..bins.len() {
                if bins.is_null(i) {
                    results.push(ScalarValue::Null);
                    continue;
                }
                let mut sketch = KllSketch::deserialize(bins.value(i))?;
                results.push(match sketch.quantile(rank) {
                    Some(v) => make_scalar(v),
                    None => ScalarValue::Null,
                });
            }
            ScalarValue::iter_to_array(results).map(ColumnarValue::Array)
        }
    }
}
