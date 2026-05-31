use std::any::Any;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BinaryArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{plan_datafusion_err, Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, TypeSignature, Volatility};

// ---------------------------------------------------------------------------
// Theta Sketch — compact K-Minimum Values (KMV) sketch
// ---------------------------------------------------------------------------
//
// Keeps the K smallest 64-bit hashes of all seen values.
// Cardinality estimate = K / theta where theta = max_retained_hash / u64::MAX.
// When fewer than K distinct values have been seen the count is exact.
//
// Binary serialisation (little-endian):
//   [4 bytes] magic = 0x54484554 ("THET")
//   [4 bytes] u32 count of stored hashes
//   [8 bytes] u64 theta (= max retained hash; u64::MAX when not saturated)
//   [count × 8 bytes] sorted u64 hash values

const THETA_K: usize = 4096;
const THETA_MAGIC: u32 = 0x5448_4554; // "THET"

fn hash_scalar(v: &ScalarValue) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

#[derive(Debug, Default)]
struct ThetaSketch {
    hashes: BTreeSet<u64>, // sorted; never larger than THETA_K
    theta: u64,            // current max retained hash (u64::MAX when not saturated)
}

impl ThetaSketch {
    fn new() -> Self {
        Self { hashes: BTreeSet::new(), theta: u64::MAX }
    }

    fn insert(&mut self, h: u64) {
        if h >= self.theta {
            return;
        }
        self.hashes.insert(h);
        if self.hashes.len() > THETA_K {
            let max = *self.hashes.iter().next_back().unwrap();
            self.hashes.remove(&max);
            self.theta = *self.hashes.iter().next_back().unwrap_or(&u64::MAX);
        }
    }

    fn merge(&mut self, other: &ThetaSketch) {
        for &h in &other.hashes {
            self.insert(h);
        }
        self.theta = self.theta.min(other.theta);
        // Re-trim after theta update
        self.hashes.retain(|&h| h < self.theta);
    }

    fn estimate(&self) -> f64 {
        let n = self.hashes.len();
        if n < THETA_K {
            return n as f64; // exact when not saturated
        }
        let theta_f = self.theta as f64 / u64::MAX as f64;
        n as f64 / theta_f
    }

    fn serialize(&self) -> Vec<u8> {
        let count = self.hashes.len() as u32;
        let mut buf = Vec::with_capacity(16 + count as usize * 8);
        buf.extend_from_slice(&THETA_MAGIC.to_le_bytes());
        buf.extend_from_slice(&count.to_le_bytes());
        buf.extend_from_slice(&self.theta.to_le_bytes());
        for &h in &self.hashes {
            buf.extend_from_slice(&h.to_le_bytes());
        }
        buf
    }

    fn deserialize(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 16 {
            return Err(plan_datafusion_err!("theta sketch: buffer too short"));
        }
        let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if magic != THETA_MAGIC {
            return Err(plan_datafusion_err!("theta sketch: bad magic"));
        }
        let count = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
        let theta = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        if bytes.len() < 16 + count * 8 {
            return Err(plan_datafusion_err!("theta sketch: truncated hash list"));
        }
        let mut hashes = BTreeSet::new();
        for i in 0..count {
            let off = 16 + i * 8;
            let h = u64::from_le_bytes(bytes[off..off + 8].try_into().unwrap());
            hashes.insert(h);
        }
        Ok(Self { hashes, theta })
    }
}

// ---------------------------------------------------------------------------
// ThetaSketchAgg — aggregate values into a theta sketch binary
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ThetaSketchAgg {
    name: String,
    signature: Signature,
}

impl ThetaSketchAgg {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ThetaSketchAgg {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { &self.name }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Binary) }

    fn accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ThetaSketchAccumulator { sketch: ThetaSketch::new() }))
    }

    fn state_fields(&self, _: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new("theta_state", DataType::Binary, true))])
    }
}

#[derive(Debug)]
struct ThetaSketchAccumulator {
    sketch: ThetaSketch,
}

impl Accumulator for ThetaSketchAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(col) = values.first() else { return Ok(()) };
        for row in 0..col.len() {
            if col.is_null(row) { continue; }
            let sv = ScalarValue::try_from_array(col.as_ref(), row)?;
            self.sketch.insert(hash_scalar(&sv));
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some(col) = states.first() else { return Ok(()) };
        let bins = col.as_any().downcast_ref::<BinaryArray>()
            .ok_or_else(|| plan_datafusion_err!("theta_sketch merge: expected Binary"))?;
        for row in 0..bins.len() {
            if bins.is_null(row) { continue; }
            let other = ThetaSketch::deserialize(bins.value(row))?;
            self.sketch.merge(&other);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.sketch.serialize())))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.sketch.serialize()))])
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.sketch.hashes.len() * 8
    }
}

// ---------------------------------------------------------------------------
// ThetaSketchUnionAgg — merge existing sketch binaries
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ThetaSketchUnionAgg {
    name: String,
    signature: Signature,
}

impl ThetaSketchUnionAgg {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            // Accept (sketch) or (sketch, dedup_flag: bool) — dedup flag is ignored
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary]),
                    TypeSignature::Exact(vec![DataType::Binary, DataType::Boolean]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for ThetaSketchUnionAgg {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { &self.name }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Binary) }

    fn accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ThetaUnionAccumulator { sketch: ThetaSketch::new() }))
    }

    fn state_fields(&self, _: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new("theta_state", DataType::Binary, true))])
    }
}

/// Accumulator for `theta_union_agg`: input column contains Binary sketch values.
#[derive(Debug)]
struct ThetaUnionAccumulator {
    sketch: ThetaSketch,
}

impl Accumulator for ThetaUnionAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(col) = values.first() else { return Ok(()) };
        let bins = col.as_any().downcast_ref::<BinaryArray>()
            .ok_or_else(|| plan_datafusion_err!("theta_union: expected Binary input"))?;
        for row in 0..bins.len() {
            if bins.is_null(row) { continue; }
            let other = ThetaSketch::deserialize(bins.value(row))?;
            self.sketch.merge(&other);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.sketch.serialize())))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.sketch.serialize()))])
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.sketch.hashes.len() * 8
    }
}

// ---------------------------------------------------------------------------
// ThetaSketchDistinctAgg — returns f64 cardinality directly (no binary)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ThetaSketchDistinctAgg {
    name: String,
    signature: Signature,
}

impl ThetaSketchDistinctAgg {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ThetaSketchDistinctAgg {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { &self.name }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }

    fn accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ThetaDistinctAccumulator { sketch: ThetaSketch::new() }))
    }

    fn state_fields(&self, _: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new("theta_state", DataType::Binary, true))])
    }
}

#[derive(Debug)]
struct ThetaDistinctAccumulator {
    sketch: ThetaSketch,
}

impl Accumulator for ThetaDistinctAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(col) = values.first() else { return Ok(()) };
        for row in 0..col.len() {
            if col.is_null(row) { continue; }
            let sv = ScalarValue::try_from_array(col.as_ref(), row)?;
            self.sketch.insert(hash_scalar(&sv));
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some(col) = states.first() else { return Ok(()) };
        let bins = col.as_any().downcast_ref::<BinaryArray>()
            .ok_or_else(|| plan_datafusion_err!("theta_distinct merge: expected Binary"))?;
        for row in 0..bins.len() {
            if bins.is_null(row) { continue; }
            let other = ThetaSketch::deserialize(bins.value(row))?;
            self.sketch.merge(&other);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(Some(self.sketch.estimate())))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.sketch.serialize()))])
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.sketch.hashes.len() * 8
    }
}

/// Generic stub aggregate returning empty Binary for unimplemented sketch functions.
/// Accepts variadic arguments of any type, returns BINARY.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SketchAggStub {
    name: String,
    signature: Signature,
}

impl SketchAggStub {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for SketchAggStub {
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
        Ok(DataType::Binary)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(NullBinaryAccumulator))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new("state", DataType::Binary, true))])
    }
}

#[derive(Debug, Default)]
struct NullBinaryAccumulator;

impl Accumulator for NullBinaryAccumulator {
    fn update_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(vec![])))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(vec![]))])
    }

    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        Ok(())
    }
}

/// Generic stub aggregate returning 0i64 for grouping_id and similar integer aggregates.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Int64AggStub {
    name: String,
    signature: Signature,
}

impl Int64AggStub {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::one_of(
                vec![TypeSignature::Nullary, TypeSignature::VariadicAny],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for Int64AggStub {
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
        Ok(DataType::Int64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ZeroInt64Accumulator))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new("state", DataType::Int64, true))])
    }
}

#[derive(Debug, Default)]
struct ZeroInt64Accumulator;

impl Accumulator for ZeroInt64Accumulator {
    fn update_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(0)))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(0))])
    }

    fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
        Ok(())
    }
}
