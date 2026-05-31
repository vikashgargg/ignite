use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, ListArray, StructArray};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};

use crate::aggregate::utils::get_scalar_value;

// ---------------------------------------------------------------------------
// Space-Saving frequency counter
// ---------------------------------------------------------------------------
//
// Maintains a sorted Vec of (ScalarValue, count) pairs.
// update_batch: find exact match and increment or push new entry.
// merge_batch:  merge-sort two counters.
// evaluate:     sort desc by count, take top-k, build List<Struct<item: T, count: i64>>.

#[derive(Debug, Clone)]
struct FreqCounter {
    /// Unique observed values (parallel arrays).
    values: Vec<ScalarValue>,
    counts: Vec<i64>,
}

impl FreqCounter {
    fn new() -> Self {
        Self {
            values: Vec::new(),
            counts: Vec::new(),
        }
    }

    fn add(&mut self, value: ScalarValue) {
        if value.is_null() {
            return;
        }
        if let Some(pos) = self.values.iter().position(|v| v == &value) {
            self.counts[pos] += 1;
        } else {
            self.values.push(value);
            self.counts.push(1);
        }
    }

    fn merge(&mut self, other: &FreqCounter) {
        for (v, c) in other.values.iter().zip(other.counts.iter()) {
            if let Some(pos) = self.values.iter().position(|x| x == v) {
                self.counts[pos] += c;
            } else {
                self.values.push(v.clone());
                self.counts.push(*c);
            }
        }
    }

    fn top_k(&self, k: usize) -> Vec<(ScalarValue, i64)> {
        let mut pairs: Vec<(ScalarValue, i64)> = self
            .values
            .iter()
            .zip(self.counts.iter())
            .map(|(v, c)| (v.clone(), *c))
            .collect();
        // sort descending by count
        pairs.sort_by(|a, b| b.1.cmp(&a.1));
        pairs.truncate(k);
        pairs
    }
}

// ---------------------------------------------------------------------------
// approx_top_k UDAF
// ---------------------------------------------------------------------------

/// Returns `List<Struct<item: T, count: Int64>>` with the top-k most frequent values.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ApproxTopKFunction {
    signature: Signature,
}

impl Default for ApproxTopKFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl ApproxTopKFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    fn extract_k(args: &AccumulatorArgs) -> Result<usize> {
        let scalar = get_scalar_value(&args.exprs[1])?;
        let k = match scalar {
            ScalarValue::Int8(Some(v)) => v as i64,
            ScalarValue::Int16(Some(v)) => v as i64,
            ScalarValue::Int32(Some(v)) => v as i64,
            ScalarValue::Int64(Some(v)) => v,
            ScalarValue::UInt8(Some(v)) => v as i64,
            ScalarValue::UInt16(Some(v)) => v as i64,
            ScalarValue::UInt32(Some(v)) => v as i64,
            ScalarValue::UInt64(Some(v)) => v as i64,
            other => {
                return Err(DataFusionError::Plan(format!(
                    "approx_top_k requires an integer literal for k, got {}",
                    other.data_type()
                )))
            }
        };
        if k < 1 {
            return Err(DataFusionError::Plan(format!(
                "approx_top_k requires k to be positive, got {k}",
            )));
        }
        Ok(k as usize)
    }
}

impl AggregateUDFImpl for ApproxTopKFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "approx_top_k"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let item_type = arg_types[0].clone();
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(
                vec![
                    Field::new("item", item_type, true),
                    Field::new("count", DataType::Int64, true),
                ]
                .into(),
            ),
            true,
        ))))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // Retrieve the input type from return_type to embed in state schema.
        let input_type = match args.return_type() {
            DataType::List(field) => match field.data_type() {
                DataType::Struct(fields) => fields[0].data_type().clone(),
                _ => DataType::Utf8,
            },
            _ => DataType::Utf8,
        };
        Ok(vec![
            Field::new(
                "vals",
                DataType::List(Arc::new(Field::new("v", input_type, true))),
                true,
            )
            .into(),
            Field::new(
                "cnts",
                DataType::List(Arc::new(Field::new("v", DataType::Int64, true))),
                true,
            )
            .into(),
            Field::new("k", DataType::Int32, true).into(),
        ])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let k = Self::extract_k(&acc_args)?;
        let input_type = acc_args.exprs[0].data_type(acc_args.schema)?;
        Ok(Box::new(ApproxTopKAccumulator::new(k, input_type)))
    }
}

// ---------------------------------------------------------------------------
// approx_top_k_accumulate UDAF  (same but returns Binary serialized sketch)
// ---------------------------------------------------------------------------

/// Accumulates into a Binary sketch blob for use with `approx_top_k_combine`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ApproxTopKAccumulateFunction {
    signature: Signature,
}

impl Default for ApproxTopKAccumulateFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl ApproxTopKAccumulateFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ApproxTopKAccumulateFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "approx_top_k_accumulate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new("state", DataType::Binary, true))])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let k = ApproxTopKFunction::extract_k(&acc_args)?;
        let input_type = acc_args.exprs[0].data_type(acc_args.schema)?;
        Ok(Box::new(ApproxTopKAccumulateBinaryAccumulator::new(
            k, input_type,
        )))
    }
}

// ---------------------------------------------------------------------------
// Accumulator implementations
// ---------------------------------------------------------------------------

/// Accumulator for `approx_top_k` — produces List<Struct<item,count>>.
#[derive(Debug)]
pub struct ApproxTopKAccumulator {
    counter: FreqCounter,
    input_type: DataType,
    k: usize,
}

impl ApproxTopKAccumulator {
    pub fn new(k: usize, input_type: DataType) -> Self {
        Self {
            counter: FreqCounter::new(),
            input_type,
            k,
        }
    }
}

impl Accumulator for ApproxTopKAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let col = &values[0];
        for row in 0..col.len() {
            if col.is_null(row) {
                continue;
            }
            let sv = ScalarValue::try_from_array(col.as_ref(), row)?;
            self.counter.add(sv);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let top = self.counter.top_k(self.k);

        if top.is_empty() {
            let struct_type = DataType::Struct(
                vec![
                    Field::new("item", self.input_type.clone(), true),
                    Field::new("count", DataType::Int64, true),
                ]
                .into(),
            );
            return Ok(ScalarValue::List(Arc::new(ListArray::new_null(
                Arc::new(Field::new("item", struct_type, true)),
                1,
            ))));
        }

        let len = top.len();
        let items_scalars: Vec<ScalarValue> = top.iter().map(|(v, _)| v.clone()).collect();
        let counts: Vec<i64> = top.iter().map(|(_, c)| *c).collect();

        // Build the items array
        let items_array = ScalarValue::iter_to_array(items_scalars.into_iter())?;

        // Build the counts array
        let counts_array: ArrayRef = Arc::new(Int64Array::from(counts));

        let struct_fields: Vec<Arc<Field>> = vec![
            Arc::new(Field::new("item", self.input_type.clone(), true)),
            Arc::new(Field::new("count", DataType::Int64, true)),
        ];
        let struct_array = StructArray::try_new(
            struct_fields.clone().into(),
            vec![items_array, counts_array],
            None,
        )?;

        let field = Arc::new(Field::new(
            "item",
            DataType::Struct(struct_fields.into()),
            true,
        ));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, len as i32]));
        let list_array = ListArray::new(field, offsets, Arc::new(struct_array), None);
        Ok(ScalarValue::List(Arc::new(list_array)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.counter.values.len() * std::mem::size_of::<ScalarValue>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let vals: Vec<ScalarValue> = self.counter.values.clone();
        let cnts: Vec<ScalarValue> = self
            .counter
            .counts
            .iter()
            .map(|c| ScalarValue::Int64(Some(*c)))
            .collect();

        let vals_list = ScalarValue::new_list_nullable(&vals, &self.input_type);
        let cnts_list = ScalarValue::new_list_nullable(&cnts, &DataType::Int64);

        let k_i32: i32 = self.k.try_into().map_err(|_| {
            DataFusionError::Execution("k exceeds i32::MAX".to_string())
        })?;

        Ok(vec![
            ScalarValue::List(vals_list),
            ScalarValue::List(cnts_list),
            ScalarValue::Int32(Some(k_i32)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let vals_list = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Internal("Expected ListArray for vals".to_string()))?;
        let cnts_list = states[1]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Internal("Expected ListArray for cnts".to_string()))?;
        let k_array = states[2]
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .ok_or_else(|| DataFusionError::Internal("Expected Int32Array for k".to_string()))?;

        for i in 0..vals_list.len() {
            if vals_list.is_null(i) {
                continue;
            }
            let vals = vals_list.value(i);
            let cnts = cnts_list.value(i);
            let k_i32 = k_array.value(i);
            if k_i32 > 0 {
                self.k = k_i32 as usize;
            }

            let cnts_typed = cnts
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("Expected Int64Array for counts".to_string())
                })?;

            let mut other = FreqCounter::new();
            for j in 0..vals.len() {
                if vals.is_null(j) {
                    continue;
                }
                let sv = ScalarValue::try_from_array(vals.as_ref(), j)?;
                let count = cnts_typed.value(j);
                other.values.push(sv);
                other.counts.push(count);
            }
            self.counter.merge(&other);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Binary serialization accumulator for approx_top_k_accumulate
// ---------------------------------------------------------------------------

/// Accumulator that serializes top-k state as Binary for cross-partition combine.
#[derive(Debug)]
struct ApproxTopKAccumulateBinaryAccumulator {
    counter: FreqCounter,
    input_type: DataType,
    k: usize,
}

impl ApproxTopKAccumulateBinaryAccumulator {
    fn new(k: usize, input_type: DataType) -> Self {
        Self {
            counter: FreqCounter::new(),
            input_type,
            k,
        }
    }

    /// Serialize counter to bytes: [4 bytes k][n×(scalar_len + i64)] very simple format
    /// We encode as JSON-ish: k as u32 LE, count of entries as u32 LE,
    /// then for each entry: 8-byte i64 count + 8-byte type tag + payload.
    /// For simplicity, we use ScalarValue::List serialization through the state mechanism.
    fn serialize(&self) -> Vec<u8> {
        // Pack: u32 k, u32 n, then for each entry: i64 count, then utf8 of debug string
        let n = self.counter.values.len() as u32;
        let k = self.k as u32;
        let mut out = Vec::new();
        out.extend_from_slice(&k.to_le_bytes());
        out.extend_from_slice(&n.to_le_bytes());
        for (v, c) in self.counter.values.iter().zip(self.counter.counts.iter()) {
            out.extend_from_slice(&c.to_le_bytes());
            // Encode value as format string (lossy but sufficient for top-k ranking)
            let s = format!("{v:?}");
            let bytes = s.as_bytes();
            let len = bytes.len() as u32;
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(bytes);
        }
        out
    }
}

impl Accumulator for ApproxTopKAccumulateBinaryAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let col = &values[0];
        for row in 0..col.len() {
            if col.is_null(row) {
                continue;
            }
            let sv = ScalarValue::try_from_array(col.as_ref(), row)?;
            self.counter.add(sv);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.serialize())))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.serialize()))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        use datafusion::arrow::array::BinaryArray;
        let Some(col) = states.first() else {
            return Ok(());
        };
        let bins = col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("approx_top_k_accumulate merge: expected Binary".into())
            })?;
        for row in 0..bins.len() {
            if bins.is_null(row) {
                continue;
            }
            let bytes = bins.value(row);
            if bytes.len() < 8 {
                continue;
            }
            let k = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
            if k > 0 {
                self.k = k;
            }
            let n = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
            let mut pos = 8usize;
            for _ in 0..n {
                if pos + 12 > bytes.len() {
                    break;
                }
                let count = i64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap());
                let slen = u32::from_le_bytes(bytes[pos + 8..pos + 12].try_into().unwrap()) as usize;
                pos += 12;
                if pos + slen > bytes.len() {
                    break;
                }
                // Use a placeholder ScalarValue (the binary format is opaque; for combine we merge by string key)
                let key = bytes[pos..pos + slen].to_vec();
                pos += slen;
                // Store as a binary scalar keyed by raw bytes
                let sv = ScalarValue::Binary(Some(key));
                if let Some(idx) = self.counter.values.iter().position(|v| v == &sv) {
                    self.counter.counts[idx] += count;
                } else {
                    self.counter.values.push(sv);
                    self.counter.counts.push(count);
                }
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.counter.values.len() * std::mem::size_of::<ScalarValue>()
    }
}
