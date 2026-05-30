use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, TypeSignature, Volatility};

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
