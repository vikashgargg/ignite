use std::fmt::{Debug, Formatter};
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowNumericType, AsArray, Float64Array, ListArray, PrimitiveArray,
};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, Field, FieldRef, Float16Type, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type,
    UInt8Type,
};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, GroupsAccumulator, Signature, Volatility,
};

use crate::aggregate::percentile_disc_groups::PercentileDiscGroupsAccumulator;
use crate::aggregate::utils::{
    calculate_percentile_disc, cast_to_type, extract_percentile_literal, extract_percentiles_array,
    percentile_disc_index,
};

macro_rules! dispatch_numeric_type {
    ($input_dt:expr, $helper:ident, $err_msg:expr) => {
        match &$input_dt {
            DataType::Int8 => $helper!(Int8Type, $input_dt),
            DataType::Int16 => $helper!(Int16Type, $input_dt),
            DataType::Int32 => $helper!(Int32Type, $input_dt),
            DataType::Int64 => $helper!(Int64Type, $input_dt),
            DataType::UInt8 => $helper!(UInt8Type, $input_dt),
            DataType::UInt16 => $helper!(UInt16Type, $input_dt),
            DataType::UInt32 => $helper!(UInt32Type, $input_dt),
            DataType::UInt64 => $helper!(UInt64Type, $input_dt),
            DataType::Float16 => $helper!(Float16Type, $input_dt),
            DataType::Float32 => $helper!(Float32Type, $input_dt),
            DataType::Float64 => $helper!(Float64Type, $input_dt),
            DataType::Decimal128(_, _) => $helper!(Decimal128Type, $input_dt),
            DataType::Decimal256(_, _) => $helper!(Decimal256Type, $input_dt),
            _ => Err(DataFusionError::NotImplemented(format!(
                "{} not supported for {}",
                $err_msg, $input_dt,
            ))),
        }
    };
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PercentileDisc {
    signature: Signature,
}

impl Default for PercentileDisc {
    fn default() -> Self {
        Self::new()
    }
}

impl PercentileDisc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    fn is_descending(args: &AccumulatorArgs) -> bool {
        args.order_bys
            .first()
            .map(|sort_expr| sort_expr.options.descending)
            .unwrap_or(false)
    }

    fn resolve_percentile(args: &AccumulatorArgs) -> Result<f64> {
        extract_percentile_literal(&args.exprs[1])
    }

    fn try_resolve_percentiles_array(args: &AccumulatorArgs) -> Result<Option<Vec<f64>>> {
        if !matches!(args.exprs[1].data_type(args.schema)?, DataType::List(_)) {
            return Ok(None);
        }
        let percentiles = extract_percentiles_array(&args.exprs[1])?;
        Ok(Some(percentiles))
    }
}

impl AggregateUDFImpl for PercentileDisc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "percentile_disc"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn supports_within_group_clause(&self) -> bool {
        true
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(DataFusionError::Plan(format!(
                "percentile_disc expects 2 arguments, got {}",
                arg_types.len()
            )));
        }
        let order_by = match &arg_types[0] {
            dt if dt.is_numeric() => DataType::Float64,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Float64,
            other => {
                return Err(DataFusionError::Plan(format!(
                    "percentile_disc: ORDER BY column must be numeric or string, got {other}"
                )));
            }
        };
        let percentile = match &arg_types[1] {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => {
                let elem = field.data_type();
                if !elem.is_numeric() && !matches!(elem, DataType::Null) {
                    return Err(DataFusionError::Plan(format!(
                        "percentile_disc: percentile array elements must be numeric, got {elem}"
                    )));
                }
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
            }
            dt if dt.is_numeric() => DataType::Float64,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Float64,
            other => {
                return Err(DataFusionError::Plan(format!(
                    "percentile_disc: percentile must be numeric, got {other}"
                )));
            }
        };
        Ok(vec![order_by, percentile])
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if matches!(arg_types.get(1), Some(DataType::List(_))) {
            return Ok(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Float64,
                true,
            ))));
        }
        Ok(arg_types[0].clone())
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let is_array_percentiles = matches!(
            args.input_fields.get(1).map(|f| f.data_type()),
            Some(DataType::List(_))
        );
        let value_type = if is_array_percentiles {
            DataType::Float64
        } else {
            args.input_fields[0].data_type().clone()
        };
        let field = Field::new_list_field(value_type, true);
        let state_name = if args.is_distinct {
            "distinct_percentile_disc"
        } else {
            "percentile_disc"
        };

        Ok(vec![Field::new(
            format_state_name(args.name, state_name),
            DataType::List(Arc::new(field)),
            true,
        )
        .into()])
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.is_distinct {
            return Err(DataFusionError::Plan(
                "percentile_disc does not support DISTINCT with WITHIN GROUP".into(),
            ));
        }

        if let Some(percentiles) = Self::try_resolve_percentiles_array(&args)? {
            return Ok(Box::new(MultiPercentileDiscAccumulator {
                all_values: vec![],
                percentiles,
                descending: Self::is_descending(&args),
            }));
        }

        let percentile = Self::resolve_percentile(&args)?;
        let descending = Self::is_descending(&args);

        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                Ok(Box::new(PercentileDiscAccumulator::<$t> {
                    data_type: $dt.clone(),
                    all_values: vec![],
                    percentile,
                    descending,
                }))
            };
        }

        let input_dt = args.exprs[0].data_type(args.schema)?;
        dispatch_numeric_type!(input_dt, helper, "PercentileDiscAccumulator")
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        if args.is_distinct {
            return false;
        }
        let arr_form = args
            .exprs
            .get(1)
            .and_then(|e| e.data_type(args.schema).ok())
            .map(|dt| matches!(dt, DataType::List(_)))
            .unwrap_or(false);
        !arr_form
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let percentile = Self::resolve_percentile(&args)?;
        let descending = Self::is_descending(&args);

        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                Ok(Box::new(PercentileDiscGroupsAccumulator::<$t>::new(
                    $dt, percentile, descending,
                )))
            };
        }

        let input_dt = args.exprs[0].data_type(args.schema)?;
        dispatch_numeric_type!(input_dt, helper, "PercentileDiscGroupsAccumulator")
    }
}

struct PercentileDiscAccumulator<T: ArrowNumericType> {
    data_type: DataType,
    all_values: Vec<T::Native>,
    percentile: f64,
    descending: bool,
}

impl<T: ArrowNumericType> Debug for PercentileDiscAccumulator<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PercentileDiscAccumulator({}, percentile={}, descending={})",
            self.data_type, self.percentile, self.descending
        )
    }
}

impl<T: ArrowNumericType> Accumulator for PercentileDiscAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, self.all_values.len() as i32]));

        let values_array = PrimitiveArray::<T>::new(
            ScalarBuffer::from(std::mem::take(&mut self.all_values)),
            None,
        )
        .with_data_type(self.data_type.clone());

        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(self.data_type.clone(), true)),
            offsets,
            Arc::new(values_array),
            None,
        );

        Ok(vec![ScalarValue::List(Arc::new(list_array))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values_array = cast_to_type(&values[0], &self.data_type)?;

        let null_count = values_array.null_count();
        let values = values_array.as_primitive::<T>();
        self.all_values.reserve(values.len() - null_count);
        self.all_values.extend(values.iter().flatten());
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let array = states[0].as_list::<i32>();
        for v in array.iter().flatten() {
            self.update_batch(&[v])?
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let d = std::mem::take(&mut self.all_values);
        let value = calculate_percentile_disc::<T>(d, self.percentile, self.descending);
        ScalarValue::new_primitive::<T>(value, &self.data_type)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.all_values.capacity() * size_of::<T::Native>()
    }
}

struct MultiPercentileDiscAccumulator {
    all_values: Vec<f64>,
    percentiles: Vec<f64>,
    descending: bool,
}

impl Debug for MultiPercentileDiscAccumulator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MultiPercentileDiscAccumulator(percentiles={:?}, len={}, descending={})",
            self.percentiles,
            self.all_values.len(),
            self.descending
        )
    }
}

impl Accumulator for MultiPercentileDiscAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![
            0_i32,
            self.all_values.len() as i32,
        ]));
        let values_array = Float64Array::new(
            ScalarBuffer::from(std::mem::take(&mut self.all_values)),
            None,
        );
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Float64, true)),
            offsets,
            Arc::new(values_array),
            None,
        );
        Ok(vec![ScalarValue::List(Arc::new(list_array))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values_array = cast_to_type(&values[0], &DataType::Float64)?;
        let null_count = values_array.null_count();
        let values = values_array.as_primitive::<Float64Type>();
        self.all_values.reserve(values.len() - null_count);
        self.all_values.extend(values.iter().flatten());
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let array = states[0].as_list::<i32>();
        for v in array.iter().flatten() {
            self.update_batch(&[v])?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.all_values.is_empty() || self.percentiles.is_empty() {
            let field = Arc::new(Field::new_list_field(DataType::Float64, true));
            return Ok(ScalarValue::List(Arc::new(ListArray::new_null(field, 1))));
        }
        let mut sorted = std::mem::take(&mut self.all_values);
        sorted.sort_unstable_by(|a, b| a.total_cmp(b));
        let len = sorted.len();
        let results: Vec<Option<f64>> = self
            .percentiles
            .iter()
            .map(|&p| Some(sorted[percentile_disc_index(len, p, self.descending)]))
            .collect();
        let values_array = Float64Array::from_iter(results);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, values_array.len() as i32]));
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Float64, true)),
            offsets,
            Arc::new(values_array),
            None,
        );
        Ok(ScalarValue::List(Arc::new(list_array)))
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.all_values.capacity() * size_of::<f64>()
            + self.percentiles.capacity() * size_of::<f64>()
    }
}

pub fn percentile_disc_udaf() -> Arc<datafusion::logical_expr::AggregateUDF> {
    Arc::new(datafusion::logical_expr::AggregateUDF::from(
        PercentileDisc::new(),
    ))
}
