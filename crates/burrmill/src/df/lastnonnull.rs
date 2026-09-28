//! `last_value(x) IGNORE NULLS` over a frame from the partition's start to the current row, as a
//! running aggregate that keeps the last non-null value.
//!
//! DataFusion 55 evaluates that window by rescanning its list of non-null positions for every row,
//! so it is quadratic in a partition's non-null count: 222 ms at 50k rows, 1,044 ms at 100k, and
//! 39 s on the allocations ledger, where DuckDB takes 2-6 s.

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::tree_node::Transformed;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::expr::{NullTreatment, WindowFunction, WindowFunctionDefinition};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Signature, Volatility, WindowFrameBound,
    WindowFrameUnits,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LastNonNull {
    sig: Signature,
}

impl LastNonNull {
    pub fn udaf() -> Arc<AggregateUDF> {
        Arc::new(AggregateUDF::from(Self {
            sig: Signature::any(1, Volatility::Immutable),
        }))
    }
}

impl AggregateUDFImpl for LastNonNull {
    fn name(&self) -> &str {
        "burrmill_last_non_null"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(
            args.input_fields[0]
                .as_ref()
                .clone()
                .with_name(format!("{}[last]", args.name))
                .with_nullable(true),
        )])
    }
    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(Last(ScalarValue::try_from(
            args.return_field.data_type(),
        )?)))
    }
}

#[derive(Debug)]
struct Last(ScalarValue);

impl Accumulator for Last {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let v = &values[0];
        if let Some(i) = (0..v.len()).rev().find(|&i| v.is_valid(i)) {
            self.0 = ScalarValue::try_from_array(v, i)?;
        }
        Ok(())
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.0.clone()])
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.0.clone())
    }
    fn size(&self) -> usize {
        size_of_val(self) + self.0.size() - size_of_val(&self.0)
    }
}

/// Only where the two are the same function: one argument, `IGNORE NULLS`, and a `ROWS` frame from
/// the partition's start to the current row. `RANGE` would take the current row's peers too.
pub fn rewrite(mut wf: WindowFunction) -> Transformed<Expr> {
    let f = &wf.params.window_frame;
    let fits = matches!(&wf.fun, WindowFunctionDefinition::WindowUDF(u) if u.name() == "last_value")
        && wf.params.args.len() == 1
        && wf.params.null_treatment == Some(NullTreatment::IgnoreNulls)
        && !wf.params.distinct
        && f.units == WindowFrameUnits::Rows
        && matches!(&f.start_bound, WindowFrameBound::Preceding(v) if v.is_null())
        && matches!(f.end_bound, WindowFrameBound::CurrentRow);
    if !fits {
        return Transformed::no(Expr::WindowFunction(Box::new(wf)));
    }
    wf.fun = WindowFunctionDefinition::AggregateUDF(LastNonNull::udaf());
    wf.params.null_treatment = None;
    Transformed::yes(Expr::WindowFunction(Box::new(wf)))
}
