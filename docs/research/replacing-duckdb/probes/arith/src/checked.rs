//! Overflow-refusing SUM / AVG aggregates and + - * scalars for DataFusion.
//!
//! The aggregates accumulate in `I320` regardless of input type, so a partial
//! sum can never overflow in practice; only the final narrowing to the declared
//! output type can, and that is an error, never NULL and never a wrapped value.

use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::compute::kernels::numeric;
use datafusion::arrow::datatypes::*;
use datafusion::common::{exec_err, plan_err, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, EmitTo, GroupsAccumulator,
    Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::logical_expr::type_coercion::binary::BinaryTypeCoercer;

use crate::i320::I320;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Mode {
    /// Exact sum in the input's own family: Int64, Decimal128(38,s), Decimal256(76,s); text in → Decimal256(76,0).
    Sum,
    /// Exact sum as canonical decimal text. Never overflows short of 320 bits.
    SumText,
    /// Exact sum divided by count, four extra decimal places, truncated.
    Avg,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CheckedAgg {
    sig: Signature,
    mode: Mode,
}

impl CheckedAgg {
    pub fn new(mode: Mode) -> Self {
        Self { sig: Signature::user_defined(Volatility::Immutable), mode }
    }
    pub fn udaf(mode: Mode) -> Arc<AggregateUDF> {
        Arc::new(AggregateUDF::from(Self::new(mode)))
    }
}

pub fn supported_input(t: &DataType) -> bool {
    use DataType::*;
    matches!(
        t,
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32
            | Decimal128(_, _) | Decimal256(_, _) | Utf8 | LargeUtf8 | Utf8View
    )
}

fn output_type(mode: Mode, input: &DataType) -> Result<DataType> {
    use DataType::*;
    Ok(match (mode, input) {
        (Mode::SumText, _) => Utf8,
        (Mode::Sum, Int64) => Int64,
        (Mode::Sum, Decimal128(_, s)) => Decimal128(38, *s),
        (Mode::Sum, Decimal256(_, s)) => Decimal256(76, *s),
        (Mode::Sum, Utf8) => Decimal256(76, 0),
        (Mode::Avg, Int64) => Decimal128(38, 4),
        (Mode::Avg, Decimal128(_, s)) => Decimal128(38, (*s + 4).min(38)),
        (Mode::Avg, Decimal256(_, s)) => Decimal256(76, (*s + 4).min(76)),
        (Mode::Avg, Utf8) => Decimal256(76, 4),
        _ => return plan_err!("checked aggregate does not support input type {input}"),
    })
}

impl AggregateUDFImpl for CheckedAgg {
    fn name(&self) -> &str {
        match self.mode {
            Mode::Sum => "checked_sum",
            Mode::SumText => "checked_sum_text",
            Mode::Avg => "checked_avg",
        }
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        use DataType::*;
        if arg_types.len() != 1 {
            return plan_err!("{} takes one argument", self.name());
        }
        Ok(vec![match &arg_types[0] {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 => Int64,
            t @ (Decimal128(_, _) | Decimal256(_, _)) => t.clone(),
            Utf8 | LargeUtf8 | Utf8View => Utf8,
            t => return plan_err!("{} does not support input type {t}", self.name()),
        }])
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        output_type(self.mode, &arg_types[0])
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(format!("{}[sum320]", args.name), DataType::FixedSizeBinary(40), true)),
            Arc::new(Field::new(format!("{}[count]", args.name), DataType::UInt64, true)),
        ])
    }
    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(Single(self.groups(args)?)))
    }
    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }
    fn create_groups_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(self.groups(args)?))
    }
}

impl CheckedAgg {
    fn groups(&self, args: AccumulatorArgs) -> Result<Groups> {
        let in_ty = args.exprs[0].data_type(args.schema)?;
        let out_ty = args.return_field.data_type().clone();
        Ok(Groups { mode: self.mode, in_ty, out_ty, sums: vec![], counts: vec![] })
    }
}

pub struct Groups {
    mode: Mode,
    in_ty: DataType,
    out_ty: DataType,
    sums: Vec<I320>,
    counts: Vec<u64>,
}

impl Groups {
    fn grow(&mut self, n: usize) {
        if self.sums.len() < n {
            self.sums.resize(n, I320::ZERO);
            self.counts.resize(n, 0);
        }
    }

    fn add(&mut self, g: usize, v: I320) -> Result<()> {
        match self.sums[g].checked_add(v) {
            Some(s) => {
                self.sums[g] = s;
                self.counts[g] += 1;
                Ok(())
            }
            None => exec_err!("{}: running total left 320 bits", name_of(self.mode)),
        }
    }

    fn ingest<A: Array, F: Fn(&A, usize) -> Result<I320>>(
        &mut self,
        arr: &A,
        get: F,
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<()> {
        for (i, &g) in group_indices.iter().enumerate() {
            if arr.is_null(i) {
                continue;
            }
            if let Some(f) = opt_filter {
                if !(f.is_valid(i) && f.value(i)) {
                    continue;
                }
            }
            let v = get(arr, i)?;
            self.add(g, v)?;
        }
        Ok(())
    }

    fn finish_one(&self, sum: I320, count: u64) -> Result<I320> {
        match self.mode {
            Mode::Sum | Mode::SumText => Ok(sum),
            Mode::Avg => {
                let in_scale = match &self.in_ty {
                    DataType::Decimal128(_, s) | DataType::Decimal256(_, s) => *s,
                    _ => 0,
                };
                let out_scale = match &self.out_ty {
                    DataType::Decimal128(_, s) | DataType::Decimal256(_, s) => *s,
                    _ => 0,
                };
                let scale_up = 10u64.pow((out_scale - in_scale) as u32);
                let scaled = sum
                    .checked_mul_small(scale_up)
                    .ok_or_else(|| datafusion::common::DataFusionError::Execution("checked_avg: scaled sum left 320 bits".into()))?;
                Ok(scaled.div_small(count))
            }
        }
    }

    fn build(&self, sums: &[I320], counts: &[u64]) -> Result<ArrayRef> {
        let name = name_of(self.mode);
        let n = sums.len();
        macro_rules! overflow {
            ($v:expr) => {
                exec_err!("{name} overflow: exact result {} does not fit {}", $v, self.out_ty)
            };
        }
        Ok(match &self.out_ty {
            DataType::Utf8 => {
                let mut b = StringBuilder::new();
                for i in 0..n {
                    if counts[i] == 0 {
                        b.append_null();
                    } else {
                        b.append_value(self.finish_one(sums[i], counts[i])?.to_decimal_string());
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Int64 => {
                let mut b = Int64Builder::with_capacity(n);
                for i in 0..n {
                    if counts[i] == 0 {
                        b.append_null();
                    } else {
                        let v = self.finish_one(sums[i], counts[i])?;
                        match v.to_i64() {
                            Some(x) => b.append_value(x),
                            None => return overflow!(v),
                        }
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Decimal128(p, s) => {
                let mut b = Decimal128Builder::with_capacity(n);
                for i in 0..n {
                    if counts[i] == 0 {
                        b.append_null();
                    } else {
                        let v = self.finish_one(sums[i], counts[i])?;
                        match v.to_i128() {
                            Some(x) if Decimal128Type::validate_decimal_precision(x, *p, *s).is_ok() => b.append_value(x),
                            _ => return overflow!(v),
                        }
                    }
                }
                Arc::new(b.finish().with_precision_and_scale(*p, *s)?)
            }
            DataType::Decimal256(p, s) => {
                let mut b = Decimal256Builder::with_capacity(n);
                for i in 0..n {
                    if counts[i] == 0 {
                        b.append_null();
                    } else {
                        let v = self.finish_one(sums[i], counts[i])?;
                        match v.to_i256() {
                            Some(x) if Decimal256Type::validate_decimal_precision(x, *p, *s).is_ok() => b.append_value(x),
                            _ => return overflow!(v),
                        }
                    }
                }
                Arc::new(b.finish().with_precision_and_scale(*p, *s)?)
            }
            t => return exec_err!("{name}: unsupported output type {t}"),
        })
    }
}

fn name_of(mode: Mode) -> &'static str {
    match mode {
        Mode::Sum => "checked_sum",
        Mode::SumText => "checked_sum_text",
        Mode::Avg => "checked_avg",
    }
}

impl GroupsAccumulator for Groups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.grow(total_num_groups);
        let a = &values[0];
        match a.data_type() {
            DataType::Int64 => {
                let a = a.as_primitive::<Int64Type>();
                self.ingest(a, |a, i| Ok(I320::from_i128(a.value(i) as i128)), group_indices, opt_filter)
            }
            DataType::Decimal128(_, _) => {
                let a = a.as_primitive::<Decimal128Type>();
                self.ingest(a, |a, i| Ok(I320::from_i128(a.value(i))), group_indices, opt_filter)
            }
            DataType::Decimal256(_, _) => {
                let a = a.as_primitive::<Decimal256Type>();
                self.ingest(a, |a, i| Ok(I320::from_i256(a.value(i))), group_indices, opt_filter)
            }
            DataType::Utf8 => {
                let a = a.as_string::<i32>();
                self.ingest(
                    a,
                    |a, i| I320::parse_canonical(a.value(i)).map_err(|e| datafusion::common::DataFusionError::Execution(format!("checked aggregate: {e}"))),
                    group_indices,
                    opt_filter,
                )
            }
            t => exec_err!("checked aggregate: unsupported input {t}"),
        }
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let sums = emit_to.take_needed(&mut self.sums);
        let counts = emit_to.take_needed(&mut self.counts);
        self.build(&sums, &counts)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let sums = emit_to.take_needed(&mut self.sums);
        let counts = emit_to.take_needed(&mut self.counts);
        let mut b = FixedSizeBinaryBuilder::with_capacity(sums.len(), 40);
        for s in &sums {
            b.append_value(s.to_le_bytes())?;
        }
        Ok(vec![Arc::new(b.finish()), Arc::new(UInt64Array::from(counts))])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.grow(total_num_groups);
        let sums = values[0].as_fixed_size_binary();
        let counts = values[1].as_primitive::<UInt64Type>();
        for (i, &g) in group_indices.iter().enumerate() {
            if sums.is_null(i) {
                continue;
            }
            let v = I320::from_le_bytes(sums.value(i));
            match self.sums[g].checked_add(v) {
                Some(s) => self.sums[g] = s,
                None => return exec_err!("{}: merged total left 320 bits", name_of(self.mode)),
            }
            self.counts[g] += counts.value(i);
        }
        Ok(())
    }

    fn convert_to_state(&self, values: &[ArrayRef], opt_filter: Option<&BooleanArray>) -> Result<Vec<ArrayRef>> {
        let n = values[0].len();
        let mut tmp = Groups { mode: self.mode, in_ty: self.in_ty.clone(), out_ty: self.out_ty.clone(), sums: vec![], counts: vec![] };
        let idx: Vec<usize> = (0..n).collect();
        tmp.update_batch(values, &idx, opt_filter, n)?;
        tmp.state(EmitTo::All)
    }

    fn size(&self) -> usize {
        self.sums.capacity() * std::mem::size_of::<I320>() + self.counts.capacity() * 8
    }
}

/// The single-group view of the same state, for the non-grouped and window paths.
pub struct Single(Groups);

impl std::fmt::Debug for Single {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CheckedAgg::Single({:?})", self.0.mode)
    }
}

impl Accumulator for Single {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let idx = vec![0usize; values[0].len()];
        self.0.update_batch(values, &idx, None, 1)
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.0.grow(1);
        let a = self.0.build(&self.0.sums.clone(), &self.0.counts.clone())?;
        ScalarValue::try_from_array(&a, 0)
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.0.size()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.0.grow(1);
        let sums = vec![self.0.sums[0]];
        let counts = vec![self.0.counts[0]];
        let mut b = FixedSizeBinaryBuilder::with_capacity(1, 40);
        b.append_value(sums[0].to_le_bytes())?;
        Ok(vec![
            ScalarValue::try_from_array(&(Arc::new(b.finish()) as ArrayRef), 0)?,
            ScalarValue::UInt64(Some(counts[0])),
        ])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let idx = vec![0usize; states[0].len()];
        self.0.merge_batch(states, &idx, 1)
    }
}

/// `checked_add`, `checked_sub`, `checked_mul`, `checked_neg`: arrow's checked
/// kernels plus a decimal precision check the kernels do not do themselves.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CheckedBinary {
    sig: Signature,
    /// `None` is unary negation.
    op: Option<Operator>,
}

impl CheckedBinary {
    pub fn udf(op: Option<Operator>) -> Arc<ScalarUDF> {
        let n = if op.is_some() { 2 } else { 1 };
        Arc::new(ScalarUDF::from(Self { sig: Signature::any(n, Volatility::Immutable), op }))
    }
}

fn validate_precision(a: &ArrayRef) -> Result<()> {
    match a.data_type() {
        DataType::Decimal128(p, _) => a.as_primitive::<Decimal128Type>().validate_decimal_precision(*p)?,
        DataType::Decimal256(p, _) => a.as_primitive::<Decimal256Type>().validate_decimal_precision(*p)?,
        _ => {}
    }
    Ok(())
}

impl ScalarUDFImpl for CheckedBinary {
    fn name(&self) -> &str {
        match self.op {
            Some(Operator::Plus) => "checked_add",
            Some(Operator::Minus) => "checked_sub",
            Some(Operator::Multiply) => "checked_mul",
            _ => "checked_neg",
        }
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match self.op {
            None => Ok(arg_types[0].clone()),
            Some(op) => BinaryTypeCoercer::new(&arg_types[0], &op, &arg_types[1]).get_result_type(),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let all_scalar = args.args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let out: ArrayRef = match self.op {
            Some(Operator::Plus) => numeric::add(&arrays[0], &arrays[1])?,
            Some(Operator::Minus) => numeric::sub(&arrays[0], &arrays[1])?,
            Some(Operator::Multiply) => numeric::mul(&arrays[0], &arrays[1])?,
            _ => numeric::neg(&arrays[0])?,
        };
        validate_precision(&out)?;
        let want = args.return_field.data_type();
        let out = if out.data_type() != want {
            let opts = datafusion::arrow::compute::CastOptions { safe: false, ..Default::default() };
            let c = datafusion::arrow::compute::cast_with_options(&out, want, &opts)?;
            validate_precision(&c)?;
            c
        } else {
            out
        };
        if all_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
        } else {
            Ok(ColumnarValue::Array(out))
        }
    }
}

pub fn register_all(ctx: &datafusion::prelude::SessionContext) {
    ctx.register_udaf(AggregateUDF::from(CheckedAgg::new(Mode::Sum)));
    ctx.register_udaf(AggregateUDF::from(CheckedAgg::new(Mode::SumText)));
    ctx.register_udaf(AggregateUDF::from(CheckedAgg::new(Mode::Avg)));
    for op in [Some(Operator::Plus), Some(Operator::Minus), Some(Operator::Multiply), None] {
        ctx.register_udf(Arc::unwrap_or_clone(CheckedBinary::udf(op)));
    }
}
