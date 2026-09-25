//! Overflow-refusing SUM / AVG aggregates and `+ - *` / unary `-` scalars (roadmap 6.3).
//!
//! The aggregates accumulate in a [`Wide`] integer sized to the input, so a partial sum cannot
//! overflow and refusal is order-independent: `MAX, +1, -1` answers and `MAX, +1` refuses. Only the
//! final narrowing to the output type can fail, and that is an error naming the exact value, never
//! NULL and never a wrapped number.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Decimal128Builder, Decimal256Builder,
    FixedSizeBinaryBuilder, StringBuilder, UInt64Array,
};
use arrow::compute::kernels::numeric;
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, DecimalType, Field, FieldRef, Int64Type, UInt64Type,
};
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::binary::BinaryTypeCoercer;
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, EmitTo, GroupsAccumulator,
    Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::wide::Wide;

/// `exact_wide`'s output: a 320-bit value, little-endian.
const WIDE_BYTES: i32 = 40;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Mode {
    Sum,
    /// The exact sum as canonical decimal text, which no uint256 total can overflow.
    SumText,
    /// The exact sum over the count, truncated to the output scale.
    Avg,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CheckedAgg {
    sig: Signature,
    mode: Mode,
    /// A fixed output type: the built-in's own for `avg`, the `TRY_CAST` target's sum type for
    /// text that stood behind one. `None` derives it from the input.
    out: Option<DataType>,
}

impl CheckedAgg {
    pub fn udaf(mode: Mode, out: Option<DataType>) -> Arc<AggregateUDF> {
        Arc::new(AggregateUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
            mode,
            out,
        }))
    }
}

pub fn is_text(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

pub fn is_exact(t: &DataType) -> bool {
    t.is_integer()
        || matches!(
            t,
            DataType::Decimal32(..)
                | DataType::Decimal64(..)
                | DataType::Decimal128(..)
                | DataType::Decimal256(..)
        )
}

/// DuckDB's widening: any integer sums to HUGEINT, which here is `Decimal128(38, 0)`.
pub fn sum_type(input: &DataType) -> Option<DataType> {
    use DataType::*;
    Some(match input {
        t if t.is_integer() => Decimal128(38, 0),
        Decimal32(_, s) | Decimal64(_, s) | Decimal128(_, s) => Decimal128(38, *s),
        Decimal256(_, s) => Decimal256(76, *s),
        _ => return None,
    })
}

fn scale_of(t: &DataType) -> i8 {
    match t {
        DataType::Decimal32(_, s)
        | DataType::Decimal64(_, s)
        | DataType::Decimal128(_, s)
        | DataType::Decimal256(_, s) => *s,
        _ => 0,
    }
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
        let [t] = arg_types else {
            return plan_err!("{} takes one argument", self.name());
        };
        Ok(vec![match t {
            Int8 | Int16 | Int32 | Int64 => Int64,
            UInt8 | UInt16 | UInt32 | UInt64 => UInt64,
            Decimal32(p, s) | Decimal64(p, s) => Decimal128(*p, *s),
            Decimal128(..) | Decimal256(..) | Utf8 | LargeUtf8 | Utf8View => t.clone(),
            FixedSizeBinary(WIDE_BYTES) => t.clone(),
            t => return plan_err!("{} has no checked form for {t}", self.name()),
        }])
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if let Some(t) = &self.out {
            return Ok(t.clone());
        }
        match (self.mode, &arg_types[0]) {
            (Mode::SumText, _) => Ok(DataType::Utf8),
            (Mode::Sum, t) if is_text(t) => Ok(DataType::Decimal256(76, 0)),
            (Mode::Sum, t) => match sum_type(t) {
                Some(o) => Ok(o),
                None => plan_err!("checked_sum has no checked form for {t}"),
            },
            (Mode::Avg, t) => plan_err!("checked_avg needs its output type fixed, input {t}"),
        }
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let width = width_for(args.input_fields[0].data_type());
        Ok(vec![
            Arc::new(Field::new(
                format!("{}[sum]", args.name),
                DataType::FixedSizeBinary((width * 8) as i32),
                true,
            )),
            Arc::new(Field::new(
                format!("{}[count]", args.name),
                DataType::UInt64,
                true,
            )),
        ])
    }
    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let spec = self.spec(&args)?;
        Ok(match width_for(&spec.in_ty) {
            2 => Box::new(Single(Groups::<2>::new(spec))),
            3 => Box::new(Single(Groups::<3>::new(spec))),
            _ => Box::new(Single(Groups::<5>::new(spec))),
        })
    }
    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }
    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let spec = self.spec(&args)?;
        Ok(match width_for(&spec.in_ty) {
            2 => Box::new(Groups::<2>::new(spec)),
            3 => Box::new(Groups::<3>::new(spec)),
            _ => Box::new(Groups::<5>::new(spec)),
        })
    }
    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.accumulator(args)
    }
}

/// Limbs enough that 2^63 inputs of this type cannot leave the accumulator.
fn width_for(t: &DataType) -> usize {
    match t {
        t if t.is_integer() => 2,
        DataType::Decimal32(..) | DataType::Decimal64(..) | DataType::Decimal128(..) => 3,
        _ => 5,
    }
}

#[derive(Clone)]
struct Spec {
    mode: Mode,
    name: &'static str,
    in_ty: DataType,
    out_ty: DataType,
    /// Text as DuckDB's `TRY_CAST` reads it, where that is still exactly an integer. Otherwise
    /// canonical only.
    lenient: bool,
}

impl CheckedAgg {
    fn spec(&self, args: &AccumulatorArgs) -> Result<Spec> {
        let name = match self.mode {
            Mode::Sum => "checked_sum",
            Mode::SumText => "checked_sum_text",
            Mode::Avg => "checked_avg",
        };
        if args.is_distinct {
            return plan_err!("{name}(DISTINCT ...) has no checked form");
        }
        Ok(Spec {
            mode: self.mode,
            name,
            in_ty: args.exprs[0].data_type(args.schema)?,
            out_ty: args.return_field.data_type().clone(),
            lenient: false,
        })
    }
}

struct Groups<const N: usize> {
    spec: Spec,
    sums: Vec<Wide<N>>,
    /// Doubles as the null tracker: a group that saw no value answers NULL.
    counts: Vec<u64>,
}

fn exec(msg: String) -> DataFusionError {
    DataFusionError::Execution(msg)
}

impl<const N: usize> Groups<N> {
    fn new(spec: Spec) -> Self {
        Self {
            spec,
            sums: vec![],
            counts: vec![],
        }
    }

    fn grow(&mut self, n: usize) {
        if self.sums.len() < n {
            self.sums.resize(n, Wide::ZERO);
            self.counts.resize(n, 0);
        }
    }

    /// Calls `f(row, value)` for every non-null, unfiltered input row.
    fn each(
        &self,
        a: &ArrayRef,
        filter: Option<&BooleanArray>,
        mut f: impl FnMut(usize, Wide<N>) -> Result<()>,
    ) -> Result<()> {
        let keep = |i: usize| a.is_valid(i) && filter.is_none_or(|f| f.is_valid(i) && f.value(i));
        let name = self.spec.name;
        let lenient = self.spec.lenient;
        let parse = |t: &str| {
            if lenient { Wide::parse_integer(t) } else { Wide::parse_canonical(t) }
        };
        match a.data_type() {
            DataType::Int64 => {
                let a = a.as_primitive::<Int64Type>();
                for i in (0..a.len()).filter(|&i| keep(i)) {
                    f(i, Wide::from_i128(a.value(i) as i128))?;
                }
            }
            DataType::UInt64 => {
                let a = a.as_primitive::<UInt64Type>();
                for i in (0..a.len()).filter(|&i| keep(i)) {
                    f(i, Wide::from_i128(a.value(i) as i128))?;
                }
            }
            DataType::Decimal128(..) => {
                let a = a.as_primitive::<Decimal128Type>();
                for i in (0..a.len()).filter(|&i| keep(i)) {
                    f(i, Wide::from_i128(a.value(i)))?;
                }
            }
            DataType::Decimal256(..) => {
                let a = a.as_primitive::<Decimal256Type>();
                for i in (0..a.len()).filter(|&i| keep(i)) {
                    f(i, Wide::from_i256(a.value(i)))?;
                }
            }
            DataType::Utf8View => {
                let a = a.as_string_view();
                for i in (0..a.len()).filter(|&i| keep(i)) {
                    f(
                        i,
                        parse(a.value(i))
                            .map_err(|e| exec(format!("{name}: {e}")))?,
                    )?;
                }
            }
            DataType::Utf8 => {
                let a = a.as_string::<i32>();
                for i in (0..a.len()).filter(|&i| keep(i)) {
                    f(
                        i,
                        parse(a.value(i))
                            .map_err(|e| exec(format!("{name}: {e}")))?,
                    )?;
                }
            }
            DataType::FixedSizeBinary(WIDE_BYTES) if N == 5 => {
                let a = a.as_fixed_size_binary();
                for i in (0..a.len()).filter(|&i| keep(i)) {
                    f(i, Wide::from_le(a.value(i)))?;
                }
            }
            DataType::LargeUtf8 => {
                let a = a.as_string::<i64>();
                for i in (0..a.len()).filter(|&i| keep(i)) {
                    f(
                        i,
                        parse(a.value(i))
                            .map_err(|e| exec(format!("{name}: {e}")))?,
                    )?;
                }
            }
            t => return exec_err!("{name}: unsupported input {t}"),
        }
        Ok(())
    }

    fn fold(&mut self, g: usize, v: Wide<N>, count: u64) -> Result<()> {
        match self.sums[g].checked_add(v) {
            Some(s) => {
                self.sums[g] = s;
                self.counts[g] += count;
                Ok(())
            }
            None => exec_err!("{}: running total left {} bits", self.spec.name, N * 64),
        }
    }

    fn finish_one(&self, sum: Wide<N>, count: u64) -> Result<Wide<N>> {
        match self.spec.mode {
            Mode::Sum | Mode::SumText => Ok(sum),
            Mode::Avg => {
                let up = scale_of(&self.spec.out_ty) - scale_of(&self.spec.in_ty);
                let scaled = sum
                    .checked_mul_small(10u64.pow(up.max(0) as u32))
                    .ok_or_else(|| {
                        exec(format!(
                            "{}: scaled sum left {} bits",
                            self.spec.name,
                            N * 64
                        ))
                    })?;
                Ok(scaled.div_small(count))
            }
        }
    }

    fn build(&self, sums: &[Wide<N>], counts: &[u64]) -> Result<ArrayRef> {
        let name = self.spec.name;
        let out_ty = &self.spec.out_ty;
        let overflow = |v: Wide<N>| -> Result<ArrayRef> {
            exec_err!("{name} overflow: exact result {v} does not fit {out_ty}")
        };
        let n = sums.len();
        Ok(match out_ty {
            DataType::Utf8 => {
                let mut b = StringBuilder::new();
                for i in 0..n {
                    if counts[i] == 0 {
                        b.append_null();
                    } else {
                        b.append_value(self.finish_one(sums[i], counts[i])?.to_string());
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Decimal128(p, s) => {
                let mut b = Decimal128Builder::with_capacity(n);
                for i in 0..n {
                    if counts[i] == 0 {
                        b.append_null();
                        continue;
                    }
                    let v = self.finish_one(sums[i], counts[i])?;
                    match v.to_i128() {
                        Some(x) if Decimal128Type::is_valid_decimal_precision(x, *p) => {
                            b.append_value(x)
                        }
                        _ => return overflow(v),
                    }
                }
                Arc::new(b.finish().with_precision_and_scale(*p, *s)?)
            }
            DataType::Decimal256(p, s) => {
                let mut b = Decimal256Builder::with_capacity(n);
                for i in 0..n {
                    if counts[i] == 0 {
                        b.append_null();
                        continue;
                    }
                    let v = self.finish_one(sums[i], counts[i])?;
                    match v.to_i256() {
                        Some(x) if Decimal256Type::is_valid_decimal_precision(x, *p) => {
                            b.append_value(x)
                        }
                        _ => return overflow(v),
                    }
                }
                Arc::new(b.finish().with_precision_and_scale(*p, *s)?)
            }
            // DuckDB's `avg` is DOUBLE for every exact input: the exact sum as a DOUBLE, rounded as
            // DuckDB rounds a hugeint, over the count times 10^scale.
            DataType::Float64 if self.spec.mode == Mode::Avg => {
                let unit = 10f64.powi(scale_of(&self.spec.in_ty) as i32);
                let mut b = arrow::array::Float64Builder::with_capacity(n);
                for i in 0..n {
                    if counts[i] == 0 {
                        b.append_null();
                        continue;
                    }
                    let sum = match sums[i].to_i128() {
                        Some(x) => super::doubles::decimal_to_double(x, 38, 0),
                        None => sums[i].to_string().parse::<f64>().unwrap_or(f64::NAN),
                    };
                    b.append_value(sum / (counts[i] as f64 * unit));
                }
                Arc::new(b.finish())
            }
            t => return exec_err!("{name}: unsupported output type {t}"),
        })
    }

    fn encode(sums: &[Wide<N>], counts: Vec<u64>) -> Result<Vec<ArrayRef>> {
        let mut b = FixedSizeBinaryBuilder::with_capacity(sums.len(), Wide::<N>::BYTES as i32);
        let mut buf = vec![0u8; Wide::<N>::BYTES];
        for s in sums {
            s.write_le(&mut buf);
            b.append_value(&buf)?;
        }
        Ok(vec![
            Arc::new(b.finish()),
            Arc::new(UInt64Array::from(counts)),
        ])
    }
}

impl<const N: usize> GroupsAccumulator for Groups<N> {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.grow(total_num_groups);
        let mut sums = std::mem::take(&mut self.sums);
        let mut counts = std::mem::take(&mut self.counts);
        let name = self.spec.name;
        let r = self.each(&values[0], opt_filter, |i, v| {
            let g = group_indices[i];
            sums[g] = sums[g]
                .checked_add(v)
                .ok_or_else(|| exec(format!("{name}: running total left {} bits", N * 64)))?;
            counts[g] += 1;
            Ok(())
        });
        self.sums = sums;
        self.counts = counts;
        r
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let sums = emit_to.take_needed(&mut self.sums);
        let counts = emit_to.take_needed(&mut self.counts);
        self.build(&sums, &counts)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let sums = emit_to.take_needed(&mut self.sums);
        let counts = emit_to.take_needed(&mut self.counts);
        Self::encode(&sums, counts)
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
            if sums.is_valid(i) {
                self.fold(g, Wide::from_le(sums.value(i)), counts.value(i))?;
            }
        }
        Ok(())
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let n = values[0].len();
        let mut sums = vec![Wide::<N>::ZERO; n];
        let mut counts = vec![0u64; n];
        self.each(&values[0], opt_filter, |i, v| {
            sums[i] = v;
            counts[i] = 1;
            Ok(())
        })?;
        Self::encode(&sums, counts)
    }

    fn size(&self) -> usize {
        self.sums.capacity() * std::mem::size_of::<Wide<N>>() + self.counts.capacity() * 8
    }
}

/// The one-group form of the same state, for ungrouped aggregates and windows.
struct Single<const N: usize>(Groups<N>);

impl<const N: usize> std::fmt::Debug for Single<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.spec.name)
    }
}

impl<const N: usize> Accumulator for Single<N> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let idx = vec![0usize; values[0].len()];
        self.0.update_batch(values, &idx, None, 1)
    }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.0.grow(1);
        let (mut sum, mut count) = (self.0.sums[0], self.0.counts[0]);
        let name = self.0.spec.name;
        self.0.each(&values[0], None, |_, v| {
            sum = sum
                .checked_sub(v)
                .ok_or_else(|| exec(format!("{name}: running total left {} bits", N * 64)))?;
            count -= 1;
            Ok(())
        })?;
        self.0.sums[0] = sum;
        self.0.counts[0] = count;
        Ok(())
    }
    fn supports_retract_batch(&self) -> bool {
        true
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.0.grow(1);
        let a = self.0.build(&self.0.sums, &self.0.counts)?;
        ScalarValue::try_from_array(&a, 0)
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.0.size()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.0.grow(1);
        Groups::<N>::encode(&self.0.sums[..1], vec![self.0.counts[0]])?
            .iter()
            .map(|a| ScalarValue::try_from_array(a, 0))
            .collect()
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let idx = vec![0usize; states[0].len()];
        self.0.merge_batch(states, &idx, 1)
    }
}

/// `checked_add`, `checked_sub`, `checked_mul`, `checked_neg`: arrow's checked kernels, where
/// DataFusion uses the wrapping ones, plus the decimal precision check neither does.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CheckedBinary {
    sig: Signature,
    /// `None` is unary negation.
    op: Option<Operator>,
}

impl CheckedBinary {
    pub fn udf(op: Option<Operator>) -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
            op,
        }))
    }
}

fn validate_precision(a: &ArrayRef) -> Result<()> {
    match a.data_type() {
        DataType::Decimal128(p, _) => a
            .as_primitive::<Decimal128Type>()
            .validate_decimal_precision(*p)?,
        DataType::Decimal256(p, _) => a
            .as_primitive::<Decimal256Type>()
            .validate_decimal_precision(*p)?,
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
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match (self.op, arg_types) {
            (None, [t]) => Ok(vec![t.clone()]),
            (Some(op), [l, r]) => {
                let (l, r) = BinaryTypeCoercer::new(l, &op, r).get_input_types()?;
                Ok(vec![l, r])
            }
            _ => plan_err!("{}: wrong number of arguments", self.name()),
        }
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match self.op {
            None => Ok(arg_types[0].clone()),
            Some(op) => BinaryTypeCoercer::new(&arg_types[0], &op, &arg_types[1]).get_result_type(),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let all_scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let out: ArrayRef = match self.op {
            Some(Operator::Plus) => numeric::add(&arrays[0], &arrays[1])?,
            Some(Operator::Minus) => numeric::sub(&arrays[0], &arrays[1])?,
            Some(Operator::Multiply) => numeric::mul(&arrays[0], &arrays[1])?,
            _ => numeric::neg(&arrays[0])?,
        };
        validate_precision(&out)?;
        let want = args.return_field.data_type();
        let out = if out.data_type() == want {
            out
        } else {
            let c = cast_with_options(
                &out,
                want,
                &CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )?;
            validate_precision(&c)?;
            c
        };
        if all_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
        } else {
            Ok(ColumnarValue::Array(out))
        }
    }
}

/// `exact_wide(x)` / `exact_wide_neg(x)`: an exact integer, or canonical decimal text, as a 320-bit
/// value. Internal: the rule uses it to carry a `TRY_CAST`'s source up to the sum that reads it, so
/// a negated uint256 never has to exist as text.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ExactWide {
    sig: Signature,
    neg: bool,
}

impl ExactWide {
    pub fn udf(neg: bool) -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
            neg,
        }))
    }
}

impl ScalarUDFImpl for ExactWide {
    fn name(&self) -> &str {
        if self.neg {
            "exact_wide_neg"
        } else {
            "exact_wide"
        }
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        use DataType::*;
        Ok(vec![match arg_types {
            [Int8 | Int16 | Int32 | Int64] => Int64,
            [UInt8 | UInt16 | UInt32 | UInt64] => UInt64,
            [
                t @ (Decimal128(_, 0)
                | Decimal256(_, 0)
                | Utf8
                | LargeUtf8
                | Utf8View
                | FixedSizeBinary(WIDE_BYTES)),
            ] => t.clone(),
            t => return plan_err!("{} has no exact form for {t:?}", self.name()),
        }])
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::FixedSizeBinary(WIDE_BYTES))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let all_scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let spec = Spec {
            mode: Mode::Sum,
            name: if self.neg {
                "exact_wide_neg"
            } else {
                "exact_wide"
            },
            in_ty: a.data_type().clone(),
            out_ty: DataType::FixedSizeBinary(WIDE_BYTES),
            lenient: true,
        };
        let g = Groups::<5>::new(spec);
        let mut b = FixedSizeBinaryBuilder::with_capacity(a.len(), WIDE_BYTES);
        let mut buf = [0u8; WIDE_BYTES as usize];
        let mut next = 0;
        g.each(&a, None, |i, v| {
            for _ in next..i {
                b.append_null();
            }
            let v = if self.neg {
                v.checked_neg()
                    .ok_or_else(|| exec(format!("{}: {v} has no negation", self.name())))?
            } else {
                v
            };
            v.write_le(&mut buf);
            b.append_value(buf)?;
            next = i + 1;
            Ok(())
        })?;
        for _ in next..a.len() {
            b.append_null();
        }
        let out: ArrayRef = Arc::new(b.finish());
        if all_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?))
        } else {
            Ok(ColumnarValue::Array(out))
        }
    }
}
