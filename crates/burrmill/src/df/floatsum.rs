//! `sum` and `avg` over DOUBLE, exact until the answer is rounded (nuthatch #1849).
//!
//! DataFusion adds doubles in whatever order partitions, batches and the final merge happen to
//! deliver them, so one view gave a different last digit from one server process to the next.
//! Here every value is added exactly into 64 fixed-point buckets of 32 binary orders each, and the
//! one rounding happens at the end, so the answer is the correctly rounded exact sum whatever the
//! order. NaN and infinities are counted beside it and answer as IEEE addition would.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BinaryBuilder, BooleanArray, Float64Builder};
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::{AggregateOrderSensitivity, format_state_name};
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, EmitTo, GroupsAccumulator,
    ReversedUDAF, SetMonotonicity, Signature, StatisticsArgs,
};

const BUCKETS: usize = 64;
const NONE: u8 = u8::MAX;
/// A bucket holds integers below 2^84, so this is some 2^41 values before it refuses.
const LIMIT: u128 = 1 << 125;

#[derive(Clone)]
struct Rare {
    nan: u64,
    pos_inf: u64,
    neg_inf: u64,
    dense: [i128; BUCKETS],
}

/// One group's exact running sum. Values within some 2^32 binary orders of each other share two
/// inline buckets; anything wider, or a NaN or infinity, moves the group to all 64.
#[derive(Clone)]
struct Exact {
    sums: [i128; 2],
    ids: [u8; 2],
    n: u64,
    rare: Option<Box<Rare>>,
}

impl Default for Exact {
    fn default() -> Self {
        Self {
            sums: [0; 2],
            ids: [NONE; 2],
            n: 0,
            rare: None,
        }
    }
}

fn too_wide() -> datafusion_common::DataFusionError {
    datafusion_common::DataFusionError::Execution(
        "sum of DOUBLE: exact running total out of range".into(),
    )
}

impl Exact {
    fn promote(&mut self) -> &mut Rare {
        if self.rare.is_none() {
            let mut r = Box::new(Rare {
                nan: 0,
                pos_inf: 0,
                neg_inf: 0,
                dense: [0; BUCKETS],
            });
            for j in 0..2 {
                if self.ids[j] != NONE {
                    r.dense[self.ids[j] as usize] = self.sums[j];
                }
            }
            self.ids = [NONE; 2];
            self.sums = [0; 2];
            self.rare = Some(r);
        }
        self.rare.as_mut().expect("promoted")
    }

    fn bucket(&mut self, b: u8, v: i128) -> Result<()> {
        let slot = if let Some(r) = &mut self.rare {
            &mut r.dense[b as usize]
        } else if let Some(j) = self.ids.iter().position(|&i| i == b) {
            &mut self.sums[j]
        } else if let Some(j) = self.ids.iter().position(|&i| i == NONE) {
            self.ids[j] = b;
            &mut self.sums[j]
        } else {
            &mut self.promote().dense[b as usize]
        };
        *slot += v;
        if slot.unsigned_abs() > LIMIT {
            return Err(too_wide());
        }
        let emptied = *slot == 0;
        if emptied
            && self.rare.is_none()
            && let Some(j) = self.ids.iter().position(|&i| i == b)
        {
            self.ids[j] = NONE;
        }
        Ok(())
    }

    /// Adds `x`, or takes it back out when `back`. The caller counts it in `n`.
    fn add(&mut self, x: f64, back: bool) -> Result<()> {
        let bits = x.to_bits();
        let exp = ((bits >> 52) & 0x7ff) as u32;
        let frac = bits & ((1 << 52) - 1);
        if exp == 0x7ff {
            let r = self.promote();
            let count = match (frac, bits >> 63) {
                (0, 0) => &mut r.pos_inf,
                (0, _) => &mut r.neg_inf,
                _ => &mut r.nan,
            };
            *count = if back { *count - 1 } else { *count + 1 };
            return Ok(());
        }
        let (m, e) = if exp == 0 {
            (frac, 1)
        } else {
            (frac | 1 << 52, exp)
        };
        if m == 0 {
            return Ok(());
        }
        // `x` is m * 2^(e - 1075); bucket b counts in units of 2^(32b - 1075).
        let v = (m as i128) << (e & 31);
        let v = if (bits >> 63 == 1) != back { -v } else { v };
        self.bucket((e >> 5) as u8, v)
    }

    fn entries(&self) -> Vec<(u8, i128)> {
        match &self.rare {
            Some(r) => (0..BUCKETS)
                .filter(|&b| r.dense[b] != 0)
                .map(|b| (b as u8, r.dense[b]))
                .collect(),
            None => (0..2)
                .filter(|&j| self.ids[j] != NONE)
                .map(|j| (self.ids[j], self.sums[j]))
                .collect(),
        }
    }

    fn merge(&mut self, o: &Exact) -> Result<()> {
        self.n += o.n;
        if let Some(r) = &o.rare {
            let (nan, pos, neg) = (r.nan, r.pos_inf, r.neg_inf);
            if nan + pos + neg > 0 {
                let s = self.promote();
                s.nan += nan;
                s.pos_inf += pos;
                s.neg_inf += neg;
            }
        }
        for (b, v) in o.entries() {
            self.bucket(b, v)?;
        }
        Ok(())
    }

    fn encode(&self, out: &mut Vec<u8>) {
        let (nan, pos, neg) = self
            .rare
            .as_ref()
            .map_or((0, 0, 0), |r| (r.nan, r.pos_inf, r.neg_inf));
        for c in [self.n, nan, pos, neg] {
            out.extend_from_slice(&c.to_le_bytes());
        }
        for (b, v) in self.entries() {
            out.push(b);
            out.extend_from_slice(&v.to_le_bytes());
        }
    }

    fn decode(mut buf: &[u8]) -> Result<Self> {
        let mut e = Exact::default();
        if buf.len() < 32 || !(buf.len() - 32).is_multiple_of(17) {
            return internal_err!("exact sum state of {} bytes", buf.len());
        }
        let word = |b: &[u8]| u64::from_le_bytes(b.try_into().expect("8 bytes"));
        e.n = word(&buf[0..8]);
        let (nan, pos, neg) = (word(&buf[8..16]), word(&buf[16..24]), word(&buf[24..32]));
        if nan + pos + neg > 0 {
            let r = e.promote();
            (r.nan, r.pos_inf, r.neg_inf) = (nan, pos, neg);
        }
        buf = &buf[32..];
        while let [b, rest @ ..] = buf {
            if *b as usize >= BUCKETS {
                return internal_err!("exact sum state names bucket {b}");
            }
            let v = i128::from_le_bytes(rest[..16].try_into().expect("16 bytes"));
            e.bucket(*b, v)?;
            buf = &rest[16..];
        }
        Ok(e)
    }

    /// The exact sum, rounded once to nearest, ties to even.
    fn value(&self) -> f64 {
        if let Some(r) = &self.rare {
            if r.nan > 0 || (r.pos_inf > 0 && r.neg_inf > 0) {
                return f64::NAN;
            } else if r.pos_inf > 0 {
                return f64::INFINITY;
            } else if r.neg_inf > 0 {
                return f64::NEG_INFINITY;
            }
        }
        // 32-bit limbs of the total in units of 2^-1075; four spare above the top bucket.
        let mut c = [0i128; BUCKETS + 4];
        for (b, v) in self.entries() {
            c[b as usize] = v;
        }
        let carry = |c: &mut [i128]| {
            for i in 0..c.len() - 1 {
                let k = c[i] >> 32;
                c[i] -= k << 32;
                c[i + 1] += k;
            }
        };
        carry(&mut c);
        let negative = c[c.len() - 1] < 0;
        if negative {
            c.iter_mut().for_each(|x| *x = -*x);
            carry(&mut c);
        }
        let limbs: Vec<u64> = c.iter().map(|&x| x as u64).collect();
        let Some(h) = limbs.iter().rposition(|&l| l != 0) else {
            return 0.0;
        };
        let top = 32 * h + 63 - limbs[h].leading_zeros() as usize;
        // Bit p of the total is 2^(p - 1075): 53 bits from the top, but never below 2^-1074.
        let keep = top.saturating_sub(52).max(1);
        let bit = |p: usize| limbs[p / 32] >> (p % 32) & 1 == 1;
        let mut window = 0u128;
        for j in 0..3 {
            if let Some(&l) = limbs.get(keep / 32 + j) {
                window |= (l as u128) << (32 * j);
            }
        }
        let mut q = (window >> (keep % 32)) as u64;
        let below = keep - 1;
        let sticky = limbs[..below / 32].iter().any(|&l| l != 0)
            || limbs[below / 32] & ((1 << (below % 32)) - 1) != 0;
        if bit(below) && (sticky || q & 1 == 1) {
            q += 1;
        }
        let v = q as f64 * pow2(keep as i32 - 1075);
        if negative { -v } else { v }
    }
}

/// 2^k for k in [-1074, 1023], exactly.
fn pow2(k: i32) -> f64 {
    if k >= -1022 {
        f64::from_bits(((k + 1023) as u64) << 52)
    } else {
        f64::from_bits(1 << (k + 1074))
    }
}

/// The built-in `sum` or `avg`, with an exact accumulator wherever the input is DOUBLE.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ExactDoubles {
    inner: Arc<AggregateUDF>,
}

impl ExactDoubles {
    pub fn udaf(inner: Arc<AggregateUDF>) -> Arc<AggregateUDF> {
        Arc::new(AggregateUDF::from(Self { inner }))
    }

    fn avg(&self) -> bool {
        self.inner.name() == "avg"
    }
}

fn doubles(input: &[FieldRef], distinct: bool) -> bool {
    !distinct && matches!(input, [f] if f.data_type() == &DataType::Float64)
}

impl AggregateUDFImpl for ExactDoubles {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }
    fn is_nullable(&self) -> bool {
        self.inner.is_nullable()
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if !doubles(args.input_fields, args.is_distinct) {
            return self.inner.state_fields(args);
        }
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "exact"),
            DataType::Binary,
            true,
        ))])
    }
    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if !doubles(args.expr_fields, args.is_distinct) {
            return self.inner.accumulator(args);
        }
        Ok(Box::new(Single(Groups::new(self.avg()))))
    }
    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if !doubles(args.expr_fields, args.is_distinct) {
            return self.inner.create_sliding_accumulator(args);
        }
        Ok(Box::new(Single(Groups::new(self.avg()))))
    }
    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        doubles(args.expr_fields, args.is_distinct) || self.inner.groups_accumulator_supported(args)
    }
    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if !doubles(args.expr_fields, args.is_distinct) {
            return self.inner.create_groups_accumulator(args);
        }
        Ok(Box::new(Groups::new(self.avg())))
    }
    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner.inner().order_sensitivity()
    }
    fn reverse_expr(&self) -> ReversedUDAF {
        self.inner.inner().reverse_expr()
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }
    fn value_from_stats(&self, args: &StatisticsArgs) -> Option<ScalarValue> {
        // A statistics sum of doubles was added in whatever order the writer chose.
        if args.return_type == &DataType::Float64 {
            return None;
        }
        self.inner.value_from_stats(args)
    }
    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        self.inner.default_value(data_type)
    }
    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        self.inner.inner().set_monotonicity(data_type)
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

struct Groups {
    avg: bool,
    groups: Vec<Exact>,
    /// Groups holding a boxed `Rare`, for `size`.
    rare: usize,
}

impl Groups {
    fn new(avg: bool) -> Self {
        Self {
            avg,
            groups: Vec::new(),
            rare: 0,
        }
    }

    fn each(
        values: &ArrayRef,
        opt_filter: Option<&BooleanArray>,
        mut f: impl FnMut(usize, f64) -> Result<()>,
    ) -> Result<()> {
        let values = values.as_primitive::<Float64Type>();
        for (i, &v) in values.values().iter().enumerate() {
            if values.is_valid(i) && opt_filter.is_none_or(|m| m.is_valid(i) && m.value(i)) {
                f(i, v)?;
            }
        }
        Ok(())
    }

    fn add(&mut self, g: usize, v: f64, back: bool) -> Result<()> {
        let e = &mut self.groups[g];
        let was = e.rare.is_some();
        e.add(v, back)?;
        e.n = if back { e.n - 1 } else { e.n + 1 };
        self.rare += usize::from(!was && e.rare.is_some());
        Ok(())
    }

    fn merge_one(&mut self, g: usize, state: &[u8]) -> Result<()> {
        let o = Exact::decode(state)?;
        let e = &mut self.groups[g];
        let was = e.rare.is_some();
        e.merge(&o)?;
        self.rare += usize::from(!was && e.rare.is_some());
        Ok(())
    }

    fn take(&mut self, emit_to: EmitTo) -> Vec<Exact> {
        let taken = emit_to.take_needed(&mut self.groups);
        self.rare -= taken.iter().filter(|e| e.rare.is_some()).count();
        taken
    }

    fn finish(&self, groups: &[Exact]) -> ArrayRef {
        let mut b = Float64Builder::with_capacity(groups.len());
        for e in groups {
            match e.n {
                0 => b.append_null(),
                n if self.avg => b.append_value(e.value() / n as f64),
                _ => b.append_value(e.value()),
            }
        }
        Arc::new(b.finish())
    }

    fn encode(groups: &[Exact]) -> ArrayRef {
        let mut b = BinaryBuilder::with_capacity(groups.len(), groups.len() * 49);
        let mut buf = Vec::with_capacity(64);
        for e in groups {
            buf.clear();
            e.encode(&mut buf);
            b.append_value(&buf);
        }
        Arc::new(b.finish())
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
        self.groups.resize_with(total_num_groups, Exact::default);
        Self::each(&values[0], opt_filter, |i, v| {
            self.add(group_indices[i], v, false)
        })
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.groups.resize_with(total_num_groups, Exact::default);
        let states = values[0].as_binary::<i32>();
        for (i, &g) in group_indices.iter().enumerate() {
            if states.is_valid(i) {
                self.merge_one(g, states.value(i))?;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let taken = self.take(emit_to);
        Ok(self.finish(&taken))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let taken = self.take(emit_to);
        Ok(vec![Self::encode(&taken)])
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let mut rows = vec![Exact::default(); values[0].len()];
        Self::each(&values[0], opt_filter, |i, v| {
            rows[i].add(v, false)?;
            rows[i].n = 1;
            Ok(())
        })?;
        Ok(vec![Self::encode(&rows)])
    }

    fn size(&self) -> usize {
        self.groups.capacity() * size_of::<Exact>() + self.rare * size_of::<Rare>()
    }
}

/// The one-group form, for ungrouped aggregates and window frames.
struct Single(Groups);

impl std::fmt::Debug for Single {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.avg { "exact avg" } else { "exact sum" })
    }
}

impl Single {
    fn group(&mut self) -> &mut Groups {
        if self.0.groups.is_empty() {
            self.0.groups.push(Exact::default());
        }
        &mut self.0
    }
}

impl Accumulator for Single {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let g = self.group();
        Groups::each(&values[0], None, |_, v| g.add(0, v, false))
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let g = self.group();
        Groups::each(&values[0], None, |_, v| g.add(0, v, true))
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let g = self.group();
        let states = states[0].as_binary::<i32>();
        for i in 0..states.len() {
            if states.is_valid(i) {
                g.merge_one(0, states.value(i))?;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let g = self.group();
        ScalarValue::try_from_array(&g.finish(&g.groups), 0)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let g = self.group();
        let mut buf = Vec::new();
        g.groups[0].encode(&mut buf);
        Ok(vec![ScalarValue::Binary(Some(buf))])
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.0.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exact(xs: &[f64]) -> f64 {
        let mut e = Exact::default();
        for &x in xs {
            e.add(x, false).unwrap();
            e.n += 1;
        }
        e.value()
    }

    #[test]
    fn rounds_once_to_nearest_even() {
        assert_eq!(exact(&[]).to_bits(), 0.0f64.to_bits());
        assert_eq!(exact(&[-0.0]).to_bits(), 0.0f64.to_bits());
        assert_eq!(exact(&[1.0, 2f64.powi(-53)]), 1.0);
        assert_eq!(
            exact(&[1.0, 2f64.powi(-53), 2f64.powi(-100)]),
            1.0 + f64::EPSILON
        );
        assert_eq!(
            exact(&[1.0 + f64::EPSILON, 2f64.powi(-53)]),
            1.0 + 2.0 * f64::EPSILON
        );
        assert_eq!(exact(&[f64::MAX, -f64::MAX, 1.5]), 1.5);
        assert_eq!(exact(&[f64::MAX, f64::MAX]), f64::INFINITY);
        assert_eq!(exact(&[-f64::MAX, -f64::MAX]), f64::NEG_INFINITY);
        assert_eq!(exact(&[f64::MAX, 2f64.powi(970)]), f64::INFINITY);
        assert_eq!(exact(&[f64::MAX, 2f64.powi(969)]), f64::MAX);
        let tiny = f64::from_bits(1);
        assert_eq!(exact(&[tiny, tiny, tiny]), f64::from_bits(3));
        assert_eq!(
            exact(&[f64::MIN_POSITIVE, -tiny]),
            f64::from_bits((1 << 52) - 1)
        );
        assert_eq!(exact(&[-3.0, 1.0]), -2.0);
        assert_eq!(exact(&[1e300, 1e-300, -1e300]), 1e-300);
    }

    #[test]
    fn order_and_split_do_not_matter() {
        let mut s = 7u64;
        let xs: Vec<f64> = (0..5000)
            .map(|_| {
                s = s
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                f64::from_bits(s >> 1 & !(0x7ffu64 << 52) | ((s >> 20) % 2000 + 20) << 52)
                    * if s & 1 == 0 { 1.0 } else { -1.0 }
            })
            .collect();
        let whole = exact(&xs);
        let mut rev = xs.clone();
        rev.reverse();
        assert_eq!(exact(&rev).to_bits(), whole.to_bits());
        let mut a = Exact::default();
        let mut b = Exact::default();
        for (i, &x) in xs.iter().enumerate() {
            let t = if i % 3 == 0 { &mut a } else { &mut b };
            t.add(x, false).unwrap();
            t.n += 1;
        }
        let mut buf = Vec::new();
        b.encode(&mut buf);
        a.merge(&Exact::decode(&buf).unwrap()).unwrap();
        assert_eq!(a.value().to_bits(), whole.to_bits());
        for &x in &xs[100..] {
            a.add(x, true).unwrap();
        }
        assert_eq!(a.value().to_bits(), exact(&xs[..100]).to_bits());
    }
}
