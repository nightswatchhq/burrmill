//! `sum` and `avg` over DOUBLE, exact until the answer is rounded (nuthatch #1849).
//!
//! DataFusion adds doubles in whatever order partitions, batches and the final merge happen to
//! deliver them, so one view gave a different last digit from one server process to the next.
//! Here every value is added exactly into 64 fixed-point buckets of 32 binary orders each, and the
//! one rounding happens at the end, so the answer is the correctly rounded exact sum whatever the
//! order. NaN and infinities are counted beside it and answer as IEEE addition would.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryBuilder, BooleanArray, Float64Array, Float64Builder,
};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::{AggregateOrderSensitivity, format_state_name};
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, EmitTo, GroupsAccumulator,
    ReversedUDAF, SetMonotonicity, Signature, StatisticsArgs,
};

const BUCKETS: usize = 64;
/// Interleaved partial sums per bucket, so that one bucket's additions do not wait on each other.
const LANES: usize = 4;
type Lanes = [[i128; LANES]; BUCKETS];
const NONE: u8 = u8::MAX;
/// A bucket holds integers below 2^84, so this is some 2^41 values before it refuses.
const LIMIT: u128 = 1 << 125;
/// Values the grouped path adds between range checks of every group. Each is under 2^85 units, so a
/// bucket within `LIMIT` at one check is under 2^126 at the next and no `i128` has overflowed.
const CHECK_EVERY: u64 = 1 << 20;

#[derive(Clone)]
struct Rare {
    nan: u64,
    pos_inf: u64,
    neg_inf: u64,
    dense: [i128; BUCKETS],
}

impl Rare {
    fn special_mut(&mut self) -> [&mut u64; 3] {
        [&mut self.nan, &mut self.pos_inf, &mut self.neg_inf]
    }
}

/// `x` as `v` units of bucket `b`, or `None` for a NaN or an infinity. `x` is m * 2^(e - 1075),
/// and bucket b counts in units of 2^(32b - 1075).
#[inline(always)]
fn units(x: f64) -> Option<(usize, i128)> {
    let bits = x.to_bits();
    let exp = (bits >> 52) as u32 & 0x7ff;
    if exp == 0x7ff {
        return None;
    }
    let e = exp.max(1);
    let m = bits & ((1 << 52) - 1) | u64::from(exp != 0) << 52;
    let v = i128::from(m) << (e & 31);
    let neg = -i128::from(bits >> 63);
    Some(((e >> 5) as usize & (BUCKETS - 1), (v ^ neg) - neg))
}

/// Which of `Rare::special_mut` counts a NaN or an infinity.
fn special(x: f64) -> usize {
    match (x.is_nan(), x.is_sign_positive()) {
        (true, _) => 0,
        (false, true) => 1,
        (false, false) => 2,
    }
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
        // Branch-free in which inline slot holds `b`: the slot a value lands in is as random as the
        // data, and a mispredicted search cost more than the addition. A group in `Rare` has no ids.
        let (hit0, hit1) = (self.ids[0] == b, self.ids[1] == b);
        if hit0 | hit1 {
            let j = usize::from(hit1);
            let slot = &mut self.sums[j];
            *slot += v;
            if slot.unsigned_abs() > LIMIT {
                return Err(too_wide());
            }
            if *slot == 0 {
                self.ids[j] = NONE;
            }
            return Ok(());
        }
        self.bucket_slow(b, v)
    }

    fn bucket_slow(&mut self, b: u8, v: i128) -> Result<()> {
        let slot = if let Some(r) = &mut self.rare {
            &mut r.dense[b as usize]
        } else if let Some(j) = (0..2).find(|&j| self.ids[j] == NONE || self.sums[j] == 0) {
            self.ids[j] = b;
            &mut self.sums[j]
        } else {
            &mut self.promote().dense[b as usize]
        };
        *slot += v;
        if slot.unsigned_abs() > LIMIT {
            return Err(too_wide());
        }
        if *slot == 0
            && self.rare.is_none()
            && let Some(j) = self.ids.iter().position(|&i| i == b)
        {
            self.ids[j] = NONE;
        }
        Ok(())
    }

    /// Adds `x`, or takes it back out when `back`. The caller counts it in `n`.
    fn add(&mut self, x: f64, back: bool) -> Result<()> {
        match units(x) {
            Some((_, 0)) => Ok(()),
            Some((b, v)) => self.bucket(b as u8, if back { -v } else { v }),
            None => {
                let (counts, k) = (self.promote().special_mut(), special(x));
                *counts[k] = if back { *counts[k] - 1 } else { *counts[k] + 1 };
                Ok(())
            }
        }
    }

    /// Adds `xs` into this group, or takes them back out, and returns how many there were. Each
    /// bucket is summed exactly in `lanes` first and added here once: a batch of under 2^32 values
    /// of under 2^85 units cannot overflow an `i128`, and the total is the same integer.
    fn add_many(
        &mut self,
        xs: impl Iterator<Item = f64>,
        back: bool,
        lanes: &mut Lanes,
    ) -> Result<u64> {
        let mut specials = [0u64; 3];
        let mut touched = 0u64;
        let mut n = 0u64;
        for x in xs {
            match units(x) {
                Some((b, v)) => {
                    lanes[b][n as usize % LANES] += v;
                    touched |= 1 << b;
                }
                None => specials[special(x)] += 1,
            }
            n += 1;
        }
        while touched != 0 {
            let b = touched.trailing_zeros() as usize;
            touched &= touched - 1;
            let s: i128 = lanes[b].iter().sum();
            lanes[b] = [0; LANES];
            if s != 0 {
                self.bucket(b as u8, if back { -s } else { s })?;
            }
        }
        if specials.iter().any(|&c| c > 0) {
            let r = self.promote();
            for (count, c) in r.special_mut().into_iter().zip(specials) {
                *count = if back { *count - c } else { *count + c };
            }
        }
        Ok(n)
    }

    fn out_of_range(&self) -> bool {
        let wide = |s: &i128| s.unsigned_abs() > LIMIT;
        self.sums.iter().any(wide) || self.rare.as_ref().is_some_and(|r| r.dense.iter().any(wide))
    }

    fn entries(&self) -> impl Iterator<Item = (u8, i128)> + '_ {
        let dense = self.rare.iter().flat_map(|r| {
            (0..BUCKETS)
                .filter(|&b| r.dense[b] != 0)
                .map(|b| (b as u8, r.dense[b]))
        });
        let inline = (0..2)
            .filter(|&j| self.rare.is_none() && self.ids[j] != NONE && self.sums[j] != 0)
            .map(|j| (self.ids[j], self.sums[j]));
        dense.chain(inline)
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

    /// Adds another group's `encode`d state, without building it first.
    fn merge_encoded(&mut self, mut buf: &[u8]) -> Result<()> {
        if buf.len() < 32 || !(buf.len() - 32).is_multiple_of(17) {
            return internal_err!("exact sum state of {} bytes", buf.len());
        }
        let word = |b: &[u8]| u64::from_le_bytes(b.try_into().expect("8 bytes"));
        self.n += word(&buf[0..8]);
        let (nan, pos, neg) = (word(&buf[8..16]), word(&buf[16..24]), word(&buf[24..32]));
        if nan + pos + neg > 0 {
            let r = self.promote();
            r.nan += nan;
            r.pos_inf += pos;
            r.neg_inf += neg;
        }
        buf = &buf[32..];
        while let [b, rest @ ..] = buf {
            if *b as usize >= BUCKETS {
                return internal_err!("exact sum state names bucket {b}");
            }
            let v = i128::from_le_bytes(rest[..16].try_into().expect("16 bytes"));
            self.bucket(*b, v)?;
            buf = &rest[16..];
        }
        Ok(())
    }

    /// The exact sum, rounded once to nearest, ties to even.
    fn value(&self) -> f64 {
        self.inline_value().unwrap_or_else(|| self.limb_value())
    }

    /// `value` for a group in one bucket or two adjacent ones: their total fits an `i128`, which
    /// `as f64` rounds to nearest even, and the power of two that scales it adds no second rounding.
    fn inline_value(&self) -> Option<f64> {
        if self.rare.is_some() {
            return None;
        }
        let (lo, total) = match (self.ids, self.sums) {
            ([NONE, NONE], _) => return Some(0.0),
            ([b, NONE], [s, _]) | ([NONE, b], [_, s]) => (b, s),
            ([b0, b1], [s0, s1]) => {
                let ((lo, low), (hi, high)) = if b0 < b1 {
                    ((b0, s0), (b1, s1))
                } else {
                    ((b1, s1), (b0, s0))
                };
                if hi - lo != 1 {
                    return None;
                }
                (lo, high.checked_mul(1 << 32)?.checked_add(low)?)
            }
        };
        // Bucket 0 counts half the least subnormal. From bucket 1 a subnormal total is under 2^21
        // units, so already exact, and a total past the largest double overflows here as it should.
        if lo == 0 {
            return None;
        }
        Some(total as f64 * pow2(32 * i32::from(lo) - 1075))
    }

    fn limb_value(&self) -> f64 {
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
        let limbs = c.map(|x| x as u64);
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
    /// The one group's batch sums, zero between batches.
    lanes: Option<Box<Lanes>>,
    /// Values `add_rows` has added since it last checked every group's range.
    unchecked: u64,
}

impl Groups {
    fn new(avg: bool) -> Self {
        Self {
            avg,
            groups: Vec::new(),
            rare: 0,
            lanes: None,
            unchecked: 0,
        }
    }

    fn each(
        values: &ArrayRef,
        opt_filter: Option<&BooleanArray>,
        mut f: impl FnMut(usize, f64) -> Result<()>,
    ) -> Result<()> {
        let values = values.as_primitive::<Float64Type>();
        match Self::mask(values, opt_filter) {
            None => {
                for (i, &v) in values.values().iter().enumerate() {
                    f(i, v)?;
                }
            }
            Some(m) => {
                for i in m.set_indices() {
                    f(i, values.value(i))?;
                }
            }
        }
        Ok(())
    }

    /// The rows to add: valid, and passed by the filter where there is one. `None` is all of them.
    fn mask(values: &Float64Array, opt_filter: Option<&BooleanArray>) -> Option<BooleanBuffer> {
        let filter = opt_filter.map(|m| match m.nulls() {
            Some(n) => m.values() & n.inner(),
            None => m.values().clone(),
        });
        match (values.nulls().map(|n| n.inner()), filter) {
            (None, f) => f,
            (Some(n), None) => Some(n.clone()),
            (Some(n), Some(f)) => Some(n & &f),
        }
    }

    /// Adds a whole batch to group 0, or takes it back out.
    fn add_all(&mut self, values: &ArrayRef, back: bool) -> Result<()> {
        let values = values.as_primitive::<Float64Type>();
        let lanes = self
            .lanes
            .get_or_insert_with(|| Box::new([[0; LANES]; BUCKETS]));
        let e = &mut self.groups[0];
        let was = e.rare.is_some();
        let n = match Self::mask(values, None) {
            None => e.add_many(values.values().iter().copied(), back, lanes)?,
            Some(m) => e.add_many(m.set_indices().map(|i| values.value(i)), back, lanes)?,
        };
        e.n = if back { e.n - n } else { e.n + n };
        self.rare += usize::from(!was && e.rare.is_some());
        Ok(())
    }

    /// Adds each `(group, value)`. A value landing in a bucket its group already holds is added with
    /// no branch on which, and range is checked over every group each `CHECK_EVERY` values or more.
    fn add_rows(&mut self, rows: impl Iterator<Item = (usize, f64)>) -> Result<()> {
        let (mut added, mut wide) = (0, false);
        for (g, x) in rows {
            let e = &mut self.groups[g];
            e.n += 1;
            added += 1;
            let Some((b, v)) = units(x) else {
                let was = e.rare.is_some();
                e.add(x, false)?;
                self.rare += usize::from(!was && e.rare.is_some());
                continue;
            };
            if v == 0 {
                continue;
            }
            let (hit0, hit1) = (e.ids[0] == b as u8, e.ids[1] == b as u8);
            if hit0 | hit1 {
                e.sums[usize::from(hit1)] += v;
            } else if let Some(r) = &mut e.rare {
                r.dense[b] += v;
            } else {
                e.bucket_slow(b as u8, v)?;
                self.rare += usize::from(e.rare.is_some());
            }
        }
        self.unchecked += added;
        // No more often than once per slot it reads, so the scan costs at most one compare a value.
        if self.unchecked >= CHECK_EVERY.max((self.groups.len() + BUCKETS * self.rare) as u64) {
            self.unchecked = 0;
            wide |= self.groups.iter().any(Exact::out_of_range);
        }
        if wide {
            return Err(too_wide());
        }
        Ok(())
    }

    fn merge_one(&mut self, g: usize, state: &[u8]) -> Result<()> {
        let e = &mut self.groups[g];
        let was = e.rare.is_some();
        e.merge_encoded(state)?;
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
            b.append_option(self.answer(e));
        }
        Arc::new(b.finish())
    }

    fn answer(&self, e: &Exact) -> Option<f64> {
        match e.n {
            0 => None,
            n if self.avg => Some(e.value() / n as f64),
            _ => Some(e.value()),
        }
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
        let values = values[0].as_primitive::<Float64Type>();
        match Self::mask(values, opt_filter) {
            None => self.add_rows(
                group_indices
                    .iter()
                    .copied()
                    .zip(values.values().iter().copied()),
            ),
            Some(m) => self.add_rows(m.set_indices().map(|i| (group_indices[i], values.value(i)))),
        }
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
        self.groups.capacity() * size_of::<Exact>()
            + self.rare * size_of::<Rare>()
            + self.lanes.as_ref().map_or(0, |_| size_of::<Lanes>())
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
        self.group().add_all(&values[0], false)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.group().add_all(&values[0], true)
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
        Ok(ScalarValue::Float64(g.answer(&g.groups[0])))
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
        a.merge_encoded(&buf).unwrap();
        assert_eq!(a.value().to_bits(), whole.to_bits());
        for &x in &xs[100..] {
            a.add(x, true).unwrap();
        }
        assert_eq!(a.value().to_bits(), exact(&xs[..100]).to_bits());
    }

    #[test]
    fn inline_buckets_round_as_the_limbs_do() {
        let mut s = 1849u64;
        let mut next = || {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            s
        };
        let mut checked = 0;
        for case in 0..100_000 {
            let lo = (next() % 64) as u8;
            let hi = (lo + (next() % 3) as u8).min(63);
            let mut sum = || {
                let bits = next() % 85;
                let v = (i128::from(next()) << 64 | i128::from(next())) >> (128 - bits.max(1));
                if next() & 1 == 0 { v } else { -v }
            };
            let mut e = Exact::default();
            e.bucket(lo, sum()).unwrap();
            if hi != lo {
                e.bucket(hi, sum()).unwrap();
            }
            if let Some(v) = e.inline_value() {
                assert_eq!(v.to_bits(), e.limb_value().to_bits(), "case {case}");
                checked += 1;
            }
        }
        assert!(
            checked > 50_000,
            "only {checked} cases took the inline path"
        );
    }

    /// The batched and branch-free paths give the bits that adding one value at a time gives.
    #[test]
    fn batches_add_as_single_values_do() {
        let mut s = 1887u64;
        let mut next = || {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            s >> 1
        };
        for case in 0..400 {
            let len = [1, 7, 255, 256, 3000][case % 5];
            let groups = 1 + (next() % 5) as usize;
            let span = 1 + next() % 2000;
            let xs: Vec<Option<f64>> = (0..len)
                .map(|_| {
                    let r = next();
                    let sign = (r & 1) << 63;
                    Some(f64::from_bits(
                        sign | match r % 64 {
                            0 => return None,
                            1 => 0,
                            2 if case % 7 == 0 => (0x7ff << 52) | ((r >> 40) % 2),
                            3 => (r >> 12) & ((1 << 52) - 1),
                            _ => {
                                (((1000 + (r >> 20) % span).min(0x7fe)) << 52)
                                    | ((r >> 11) & ((1 << 52) - 1))
                            }
                        },
                    ))
                })
                .collect();
            let gi: Vec<usize> = (0..len).map(|_| next() as usize % groups).collect();
            let keep: Vec<Option<bool>> = (0..len)
                .map(|_| [None, Some(false), Some(true), Some(true)][next() as usize % 4])
                .collect();
            let values: ArrayRef = Arc::new(Float64Array::from(xs.clone()));

            let mut want = vec![Exact::default(); groups];
            for i in 0..len {
                if let (Some(x), Some(true)) = (xs[i], keep[i]) {
                    want[gi[i]].add(x, false).unwrap();
                    want[gi[i]].n += 1;
                }
            }
            let mut got = Groups::new(false);
            let filter = BooleanArray::from(keep);
            got.update_batch(std::slice::from_ref(&values), &gi, Some(&filter), groups)
                .unwrap();
            for (g, (w, e)) in want.iter().zip(&got.groups).enumerate() {
                assert_eq!(
                    (e.n, e.value().to_bits()),
                    (w.n, w.value().to_bits()),
                    "case {case} group {g}"
                );
            }
            assert_eq!(
                got.rare,
                got.groups.iter().filter(|e| e.rare.is_some()).count()
            );

            let cut = len / 3;
            let mut one = Single(Groups::new(false));
            one.update_batch(std::slice::from_ref(&values)).unwrap();
            one.retract_batch(&[values.slice(0, cut)]).unwrap();
            let mut want = Exact::default();
            for x in xs[cut..].iter().flatten() {
                want.add(*x, false).unwrap();
                want.n += 1;
            }
            let e = &one.0.groups[0];
            assert_eq!(
                (e.n, e.value().to_bits()),
                (want.n, want.value().to_bits()),
                "case {case}"
            );
        }
    }

    /// The grouped path checks range every `CHECK_EVERY` values rather than on each.
    #[test]
    fn a_group_past_the_limit_is_refused_at_the_next_check() {
        let mut g = Groups::new(false);
        g.groups.resize_with(1, Exact::default);
        let (b, _) = units(1.0).unwrap();
        g.groups[0].bucket(b as u8, LIMIT as i128).unwrap();
        g.unchecked = CHECK_EVERY - 2;
        g.add_rows([(0, 1.0)].into_iter()).unwrap();
        assert!(g.add_rows([(0, 1.0)].into_iter()).is_err());

        let mut g = Groups::new(false);
        g.groups.resize_with(1, Exact::default);
        g.add_rows([(0, 2f64.powi(-40)), (0, 2f64.powi(40)), (0, 2f64.powi(100))].into_iter())
            .unwrap();
        assert!(g.groups[0].rare.is_some());
        g.groups[0].bucket(b as u8, LIMIT as i128).unwrap();
        g.unchecked = CHECK_EVERY - 1;
        assert!(g.add_rows([(0, 1.0)].into_iter()).is_err());
    }
}
