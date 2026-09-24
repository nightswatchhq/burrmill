//! A fixed-width two's complement integer of `N` 64-bit limbs, little-endian.
//!
//! The checked sums pick the width from the input: 2 limbs for 64-bit integers, 3 for Decimal128,
//! 5 for Decimal256 and text. Each is wide enough that 2^63 inputs cannot leave it, so the
//! accumulator never refuses in practice; only the final narrowing does.

use arrow::datatypes::i256;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Wide<const N: usize>(pub [u64; N]);

impl<const N: usize> Default for Wide<N> {
    fn default() -> Self {
        Self::ZERO
    }
}

impl<const N: usize> Wide<N> {
    pub const ZERO: Self = Wide([0; N]);
    pub const BYTES: usize = N * 8;

    fn min() -> Self {
        let mut l = [0; N];
        l[N - 1] = 1 << 63;
        Wide(l)
    }

    pub fn is_negative(&self) -> bool {
        self.0[N - 1] >> 63 == 1
    }

    pub fn checked_add(self, o: Self) -> Option<Self> {
        let mut r = [0u64; N];
        let mut carry = false;
        for (i, out) in r.iter_mut().enumerate() {
            let (s1, c1) = self.0[i].overflowing_add(o.0[i]);
            let (s2, c2) = s1.overflowing_add(carry as u64);
            *out = s2;
            carry = c1 | c2;
        }
        let r = Wide(r);
        if self.is_negative() == o.is_negative() && r.is_negative() != self.is_negative() {
            None
        } else {
            Some(r)
        }
    }

    fn wrapping_neg(self) -> Self {
        let mut r = [0u64; N];
        let mut carry = true;
        for (i, out) in r.iter_mut().enumerate() {
            let (s, c) = (!self.0[i]).overflowing_add(carry as u64);
            *out = s;
            carry = c;
        }
        Wide(r)
    }

    pub fn checked_neg(self) -> Option<Self> {
        if self == Self::min() {
            None
        } else {
            Some(self.wrapping_neg())
        }
    }

    pub fn checked_sub(self, o: Self) -> Option<Self> {
        match o.checked_neg() {
            Some(n) => self.checked_add(n),
            // x - MIN = x + 2^(64N-1): fits only when x is negative.
            None => self.wrapping_sub_min(),
        }
    }

    fn wrapping_sub_min(self) -> Option<Self> {
        if !self.is_negative() {
            return None;
        }
        let mut r = self.0;
        r[N - 1] ^= 1 << 63;
        Some(Wide(r))
    }

    pub fn from_i128(v: i128) -> Self {
        let lo = v as u128;
        let sign = if v < 0 { u64::MAX } else { 0 };
        let mut l = [sign; N];
        l[0] = lo as u64;
        l[1] = (lo >> 64) as u64;
        Wide(l)
    }

    pub fn from_i256(v: i256) -> Self {
        debug_assert!(N >= 4);
        let (lo, hi) = v.to_parts();
        let sign = if hi < 0 { u64::MAX } else { 0 };
        let hu = hi as u128;
        let mut l = [sign; N];
        l[0] = lo as u64;
        l[1] = (lo >> 64) as u64;
        l[2] = hu as u64;
        l[3] = (hu >> 64) as u64;
        Wide(l)
    }

    /// True when every limb from `k` up is the sign extension of limb `k - 1`.
    fn fits_limbs(&self, k: usize) -> bool {
        let sign = if self.0[k - 1] >> 63 == 1 {
            u64::MAX
        } else {
            0
        };
        self.0[k..].iter().all(|&l| l == sign)
    }

    pub fn to_i128(self) -> Option<i128> {
        if !self.fits_limbs(2) {
            return None;
        }
        Some((self.0[0] as u128 | ((self.0[1] as u128) << 64)) as i128)
    }

    pub fn to_i256(self) -> Option<i256> {
        if N < 4 {
            return self.to_i128().map(i256::from_i128);
        }
        if !self.fits_limbs(4) {
            return None;
        }
        let lo = self.0[0] as u128 | ((self.0[1] as u128) << 64);
        let hi = (self.0[2] as u128 | ((self.0[3] as u128) << 64)) as i128;
        Some(i256::from_parts(lo, hi))
    }

    pub fn write_le(&self, out: &mut [u8]) {
        for (i, l) in self.0.iter().enumerate() {
            out[i * 8..i * 8 + 8].copy_from_slice(&l.to_le_bytes());
        }
    }

    pub fn from_le(b: &[u8]) -> Self {
        let mut l = [0u64; N];
        for (i, out) in l.iter_mut().enumerate() {
            *out = u64::from_le_bytes(b[i * 8..i * 8 + 8].try_into().expect("8 bytes"));
        }
        Wide(l)
    }

    /// `limbs * m + a` in place; true on carry out of the top limb.
    fn mul_small_add(limbs: &mut [u64; N], m: u64, a: u64) -> bool {
        let mut carry = a as u128;
        for l in limbs.iter_mut() {
            let p = (*l as u128) * (m as u128) + carry;
            *l = p as u64;
            carry = p >> 64;
        }
        carry != 0
    }

    fn divrem_small(limbs: &mut [u64; N], d: u64) -> u64 {
        let mut rem = 0u128;
        for l in limbs.iter_mut().rev() {
            let cur = (rem << 64) | *l as u128;
            *l = (cur / d as u128) as u64;
            rem = cur % d as u128;
        }
        rem as u64
    }

    /// Sign and unsigned magnitude. MIN maps to itself, whose limbs read as 2^(64N-1) unsigned.
    fn magnitude(self) -> (bool, [u64; N]) {
        if self.is_negative() {
            (true, self.wrapping_neg().0)
        } else {
            (false, self.0)
        }
    }

    fn from_magnitude(neg: bool, mag: [u64; N]) -> Option<Self> {
        let v = Wide(mag);
        if neg {
            if v.is_negative() {
                return (v == Self::min()).then_some(v);
            }
            Some(v.wrapping_neg())
        } else if v.is_negative() {
            None
        } else {
            Some(v)
        }
    }

    pub fn checked_mul_small(self, m: u64) -> Option<Self> {
        let (neg, mut mag) = self.magnitude();
        if Self::mul_small_add(&mut mag, m, 0) {
            return None;
        }
        Self::from_magnitude(neg, mag)
    }

    /// Division by a small positive divisor, truncating toward zero.
    pub fn div_small(self, d: u64) -> Self {
        let (neg, mut mag) = self.magnitude();
        Self::divrem_small(&mut mag, d);
        Self::from_magnitude(neg, mag).expect("quotient magnitude is below the dividend's")
    }

    /// Integer text as DuckDB's `TRY_CAST(... AS DECIMAL(38,0))` reads it, where that reading is
    /// exact: surrounding spaces, a sign, leading zeros, and a fraction of zeros only. `7.9` (which
    /// DuckDB rounds) and exponents are refused.
    pub fn parse_integer(s: &str) -> Result<Self, String> {
        let t = s.trim_matches(|c: char| c.is_ascii_whitespace());
        let (neg, rest) = match t.as_bytes().first() {
            Some(b'-') => (true, &t[1..]),
            Some(b'+') => (false, &t[1..]),
            _ => (false, t),
        };
        let int = match rest.split_once('.') {
            Some((i, f)) if f.bytes().all(|b| b == b'0') && !(i.is_empty() && f.is_empty()) => i,
            Some(_) => return Err(format!("not an exact integer: {s:?}")),
            None => rest,
        };
        let digits = int.trim_start_matches('0');
        if (int.is_empty() && !rest.contains('.')) || !int.bytes().all(|b| b.is_ascii_digit()) {
            return Err(format!("not an exact integer: {s:?}"));
        }
        let digits = if digits.is_empty() { "0" } else { digits };
        let canonical = if neg && digits != "0" { format!("-{digits}") } else { digits.to_string() };
        Self::parse_canonical(&canonical)
    }

    /// Canonical text only: `0` or `-?[1-9][0-9]*`. Anything else is refused by name.
    pub fn parse_canonical(s: &str) -> Result<Self, String> {
        let (neg, digits) = match s.strip_prefix('-') {
            Some(d) => (true, d),
            None => (false, s),
        };
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(format!("not a canonical decimal integer: {s:?}"));
        }
        if (digits.len() > 1 && digits.starts_with('0')) || (neg && digits == "0") {
            return Err(format!(
                "not a canonical decimal integer (leading zero or negative zero): {s:?}"
            ));
        }
        let mut mag = [0u64; N];
        for b in digits.bytes() {
            if Self::mul_small_add(&mut mag, 10, (b - b'0') as u64) {
                return Err(format!("does not fit in {} bits: {s:?}", N * 64));
            }
        }
        Self::from_magnitude(neg, mag)
            .ok_or_else(|| format!("does not fit in {} bits: {s:?}", N * 64))
    }
}

impl<const N: usize> std::fmt::Display for Wide<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (neg, mut mag) = self.magnitude();
        if mag.iter().all(|&l| l == 0) {
            return f.write_str("0");
        }
        let mut chunks = Vec::new();
        while mag.iter().any(|&l| l != 0) {
            chunks.push(Self::divrem_small(&mut mag, 10_000_000_000_000_000_000));
        }
        if neg {
            f.write_str("-")?;
        }
        write!(f, "{}", chunks.last().expect("non-zero"))?;
        for c in chunks.iter().rev().skip(1) {
            write!(f, "{c:019}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type I320 = Wide<5>;

    const U256_MAX: &str =
        "115792089237316195423570985008687907853269984665640564039457584007913129639935";

    #[test]
    fn round_trips() {
        for s in [
            "0",
            "1",
            "-1",
            "9223372036854775807",
            "-9223372036854775808",
            U256_MAX,
        ] {
            assert_eq!(I320::parse_canonical(s).unwrap().to_string(), s);
        }
        let v = I320::from_i128(i128::MIN);
        assert_eq!(v.to_string(), i128::MIN.to_string());
        assert_eq!(v.to_i128(), Some(i128::MIN));
        let max = I320::from_i128(i128::MAX);
        assert_eq!(max.checked_add(I320::from_i128(1)).unwrap().to_i128(), None);
        assert_eq!(
            Wide::<2>::from_i128(i128::MIN).to_string(),
            i128::MIN.to_string()
        );
    }

    #[test]
    fn narrow_widths_refuse_at_their_edge() {
        let max = Wide::<2>::from_i128(i128::MAX);
        assert_eq!(max.checked_add(Wide::from_i128(1)), None);
        assert_eq!(Wide::<2>::from_i128(i128::MIN).checked_neg(), None);
        assert_eq!(
            Wide::<2>::from_i128(-1).checked_sub(Wide::from_i128(i128::MIN)),
            Some(max)
        );
        assert_eq!(
            Wide::<2>::from_i128(0).checked_sub(Wide::from_i128(i128::MIN)),
            None
        );
        let w = Wide::<3>::from_i128(i128::MAX)
            .checked_add(Wide::from_i128(i128::MAX))
            .unwrap();
        assert_eq!(w.to_i128(), None);
        assert_eq!(
            w.checked_sub(Wide::from_i128(i128::MAX)).unwrap().to_i128(),
            Some(i128::MAX)
        );
        assert!(Wide::<2>::parse_canonical("170141183460469231731687303715884105728").is_err());
    }

    #[test]
    fn reads_integers_as_duckdb_does_where_exact() {
        for (s, v) in [("010", "10"), (" 7 ", "7"), ("+1", "1"), ("7.0", "7"), ("-0", "0"), ("-007.00", "-7"), (".0", "0")] {
            assert_eq!(I320::parse_integer(s).unwrap().to_string(), v, "{s:?}");
        }
        for s in ["7.9", "1e3", "", "-", ".", "1_000", "0x10", "7."] {
            if s == "7." {
                assert_eq!(I320::parse_integer(s).unwrap().to_string(), "7");
                continue;
            }
            assert!(I320::parse_integer(s).is_err(), "{s:?}");
        }
    }

    #[test]
    fn refuses_non_canonical() {
        for s in ["", "-", "-0", "007", " 7", "+1", "1e3", "7.0", "1_000"] {
            assert!(I320::parse_canonical(s).is_err(), "{s:?}");
        }
    }

    #[test]
    fn i256_edges() {
        let max = I320::parse_canonical(U256_MAX).unwrap();
        assert_eq!(max.to_i256(), None);
        let p255m1 = I320::parse_canonical(
            "57896044618658097711785492504343953926634992332820282019728792003956564819967",
        )
        .unwrap();
        assert_eq!(p255m1.to_i256().unwrap().to_string(), p255m1.to_string());
        assert_eq!(
            p255m1.checked_add(I320::from_i128(1)).unwrap().to_i256(),
            None
        );
        let mut s = I320::ZERO;
        for _ in 0..6 {
            s = s.checked_add(max).unwrap();
        }
        for _ in 0..6 {
            s = s.checked_sub(max).unwrap();
        }
        assert_eq!(s, I320::ZERO);
        assert_eq!(I320::from_i256(i256::MIN).to_i256(), Some(i256::MIN));
        assert_eq!(I320::from_i256(i256::MAX).to_i256(), Some(i256::MAX));
    }

    #[test]
    fn bytes_and_small_ops() {
        let v = I320::parse_canonical("-123456789012345678901234567890").unwrap();
        let mut b = [0u8; 40];
        v.write_le(&mut b);
        assert_eq!(I320::from_le(&b), v);
        assert_eq!(
            v.checked_mul_small(10000).unwrap().to_string(),
            "-1234567890123456789012345678900000"
        );
        assert_eq!(v.div_small(7).to_string(), "-17636684144620811271604938270");
    }
}
