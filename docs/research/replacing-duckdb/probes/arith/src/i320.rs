//! A fixed 320-bit two's complement integer: i256 plus one carry word.
//!
//! Wide enough that a signed sum of 2^63 uint256 values cannot leave it, so the
//! accumulator itself never needs to refuse; only the final narrowing does.

use datafusion::arrow::datatypes::i256;

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct I320(pub [u64; 5]); // little-endian limbs

impl I320 {
    pub const ZERO: I320 = I320([0; 5]);
    pub const MIN: I320 = I320([0, 0, 0, 0, 1 << 63]);

    pub fn is_negative(&self) -> bool {
        self.0[4] >> 63 == 1
    }

    pub fn checked_add(self, o: I320) -> Option<I320> {
        let mut r = [0u64; 5];
        let mut carry = 0u64;
        for i in 0..5 {
            let (s1, c1) = self.0[i].overflowing_add(o.0[i]);
            let (s2, c2) = s1.overflowing_add(carry);
            r[i] = s2;
            carry = c1 as u64 + c2 as u64;
        }
        let r = I320(r);
        if self.is_negative() == o.is_negative() && r.is_negative() != self.is_negative() {
            None
        } else {
            Some(r)
        }
    }

    fn wrapping_neg(self) -> I320 {
        let mut r = [0u64; 5];
        let mut carry = 1u64;
        for i in 0..5 {
            let (s, c) = (!self.0[i]).overflowing_add(carry);
            r[i] = s;
            carry = c as u64;
        }
        I320(r)
    }

    pub fn checked_neg(self) -> Option<I320> {
        if self == I320::MIN {
            None
        } else {
            Some(self.wrapping_neg())
        }
    }

    pub fn checked_sub(self, o: I320) -> Option<I320> {
        self.checked_add(o.checked_neg()?)
    }

    pub fn from_i128(v: i128) -> I320 {
        let lo = v as u128;
        let sign = if v < 0 { u64::MAX } else { 0 };
        I320([lo as u64, (lo >> 64) as u64, sign, sign, sign])
    }

    pub fn from_i256(v: i256) -> I320 {
        let (lo, hi) = v.to_parts();
        let sign = if hi < 0 { u64::MAX } else { 0 };
        let hu = hi as u128;
        I320([lo as u64, (lo >> 64) as u64, hu as u64, (hu >> 64) as u64, sign])
    }

    pub fn to_i256(self) -> Option<i256> {
        let sign = if self.0[3] >> 63 == 1 { u64::MAX } else { 0 };
        if self.0[4] != sign {
            return None;
        }
        let lo = self.0[0] as u128 | ((self.0[1] as u128) << 64);
        let hi = (self.0[2] as u128 | ((self.0[3] as u128) << 64)) as i128;
        Some(i256::from_parts(lo, hi))
    }

    pub fn to_i128(self) -> Option<i128> {
        let sign = if self.0[1] >> 63 == 1 { u64::MAX } else { 0 };
        if self.0[2] != sign || self.0[3] != sign || self.0[4] != sign {
            return None;
        }
        Some((self.0[0] as u128 | ((self.0[1] as u128) << 64)) as i128)
    }

    pub fn to_i64(self) -> Option<i64> {
        self.to_i128().and_then(|v| i64::try_from(v).ok())
    }

    pub fn to_le_bytes(self) -> [u8; 40] {
        let mut b = [0u8; 40];
        for (i, l) in self.0.iter().enumerate() {
            b[i * 8..i * 8 + 8].copy_from_slice(&l.to_le_bytes());
        }
        b
    }

    pub fn from_le_bytes(b: &[u8]) -> I320 {
        let mut l = [0u64; 5];
        for i in 0..5 {
            l[i] = u64::from_le_bytes(b[i * 8..i * 8 + 8].try_into().unwrap());
        }
        I320(l)
    }

    /// magnitude * m + a, returns true on carry out of 320 bits
    fn mul_small_add(limbs: &mut [u64; 5], m: u64, a: u64) -> bool {
        let mut carry = a as u128;
        for l in limbs.iter_mut() {
            let p = (*l as u128) * (m as u128) + carry;
            *l = p as u64;
            carry = p >> 64;
        }
        carry != 0
    }

    fn divrem_small(limbs: &mut [u64; 5], d: u64) -> u64 {
        let mut rem = 0u128;
        for i in (0..5).rev() {
            let cur = (rem << 64) | limbs[i] as u128;
            limbs[i] = (cur / d as u128) as u64;
            rem = cur % d as u128;
        }
        rem as u64
    }

    fn magnitude(self) -> (bool, [u64; 5]) {
        if self.is_negative() {
            (true, self.wrapping_neg().0) // MIN maps to itself, whose limbs read as 2^319 unsigned
        } else {
            (false, self.0)
        }
    }

    fn from_magnitude(neg: bool, mag: [u64; 5]) -> Option<I320> {
        let v = I320(mag);
        if neg {
            if mag[4] >> 63 == 1 {
                return if v == I320::MIN { Some(v) } else { None };
            }
            Some(v.wrapping_neg())
        } else if v.is_negative() {
            None
        } else {
            Some(v)
        }
    }

    pub fn checked_mul_small(self, m: u64) -> Option<I320> {
        let (neg, mut mag) = self.magnitude();
        if Self::mul_small_add(&mut mag, m, 0) {
            return None;
        }
        Self::from_magnitude(neg, mag)
    }

    /// Truncating division by a small positive divisor.
    pub fn div_small(self, d: u64) -> I320 {
        let (neg, mut mag) = self.magnitude();
        Self::divrem_small(&mut mag, d);
        Self::from_magnitude(neg, mag).expect("quotient magnitude is smaller than the dividend")
    }

    /// Canonical text is `0` or `-?[1-9][0-9]*`. Anything else is refused by name.
    pub fn parse_canonical(s: &str) -> Result<I320, String> {
        let (neg, digits) = match s.strip_prefix('-') {
            Some(d) => (true, d),
            None => (false, s),
        };
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(format!("not a canonical decimal integer: {s:?}"));
        }
        if (digits.len() > 1 && digits.starts_with('0')) || (neg && digits == "0") {
            return Err(format!("not a canonical decimal integer (leading zero or negative zero): {s:?}"));
        }
        let mut mag = [0u64; 5];
        for b in digits.bytes() {
            if Self::mul_small_add(&mut mag, 10, (b - b'0') as u64) {
                return Err(format!("does not fit in 320 bits: {s:?}"));
            }
        }
        Self::from_magnitude(neg, mag).ok_or_else(|| format!("does not fit in 320 bits: {s:?}"))
    }

    pub fn to_decimal_string(self) -> String {
        let (neg, mut mag) = self.magnitude();
        if mag == [0; 5] {
            return "0".into();
        }
        let mut chunks = Vec::new();
        while mag != [0; 5] {
            chunks.push(Self::divrem_small(&mut mag, 10_000_000_000_000_000_000));
        }
        let mut s = String::new();
        if neg {
            s.push('-');
        }
        s.push_str(&chunks.last().unwrap().to_string());
        for c in chunks.iter().rev().skip(1) {
            s.push_str(&format!("{c:019}"));
        }
        s
    }
}

impl std::fmt::Display for I320 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_decimal_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const U256_MAX: &str =
        "115792089237316195423570985008687907853269984665640564039457584007913129639935";

    #[test]
    fn round_trips() {
        for s in ["0", "1", "-1", "9223372036854775807", "-9223372036854775808", U256_MAX] {
            let v = I320::parse_canonical(s).unwrap();
            assert_eq!(v.to_decimal_string(), s);
        }
        let v = I320::from_i128(i128::MIN);
        assert_eq!(v.to_decimal_string(), i128::MIN.to_string());
        assert_eq!(v.to_i128(), Some(i128::MIN));
        assert_eq!(I320::from_i128(i128::MAX).to_i128(), Some(i128::MAX));
        assert_eq!(I320::from_i128(i128::MAX).checked_add(I320::from_i128(1)).unwrap().to_i128(), None);
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
        assert_eq!(p255m1.to_i256().unwrap().to_string(), p255m1.to_decimal_string());
        assert_eq!(p255m1.checked_add(I320::from_i128(1)).unwrap().to_i256(), None);
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
        assert_eq!(I320::from_le_bytes(&v.to_le_bytes()), v);
        assert_eq!(v.checked_mul_small(10000).unwrap().to_decimal_string(), "-1234567890123456789012345678900000");
        assert_eq!(v.div_small(7).to_decimal_string(), "-17636684144620811271604938270");
    }
}
