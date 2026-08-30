//! Exact integer arithmetic that refuses rather than lies.
//!
//! This is the smallest file in the crate and the one carrying the headline claim. DataFusion's
//! integer arithmetic silently wraps - `SELECT 10000000000 * 10000000000` returns
//! `7766279631452241920` where Postgres, Trino and Snowflake all raise (#17539, open since September
//! 2025, assigned, no PR, no milestone). Its behaviour is also *inconsistent by operation*: `%`
//! errors on `i32::MIN % -1` while `+` wraps on `i32::MIN + -1` (#14771). There is no config flag
//! in core v55 to turn any of this off; the ANSI work lives in the Spark accelerator and its own
//! code comment says "all operations currently use wrapping behavior" (#20034).
//!
//! An inconsistent overflow policy is worse than a bad one, because you cannot reason about it.
//! Burrmill has one policy, in one place: refuse.

use crate::error::{BurrmillError, Result};

/// A refusing i128 accumulator.
///
/// DuckDB's `HUGEINT` errors on overflow - `Out of Range Error: Overflow in addition of INT128` -
/// so the message deliberately echoes it. A migration should not have to learn a new vocabulary for
/// the same refusal.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CheckedSumI128(i128);

impl CheckedSumI128 {
    pub const fn new() -> Self {
        Self(0)
    }

    pub const fn from_value(v: i128) -> Self {
        Self(v)
    }

    pub fn add(&mut self, v: i128, context: &str) -> Result<()> {
        self.0 = checked_add(self.0, v, context)?;
        Ok(())
    }

    pub const fn value(self) -> i128 {
        self.0
    }
}

pub fn checked_add(a: i128, b: i128, context: &str) -> Result<i128> {
    a.checked_add(b).ok_or_else(|| {
        BurrmillError::Overflow(format!("Overflow in addition of INT128 ({a} + {b}) for {context}"))
    })
}

/// `i128::MIN` has no positive counterpart, so a debit of it cannot be represented.
///
/// Plain `-d` wraps in release and returns `i128::MIN` again - the same value with the wrong sign,
/// which no downstream check would catch.
pub fn checked_neg(v: i128, context: &str) -> Result<i128> {
    v.checked_neg().ok_or_else(|| {
        BurrmillError::Overflow(format!(
            "Overflow in negation of INT128 ({v}) for {context} - i128::MIN has no positive"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The boundary the 24-configuration parity sweep could not reach: its largest value is ~1e20
    /// against an `i128::MAX` of ~1.7e38, so the fixture never came near overflow and the parity
    /// claim was never tested where it matters.
    #[test]
    fn a_sum_past_the_maximum_is_refused_not_wrapped() {
        let half = i128::MAX / 2 + 1;
        let mut acc = CheckedSumI128::from_value(half);
        let err = acc.add(half, "0xdead").unwrap_err();
        assert!(matches!(err, BurrmillError::Overflow(_)), "got {err:?}");
        assert!(
            half.wrapping_add(half) < 0,
            "wrapping flips the sign: a huge credit becomes a huge debt, and it looks like a balance"
        );
    }

    #[test]
    fn negating_the_minimum_is_refused_not_wrapped() {
        assert!(checked_neg(i128::MIN, "0xdead").is_err());
        assert_eq!(i128::MIN.wrapping_neg(), i128::MIN, "wrapping returns the same value");
    }

    /// Two partials each inside the range can leave it when merged, so the merge is checked for the
    /// same reason the accumulation is. Parallelism does not get an exemption.
    #[test]
    fn merging_partials_can_overflow_when_neither_partial_does() {
        assert!(checked_add(i128::MAX - 1, 2, "merge").is_err());
    }

    #[test]
    fn ordinary_arithmetic_is_still_exact() {
        let mut acc = CheckedSumI128::new();
        acc.add(1_000_000_000_000_000_000_000_000_000_000i128, "x").unwrap();
        acc.add(-1i128, "x").unwrap();
        assert_eq!(acc.value(), 999_999_999_999_999_999_999_999_999_999i128);
    }
}
