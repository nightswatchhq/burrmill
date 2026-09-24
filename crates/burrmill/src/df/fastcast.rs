//! `FastTextCasts`: `CAST(text AS DECIMAL(p,0))` without Arrow's general-purpose string parser.
//!
//! nuthatch's views cast text to HUGEINT at 305 sites, and on graph-allocations the cast was most
//! of a scan's time. Text that is an optional `-` and at most `p` significant digits is parsed
//! straight to i128. Anything else, including an error, goes through Arrow's own cast for that
//! value, so the answer is Arrow's by construction. `TRY_CAST` is never touched: the checked
//! arithmetic rule finds lossy values by it. This runs last, after the rules that match `CAST`.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Decimal128Builder};
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{DataType, Decimal128Type};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, Result, ScalarValue, plan_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    Cast, ColumnarValue, Expr, ExprSchemable, LogicalPlan, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion_optimizer::analyzer::AnalyzerRule;

#[derive(Debug, Default)]
pub struct FastTextCasts;

impl AnalyzerRule for FastTextCasts {
    fn name(&self) -> &str {
        "fast_text_casts"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|p| {
            let mut schema = DFSchema::empty();
            for i in p.inputs() {
                schema.merge(i.schema());
            }
            let names_matter = matches!(
                p,
                LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_) | LogicalPlan::Window(_)
            );
            let t = p.map_expressions(|e| {
                let name = e.schema_name().to_string();
                let t = e.transform_up(|e| fast(e, &schema))?;
                if t.transformed && names_matter && t.data.schema_name().to_string() != name {
                    Ok(Transformed::yes(t.data.alias(name)))
                } else {
                    Ok(t)
                }
            })?;
            if t.transformed {
                Ok(Transformed::yes(t.data.recompute_schema()?))
            } else {
                Ok(t)
            }
        })
        .map(|t| t.data)
    }
}

fn is_text(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8)
}

fn fast(e: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Expr::Cast(Cast { expr, field }) = &e else {
        return Ok(Transformed::no(e));
    };
    let DataType::Decimal128(p, 0) = field.data_type() else {
        return Ok(Transformed::no(e));
    };
    if !is_text(&expr.get_type(schema)?) {
        return Ok(Transformed::no(e));
    }
    let udf = Arc::new(ScalarUDF::from(TextToDecimal {
        sig: Signature::any(1, Volatility::Immutable),
        precision: *p,
    }));
    Ok(Transformed::yes(Expr::ScalarFunction(
        ScalarFunction::new_udf(udf, vec![expr.as_ref().clone()]),
    )))
}

/// `burrmill_text_to_decimal(x)`: `CAST(x AS DECIMAL(p,0))`, with the common case parsed directly.
#[derive(Debug, PartialEq, Eq, Hash)]
struct TextToDecimal {
    sig: Signature,
    precision: u8,
}

/// An optional `-` and 1..=p significant digits, as i128; `None` sends the value to Arrow.
fn plain(s: &str, precision: u8) -> Option<i128> {
    let b = s.as_bytes();
    let (neg, digits) = match b.first()? {
        b'-' => (true, &b[1..]),
        _ => (false, b),
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let significant = digits.iter().skip_while(|&&d| d == b'0').count();
    if significant > precision as usize {
        return None;
    }
    let mut v: i128 = 0;
    for &d in digits {
        v = v * 10 + (d - b'0') as i128;
    }
    Some(if neg { -v } else { v })
}

impl ScalarUDFImpl for TextToDecimal {
    fn name(&self) -> &str {
        "burrmill_text_to_decimal"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if !is_text(&args[0]) {
            return plan_err!("burrmill_text_to_decimal takes text, not {}", args[0]);
        }
        Ok(DataType::Decimal128(self.precision, 0))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let want = DataType::Decimal128(self.precision, 0);
        let strict = CastOptions {
            safe: false,
            ..Default::default()
        };
        let mut out = Decimal128Builder::with_capacity(a.len());
        let mut each = |i: usize, s: Option<&str>| -> Result<()> {
            let Some(s) = s else {
                out.append_null();
                return Ok(());
            };
            match plain(s, self.precision) {
                Some(v) => out.append_value(v),
                None => {
                    let one = cast_with_options(&a.slice(i, 1), &want, &strict)?;
                    let one = one.as_primitive::<Decimal128Type>();
                    if one.is_null(0) {
                        out.append_null()
                    } else {
                        out.append_value(one.value(0))
                    }
                }
            }
            Ok(())
        };
        match a.data_type() {
            DataType::Utf8View => {
                let s = a.as_string_view();
                for i in 0..s.len() {
                    each(i, s.is_valid(i).then(|| s.value(i)))?
                }
            }
            DataType::Utf8 => {
                let s = a.as_string::<i32>();
                for i in 0..s.len() {
                    each(i, s.is_valid(i).then(|| s.value(i)))?
                }
            }
            _ => {
                let s = a.as_string::<i64>();
                for i in 0..s.len() {
                    each(i, s.is_valid(i).then(|| s.value(i)))?
                }
            }
        }
        let arr: ArrayRef = Arc::new(out.finish().with_precision_and_scale(self.precision, 0)?);
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?))
        } else {
            Ok(ColumnarValue::Array(arr))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::plain;

    #[test]
    fn the_fast_path_takes_only_what_is_plainly_an_integer() {
        assert_eq!(plain("0", 38), Some(0));
        assert_eq!(plain("-17", 38), Some(-17));
        assert_eq!(plain("00042", 38), Some(42));
        assert_eq!(plain(&"9".repeat(38), 38), Some(10i128.pow(38) - 1));
        for s in ["", "-", "+1", " 1", "1 ", "1.0", "1e3", &"9".repeat(39)] {
            assert_eq!(plain(s, 38), None, "{s:?}");
        }
        assert_eq!(plain("123", 2), None);
    }
}
