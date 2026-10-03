//! DuckDB's `to_json(x)`: the value as compact JSON text, keys in struct order. nuthatch's GraphQL
//! lowering writes `to_json(list(struct_pack(...)))` for a `@derivedFrom` list.
//!
//! Types whose DuckDB spelling has not been measured (time, binary) are refused rather than guessed.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, StringBuilder};
use arrow::datatypes::{
    DataType, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion_common::{Result as DFResult, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ToJson {
    sig: Signature,
}

impl ToJson {
    pub fn udf() -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::any(1, Volatility::Immutable),
        }))
    }
}

impl ScalarUDFImpl for ToJson {
    fn name(&self) -> &str {
        "to_json"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        if !writable(&args[0]) {
            return plan_err!("to_json of {} is not supported here", args[0]);
        }
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let mut out = StringBuilder::with_capacity(a.len(), a.len() * 16);
        let mut buf = String::new();
        for i in 0..a.len() {
            if a.is_null(i) || a.data_type() == &DataType::Null {
                out.append_null();
                continue;
            }
            buf.clear();
            write(&a, i, &mut buf)?;
            out.append_value(&buf);
        }
        let out: ArrayRef = Arc::new(out.finish());
        Ok(if scalar {
            ColumnarValue::Scalar(datafusion_common::ScalarValue::try_from_array(&out, 0)?)
        } else {
            ColumnarValue::Array(out)
        })
    }
}

fn writable(t: &DataType) -> bool {
    use DataType::*;
    match t {
        Null | Boolean | Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Utf8
        | LargeUtf8 | Utf8View | Float32 | Float64 | Decimal128(..) => true,
        Struct(fields) => fields.iter().all(|f| writable(f.data_type())),
        List(f) | LargeList(f) => writable(f.data_type()),
        _ => false,
    }
}

fn write(a: &ArrayRef, i: usize, out: &mut String) -> DFResult<()> {
    use std::fmt::Write;
    if a.is_null(i) {
        out.push_str("null");
        return Ok(());
    }
    macro_rules! num {
        ($t:ty) => {
            write!(out, "{}", a.as_primitive::<$t>().value(i)).unwrap()
        };
    }
    match a.data_type() {
        DataType::Null => out.push_str("null"),
        DataType::Boolean => out.push_str(if a.as_boolean().value(i) {
            "true"
        } else {
            "false"
        }),
        DataType::Int8 => num!(Int8Type),
        DataType::Int16 => num!(Int16Type),
        DataType::Int32 => num!(Int32Type),
        DataType::Int64 => num!(Int64Type),
        DataType::UInt8 => num!(UInt8Type),
        DataType::UInt16 => num!(UInt16Type),
        DataType::UInt32 => num!(UInt32Type),
        DataType::UInt64 => num!(UInt64Type),
        DataType::Decimal128(_, 0) => num!(Decimal128Type),
        DataType::Decimal128(_, s) => decimal(a.as_primitive::<Decimal128Type>().value(i), *s, out),
        DataType::Float32 => double(a.as_primitive::<Float32Type>().value(i).into(), out),
        DataType::Float64 => double(a.as_primitive::<Float64Type>().value(i), out),
        DataType::Utf8 => string(a.as_string::<i32>().value(i), out),
        DataType::LargeUtf8 => string(a.as_string::<i64>().value(i), out),
        DataType::Utf8View => string(a.as_string_view().value(i), out),
        DataType::Struct(fields) => {
            let s = a.as_struct();
            out.push('{');
            for (k, (f, c)) in fields.iter().zip(s.columns()).enumerate() {
                if k > 0 {
                    out.push(',');
                }
                string(f.name(), out);
                out.push(':');
                write(c, i, out)?;
            }
            out.push('}');
        }
        DataType::List(_) => items(&a.as_list::<i32>().value(i), out)?,
        DataType::LargeList(_) => items(&a.as_list::<i64>().value(i), out)?,
        t => return exec_err!("to_json of {t} is not supported here"),
    }
    Ok(())
}

fn items(v: &ArrayRef, out: &mut String) -> DFResult<()> {
    out.push('[');
    for j in 0..v.len() {
        if j > 0 {
            out.push(',');
        }
        write(v, j, out)?;
    }
    out.push(']');
    Ok(())
}

/// As DuckDB's yyjson escapes: the two-character forms where JSON has them, `\u` with upper-case
/// hex for the other control characters, everything else (DEL, `/`, non-ASCII) as written.
fn string(s: &str, out: &mut String) {
    use std::fmt::Write;
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => write!(out, "\\u{:04X}", c as u32).unwrap(),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// As yyjson writes a double: the shortest digits that read back, fixed from 1e-6 up to 1e21 with
/// at least one fractional digit, scientific outside that with no `+`.
fn double(v: f64, out: &mut String) {
    if v.is_nan() {
        return out.push_str("NaN");
    }
    if v.is_infinite() {
        return out.push_str(if v > 0.0 { "Infinity" } else { "-Infinity" });
    }
    let sci = format!("{v:e}");
    let exp: i32 = sci
        .split_once('e')
        .map_or(0, |(_, e)| e.parse().unwrap_or(0));
    if v != 0.0 && !(-6..21).contains(&exp) {
        return out.push_str(&sci);
    }
    let fixed = format!("{v}");
    out.push_str(&fixed);
    if !fixed.contains('.') {
        out.push_str(".0");
    }
}

/// A scaled decimal as DuckDB writes it: its digits, trailing fractional zeros dropped down to one.
fn decimal(v: i128, scale: i8, out: &mut String) {
    let scale = scale as usize;
    let digits = format!("{:0>width$}", v.unsigned_abs(), width = scale + 1);
    let (int, frac) = digits.split_at(digits.len() - scale);
    let frac = frac.trim_end_matches('0');
    if v < 0 {
        out.push('-');
    }
    out.push_str(int);
    out.push('.');
    out.push_str(if frac.is_empty() { "0" } else { frac });
}
