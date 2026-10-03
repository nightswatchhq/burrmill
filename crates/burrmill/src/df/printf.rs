//! DuckDB's `printf(fmt, ...)` (C-style `%` specs) and `format(fmt, ...)` (`{}` specs), both from
//! the fmt library. A subset, measured against DuckDB in `dialect-parity`: flags `-+ 0#`, width,
//! precision, and `d i u x X o s f F e E g G c %`; `{}`, `{N}` and `{:[fill]align][sign][0][width]
//! [.precision][type]}`. Anything else is refused rather than guessed.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringBuilder};
use arrow::datatypes::DataType;
use datafusion_common::{Result as DFResult, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Printf {
    sig: Signature,
    braces: bool,
}

impl Printf {
    pub fn udfs() -> [Arc<ScalarUDF>; 2] {
        [false, true].map(|braces| {
            Arc::new(ScalarUDF::from(Self {
                sig: Signature::variadic_any(Volatility::Immutable),
                braces,
            }))
        })
    }
}

impl ScalarUDFImpl for Printf {
    fn name(&self) -> &str {
        if self.braces { "format" } else { "printf" }
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        let Some(first) = args.first() else {
            return plan_err!("{} takes a format string", self.name());
        };
        if !matches!(
            first,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
        ) {
            return plan_err!("{}'s format must be text, not {first}", self.name());
        }
        for t in &args[1..] {
            if arg_kind(t).is_none() {
                return plan_err!("{} of {t} is not supported here", self.name());
            }
        }
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let n = arrays[0].len();
        let mut out = StringBuilder::with_capacity(n, n * 16);
        for i in 0..n {
            if arrays
                .iter()
                .any(|a| a.is_null(i) || a.data_type() == &DataType::Null)
            {
                out.append_null();
                continue;
            }
            let fmt = text(&arrays[0], i)?;
            let vals = arrays[1..]
                .iter()
                .map(|a| value(a, i))
                .collect::<DFResult<Vec<_>>>()?;
            let s = if self.braces {
                braces(&fmt, &vals)?
            } else {
                percent(&fmt, &vals)?
            };
            out.append_value(s);
        }
        let out: ArrayRef = Arc::new(out.finish());
        Ok(if scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?)
        } else {
            ColumnarValue::Array(out)
        })
    }
}

#[derive(Debug, Clone)]
enum V {
    Int(i128),
    Float(f64),
    Str(String),
    Bool(bool),
}

fn arg_kind(t: &DataType) -> Option<()> {
    use DataType::*;
    matches!(
        t,
        Null | Boolean
            | Int8
            | Int16
            | Int32
            | Int64
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Float32
            | Float64
            | Utf8
            | LargeUtf8
            | Utf8View
            | Decimal128(..)
    )
    .then_some(())
}

fn text(a: &ArrayRef, i: usize) -> DFResult<String> {
    match ScalarValue::try_from_array(a, i)? {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Ok(s),
        v => exec_err!("expected text, got {v}"),
    }
}

fn value(a: &ArrayRef, i: usize) -> DFResult<V> {
    use ScalarValue as S;
    Ok(match S::try_from_array(a, i)? {
        S::Boolean(Some(b)) => V::Bool(b),
        S::Int8(Some(v)) => V::Int(v.into()),
        S::Int16(Some(v)) => V::Int(v.into()),
        S::Int32(Some(v)) => V::Int(v.into()),
        S::Int64(Some(v)) => V::Int(v.into()),
        S::UInt8(Some(v)) => V::Int(v.into()),
        S::UInt16(Some(v)) => V::Int(v.into()),
        S::UInt32(Some(v)) => V::Int(v.into()),
        S::UInt64(Some(v)) => V::Int(v.into()),
        S::Decimal128(Some(v), _, 0) => V::Int(v),
        // DuckDB formats a scaled DECIMAL as the DOUBLE it casts to.
        S::Decimal128(Some(v), p, s) => V::Float(super::doubles::decimal_to_double(v, p, s)),
        S::Float32(Some(v)) => V::Float(v.into()),
        S::Float64(Some(v)) => V::Float(v),
        S::Utf8(Some(s)) | S::LargeUtf8(Some(s)) | S::Utf8View(Some(s)) => V::Str(s),
        v => return exec_err!("printf of {} is not supported here", v.data_type()),
    })
}

#[derive(Default, Clone, Copy)]
struct Spec {
    left: bool,
    plus: bool,
    space: bool,
    zero: bool,
    alt: bool,
    /// `,`: thousands separators in a decimal integer part.
    group: bool,
    width: usize,
    precision: Option<usize>,
    fill: Option<char>,
    align: Option<char>,
}

fn percent(fmt: &str, vals: &[V]) -> DFResult<String> {
    let mut out = String::new();
    let mut next = 0;
    let mut chars = fmt.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        if chars.peek() == Some(&'%') {
            chars.next();
            out.push('%');
            continue;
        }
        let mut s = Spec::default();
        while let Some(&f) = chars.peek() {
            match f {
                '-' => s.left = true,
                '+' => s.plus = true,
                ' ' => s.space = true,
                '0' => s.zero = true,
                '#' => s.alt = true,
                ',' => s.group = true,
                _ => break,
            }
            chars.next();
        }
        s.width = digits(&mut chars).unwrap_or(0);
        if chars.peek() == Some(&'.') {
            chars.next();
            s.precision = Some(digits(&mut chars).unwrap_or(0));
        }
        let Some(ty) = chars.next() else {
            return exec_err!("Invalid Input Error: printf format ends inside a specifier");
        };
        // DuckDB groups a hex or octal spec's value in decimal.
        let ty = if s.group && matches!(ty, 'x' | 'X' | 'o') {
            'd'
        } else {
            ty
        };
        let Some(v) = vals.get(next) else {
            return exec_err!("Invalid Input Error: printf needs more arguments than it was given");
        };
        next += 1;
        if s.left {
            s.align = Some('<');
        } else if !s.zero {
            s.align = Some('>');
        }
        out.push_str(&render(v, ty, s, false)?);
    }
    Ok(out)
}

fn braces(fmt: &str, vals: &[V]) -> DFResult<String> {
    let mut out = String::new();
    let mut next = 0;
    let mut chars = fmt.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '{' if chars.peek() == Some(&'{') => {
                chars.next();
                out.push('{');
            }
            '}' if chars.peek() == Some(&'}') => {
                chars.next();
                out.push('}');
            }
            '}' => return exec_err!("Invalid Input Error: unmatched '}}' in format string"),
            '{' => {
                let mut inner = String::new();
                loop {
                    match chars.next() {
                        Some('}') => break,
                        Some(x) => inner.push(x),
                        None => {
                            return exec_err!(
                                "Invalid Input Error: unmatched '{{' in format string"
                            );
                        }
                    }
                }
                let (index, spec) = inner.split_once(':').unwrap_or((&inner, ""));
                let i = if index.is_empty() {
                    next += 1;
                    next - 1
                } else {
                    match index.parse::<usize>() {
                        Ok(i) => i,
                        Err(_) => {
                            return exec_err!("format argument {{{index}}} is not supported here");
                        }
                    }
                };
                let Some(v) = vals.get(i) else {
                    return exec_err!("Invalid Input Error: argument index out of range");
                };
                let (s, ty) = brace_spec(spec)?;
                out.push_str(&render(v, ty, s, true)?);
            }
            c => out.push(c),
        }
    }
    Ok(out)
}

fn brace_spec(spec: &str) -> DFResult<(Spec, char)> {
    let mut s = Spec::default();
    let cs: Vec<char> = spec.chars().collect();
    let mut k = 0;
    let is_align = |c: char| matches!(c, '<' | '>' | '^');
    if cs.len() >= 2 && is_align(cs[1]) {
        s.fill = Some(cs[0]);
        s.align = Some(cs[1]);
        k = 2;
    } else if !cs.is_empty() && is_align(cs[0]) {
        s.align = Some(cs[0]);
        k = 1;
    }
    match cs.get(k) {
        Some('+') => {
            s.plus = true;
            k += 1;
        }
        Some(' ') => {
            s.space = true;
            k += 1;
        }
        Some('-') => k += 1,
        _ => {}
    }
    if cs.get(k) == Some(&'#') {
        s.alt = true;
        k += 1;
    }
    if cs.get(k) == Some(&'0') {
        s.zero = true;
        k += 1;
    }
    let mut it = cs[k..].iter().copied().peekable();
    s.width = digits(&mut it).unwrap_or(0);
    if it.peek() == Some(&'.') {
        it.next();
        s.precision = Some(digits(&mut it).unwrap_or(0));
    }
    let ty = it.next().unwrap_or('\0');
    if it.next().is_some() || !"\0dxXobsfFeEgGc".contains(ty) {
        return exec_err!("format spec {{:{spec}}} is not supported here");
    }
    Ok((s, ty))
}

fn digits(it: &mut std::iter::Peekable<impl Iterator<Item = char>>) -> Option<usize> {
    let mut n: Option<usize> = None;
    while let Some(d) = it.peek().and_then(|c| c.to_digit(10)) {
        n = Some(n.unwrap_or(0) * 10 + d as usize);
        it.next();
    }
    n
}

/// One argument under one spec. `braces` selects fmt's `{}` defaults over printf's.
fn render(v: &V, ty: char, s: Spec, braces: bool) -> DFResult<String> {
    // printf reads a boolean as the integer it is to C.
    if let (V::Bool(b), 'd' | 'i' | 'u' | 'x' | 'X' | 'o' | 'c') = (v, ty)
        && !braces
    {
        return render(&V::Int(i128::from(*b)), ty, s, braces);
    }
    let (sign, body): (&str, String) = match (v, ty) {
        (V::Int(n), 'd' | 'i' | 'u' | '\0') => (int_sign(*n, s), n.unsigned_abs().to_string()),
        // printf prints a negative in hex or octal as its 64-bit two's complement; fmt keeps the sign.
        (V::Int(n), 'x' | 'X' | 'o' | 'b') if *n < 0 && !braces => {
            let Ok(n) = i64::try_from(*n) else {
                return exec_err!("printf of {n} in base {ty} is not supported here");
            };
            return render(&V::Int(i128::from(n as u64)), ty, s, braces);
        }
        (V::Int(n), 'x' | 'X' | 'o' | 'b') => {
            let a = n.unsigned_abs();
            let (b, prefix) = match ty {
                'x' => (format!("{a:x}"), "0x"),
                'X' => (format!("{a:X}"), "0X"),
                'o' => (format!("{a:o}"), "0"),
                _ => (format!("{a:b}"), "0b"),
            };
            let b = if s.alt && !(ty == 'o' && b == "0") {
                format!("{prefix}{b}")
            } else {
                b
            };
            (int_sign(*n, s), b)
        }
        (V::Int(n), 'c') => match u32::try_from(*n).ok().and_then(char::from_u32) {
            Some(c) => ("", c.to_string()),
            None => return exec_err!("Invalid Input Error: {n} is not a character"),
        },
        (V::Float(f), 'f' | 'F' | 'e' | 'E' | 'g' | 'G' | '\0') => {
            let sign = if f.is_sign_negative() && !f.is_nan() {
                "-"
            } else if s.plus {
                "+"
            } else if s.space {
                " "
            } else {
                ""
            };
            let a = f.abs();
            let body = if !a.is_finite() {
                let t = if a.is_nan() { "nan" } else { "inf" };
                if ty.is_ascii_uppercase() {
                    t.to_ascii_uppercase()
                } else {
                    t.to_string()
                }
            } else {
                match (ty, s.precision) {
                    ('\0', None) if braces => shortest(a),
                    ('f' | 'F', p) => format!("{a:.*}", p.unwrap_or(6)),
                    ('e' | 'E', p) => sci(a, p.unwrap_or(6), ty == 'E'),
                    ('g' | 'G' | '\0', p) => general(a, p.unwrap_or(6), s.alt, ty == 'G'),
                    _ => unreachable!(),
                }
            };
            (sign, body)
        }
        (V::Str(t), 's' | '\0') => (
            "",
            match s.precision {
                Some(p) => t.chars().take(p).collect(),
                None => t.clone(),
            },
        ),
        (V::Bool(b), 's' | '\0') => ("", b.to_string()),
        (v, t) => {
            let ty = match v {
                V::Int(_) => "int",
                V::Float(_) => "double",
                V::Str(_) => "string",
                V::Bool(_) => "bool",
            };
            return exec_err!(
                "Invalid Input Error: Invalid type specifier \"{t}\" for formatting a value of type {ty}"
            );
        }
    };
    let body = if s.group && matches!(ty, 'd' | 'i' | 'u' | 'f' | 'F') {
        thousands(&body)
    } else {
        body
    };
    Ok(pad(
        sign,
        &body,
        s,
        matches!(v, V::Int(_) | V::Float(_)) && ty != 's' && ty != 'c',
    ))
}

fn int_sign(n: i128, s: Spec) -> &'static str {
    if n < 0 {
        "-"
    } else if s.plus {
        "+"
    } else if s.space {
        " "
    } else {
        ""
    }
}

fn pad(sign: &str, body: &str, s: Spec, numeric: bool) -> String {
    let len = sign.chars().count() + body.chars().count();
    if len >= s.width {
        return format!("{sign}{body}");
    }
    let gap = s.width - len;
    if s.zero && numeric && s.align.is_none_or(|a| a == '=') {
        return format!("{sign}{}{body}", "0".repeat(gap));
    }
    let fill = s.fill.unwrap_or(' ').to_string();
    let default = if numeric { '>' } else { '<' };
    match s.align.unwrap_or(default) {
        '<' => format!("{sign}{body}{}", fill.repeat(gap)),
        '^' => format!(
            "{}{sign}{body}{}",
            fill.repeat(gap / 2),
            fill.repeat(gap - gap / 2)
        ),
        _ => format!("{}{sign}{body}", fill.repeat(gap)),
    }
}

/// `d.ddde±XX`, C's `%e`.
fn sci(a: f64, p: usize, upper: bool) -> String {
    let r = format!("{a:.p$e}");
    let (m, e) = r.split_once('e').expect("an exponent");
    let e: i32 = e.parse().expect("an integer exponent");
    let s = format!("{m}e{}{:02}", if e < 0 { '-' } else { '+' }, e.abs());
    if upper { s.to_ascii_uppercase() } else { s }
}

/// C's `%g`: `%e` or `%f` by the exponent, trailing zeros dropped unless `#`.
fn general(a: f64, p: usize, alt: bool, upper: bool) -> String {
    let p = p.max(1);
    let exp: i32 = if a == 0.0 {
        0
    } else {
        format!("{a:.*e}", p - 1)
            .split_once('e')
            .expect("an exponent")
            .1
            .parse()
            .expect("an integer")
    };
    if exp < -4 || exp >= p as i32 {
        let s = sci(a, p - 1, upper);
        if alt {
            s
        } else {
            let (m, e) = s.split_once(['e', 'E']).expect("an exponent");
            format!("{}{}{e}", trim(m), if upper { 'E' } else { 'e' })
        }
    } else {
        let s = format!("{a:.*}", (p as i32 - 1 - exp) as usize);
        if alt { s } else { trim(&s).to_string() }
    }
}

fn trim(s: &str) -> &str {
    if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.')
    } else {
        s
    }
}

/// fmt's `{}` for a double: the shortest digits that round-trip, fixed between 1e-5 and 1e16.
fn shortest(a: f64) -> String {
    let r = format!("{a:e}");
    let (m, e) = r.split_once('e').expect("an exponent");
    let e: i32 = e.parse().expect("an integer exponent");
    if (-5..16).contains(&e) {
        let s = format!("{a}");
        if s.contains('.') { s } else { s + ".0" }
    } else {
        format!("{m}e{}{:02}", if e < 0 { '-' } else { '+' }, e.abs())
    }
}

/// `1234567.5` as `1,234,567.5`: commas every three digits of the integer part.
fn thousands(body: &str) -> String {
    let (int, rest) = body
        .find(|c: char| !c.is_ascii_digit())
        .map_or((body, ""), |i| body.split_at(i));
    let mut out = String::with_capacity(body.len() + int.len() / 3);
    for (i, c) in int.chars().enumerate() {
        if i > 0 && (int.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out.push_str(rest);
    out
}
