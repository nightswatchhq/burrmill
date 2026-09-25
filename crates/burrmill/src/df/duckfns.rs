//! DuckDB functions DataFusion lacks or spells differently, each as measured on DuckDB 1.5 with
//! `burrmill-bench duck-eval`. Burrmill runs in UTC, so every timestamp here is read in UTC.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int64Builder, Int8Builder, StringBuilder};
use arrow::compute::cast;
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{DateTime, Datelike, NaiveDateTime, Timelike};
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};

fn udf(f: impl ScalarUDFImpl + 'static) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(f))
}

pub fn all() -> Vec<Arc<ScalarUDF>> {
    vec![
        udf(Strftime(Signature::user_defined(Volatility::Immutable))),
        udf(DateDiff(Signature::user_defined(Volatility::Immutable), vec!["datediff".into()])),
        udf(RegexpExtract(Signature::user_defined(Volatility::Immutable))),
        udf(Sign(Signature::user_defined(Volatility::Immutable))),
        udf(RegexpReplace(Signature::user_defined(Volatility::Immutable))),
    ]
}

fn scalar_out(scalar: bool, out: ArrayRef) -> Result<ColumnarValue> {
    Ok(if scalar {
        ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?)
    } else {
        ColumnarValue::Array(out)
    })
}

fn is_time(t: &DataType) -> bool {
    matches!(t, DataType::Timestamp(..) | DataType::Date32 | DataType::Date64)
}

/// Any timestamp or date as microseconds since the epoch, UTC.
fn micros(a: &ArrayRef) -> Result<ArrayRef> {
    Ok(cast(a, &DataType::Timestamp(TimeUnit::Microsecond, None))?)
}

fn naive(us: i64) -> Option<NaiveDateTime> {
    DateTime::from_timestamp_micros(us).map(|d| d.naive_utc())
}

/// `CAST(timestamp AS VARCHAR)` as DuckDB writes it: a space, the fraction's trailing zeros
/// dropped, and `+00` on one with a zone.
pub fn timestamp_text(us: i64, zoned: bool) -> Option<String> {
    let t = naive(us)?;
    let mut s = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        t.year(),
        t.month(),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    );
    let frac = us.rem_euclid(1_000_000);
    if frac != 0 {
        s.push_str(format!(".{frac:06}").trim_end_matches('0'));
    }
    if zoned {
        s.push_str("+00");
    }
    Some(s)
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TimestampText(pub Signature);

impl ScalarUDFImpl for TimestampText {
    fn name(&self) -> &str {
        "burrmill_timestamp_text"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        Ok(args.to_vec())
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let zoned = matches!(a.data_type(), DataType::Timestamp(_, Some(_)));
        let us = micros(&a)?;
        let us = us.as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
        let mut b = StringBuilder::new();
        for i in 0..us.len() {
            match us.is_valid(i).then(|| timestamp_text(us.value(i), zoned)).flatten() {
                Some(s) => b.append_value(s),
                None => b.append_null(),
            }
        }
        scalar_out(scalar, Arc::new(b.finish()))
    }
}

/// DuckDB's `strftime` specifiers, which are not chrono's: `%f` is microseconds and `%g`
/// milliseconds. One it does not know is refused, as DuckDB refuses it.
fn strftime(t: NaiveDateTime, fmt: &str) -> Result<String> {
    const DAYS: [&str; 7] = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
    const MONTHS: [&str; 12] = [
        "January", "February", "March", "April", "May", "June", "July", "August", "September",
        "October", "November", "December",
    ];
    let mut out = String::new();
    let mut chars = fmt.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let mut spec = chars.next();
        let bare = spec == Some('-');
        if bare {
            spec = chars.next();
        }
        let two = |n: u32| if bare { n.to_string() } else { format!("{n:02}") };
        let hour12 = if t.hour() % 12 == 0 { 12 } else { t.hour() % 12 };
        let wd = t.weekday().num_days_from_monday() as usize;
        match spec {
            Some('Y') => out.push_str(&format!("{:04}", t.year())),
            Some('y') => out.push_str(&two((t.year().rem_euclid(100)) as u32)),
            Some('m') => out.push_str(&two(t.month())),
            Some('d') => out.push_str(&two(t.day())),
            Some('H') => out.push_str(&two(t.hour())),
            Some('I') => out.push_str(&two(hour12)),
            Some('M') => out.push_str(&two(t.minute())),
            Some('S') => out.push_str(&two(t.second())),
            Some('p') => out.push_str(if t.hour() < 12 { "AM" } else { "PM" }),
            Some('j') => out.push_str(&if bare { t.ordinal().to_string() } else { format!("{:03}", t.ordinal()) }),
            Some('b') => out.push_str(&MONTHS[t.month0() as usize][..3]),
            Some('B') => out.push_str(MONTHS[t.month0() as usize]),
            Some('a') => out.push_str(&DAYS[wd][..3]),
            Some('A') => out.push_str(DAYS[wd]),
            Some('w') => out.push_str(&t.weekday().num_days_from_sunday().to_string()),
            Some('u') => out.push_str(&t.weekday().number_from_monday().to_string()),
            Some('f') => out.push_str(&format!("{:06}", t.nanosecond() / 1000)),
            Some('g') => out.push_str(&format!("{:03}", t.nanosecond() / 1_000_000)),
            Some('%') => out.push('%'),
            _ => {
                return exec_err!(
                    "Invalid Input Error: Failed to parse format specifier {fmt}: Unrecognized format for strftime/strptime: %{}",
                    spec.map(String::from).unwrap_or_default()
                );
            }
        }
    }
    Ok(out)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Strftime(Signature);

impl ScalarUDFImpl for Strftime {
    fn name(&self) -> &str {
        "strftime"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args {
            [t, f] if is_time(t) || t.is_null() => Ok(vec![t.clone(), coerce_text(f)]),
            _ => plan_err!("strftime takes a timestamp or date and a format"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args.args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let us = micros(&arrays[0])?;
        let us = us.as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
        let fmt = cast(&arrays[1], &DataType::Utf8)?;
        let fmt = fmt.as_string::<i32>();
        let mut b = StringBuilder::new();
        for i in 0..us.len() {
            if us.is_null(i) || fmt.is_null(i) {
                b.append_null();
                continue;
            }
            match naive(us.value(i)) {
                Some(t) => b.append_value(strftime(t, fmt.value(i))?),
                None => b.append_null(),
            }
        }
        scalar_out(scalar, Arc::new(b.finish()))
    }
}

fn coerce_text(t: &DataType) -> DataType {
    if matches!(t, DataType::Utf8View | DataType::LargeUtf8) { t.clone() } else { DataType::Utf8 }
}

/// Boundaries crossed between `a` and `b`, as DuckDB counts them: calendar months for month,
/// quarter and year, truncated units below a day, and whole weeks of days for week.
fn date_diff(part: &str, a: NaiveDateTime, b: NaiveDateTime, ua: i64, ub: i64) -> Result<i64> {
    let months = |t: NaiveDateTime| t.year() as i64 * 12 + t.month0() as i64;
    let floor = |u: i64, unit: i64| u.div_euclid(unit);
    Ok(match part.to_ascii_lowercase().trim_end_matches('s') {
        "year" | "yr" | "y" | "years" => b.year() as i64 - a.year() as i64,
        "quarter" => (months(b) / 3) - (months(a) / 3),
        "month" | "mon" => months(b) - months(a),
        "week" | "w" => (floor(ub, 86_400_000_000) - floor(ua, 86_400_000_000)) / 7,
        "day" | "d" | "dayofmonth" => floor(ub, 86_400_000_000) - floor(ua, 86_400_000_000),
        "hour" | "h" | "hr" => floor(ub, 3_600_000_000) - floor(ua, 3_600_000_000),
        "minute" | "min" | "m" => floor(ub, 60_000_000) - floor(ua, 60_000_000),
        "second" | "sec" | "s" => floor(ub, 1_000_000) - floor(ua, 1_000_000),
        "millisecond" | "ms" | "msec" => floor(ub, 1_000) - floor(ua, 1_000),
        "microsecond" | "us" | "usec" => match ub.checked_sub(ua) {
            Some(v) => v,
            None => return exec_err!("Overflow in date_diff of {ub} - {ua} microseconds"),
        },
        other => return exec_err!("date_diff: unit {other} is not supported here"),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct DateDiff(Signature, Vec<String>);

impl ScalarUDFImpl for DateDiff {
    fn name(&self) -> &str {
        "date_diff"
    }
    fn aliases(&self) -> &[String] {
        &self.1
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args {
            [p, a, b] if (is_time(a) || a.is_null()) && (is_time(b) || b.is_null()) => {
                Ok(vec![coerce_text(p), a.clone(), b.clone()])
            }
            _ => plan_err!("date_diff takes a part and two timestamps or dates"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args.args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let part = cast(&arrays[0], &DataType::Utf8)?;
        let part = part.as_string::<i32>();
        let (a, b) = (micros(&arrays[1])?, micros(&arrays[2])?);
        let (a, b) = (
            a.as_primitive::<arrow::datatypes::TimestampMicrosecondType>(),
            b.as_primitive::<arrow::datatypes::TimestampMicrosecondType>(),
        );
        let mut out = Int64Builder::new();
        for i in 0..a.len() {
            if part.is_null(i) || a.is_null(i) || b.is_null(i) {
                out.append_null();
                continue;
            }
            let (ua, ub) = (a.value(i), b.value(i));
            match (naive(ua), naive(ub)) {
                (Some(ta), Some(tb)) => out.append_value(date_diff(part.value(i), ta, tb, ua, ub)?),
                _ => out.append_null(),
            }
        }
        scalar_out(scalar, Arc::new(out.finish()))
    }
}

/// `regexp_extract(s, pattern[, group])`: the first match's group (0, the whole match, unless
/// given), `''` where nothing matches, NULL for a NULL string.
#[derive(Debug, PartialEq, Eq, Hash)]
struct RegexpExtract(Signature);

impl ScalarUDFImpl for RegexpExtract {
    fn name(&self) -> &str {
        "regexp_extract"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args {
            [s, p] => Ok(vec![coerce_text(s), coerce_text(p)]),
            [s, p, g] if g.is_integer() || g.is_null() => Ok(vec![coerce_text(s), coerce_text(p), DataType::Int64]),
            _ => plan_err!("regexp_extract takes a string, a pattern and an optional group"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args.args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let s = cast(&arrays[0], &DataType::Utf8)?;
        let s = s.as_string::<i32>();
        let p = cast(&arrays[1], &DataType::Utf8)?;
        let p = p.as_string::<i32>();
        let g = arrays.get(2).map(|g| cast(g, &DataType::Int64)).transpose()?;
        let mut cache: Option<(String, regex::Regex)> = None;
        let mut b = StringBuilder::new();
        for i in 0..s.len() {
            if s.is_null(i) || p.is_null(i) {
                b.append_null();
                continue;
            }
            let group = g
                .as_ref()
                .map(|g| g.as_primitive::<arrow::datatypes::Int64Type>())
                .map_or(0, |g| if g.is_null(i) { 0 } else { g.value(i) });
            let pat = p.value(i);
            if cache.as_ref().is_none_or(|(c, _)| c != pat) {
                let re = regex::Regex::new(pat)
                    .map_err(|e| datafusion_common::DataFusionError::Execution(format!("regexp_extract: {e}")))?;
                cache = Some((pat.to_string(), re));
            }
            let re = &cache.as_ref().expect("compiled").1;
            let found = re
                .captures(s.value(i))
                .and_then(|c| c.get(group.max(0) as usize).map(|m| m.as_str().to_string()));
            b.append_value(found.unwrap_or_default());
        }
        scalar_out(scalar, Arc::new(b.finish()))
    }
}

/// DuckDB's replacement text, RE2's: `\0`-`\9` for groups, `\\` for a backslash. `None` when it
/// names a group the pattern lacks, where RE2 refuses the rewrite and DuckDB keeps the string.
fn rewrite(caps: &regex::Captures, repl: &str, groups: usize) -> Option<String> {
    let mut out = String::new();
    let mut chars = repl.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('\\') => out.push('\\'),
            Some(d @ '0'..='9') => {
                let n = d as usize - '0' as usize;
                if n > groups {
                    return None;
                }
                out.push_str(caps.get(n).map_or("", |m| m.as_str()));
            }
            _ => return None,
        }
    }
    Some(out)
}

/// `regexp_replace(s, pattern, replacement[, options])` as DuckDB does it: the first match only
/// unless `g`, `i` for case, and a rewrite naming a missing group leaving the string as it was.
#[derive(Debug, PartialEq, Eq, Hash)]
struct RegexpReplace(Signature);

impl ScalarUDFImpl for RegexpReplace {
    fn name(&self) -> &str {
        "regexp_replace"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args.len() {
            3 | 4 => Ok(args.iter().map(coerce_text).collect()),
            _ => plan_err!("regexp_replace takes a string, a pattern, a replacement and options"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args.args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let text: Vec<ArrayRef> = arrays.iter().map(|a| cast(a, &DataType::Utf8)).collect::<std::result::Result<_, _>>()?;
        let col = |i: usize| text.get(i).map(|a| a.as_string::<i32>());
        let (s, p, r, o) = (col(0).expect("s"), col(1).expect("p"), col(2).expect("r"), col(3));
        let mut cache: Option<(String, String, regex::Regex)> = None;
        let mut b = StringBuilder::new();
        for i in 0..s.len() {
            if s.is_null(i) || p.is_null(i) || r.is_null(i) || o.is_some_and(|o| o.is_null(i)) {
                b.append_null();
                continue;
            }
            let opts = o.map_or("", |o| o.value(i));
            let (pat, global) = (p.value(i), opts.contains('g'));
            if cache.as_ref().is_none_or(|(cp, co, _)| cp != pat || co != opts) {
                let re = regex::RegexBuilder::new(pat)
                    .case_insensitive(opts.contains('i') && !opts.contains('c'))
                    .dot_matches_new_line(opts.contains('s'))
                    .multi_line(opts.contains('m'))
                    .build()
                    .map_err(|e| datafusion_common::DataFusionError::Execution(format!("regexp_replace: {e}")))?;
                cache = Some((pat.to_string(), opts.to_string(), re));
            }
            let re = &cache.as_ref().expect("compiled").2;
            let groups = re.captures_len() - 1;
            let (src, repl) = (s.value(i), r.value(i));
            let mut out = String::with_capacity(src.len());
            let mut last = 0;
            let mut kept = true;
            for caps in re.captures_iter(src) {
                let m = caps.get(0).expect("whole match");
                let Some(with) = rewrite(&caps, repl, groups) else {
                    kept = false;
                    break;
                };
                out.push_str(&src[last..m.start()]);
                out.push_str(&with);
                last = m.end();
                if !global {
                    break;
                }
            }
            if kept {
                out.push_str(&src[last..]);
                b.append_value(out);
            } else {
                b.append_value(src);
            }
        }
        scalar_out(scalar, Arc::new(b.finish()))
    }
}

/// `sign(x)`: -1, 0 or 1 as a TINYINT, whatever the numeric type.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Sign(Signature);

impl ScalarUDFImpl for Sign {
    fn name(&self) -> &str {
        "sign"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        match args {
            [t] if t.is_numeric() || t.is_null() => Ok(vec![DataType::Float64]),
            _ => plan_err!("sign takes a number"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let a = a.as_primitive::<arrow::datatypes::Float64Type>();
        let mut b = Int8Builder::new();
        for i in 0..a.len() {
            if a.is_null(i) {
                b.append_null();
            } else {
                let v = a.value(i);
                b.append_value(if v > 0.0 { 1 } else if v < 0.0 { -1 } else { 0 });
            }
        }
        scalar_out(scalar, Arc::new(b.finish()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Each as DuckDB 1.5 printed it (`duck-eval`).
    #[test]
    fn formats_and_differences_as_duckdb() {
        let t = NaiveDateTime::parse_from_str("2024-03-05 07:08:09.123", "%Y-%m-%d %H:%M:%S%.f").unwrap();
        assert_eq!(
            strftime(t, "%Y-%m-%d %H:%M:%S|%b %d|%a|%j|%y|%I %p|%-d|%A %B|%f|%g|%%").unwrap(),
            "2024-03-05 07:08:09|Mar 05|Tue|065|24|07 AM|5|Tuesday March|123000|123|%"
        );
        assert!(strftime(t, "%e").is_err());
        let us = |s: &str| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").unwrap().and_utc().timestamp_micros();
        let d = |p: &str, a: &str, b: &str| date_diff(p, naive(us(a)).unwrap(), naive(us(b)).unwrap(), us(a), us(b)).unwrap();
        assert_eq!(d("day", "2024-01-01 23:00:00", "2024-01-02 01:00:00"), 1);
        assert_eq!(d("hour", "2024-01-01 23:59:00", "2024-01-02 00:01:00"), 1);
        assert_eq!(d("month", "2024-01-31 00:00:00", "2024-02-01 00:00:00"), 1);
        assert_eq!(d("year", "2023-12-31 00:00:00", "2024-01-01 00:00:00"), 1);
        assert_eq!(d("day", "2024-01-05 00:00:00", "2024-01-01 00:00:00"), -4);
        assert_eq!(d("minute", "2024-01-01 00:00:59", "2024-01-01 00:01:00"), 1);
        assert_eq!(d("quarter", "2024-03-31 00:00:00", "2024-04-01 00:00:00"), 1);
        assert_eq!(d("week", "2024-01-06 00:00:00", "2024-01-08 00:00:00"), 0);
        assert_eq!(d("second", "2024-01-01 00:00:00.9", "2024-01-01 00:00:01.1"), 1);
        assert_eq!(timestamp_text(1704067200_000_000, true).unwrap(), "2024-01-01 00:00:00+00");
        let re = regex::Regex::new("^s").unwrap();
        let caps = re.captures("swap").unwrap();
        assert_eq!(rewrite(&caps, "\\2-\\1", 0), None);
        let re = regex::Regex::new("(b)").unwrap();
        let caps = re.captures("abc").unwrap();
        assert_eq!(rewrite(&caps, "\\1\\1[\\0]\\\\", 1).as_deref(), Some("bb[b]\\"));
        assert_eq!(timestamp_text(1704067200_500_000, true).unwrap(), "2024-01-01 00:00:00.5+00");
        assert_eq!(timestamp_text(1704071_523_000_120, false).unwrap(), "2024-01-01 01:12:03.00012");
    }
}
