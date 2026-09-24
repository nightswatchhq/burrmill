//! `encode-parity` (roadmap 6.4): Burrmill's JSON encoder against nuthatch's, byte for byte.
//!
//! Both sides encode the same DuckDB result. nuthatch's side is its own code: the #1433 rewrite that
//! casts scaled decimals to VARCHAR, then `value_to_json` over duckdb-rs's `ValueRef`, copied
//! verbatim from `analytics.rs`. Burrmill's side takes DuckDB's Arrow batches, re-reads them in
//! Burrmill's Arrow version over IPC, and encodes them with `burrmill::df::encode`. Any difference is
//! the encoder's, since the input is identical.

use duckdb::types::ValueRef;
use serde_json::{Map, Value};

/// nuthatch `src/analytics.rs` `value_to_json`, at 5513498. Verbatim.
fn value_to_json(v: ValueRef<'_>) -> Value {
    match v {
        ValueRef::Null => Value::Null,
        ValueRef::Boolean(b) => Value::Bool(b),
        ValueRef::TinyInt(i) => Value::from(i),
        ValueRef::SmallInt(i) => Value::from(i),
        ValueRef::Int(i) => Value::from(i),
        ValueRef::BigInt(i) => Value::from(i),
        ValueRef::UTinyInt(i) => Value::from(i),
        ValueRef::USmallInt(i) => Value::from(i),
        ValueRef::UInt(i) => Value::from(i),
        ValueRef::UBigInt(i) => Value::from(i),
        ValueRef::Float(f) => Value::from(f),
        ValueRef::Double(f) => Value::from(f),
        ValueRef::HugeInt(i) => Value::String(i.to_string()),
        ValueRef::Text(bytes) => Value::String(String::from_utf8_lossy(bytes).into_owned()),
        // Timestamps, decimals, nested types etc. - stringify for the skeleton surface.
        other => Value::String(format!("{other:?}")),
    }
}

const CORPUS: &[&str] = &[
    "SELECT 1::TINYINT a, -2::SMALLINT b, 3::INTEGER c, (-9223372036854775808)::BIGINT d, \
     255::UTINYINT e, 65535::USMALLINT f, 4294967295::UINTEGER g, 18446744073709551615::UBIGINT h",
    "SELECT 0.1::FLOAT a, 0.1::DOUBLE b, 'nan'::DOUBLE c, 'inf'::DOUBLE d, -0.0::DOUBLE e, \
     1e300::DOUBLE f, 5.0::DOUBLE g, (1/3)::DOUBLE h, 3.4e38::FLOAT i",
    "SELECT true a, false b, NULL::BOOLEAN c, NULL::INTEGER d, NULL::VARCHAR e, NULL::HUGEINT f, \
     NULL::DECIMAL(10,2) g, NULL::TIMESTAMP h, NULL n",
    "SELECT 'héllo' a, '' b, 'with \"quotes\" and \\ back' c, chr(10) d, '🦉' e",
    "SELECT (-170141183460469231731687303715884105728)::HUGEINT a, \
     170141183460469231731687303715884105727::HUGEINT b, \
     99999999999999999999999999999999999999::DECIMAL(38,0) c, 0::HUGEINT d, (-1)::DECIMAL(18,0) e",
    "SELECT 1.5::DECIMAL(10,4) a, (-0.05)::DECIMAL(3,2) b, 0::DECIMAL(5,3) c, \
     12345678901234567890123.456789::DECIMAL(38,6) d, (-1)::DECIMAL(18,2) e, \
     (-99999999999999999999999999999.999999999)::DECIMAL(38,9) f, 0.5::DECIMAL(1,1) g",
    "SELECT TIMESTAMP '2024-01-02 03:04:05.678901' a, TIMESTAMP_S '2024-01-02 03:04:05' b, \
     TIMESTAMP_MS '2024-01-02 03:04:05.678' c, TIMESTAMP_NS '2024-01-02 03:04:05.678901234' d, \
     TIMESTAMPTZ '2024-01-02 03:04:05+00' e, TIMESTAMP '1900-01-01' f",
    "SELECT DATE '2024-01-02' a, DATE '1960-06-30' b, TIME '03:04:05.123456' c, \
     INTERVAL '1 month 2 days 3 seconds' d, INTERVAL '-1 year' e",
    "SELECT '\\x01\\x02'::BLOB a, 'abc'::BLOB b, ''::BLOB c",
    "SELECT sum(x) s, avg(x) a, count(*) c, min(x) mn, max(x)::HUGEINT mx FROM range(10) t(x)",
    "SELECT sum(x::DECIMAL(38,0)) s, sum(x::DECIMAL(20,3)) d, avg(x::DECIMAL(20,3)) v FROM range(5) t(x)",
    "SELECT x, x::HUGEINT * 1000000000000000000000 big, x::VARCHAR s, x % 3 = 0 b FROM range(5000) t(x)",
    "SELECT (-0.05)::DECIMAL(2,2) a, 0::DECIMAL(3,3) b, 0.123::DECIMAL(3,3) c, \
     (-0.00000000000000000000000000000000000001)::DECIMAL(38,38) d, 0.1::DECIMAL(2,1) e, \
     (-0.1)::DECIMAL(4,3) f, 0::DECIMAL(1,1) g",
    "SELECT 1 a, 2 a, 3 \"A\"",
    "SELECT * FROM (VALUES (1, 'x'), (NULL, NULL), (3, 'z')) v(k, s) ORDER BY k NULLS FIRST",
];

/// Refused by Burrmill by design; listed so the run says so rather than skipping silently.
const NESTED: &[&str] = &["SELECT [1, 2] l", "SELECT {'a': 1} s"];

/// nuthatch `decimal_safe_sql`, reduced to what it does to a result: scaled decimals become VARCHAR.
pub(crate) fn nuthatch_rows(conn: &duckdb::Connection, sql: &str) -> anyhow::Result<Value> {
    let mut stmt = conn.prepare(sql)?;
    let schema = stmt.query_arrow([])?.get_schema();
    let scaled = schema
        .fields()
        .iter()
        .any(|f| matches!(f.data_type(), duckdb::arrow::datatypes::DataType::Decimal128(_, s) if *s != 0));
    let sql = if scaled {
        let proj: Vec<String> = schema
            .fields()
            .iter()
            .map(|f| {
                let ident = format!("\"{}\"", f.name().replace('"', "\"\""));
                match f.data_type() {
                    duckdb::arrow::datatypes::DataType::Decimal128(_, s) if *s != 0 => {
                        format!("CAST({ident} AS VARCHAR) AS {ident}")
                    }
                    _ => ident,
                }
            })
            .collect();
        format!("SELECT {} FROM ({sql}) AS \"__nuthatch_decimal_source\"", proj.join(", "))
    } else {
        sql.to_owned()
    };
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    let names: Vec<String> =
        rows.as_ref().map(|s| s.column_names().iter().map(|c| c.to_string()).collect()).unwrap_or_default();
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        let mut obj = Map::new();
        for (i, name) in names.iter().enumerate() {
            obj.insert(name.clone(), value_to_json(row.get_ref(i)?));
        }
        out.push(Value::Object(obj));
    }
    Ok(Value::Array(out))
}

fn burrmill_rows(conn: &duckdb::Connection, sql: &str) -> anyhow::Result<Value> {
    let mut stmt = conn.prepare(sql)?;
    let arrow = stmt.query_arrow([])?;
    let schema = arrow.get_schema();
    let mut buf = Vec::new();
    {
        let mut w = arrow58::ipc::writer::StreamWriter::try_new(&mut buf, &schema)?;
        for b in arrow {
            w.write(&b)?;
        }
        w.finish()?;
    }
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(buf), None)?;
    let mut out = Vec::new();
    for b in reader {
        out.extend(burrmill::df::encode::rows(&b?)?);
    }
    Ok(Value::Array(out))
}

fn clip(s: &str, from: usize, n: usize) -> String {
    s.chars().skip(from).take(n).collect()
}

pub fn run() -> anyhow::Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    let mut failed = 0;
    for sql in CORPUS {
        let want = serde_json::to_string(&nuthatch_rows(&conn, sql)?)?;
        let got = match burrmill_rows(&conn, sql) {
            Ok(v) => serde_json::to_string(&v)?,
            Err(e) => format!("ERROR {e}"),
        };
        if want == got {
            println!("OK        {} bytes  {}", want.len(), clip(sql, 0, 70));
        } else {
            failed += 1;
            let (w, g): (Vec<char>, Vec<char>) = (want.chars().collect(), got.chars().collect());
            let at = w.iter().zip(&g).position(|(a, b)| a != b).unwrap_or(w.len().min(g.len()));
            println!("MISMATCH  {}", clip(sql, 0, 70));
            println!("  nuthatch: …{}…", clip(&want, at.saturating_sub(60), 140));
            println!("  burrmill: …{}…", clip(&got, at.saturating_sub(60), 140));
        }
    }
    for sql in NESTED {
        match burrmill_rows(&conn, sql) {
            Err(e) => println!("REFUSED   {sql}: {e}"),
            Ok(_) => {
                failed += 1;
                println!("MISMATCH  {sql}: nested types should be refused");
            }
        }
    }
    println!("ENCODE\tqueries={}\tfailed={failed}", CORPUS.len() + NESTED.len());
    anyhow::ensure!(failed == 0, "{failed} encoder mismatches");
    Ok(())
}
