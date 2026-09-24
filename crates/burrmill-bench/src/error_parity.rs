//! `error-parity` (roadmap 6.4): do Burrmill's errors land in the class nuthatch's hints key on?
//!
//! nuthatch's `sql_errors::enrich` classifies a failure off DuckDB's wording. Each bad query here
//! runs on DuckDB (a view over the segments, as nuthatch builds one) and on `burrmill::Engine`, and
//! both messages go through the same extraction nuthatch performs. Same class and same extracted
//! name is a pass; the hint nuthatch writes from them is then the same.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

/// nuthatch `sql_errors.rs` `between`, verbatim.
fn between<'a>(s: &'a str, a: &str, b: &str) -> Option<&'a str> {
    let start = s.find(a)? + a.len();
    let rest = &s[start..];
    let end = rest.find(b)?;
    Some(&rest[..end])
}

/// nuthatch `sql_errors.rs` `quoted_after`, verbatim.
fn quoted_after(s: &str, marker: &str) -> Option<String> {
    let after = &s[s.find(marker)? + marker.len()..];
    let open = after.find('"')? + 1;
    let rest = &after[open..];
    let close = rest.find('"')?;
    Some(rest[..close].to_string())
}

/// The class `enrich` would pick, in its own order, with what it extracts.
fn classify(raw: &str) -> String {
    if let Some(name) = between(raw, "Table with name ", " does not exist") {
        return format!("unknown-table({})", name.trim());
    }
    if let Some(col) = quoted_after(raw, "Referenced column ") {
        return format!("unknown-column({col})");
    }
    if raw.contains("syntax error") {
        return "syntax".into();
    }
    if raw.contains("No function matches") && raw.contains("VARCHAR") {
        let bool_agg = raw.contains("bool_and(VARCHAR)") || raw.contains("bool_or(VARCHAR)");
        return if bool_agg { "bool-aggregate".into() } else { "function-on-varchar".into() };
    }
    if raw.contains("Cannot mix values of type") && raw.contains("VARCHAR") && raw.contains("BOOLEAN") {
        return "mixed-varchar-boolean".into();
    }
    if raw.contains("Out of Memory Error") {
        return "out-of-memory".into();
    }
    if raw.contains("Invalid Error: don't know what type:") {
        return "corrupt-segment".into();
    }
    "unclassified".into()
}

/// Differences that are not error wording, with the roadmap item that owns them.
const KNOWN: &[(&str, &str)] = &[
    ("SELECT \"Value\" FROM token__transfer", "6.5: DuckDB quoted identifiers are case-insensitive"),
    ("SELECT from FROM token__transfer", "6.7: sqlparser reads `from` as a table name, DuckDB as a syntax error"),
    ("SELECT count(*) FROM token__transfer WHERE enabled AND true", "6.5: DuckDB casts VARCHAR to BOOLEAN implicitly"),
];

const CASES: &[&str] = &[
    "SELECT * FROM nosuch",
    "SELECT count(*) FROM token__transfers",
    "SELECT valu FROM token__transfer",
    "SELECT t.valu FROM token__transfer t",
    "SELECT \"Value\" FROM token__transfer",
    "SELECT from FROM token__transfer",
    "SELECT sum(value) FROM token__transfer",
    "SELECT avg(\"from\") FROM token__transfer",
    "SELECT bool_and(enabled) FROM token__transfer",
    "SELECT count(*) FROM token__transfer WHERE enabled AND true",
    "SELECT COALESCE(enabled, true) FROM token__transfer",
    "SELECT CASE WHEN block_number > 1 THEN enabled ELSE false END FROM token__transfer",
    "SELEC 1",
    "SELECT 1 FROM",
];

fn fixture(dir: &Path) -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("from", DataType::Utf8, true),
        Field::new("to", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("enabled", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![1u64, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["0xa", "0xb"])),
            Arc::new(StringArray::from(vec!["0xb", "0xc"])),
            Arc::new(StringArray::from(vec!["10", "4"])),
            Arc::new(StringArray::from(vec!["true", "false"])),
        ],
    )?;
    let f = std::fs::File::create(dir.join(format!("token__transfer-{:064x}.parquet", 1)))?;
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None)?;
    w.write(&batch)?;
    w.close()?;
    Ok(())
}

pub fn run() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    fixture(tmp.path())?;
    let duck = duckdb::Connection::open_in_memory()?;
    duck.execute_batch(&format!(
        "CREATE VIEW token__transfer AS SELECT * FROM read_parquet('{}/*.parquet');",
        tmp.path().display()
    ))?;
    let engine = std::thread::scope(|s| {
        s.spawn(|| burrmill::Engine::open_segments(tmp.path())).join().expect("engine thread")
    })?;
    let mut failed = 0;
    for sql in CASES {
        let d = match duck.prepare(sql).and_then(|mut s| s.query([]).map(|_| ())) {
            Ok(()) => "OK".to_string(),
            Err(e) => e.to_string(),
        };
        let b = std::thread::scope(|s| {
            s.spawn(|| match engine.sql(sql) {
                Ok(_) => "OK".to_string(),
                Err(e) => e.to_string(),
            })
            .join()
            .expect("engine thread")
        });
        let (dc, bc) = (classify(&d), classify(&b));
        let ok = (d == "OK") == (b == "OK") && dc == bc;
        let known = KNOWN.iter().find(|(k, _)| k == sql).map(|(_, why)| *why);
        if !ok && known.is_none() {
            failed += 1;
        }
        let tag = match (ok, known) {
            (true, _) => "SAME ".to_string(),
            (false, Some(why)) => format!("KNOWN ({why})"),
            (false, None) => "DIFF ".to_string(),
        };
        println!("{tag}  {sql}");
        println!("    duckdb   [{dc}] {}", d.lines().next().unwrap_or(""));
        println!("    burrmill [{bc}] {}", b.replace('\n', " | "));
    }
    println!("ERRORS\tcases={}\tdiffering={failed}", CASES.len());
    // The engine owns a runtime, and this function runs inside another.
    std::thread::spawn(move || drop(engine)).join().expect("drop engine");
    anyhow::ensure!(failed == 0, "{failed} error classes differ");
    Ok(())
}
