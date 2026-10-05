//! A hash join's dynamic filter reaches the probe scan only over keys the scan reads as columns.
//! Over a key computed there, text cast to a number as nuthatch's QoS views join on, it was
//! evaluated for every row read and skipped nothing.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

const ROWS: i64 = 20_000;

fn segment(dir: &std::path::Path, name: &str, n: Vec<i64>) -> (std::path::PathBuf, u64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, false),
        Field::new("n_text", DataType::Utf8, false),
    ]));
    let text: Vec<String> = n.iter().map(|x| x.to_string()).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(n)),
            Arc::new(StringArray::from(text)),
        ],
    )
    .unwrap();
    let path = dir.join(format!("{name}-{:064x}.parquet", 0));
    let f = std::fs::File::create(&path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let len = std::fs::metadata(&path).unwrap().len();
    (path, len)
}

/// `rows` holds 0..ROWS, `keys` every tenth of them.
fn engine(dir: &std::path::Path) -> Engine {
    let mut e = Engine::open_empty_budgeted(burrmill::Budget {
        memory_bytes: 256 << 20,
        threads: 4,
        spill: None,
    })
    .unwrap();
    let rows = segment(dir, "rows", (0..ROWS).collect());
    let keys = segment(dir, "keys", (0..ROWS).step_by(10).collect());
    e.register_facts("rows", &[], vec![rows], &[], (None, None))
        .unwrap();
    e.register_facts("keys", &[], vec![keys], &[], (None, None))
        .unwrap();
    e
}

fn answer(e: &Engine, sql: &str) -> Vec<String> {
    e.sql(sql)
        .unwrap_or_else(|err| panic!("{sql}\n{err}"))
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .map(|r| r.to_string())
        .collect()
}

/// The `rows` scan's line of `EXPLAIN ANALYZE`, once the joins have filled their filters.
fn rows_scan(e: &Engine, sql: &str) -> String {
    let mut plan = String::new();
    for b in e.sql(&format!("EXPLAIN ANALYZE {sql}")).unwrap() {
        for row in burrmill::df::encode::rows(&b).unwrap() {
            plan.push_str(&row.to_string());
        }
    }
    plan.split("\\n")
        .find(|l| l.contains("DataSourceExec") && l.contains("rows-"))
        .unwrap_or_else(|| panic!("no rows scan in {plan}"))
        .to_string()
}

#[test]
fn a_computed_key_leaves_the_scan_unfiltered() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    let sql = "SELECT count(*) AS n FROM rows r JOIN keys k ON k.n = CAST(r.n_text AS BIGINT)";
    assert_eq!(answer(&e, sql), [format!(r#"{{"n":{}}}"#, ROWS / 10)]);
    let line = rows_scan(&e, sql);
    assert!(line.contains("DynamicFilter [ empty ]"), "{line}");
}

#[test]
fn a_column_key_still_filters_the_scan() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    let sql = "SELECT count(*) AS n FROM rows r JOIN keys k ON k.n = r.n";
    assert_eq!(answer(&e, sql), [format!(r#"{{"n":{}}}"#, ROWS / 10)]);
    let line = rows_scan(&e, sql);
    assert!(
        line.contains("DynamicFilter [") && !line.contains("DynamicFilter [ empty ]"),
        "{line}"
    );
}
