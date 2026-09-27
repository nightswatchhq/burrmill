//! A host registering tables itself: segment lists, hot rows, declared columns and a window, as
//! nuthatch's shadow will (RFC-0044 Amendment 2, phase 2b).

use std::sync::Arc;

use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;
use serde_json::{Value, json};

fn segment(dir: &std::path::Path, n: u64, rows: &[(u64, &str, &str)]) -> (std::path::PathBuf, u64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("who", DataType::Utf8, false),
        Field::new("amount", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let path = dir.join(format!("t-{n:064x}.parquet"));
    let f = std::fs::File::create(&path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let len = std::fs::metadata(&path).unwrap().len();
    (path, len)
}

fn rows(engine: &Engine, sql: &str) -> Vec<Value> {
    engine
        .sql(sql)
        .unwrap()
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect()
}

fn declared() -> Vec<(String, String)> {
    vec![
        ("block_number".into(), "u64".into()),
        ("who".into(), "address".into()),
        ("amount".into(), "word32".into()),
        ("memo".into(), "string".into()),
    ]
}

#[test]
fn sealed_and_hot_union_with_declared_columns_and_derived_decimals() {
    let tmp = tempfile::tempdir().unwrap();
    let a = segment(tmp.path(), 1, &[(10, "x", "5"), (11, "y", "7")]);
    let b = segment(
        tmp.path(),
        2,
        &[(12, "x", "99999999999999999999999999999999999999999")],
    );
    let hot = vec![
        json!({"block_number": 20, "who": "z", "amount": "1", "memo": "late"}),
        json!({"block_number": 21, "who": "x", "amount": null}),
    ];
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![a, b], &hot, (None, None))
        .unwrap();
    assert!(engine.has_table("t"));
    assert!(engine.has_table("T"));
    assert!(!engine.has_table("t__raw"));

    // `sum` over `UBIGINT` is `HUGEINT`, which nuthatch's JSON carries as text.
    let got = rows(
        &engine,
        "SELECT count(*) AS n, sum(block_number) AS s FROM t",
    );
    assert_eq!(got, vec![json!({"n": 5, "s": "74"})]);

    // The declared column no segment carries is NULL there and read from hot where it has it.
    let got = rows(
        &engine,
        "SELECT block_number, memo FROM t WHERE memo IS NOT NULL ORDER BY block_number",
    );
    assert_eq!(got, vec![json!({"block_number": 20, "memo": "late"})]);

    // `_dec` and `_overflow` beside the wide column, across sealed and hot alike.
    let got = rows(
        &engine,
        "SELECT block_number, amount_dec, amount_overflow FROM t ORDER BY block_number",
    );
    assert_eq!(
        got,
        vec![
            json!({"block_number": 10, "amount_dec": "5", "amount_overflow": false}),
            json!({"block_number": 11, "amount_dec": "7", "amount_overflow": false}),
            json!({"block_number": 12, "amount_dec": null, "amount_overflow": true}),
            json!({"block_number": 20, "amount_dec": "1", "amount_overflow": false}),
            json!({"block_number": 21, "amount_dec": null, "amount_overflow": false}),
        ]
    );
}

#[test]
fn a_window_bounds_the_rows_and_registration_replaces() {
    let tmp = tempfile::tempdir().unwrap();
    let a = segment(
        tmp.path(),
        1,
        &[(10, "x", "5"), (11, "y", "7"), (12, "x", "9")],
    );
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![a.clone()], &[], (Some(10), Some(11)))
        .unwrap();
    let got = rows(&engine, "SELECT block_number FROM t ORDER BY 1");
    assert_eq!(got, vec![json!({"block_number": 11})]);

    engine
        .register_facts("t", &declared(), vec![a], &[], (None, None))
        .unwrap();
    let got = rows(&engine, "SELECT count(*) AS n FROM t");
    assert_eq!(got, vec![json!({"n": 3})]);

    // Declared and never sealed: empty, typed, and its derived columns exist.
    engine
        .register_facts("u", &declared(), vec![], &[], (None, None))
        .unwrap();
    let got = rows(&engine, "SELECT count(*) AS n, sum(amount_dec) AS s FROM u");
    assert_eq!(got, vec![json!({"n": 0, "s": null})]);
}

/// A statement stopped from another thread ends as `Cancelled` at its next batch, and the engine
/// answers the next statement as if nothing happened.
#[test]
fn a_cancel_from_another_thread_stops_the_statement_and_not_the_engine() {
    // A cross join of a sealed table with itself, summed: nothing reaches the output until the
    // join has run to the end, so only a cancel seen by the scan can stop it in time.
    let tmp = tempfile::tempdir().unwrap();
    let many: Vec<(u64, &str, &str)> = (0..8_000u64).map(|i| (i, "x", "1")).collect();
    let seg = segment(tmp.path(), 1, &many);
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![seg], &[], (None, None))
        .unwrap();
    let token = engine.cancel_token();
    let stopper = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(150));
        token.cancel();
    });
    let started = std::time::Instant::now();
    let r = engine.sql("SELECT sum(a.block_number * b.block_number) AS s FROM t a, t b");
    stopper.join().unwrap();
    assert!(
        matches!(r, Err(burrmill::BurrmillError::Cancelled)),
        "{:?}",
        r.map(|_| ())
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(20));
    assert_eq!(rows(&engine, "SELECT 1 AS one"), vec![json!({"one": 1})]);
}

#[test]
fn rows_become_a_text_table() {
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_rows(
            "labels",
            &[
                json!({"address": "0xab", "label": "exchange"}),
                json!({"address": "0xcd", "label": null}),
            ],
        )
        .unwrap();
    let got = rows(
        &engine,
        "SELECT address, label FROM labels ORDER BY address",
    );
    assert_eq!(
        got,
        vec![
            json!({"address": "0xab", "label": "exchange"}),
            json!({"address": "0xcd", "label": null}),
        ]
    );
}
