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
