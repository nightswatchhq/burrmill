//! DuckDB's `list_reduce`, folded in list order per row. Expected values are DuckDB 1.5's.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

fn write(segs: &Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("kind", DataType::Utf8, true),
        Field::new("amount", DataType::UInt64, true),
        Field::new("block_number", DataType::UInt64, false),
    ]));
    let s = |v: Vec<Option<&str>>| Arc::new(StringArray::from(v)) as ArrayRef;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            s(vec![
                Some("a"),
                Some("a"),
                Some("b"),
                Some("a"),
                Some("c"),
                Some("b"),
                Some("a"),
            ]),
            s(vec![
                Some("in"),
                Some("in"),
                Some("in"),
                Some("out"),
                Some("in"),
                Some("out"),
                Some("in"),
            ]),
            Arc::new(UInt64Array::from(vec![
                Some(3),
                Some(1),
                Some(7),
                Some(2),
                Some(5),
                None,
                Some(4),
            ])),
            Arc::new(UInt64Array::from(vec![2, 1, 1, 3, 1, 2, 4])),
        ],
    )
    .unwrap();
    let f = std::fs::File::create(segs.join(format!("ev-{:064x}.parquet", 1))).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn engine() -> (tempfile::TempDir, Engine) {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    write(&segs);
    let e = Engine::open_segments(&segs).unwrap();
    (tmp, e)
}

fn text(e: &Engine, sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in e.sql(sql).unwrap_or_else(|err| panic!("{sql}\n{err}")) {
        let cols: Vec<ArrayRef> = b
            .columns()
            .iter()
            .map(|c| arrow::compute::cast(c, &DataType::Utf8).unwrap())
            .collect();
        for i in 0..b.num_rows() {
            out.push(
                cols.iter()
                    .map(|c| {
                        let s = c.as_any().downcast_ref::<StringArray>().unwrap();
                        if s.is_null(i) {
                            "NULL".to_string()
                        } else {
                            s.value(i).to_string()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("|"),
            );
        }
    }
    out.sort();
    out
}

#[test]
fn folds_each_group_in_list_order() {
    let (_t, e) = engine();
    // a: 1, 3, 2, 4 by block; b: 7, NULL.
    assert_eq!(
        text(
            &e,
            "SELECT id, list_reduce(list(amount ORDER BY block_number), lambda acc, x: acc * 10 + x) \
             FROM ev GROUP BY id"
        ),
        vec!["a|1324", "b|NULL", "c|5"]
    );
}

#[test]
fn folds_a_struct_accumulator() {
    let (_t, e) = engine();
    let sql = "SELECT id, st.n, st.total FROM (SELECT id, list_reduce(
            list_prepend({'kind': 'init', 'n': 0.0::DOUBLE, 'total': 0.0::DOUBLE},
                         list({'kind': kind, 'n': CAST(amount AS DOUBLE), 'total': 0.0::DOUBLE} ORDER BY block_number)),
            lambda acc, e: CASE WHEN e.kind = 'in'
              THEN {'kind': 's', 'n': acc.n + e.n, 'total': acc.total + e.n}
              ELSE {'kind': 's', 'n': acc.n - e.n, 'total': acc.total} END) AS st
          FROM ev WHERE amount IS NOT NULL GROUP BY id)";
    assert_eq!(text(&e, sql), vec!["a|6.0|8.0", "b|7.0|7.0", "c|5.0|5.0"]);
}

#[test]
fn casts_back_captures_and_keeps_nulls() {
    let (_t, e) = engine();
    assert_eq!(
        text(
            &e,
            "SELECT list_reduce([1, 2, 3], lambda a, x: a::VARCHAR || x::VARCHAR)"
        ),
        vec!["123"]
    );
    assert_eq!(
        text(&e, "SELECT list_reduce([1, NULL, 3], lambda a, x: a + x)"),
        vec!["NULL"]
    );
    assert_eq!(
        text(&e, "SELECT list_reduce(NULL::INT[], lambda a, x: a + x)"),
        vec!["NULL"]
    );
    assert_eq!(
        text(
            &e,
            "SELECT id, list_reduce([10, 20], lambda a, x: a + x + block_number) FROM ev \
             WHERE id = 'b'"
        ),
        vec!["b|31", "b|32"]
    );
    let err = e
        .sql("SELECT list_reduce(l, lambda a, x: a + x) FROM (VALUES ([1, 2]), ([]::INT[])) t(l)")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("Cannot perform list_reduce on an empty input list"),
        "{err}"
    );
}
