//! FoldSubstitution (roadmap 6.6): the owned signed fold inside DataFusion-planned statements.
//!
//! The reference for each answer is the same statement with `WHERE 1 = 1` in an arm, which the
//! rule does not substitute, so it runs on DataFusion's own checked aggregate.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{BurrmillError, Engine};

fn write(segs: &Path, table: &str, i: usize, cols: &[(&str, Vec<Option<&str>>)]) {
    let schema = Arc::new(Schema::new(
        cols.iter()
            .map(|(n, _)| Field::new(*n, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let arrays: Vec<ArrayRef> = cols
        .iter()
        .map(|(_, v)| Arc::new(StringArray::from(v.clone())) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let f = std::fs::File::create(segs.join(format!("{table}-{i:064x}.parquet"))).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// Transfers in two segments, a mint table, and labels.
fn nest() -> (tempfile::TempDir, Engine) {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    write(
        &segs,
        "transfer",
        0,
        &[
            ("from", vec![Some("0xA"), Some("0xb"), Some("0xa")]),
            ("to", vec![Some("0xb"), Some("0xc"), Some("")]),
            ("value", vec![Some("10"), Some("4"), Some("1")]),
        ],
    );
    write(
        &segs,
        "transfer",
        1,
        &[
            ("from", vec![Some("0xc"), Some("0xd")]),
            ("to", vec![Some("0xa"), Some("0xe")]),
            ("value", vec![Some("2"), None]),
        ],
    );
    write(
        &segs,
        "mint",
        0,
        &[
            ("to", vec![Some("0xa"), Some("0xd")]),
            ("amount", vec![Some("100"), Some("5")]),
        ],
    );
    write(
        &segs,
        "label",
        0,
        &[
            ("addr", vec![Some("0xa"), Some("0xb")]),
            ("name", vec![Some("alice"), Some("bob")]),
        ],
    );
    let e = Engine::open_segments(&segs).unwrap();
    (tmp, e)
}

fn rows(e: &Engine, sql: &str) -> Vec<Vec<String>> {
    let batches = e.sql(sql).unwrap_or_else(|err| panic!("{sql}\n{err:?}"));
    let mut out = Vec::new();
    for b in &batches {
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
                            "NULL".into()
                        } else {
                            s.value(i).to_string()
                        }
                    })
                    .collect(),
            );
        }
    }
    out
}

fn explain(e: &Engine, sql: &str) -> String {
    rows(e, &format!("EXPLAIN {sql}"))
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("\n")
}

/// Substituted, visible in EXPLAIN, and the same answer as the unsubstituted statement.
fn substituted(e: &Engine, sql: &str, reference: &str) -> Vec<Vec<String>> {
    let plan = explain(e, sql);
    assert!(plan.contains("OwnedSignedFold"), "not substituted:\n{plan}");
    assert!(
        !explain(e, reference).contains("OwnedSignedFold"),
        "reference was substituted"
    );
    let got = rows(e, sql);
    assert_eq!(got, rows(e, reference), "{sql}");
    got
}

const FOLD: &str = r#"SELECT addr, SUM(d) AS net FROM (
    SELECT "to" AS addr, CAST("value" AS DECIMAL(38,0)) AS d FROM transfer
    UNION ALL
    SELECT "from" AS addr, -CAST("value" AS DECIMAL(38,0)) AS d FROM transfer WHERE_ARM
) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr"#;

fn fold(where_arm: &str) -> String {
    FOLD.replace("WHERE_ARM", where_arm)
}

#[test]
fn the_balance_fold_is_substituted_and_agrees() {
    let (_t, e) = nest();
    let got = substituted(&e, &fold(""), &fold("WHERE 1 = 1"));
    // The fold's rows are already in the key's order; DataFusion should not sort them again.
    let plan = explain(&e, &fold(""));
    assert!(!plan.contains("SortExec"), "{plan}");
    let want: Vec<Vec<String>> = [
        ("", "1"),
        ("0xA", "-10"),
        ("0xa", "1"),
        ("0xb", "6"),
        ("0xc", "2"),
    ]
    .iter()
    .map(|(a, n)| vec![a.to_string(), n.to_string()])
    .collect();
    assert_eq!(got, want);
}

#[test]
fn try_cast_and_lower_and_several_tables() {
    let (_t, e) = nest();
    let sql = |w: &str| {
        format!(
            r#"SELECT addr, SUM(d) FROM (
                 SELECT lower("to") AS addr, TRY_CAST("value" AS DECIMAL(38,0)) AS d FROM transfer
                 UNION ALL
                 SELECT lower("from") AS addr, -TRY_CAST("value" AS DECIMAL(38,0)) AS d FROM transfer {w}
                 UNION ALL
                 SELECT "to" AS addr, TRY_CAST(amount AS DECIMAL(38,0)) AS d FROM mint
               ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr"#
        )
    };
    substituted(&e, &sql(""), &sql("WHERE 1 = 1"));
}

#[test]
fn a_fold_inside_a_larger_statement() {
    let (_t, e) = nest();
    let sql = |w: &str| {
        format!(
            r#"SELECT l.name, b.net FROM ({}) b JOIN label l ON l.addr = b.addr ORDER BY l.name"#,
            fold(w).replace(" ORDER BY addr", "")
        )
    };
    let got = substituted(&e, &sql(""), &sql("WHERE 1 = 1"));
    assert_eq!(
        got,
        vec![
            vec!["alice".to_string(), "1".into()],
            vec!["bob".to_string(), "6".into()]
        ]
    );
}

#[test]
fn nullable_values_without_a_null_rejecting_filter_are_not_substituted() {
    let (_t, e) = nest();
    let sql = fold("").replace("HAVING SUM(d) <> 0 ", "");
    assert!(!explain(&e, &sql).contains("OwnedSignedFold"));
    // 0xe received only a NULL value; DataFusion keeps the party with a NULL sum.
    assert!(rows(&e, &sql).contains(&vec!["0xe".to_string(), "NULL".into()]));
}

#[test]
fn filtered_arms_and_other_aggregates_are_not_substituted() {
    let (_t, e) = nest();
    assert!(!explain(&e, &fold("WHERE \"from\" <> '0xb'")).contains("OwnedSignedFold"));
    let max = fold("")
        .replace("SUM(d) AS net", "MAX(d) AS net")
        .replace("HAVING SUM(d) <> 0", "");
    assert!(!explain(&e, &max).contains("OwnedSignedFold"));
}

#[test]
fn refusals_carry_over() {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    let d38 = "99999999999999999999999999999999999999";
    write(
        &segs,
        "transfer",
        0,
        &[
            ("from", vec![Some("0xa"), Some("0xa")]),
            ("to", vec![Some("0xb"), Some("0xb")]),
            ("value", vec![Some(d38), Some("1")]),
        ],
    );
    let e = Engine::open_segments(&segs).unwrap();
    assert!(explain(&e, &fold("")).contains("OwnedSignedFold"));
    match e.sql(&fold("")) {
        Err(BurrmillError::Substrate(m)) => assert!(m.contains("does not fit"), "{m}"),
        other => panic!("{other:?}"),
    }

    write(
        &segs,
        "transfer",
        0,
        &[
            ("from", vec![Some("0xa")]),
            ("to", vec![Some("0xb")]),
            ("value", vec![Some("12abc")]),
        ],
    );
    let e = Engine::open_segments(&segs).unwrap();
    let try_fold = fold("").replace("CAST(", "TRY_CAST(");
    assert!(explain(&e, &try_fold).contains("OwnedSignedFold"));
    assert!(
        e.sql(&try_fold).is_err(),
        "TRY_CAST in a substituted fold must refuse, not skip"
    );
}

