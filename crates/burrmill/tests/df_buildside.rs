//! A hash join builds on the input that reads less, whichever side of `JOIN` it was written on.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

fn write(segs: &Path, name: &str, k: Vec<Option<i64>>, v: Vec<i64>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, false),
    ]));
    let arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(k)), Arc::new(Int64Array::from(v))];
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let f = std::fs::File::create(segs.join(format!("{name}-{:064x}.parquet", 1))).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// 50,000 rows over keys 0..=99 with some NULL, and 12 rows over keys 95..=106: the keys 95..=99
/// match, each side has rows the other lacks.
fn fixture() -> (tempfile::TempDir, Engine) {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    let n = 50_000i64;
    write(
        &segs,
        "big",
        (0..n).map(|i| (i % 211 != 0).then_some(i % 100)).collect(),
        (0..n).collect(),
    );
    write(
        &segs,
        "small",
        (0..12).map(|i| Some(95 + i)).collect(),
        (0..12).collect(),
    );
    (tmp, Engine::open_segments(&segs).unwrap())
}

fn one(e: &Engine, sql: &str) -> (String, String) {
    let b = &e.sql(sql).unwrap_or_else(|err| panic!("{sql}\n{err}"))[0];
    let col = |i: usize| {
        let text = arrow::compute::cast(b.column(i), &DataType::Utf8).unwrap();
        text.as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string()
    };
    (col(0), col(1))
}

/// The file the join's build side scans: the first scan under the first `HashJoinExec`.
fn build_side(e: &Engine, sql: &str) -> String {
    for b in e.sql(&format!("EXPLAIN {sql}")).unwrap() {
        let kind = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let plans = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..plans.len() {
            if kind.value(i) != "physical_plan" {
                continue;
            }
            let after = plans
                .value(i)
                .split("HashJoinExec")
                .nth(1)
                .expect("a hash join");
            let scan = after
                .split("segments/")
                .nth(1)
                .expect("a scan under the join");
            return scan.split('-').next().unwrap().to_string();
        }
    }
    panic!("no physical plan for {sql}");
}

#[test]
fn the_build_side_is_the_smaller_input_and_the_answer_is_the_same() {
    let (_tmp, e) = fixture();
    for join in ["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"] {
        let big_first = format!(
            "SELECT count(*), coalesce(sum(b.v + s.v), 0) FROM big b {join} small s ON b.k = s.k"
        );
        assert_eq!(build_side(&e, &big_first), "small", "{big_first}");
        // The same join written the other way round, which DataFusion already builds on `small`.
        let mirrored = match join {
            "LEFT JOIN" => "RIGHT JOIN",
            "RIGHT JOIN" => "LEFT JOIN",
            j => j,
        };
        let small_first = format!(
            "SELECT count(*), coalesce(sum(b.v + s.v), 0) FROM small s {mirrored} big b ON b.k = s.k"
        );
        assert_eq!(build_side(&e, &small_first), "small", "{small_first}");
        assert_eq!(one(&e, &big_first), one(&e, &small_first), "{join}");
    }
    // Brute force for the inner join: keys 95..=99 match one small row each.
    let matched = (0..50_000i64)
        .filter(|i| i % 211 != 0 && i % 100 >= 95)
        .count() as i64;
    assert_eq!(
        one(
            &e,
            "SELECT count(*), 0 FROM big b JOIN small s ON b.k = s.k"
        )
        .0,
        matched.to_string()
    );
}

/// A semi or anti join holds its build side too: the QoS nest's rows, written first and filtered by
/// two small tables, were held whole in each of two nested semi joins and refused out of memory.
#[test]
fn a_semi_or_anti_join_builds_on_the_smaller_input_and_the_answer_is_the_same() {
    let (_tmp, e) = fixture();
    let keep = |anti: bool| {
        (0..50_000i64)
            .filter(|i| {
                let k = (i % 211 != 0).then_some(i % 100);
                k.is_some_and(|k| k >= 95) != anti
            })
            .collect::<Vec<_>>()
    };
    for (sql, anti) in [
        (
            "SELECT count(*), coalesce(sum(b.v), 0) FROM big b WHERE EXISTS (SELECT 1 FROM small s WHERE s.k = b.k)",
            false,
        ),
        (
            "SELECT count(*), coalesce(sum(b.v), 0) FROM big b WHERE NOT EXISTS (SELECT 1 FROM small s WHERE s.k = b.k)",
            true,
        ),
    ] {
        assert_eq!(build_side(&e, sql), "small", "{sql}");
        let rows = keep(anti);
        let want = (rows.len().to_string(), rows.iter().sum::<i64>().to_string());
        assert_eq!(one(&e, sql), want, "{sql}");
    }
}

/// `NOT IN` is a null-aware anti join, which DataFusion cannot swap: a NULL key is never kept.
#[test]
fn not_in_keeps_its_answer() {
    let (_tmp, e) = fixture();
    let rows: Vec<i64> = (0..50_000i64)
        .filter(|i| i % 211 != 0 && i % 100 < 95)
        .collect();
    let want = (rows.len().to_string(), rows.iter().sum::<i64>().to_string());
    let sql =
        "SELECT count(*), coalesce(sum(b.v), 0) FROM big b WHERE b.k NOT IN (SELECT k FROM small)";
    assert_eq!(one(&e, sql), want, "{sql}");
}
