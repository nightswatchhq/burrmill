//! The names a statement resolves against stay right as views come and go, and defining a view
//! does not rebuild every other name.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

fn engine() -> (tempfile::TempDir, Engine) {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("Id", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef],
    )
    .unwrap();
    let f = std::fs::File::create(segs.join(format!("t-{:064x}.parquet", 1))).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    (tmp, Engine::open_segments(&segs).unwrap())
}

fn count(e: &Engine, sql: &str) -> Result<usize, String> {
    e.sql(sql)
        .map(|b| b.iter().map(|b| b.num_rows()).sum())
        .map_err(|e| e.to_string())
}

/// A replaced view's old column names no longer resolve, and its new ones do in any case.
#[test]
fn a_replaced_view_resolves_its_new_names_only() {
    let (_t, mut e) = engine();
    e.register_view("v", "SELECT Id AS Amount FROM t").unwrap();
    assert_eq!(count(&e, "SELECT amount FROM v"), Ok(2));
    e.register_view("v", "SELECT Id AS Total FROM t").unwrap();
    assert_eq!(count(&e, "SELECT TOTAL FROM v"), Ok(2));
    assert!(count(&e, "SELECT amount FROM v").is_err());
    // A name two views carry stays while either does.
    e.register_view("w", "SELECT Id AS Total FROM t").unwrap();
    e.register_view("v", "SELECT Id AS Other FROM t").unwrap();
    assert_eq!(count(&e, "SELECT total FROM w"), Ok(2));
    assert_eq!(count(&e, "SELECT other FROM v"), Ok(2));
}

/// nuthatch defines a hundred-odd relations before each statement, and each definition rebuilt
/// every name of every other: quadratic in the nest, about 30 ms of each BetSwirl request.
#[test]
fn defining_many_views_does_not_rebuild_every_name_each_time() {
    let (_t, mut e) = engine();
    let started = std::time::Instant::now();
    for i in 0..1500 {
        let cols: Vec<String> = (0..20).map(|c| format!("Id AS C{i}_{c}")).collect();
        e.register_view(
            &format!("v{i}"),
            &format!("SELECT {} FROM t", cols.join(", ")),
        )
        .unwrap();
    }
    let took = started.elapsed();
    assert_eq!(count(&e, "SELECT c1499_19 FROM V1499"), Ok(2));
    assert!(took.as_secs_f64() < 10.0, "1,500 views took {took:?}");
}
