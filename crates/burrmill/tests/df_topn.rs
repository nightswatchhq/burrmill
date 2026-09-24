//! Top row per group (the `ROW_NUMBER() ... = 1` pattern) planned as an aggregate.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

fn write(segs: &Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("poi", DataType::Utf8, true),
        Field::new("amount", DataType::Utf8, true),
        Field::new("block_number", DataType::UInt64, false),
        Field::new("log_index", DataType::UInt64, false),
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
                None,
                None,
            ]),
            s(vec![
                Some("p1"),
                Some("p2"),
                Some("q1"),
                None,
                Some("r1"),
                Some("n1"),
                Some("n2"),
            ]),
            s(vec![
                Some("10"),
                Some("5"),
                Some("7"),
                Some("1"),
                None,
                Some("3"),
                Some("4"),
            ]),
            Arc::new(UInt64Array::from(vec![1, 2, 1, 2, 5, 1, 2])),
            Arc::new(UInt64Array::from(vec![0, 0, 0, 1, 0, 0, 0])),
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

const TOP: &str = "SELECT id, poi, total, rn FROM (
    SELECT id, poi,
           SUM(CAST(amount AS HUGEINT)) OVER (PARTITION BY id) AS total,
           ROW_NUMBER() OVER (PARTITION BY id ORDER BY block_number DESC, log_index DESC) AS rn
    FROM ev) WHERE rn = 1";

#[test]
#[ignore]
fn print_plan() {
    let (_t, e) = engine();
    let sql = std::env::var("SQL").unwrap_or_else(|_| TOP.to_string());
    for l in text(&e, &format!("EXPLAIN VERBOSE {sql}")) {
        if !l.contains("SAME TEXT") && !l.contains("physical") {
            println!("{}", l.replace("\\n", "\n"));
        }
    }
}

fn same_as_unrewritten(e: &Engine, sql: &str) -> Vec<String> {
    let reference = sql.replace("rn = 1", "rn + 0 = 1");
    assert_ne!(reference, sql);
    let got = text(e, sql);
    assert_eq!(got, text(e, &reference), "{sql}");
    got
}

#[test]
fn latest_row_and_partition_sum_as_an_aggregate() {
    let (_t, e) = engine();
    let got = same_as_unrewritten(&e, TOP);
    // a: latest is (2,1), whose poi is NULL; the NULL key is a group; c's only amount is NULL.
    assert_eq!(
        got,
        vec!["NULL|n2|7|1", "a|NULL|16|1", "b|q1|7|1", "c|r1|NULL|1"]
    );
    let plan = text(&e, &format!("EXPLAIN {TOP}")).join("\n");
    assert!(plan.contains("first_value"), "{plan}");
    assert!(!plan.contains("WindowAggr"), "{plan}");
}

#[test]
fn under_a_subquery_alias_as_the_views_write_it() {
    let (_t, e) = engine();
    let sql = "WITH latest AS (
        SELECT id, poi, block_number,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY block_number DESC, log_index DESC) AS rn
        FROM ev)
      SELECT x.id, r.poi, r.block_number FROM (SELECT DISTINCT id FROM ev) x
      LEFT JOIN (SELECT * FROM latest WHERE rn = 1) r ON r.id = x.id";
    same_as_unrewritten(&e, sql);
    let plan = text(&e, &format!("EXPLAIN {sql}")).join("\n");
    assert!(plan.contains("first_value"), "{plan}");
}

#[test]
fn other_windows_leave_the_plan_alone() {
    let (_t, e) = engine();
    let sql = "SELECT id, prev FROM (
        SELECT id, LAG(poi) OVER (PARTITION BY id ORDER BY block_number) AS prev,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY block_number DESC, log_index DESC) AS rn
        FROM ev) WHERE rn = 1";
    let plan = text(&e, &format!("EXPLAIN {sql}")).join("\n");
    assert!(!plan.contains("first_value"), "{plan}");
    same_as_unrewritten(&e, sql);
}

// DistinctSplit: COUNT(DISTINCT x) beside other aggregates, in two levels.
#[test]
fn distinct_count_beside_other_aggregates() {
    let (_t, e) = engine();
    let sql = "SELECT id, COUNT(DISTINCT poi) AS pois, COUNT(*) AS n, COUNT(amount) AS amounts,
                      SUM(CAST(amount AS HUGEINT)) AS total, MIN(block_number) AS first, MAX(poi) AS top
               FROM ev GROUP BY id";
    let reference = sql.replace("COUNT(*) AS n", "COUNT(*) FILTER (WHERE true) AS n");
    let got = text(&e, sql);
    assert_eq!(got, text(&e, &reference));
    assert_eq!(
        got,
        vec![
            "NULL|2|2|2|7|1|n2",
            "a|2|3|3|16|1|p2",
            "b|1|1|1|7|1|q1",
            "c|1|1|0|NULL|5|r1"
        ]
    );
    let plan = text(&e, &format!("EXPLAIN {sql}")).join("\n");
    assert!(plan.contains("__distinct"), "{plan}");
    // Types are kept: a count stays a BIGINT.
    let b = e.sql(sql).unwrap();
    assert_eq!(
        b[0].schema().field_with_name("n").unwrap().data_type(),
        &DataType::Int64
    );
}

// ShareRepeats: a CTE used twice is computed once.
#[test]
fn a_repeated_aggregate_subquery_is_computed_once() {
    let (_t, e) = engine();
    let sql = "WITH per AS (SELECT id, MIN(block_number) AS lo, MAX(block_number) AS hi FROM ev GROUP BY id)
               SELECT a.id, a.lo, b.hi FROM per a JOIN per b ON a.id = b.id";
    let reference = "WITH per AS (SELECT id, MIN(block_number) AS lo, MAX(block_number) AS hi FROM ev GROUP BY id),
                          per2 AS (SELECT id, MIN(block_number) AS lo, MAX(block_number) AS hi FROM ev GROUP BY id HAVING count(*) > 0)
                     SELECT a.id, a.lo, b.hi FROM per a JOIN per2 b ON a.id = b.id";
    assert_eq!(text(&e, sql), text(&e, reference));
    let plan = text(&e, &format!("EXPLAIN {sql}")).join("\n");
    assert_eq!(plan.matches("SharedExec").count(), 2, "{plan}");
    assert!(
        !text(&e, &format!("EXPLAIN {reference}"))
            .join("\n")
            .contains("SharedExec")
    );
}
