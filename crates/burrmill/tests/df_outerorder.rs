//! A view's or derived table's ORDER BY orders the answer when nothing above it reorders, as DuckDB's
//! does (nuthatch#1984), and costs nothing where something does.

use std::sync::Arc;

use arrow::array::{Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;
use serde_json::Value;

fn segment(dir: &std::path::Path, n: u64) -> (std::path::PathBuf, u64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("who", DataType::Utf8, false),
        Field::new("q", DataType::UInt64, false),
    ]));
    let rows = 0..500u64;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from_iter_values(rows.clone().map(|i| n * 1000 + i))),
            Arc::new(StringArray::from_iter_values(
                rows.clone().map(|i| format!("w{}", (i * 7 + n) % 97)),
            )),
            Arc::new(UInt64Array::from_iter_values(
                rows.map(|i| (i * 31 + n * 17) % 1013),
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

/// Eight segments, so the aggregate below a view's sort runs in several partitions and its rows
/// arrive in no particular order.
fn engine(dir: &std::path::Path) -> Engine {
    let declared = [("block_number", "u64"), ("who", "string"), ("q", "u64")]
        .map(|(c, t)| (c.to_string(), t.to_string()));
    let mut e = Engine::open_empty().unwrap();
    e.register_facts("t", &declared, (1..=8).map(|n| segment(dir, n)).collect(), &[], (None, None))
        .unwrap();
    e.register_view(
        "v",
        "SELECT who, sum(q) AS s FROM t GROUP BY who ORDER BY sum(q) DESC, who",
    )
    .unwrap();
    e.register_view("top", "SELECT who, s FROM v ORDER BY s LIMIT 5").unwrap();
    e.register_view("over_v", "SELECT * FROM v WHERE s > 0").unwrap();
    e
}

fn rows(e: &Engine, sql: &str) -> Vec<Value> {
    e.sql(sql)
        .unwrap()
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect()
}

fn plan(e: &Engine, sql: &str) -> String {
    let mut plan = String::new();
    for b in e.sql(&format!("EXPLAIN {sql}")).unwrap() {
        let kind = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let text = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        (0..text.len())
            .filter(|i| kind.value(*i) == "physical_plan")
            .for_each(|i| plan.push_str(text.value(i)));
    }
    plan
}

const ORDERED: &str = "SELECT who, sum(q) AS s FROM t GROUP BY who ORDER BY sum(q) DESC, who";

#[test]
fn a_views_order_is_the_answers_when_nothing_above_reorders() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    let expect = rows(&e, ORDERED);
    assert_eq!(expect.len(), 97);
    for sql in [
        "SELECT * FROM v",
        "SELECT who, s FROM v",
        "SELECT * FROM over_v",
        "SELECT * FROM (SELECT who, sum(q) AS s FROM t GROUP BY who ORDER BY sum(q) DESC, who)",
        "WITH c AS (SELECT * FROM v) SELECT * FROM c",
    ] {
        assert_eq!(rows(&e, sql), expect, "{sql}");
    }
    assert_eq!(rows(&e, "SELECT * FROM v LIMIT 4"), expect[..4], "a limit takes the first rows");
}

#[test]
fn an_outer_order_and_a_views_limit_still_decide() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    let mut by_who = rows(&e, ORDERED);
    by_who.sort_by_key(|r| r["who"].as_str().unwrap().to_string());
    assert_eq!(rows(&e, "SELECT * FROM v ORDER BY who"), by_who);

    let mut least = rows(&e, ORDERED);
    least.reverse();
    least.truncate(5);
    let top = rows(&e, "SELECT * FROM top");
    let s = |r: &Vec<Value>| r.iter().map(|r| r["s"].clone()).collect::<Vec<_>>();
    assert_eq!(s(&top), s(&least));
    assert_eq!(
        rows(&e, "SELECT count(*) AS n FROM top JOIN t USING (who)")[0]["n"],
        rows(&e, "SELECT count(*) AS n FROM t WHERE who IN (SELECT who FROM top)")[0]["n"],
    );
}

#[test]
fn a_sort_that_cannot_reach_the_answer_is_not_run() {
    let tmp = tempfile::tempdir().unwrap();
    let e = engine(tmp.path());
    for sql in [
        "SELECT count(*) FROM v",
        "SELECT t.who, v.s FROM t JOIN v ON t.who = v.who",
        "SELECT who FROM v UNION ALL SELECT who FROM v",
        "SELECT * FROM v ORDER BY who",
    ] {
        let p = plan(&e, sql);
        let sorts = p.matches("SortExec").count();
        let wanted = usize::from(sql.ends_with("ORDER BY who"));
        assert_eq!(sorts, wanted, "{sql}\n{p}");
    }
    assert!(plan(&e, "SELECT * FROM v").contains("SortExec"));
}
