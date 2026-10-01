//! `QUALIFY x = min(x) OVER (PARTITION BY k)` planned as an aggregate joined back, checked against
//! brute force: ties kept, NULL keys one partition, a NULL value never the extreme.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

type Row = (i64, Option<i64>, Option<i64>);

fn lcg(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

fn write(segs: &Path, rows: &[Row]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("k", DataType::Int64, true),
        Field::new("x", DataType::Int64, true),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())),
        Arc::new(Int64Array::from(rows.iter().map(|r| r.2).collect::<Vec<_>>())),
    ];
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let f = std::fs::File::create(segs.join(format!("t-{:064x}.parquet", 1))).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn fixture() -> (tempfile::TempDir, Engine, Vec<Row>) {
    let mut s = 7;
    // 40 keys and a NULL one, values 0..30 so every partition has ties, some values NULL.
    let rows: Vec<Row> = (0..5000)
        .map(|i| {
            let k = if lcg(&mut s) % 37 == 0 { None } else { Some((lcg(&mut s) % 40) as i64) };
            let x = if lcg(&mut s) % 11 == 0 { None } else { Some((lcg(&mut s) % 30) as i64) };
            (i, k, x)
        })
        .collect();
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    write(&segs, &rows);
    let e = Engine::open_segments(&segs).unwrap();
    (tmp, e, rows)
}

fn ids(e: &Engine, sql: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in e.sql(sql).unwrap_or_else(|err| panic!("{sql}\n{err}")) {
        let id = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        out.extend((0..b.num_rows()).map(|i| id.value(i)));
    }
    out.sort_unstable();
    out
}

fn plan(e: &Engine, sql: &str) -> String {
    let mut out = String::new();
    for b in e.sql(&format!("EXPLAIN {sql}")).unwrap() {
        let plans = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..plans.len() {
            out.push_str(plans.value(i));
        }
    }
    out
}

fn expect(rows: &[Row], max: bool) -> Vec<i64> {
    let mut best: BTreeMap<Option<i64>, i64> = BTreeMap::new();
    for (_, k, x) in rows {
        if let Some(x) = x {
            let b = best.entry(*k).or_insert(*x);
            *b = if max { (*b).max(*x) } else { (*b).min(*x) };
        }
    }
    let mut out: Vec<i64> = rows
        .iter()
        .filter(|(_, k, x)| x.is_some() && *x == best.get(k).copied())
        .map(|r| r.0)
        .collect();
    out.sort_unstable();
    out
}

#[test]
fn the_rows_at_their_partitions_extreme_without_a_window() {
    let (_tmp, e, rows) = fixture();
    for (f, max) in [("min", false), ("max", true)] {
        for sql in [
            format!("SELECT id FROM t QUALIFY x = {f}(x) OVER (PARTITION BY k)"),
            format!("SELECT id FROM t QUALIFY {f}(x) OVER (PARTITION BY k) = x"),
            format!("SELECT id FROM (SELECT id, x, {f}(x) OVER (PARTITION BY k) AS m FROM t) WHERE x = m"),
        ] {
            assert_eq!(ids(&e, &sql), expect(&rows, max), "{sql}");
        }
        let qualify = format!("SELECT id FROM t QUALIFY x = {f}(x) OVER (PARTITION BY k)");
        let p = plan(&e, &qualify);
        assert!(!p.contains("WindowAggExec") && p.contains("HashJoinExec"), "{p}");
    }
    // Left as a window: an ordered one, and a filter that is not an equality against it.
    for sql in [
        "SELECT id FROM t QUALIFY x = min(x) OVER (PARTITION BY k ORDER BY id)",
        "SELECT id FROM t QUALIFY x > min(x) OVER (PARTITION BY k)",
    ] {
        assert!(plan(&e, sql).contains("WindowAggExec"), "{sql}");
    }
}
