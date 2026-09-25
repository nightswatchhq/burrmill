//! Range joins (`x >= lo AND x <= hi`) planned as `RangeJoinExec`, checked against brute force.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

type Row = (i64, Option<i64>);
type Interval = (i64, Option<i64>, Option<i64>);

fn lcg(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

fn write(segs: &Path, name: &str, cols: Vec<(&str, Vec<Option<i64>>)>) {
    let schema = Arc::new(Schema::new(
        cols.iter().map(|(n, _)| Field::new(*n, DataType::Int64, true)).collect::<Vec<_>>(),
    ));
    let arrays: Vec<ArrayRef> =
        cols.into_iter().map(|(_, v)| Arc::new(Int64Array::from(v)) as ArrayRef).collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let f = std::fs::File::create(segs.join(format!("{name}-{:064x}.parquet", 1))).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn fixture(seed: u64) -> (tempfile::TempDir, Engine, Vec<Row>, Vec<Interval>) {
    let mut s = seed;
    let points: Vec<Row> = (0..3000)
        .map(|i| (i, if lcg(&mut s) % 50 == 0 { None } else { Some((lcg(&mut s) % 1000) as i64) }))
        .collect();
    let intervals: Vec<Interval> = (0..60)
        .map(|k| {
            let lo = (lcg(&mut s) % 1000) as i64;
            // Mostly short, some long, so intervals overlap; a few NULL or empty.
            let len = if lcg(&mut s) % 5 == 0 { (lcg(&mut s) % 400) as i64 } else { (lcg(&mut s) % 20) as i64 };
            let hi = if lcg(&mut s) % 7 == 0 { lo - 1 } else { lo + len };
            match lcg(&mut s) % 30 {
                0 => (k, None, Some(hi)),
                1 => (k, Some(lo), None),
                _ => (k, Some(lo), Some(hi)),
            }
        })
        .collect();
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    write(
        &segs,
        "pt",
        vec![("id", points.iter().map(|p| Some(p.0)).collect()), ("x", points.iter().map(|p| p.1).collect())],
    );
    write(
        &segs,
        "iv",
        vec![
            ("k", intervals.iter().map(|i| Some(i.0)).collect()),
            ("lo", intervals.iter().map(|i| i.1).collect()),
            ("hi", intervals.iter().map(|i| i.2).collect()),
        ],
    );
    let e = Engine::open_segments(&segs).unwrap();
    (tmp, e, points, intervals)
}

/// `(id, k)` pairs as a multiset, `k` NULL for a kept unmatched row.
fn pairs(e: &Engine, sql: &str) -> BTreeMap<(i64, Option<i64>), usize> {
    let mut out = BTreeMap::new();
    for b in e.sql(sql).unwrap_or_else(|err| panic!("{sql}\n{err}")) {
        let id = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let k = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            let key = (id.value(i), k.is_valid(i).then(|| k.value(i)));
            *out.entry(key).or_default() += 1;
        }
    }
    out
}

fn uses_range_join(e: &Engine, sql: &str) -> bool {
    e.sql(&format!("EXPLAIN {sql}")).unwrap().iter().any(|b| {
        let plans = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        (0..plans.len()).any(|i| plans.value(i).contains("RangeJoinExec"))
    })
}

fn expect(
    points: &[Row],
    intervals: &[Interval],
    lo_strict: bool,
    hi_strict: bool,
    keep_points: bool,
) -> BTreeMap<(i64, Option<i64>), usize> {
    let mut out = BTreeMap::new();
    for &(id, x) in points {
        let mut matched = false;
        for &(k, lo, hi) in intervals {
            let (Some(x), Some(lo), Some(hi)) = (x, lo, hi) else { continue };
            let a = if lo_strict { x > lo } else { x >= lo };
            let b = if hi_strict { x < hi } else { x <= hi };
            if a && b {
                matched = true;
                *out.entry((id, Some(k))).or_default() += 1;
            }
        }
        if keep_points && !matched {
            *out.entry((id, None)).or_default() += 1;
        }
    }
    out
}

#[test]
fn inner_range_joins_match_brute_force_either_way_round() {
    for seed in [1, 2, 3] {
        let (_t, e, points, intervals) = fixture(seed);
        for (ge, le, lo_strict, hi_strict) in
            [(">=", "<=", false, false), (">", "<=", true, false), (">=", "<", false, true), (">", "<", true, true)]
        {
            let want = expect(&points, &intervals, lo_strict, hi_strict, false);
            for sql in [
                format!("SELECT p.id, i.k FROM pt p JOIN iv i ON p.x {ge} i.lo AND p.x {le} i.hi"),
                format!("SELECT p.id, i.k FROM iv i JOIN pt p ON p.x {ge} i.lo AND p.x {le} i.hi"),
                // Written from the interval's side, and with the conjuncts the other way about.
                format!("SELECT p.id, i.k FROM pt p JOIN iv i ON i.hi {ge} p.x AND i.lo {le} p.x",
                    ge = if hi_strict { ">" } else { ">=" }, le = if lo_strict { "<" } else { "<=" }),
            ] {
                assert!(uses_range_join(&e, &sql), "not planned as a range join: {sql}");
                assert_eq!(pairs(&e, &sql), want, "seed {seed}: {sql}");
            }
        }
    }
}

#[test]
fn outer_range_join_keeps_the_unmatched_side() {
    let (_t, e, points, intervals) = fixture(9);
    let want = expect(&points, &intervals, false, false, true);
    let sql = "SELECT p.id, i.k FROM iv i RIGHT JOIN pt p ON p.x >= i.lo AND p.x <= i.hi";
    assert!(uses_range_join(&e, sql));
    assert_eq!(pairs(&e, sql), want);
    let sql = "SELECT p.id, i.k FROM pt p LEFT JOIN iv i ON p.x >= i.lo AND p.x <= i.hi";
    assert_eq!(pairs(&e, sql), want, "{sql}");
}
