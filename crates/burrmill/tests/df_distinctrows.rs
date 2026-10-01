//! `COUNT(DISTINCT (a, b))` counted by a row key: the same count, NULL fields told apart from each
//! other and from absent rows, beside another distinct count.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

type Row = (i64, Option<String>, Option<String>);

fn lcg(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *seed >> 33
}

fn fixture() -> (tempfile::TempDir, Engine, Vec<Row>) {
    let mut s = 11;
    let text = |s: &mut u64, n: u64| (lcg(s) % 9 != 0).then(|| format!("v{}", lcg(s) % n));
    // The empty string beside NULL, and values that would collide if fields were only concatenated.
    let mut rows: Vec<Row> = (0..4000).map(|_| ((lcg(&mut s) % 7) as i64, text(&mut s, 12), text(&mut s, 5))).collect();
    rows.push((0, Some("ab".into()), Some("c".into())));
    rows.push((0, Some("a".into()), Some("bc".into())));
    rows.push((0, Some(String::new()), None));
    rows.push((0, None, Some(String::new())));
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    write(&segs, &rows);
    let e = Engine::open_segments(&segs).unwrap();
    (tmp, e, rows)
}

fn write(segs: &Path, rows: &[Row]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
    ]));
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|r| r.1.clone()).collect::<Vec<_>>())),
        Arc::new(StringArray::from(rows.iter().map(|r| r.2.clone()).collect::<Vec<_>>())),
    ];
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let f = std::fs::File::create(segs.join(format!("t-{:064x}.parquet", 1))).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn counts(e: &Engine, sql: &str) -> BTreeMap<i64, (i64, i64)> {
    let mut out = BTreeMap::new();
    for b in e.sql(sql).unwrap_or_else(|err| panic!("{sql}\n{err}")) {
        let col = |i: usize| b.column(i).as_any().downcast_ref::<Int64Array>().unwrap().clone();
        let (k, n, m) = (col(0), col(1), col(2));
        for i in 0..b.num_rows() {
            out.insert(k.value(i), (n.value(i), m.value(i)));
        }
    }
    out
}

#[test]
fn a_distinct_row_is_counted_by_its_key_and_the_count_is_the_same() {
    let (_tmp, e, rows) = fixture();
    let sql = "SELECT k, count(DISTINCT (a, b)) AS n, count(DISTINCT a) AS m FROM t GROUP BY k";
    let mut pairs: BTreeMap<i64, BTreeSet<(Option<String>, Option<String>)>> = BTreeMap::new();
    let mut firsts: BTreeMap<i64, BTreeSet<String>> = BTreeMap::new();
    for (k, a, b) in &rows {
        pairs.entry(*k).or_default().insert((a.clone(), b.clone()));
        if let Some(a) = a {
            firsts.entry(*k).or_default().insert(a.clone());
        }
    }
    let expect: BTreeMap<i64, (i64, i64)> =
        pairs.iter().map(|(k, p)| (*k, (p.len() as i64, firsts[k].len() as i64))).collect();
    assert_eq!(counts(&e, sql), expect);

    let mut plan = String::new();
    for b in e.sql(&format!("EXPLAIN {sql}")).unwrap() {
        let plans = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        (0..plans.len()).for_each(|i| plan.push_str(plans.value(i)));
    }
    assert!(plan.contains("burrmill_row_key"), "{plan}");
}
