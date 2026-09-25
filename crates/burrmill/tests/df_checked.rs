//! CheckedArithmetic on the DataFusion path (roadmap 6.3): the overflow corpus from investigation
//! 03, each case answering exactly or refusing, never wrapping and never dropping a row.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Decimal128Array, Decimal256Array, Float64Array, Int64Array, StringArray,
    UInt64Array,
};
use arrow::datatypes::{DataType, i256};
use arrow::record_batch::RecordBatch;
use burrmill::{BurrmillError, Engine};

const U256_MAX: &str =
    "115792089237316195423570985008687907853269984665640564039457584007913129639935";
const D38MAX: &str = "99999999999999999999999999999999999999";
const D76MAX: &str = "9999999999999999999999999999999999999999999999999999999999999999999999999999";

/// Each batch becomes its own segment file, so a table of several batches is summed in several
/// partitions and the partial/final split is exercised.
fn write_segments(segs: &Path, table: &str, batches: &[RecordBatch]) {
    for (i, b) in batches.iter().enumerate() {
        let f = std::fs::File::create(segs.join(format!("{table}-{i:064x}.parquet"))).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, b.schema(), None).unwrap();
        w.write(b).unwrap();
        w.close().unwrap();
    }
}

fn col(name: &str, a: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter(vec![(name, a)]).unwrap()
}

fn i64s(v: &[i64]) -> RecordBatch {
    col("v", Arc::new(Int64Array::from(v.to_vec())))
}

fn u64s(v: &[u64]) -> RecordBatch {
    col("v", Arc::new(UInt64Array::from(v.to_vec())))
}

fn dec128(v: &[&str]) -> RecordBatch {
    let a = Decimal128Array::from_iter_values(v.iter().map(|s| s.parse::<i128>().unwrap()))
        .with_precision_and_scale(38, 0)
        .unwrap();
    col("v", Arc::new(a))
}

fn dec256(v: &[&str]) -> RecordBatch {
    let a = Decimal256Array::from_iter_values(v.iter().map(|s| i256::from_string(s).unwrap()))
        .with_precision_and_scale(76, 0)
        .unwrap();
    col("v", Arc::new(a))
}

fn engine(tables: &[(&str, Vec<RecordBatch>)]) -> (tempfile::TempDir, Engine) {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    for (t, b) in tables {
        write_segments(&segs, t, b);
    }
    let e = Engine::open_segments(&segs).unwrap();
    (tmp, e)
}

/// A nest whose `transfer.value` is uint256 text with a `_dec` companion, as nuthatch seals it.
fn transfers(rows: &[&[(&str, &str)]]) -> (tempfile::TempDir, Engine) {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    let batches: Vec<RecordBatch> = rows
        .iter()
        .map(|r| {
            RecordBatch::try_from_iter(vec![
                (
                    "party",
                    Arc::new(StringArray::from(r.iter().map(|x| x.0).collect::<Vec<_>>()))
                        as ArrayRef,
                ),
                (
                    "value",
                    Arc::new(StringArray::from(r.iter().map(|x| x.1).collect::<Vec<_>>()))
                        as ArrayRef,
                ),
            ])
            .unwrap()
        })
        .collect();
    write_segments(&segs, "transfer", &batches);
    write_segments(
        &segs,
        "label",
        &[RecordBatch::try_from_iter(vec![
            (
                "party",
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            ),
            (
                "name",
                Arc::new(StringArray::from(vec!["alice", "bob"])) as ArrayRef,
            ),
        ])
        .unwrap()],
    );
    std::fs::write(
        tmp.path().join("schema.json"),
        r#"{"tables":[{"table":"transfer","columns":[
            {"name":"party","storage":"text"},{"name":"value","storage":"word32"}]}]}"#,
    )
    .unwrap();
    let e = Engine::open_nest(tmp.path()).unwrap();
    (tmp, e)
}

/// Every cell as text, rows in answer order.
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

fn one(e: &Engine, sql: &str) -> String {
    let r = rows(e, sql);
    assert_eq!(r.len(), 1, "{sql}: {r:?}");
    r[0][0].clone()
}

fn refused(e: &Engine, sql: &str) -> String {
    match e.sql(sql) {
        Ok(b) => panic!(
            "{sql} answered {:?}",
            arrow::util::pretty::pretty_format_batches(&b)
                .unwrap()
                .to_string()
        ),
        Err(BurrmillError::NotAllowed(m) | BurrmillError::Substrate(m)) => m,
        Err(other) => panic!("{sql}: {other:?}"),
    }
}

fn result_type(e: &Engine, sql: &str) -> DataType {
    e.sql(sql).unwrap()[0].schema().field(0).data_type().clone()
}

// 1a-1d: DataFusion wraps SUM(Int64); DuckDB widens to HUGEINT. So do we.
#[test]
fn integer_sums_widen_and_stay_exact() {
    let (_t, e) = engine(&[("t", vec![i64s(&[i64::MAX]), i64s(&[1])])]);
    assert_eq!(one(&e, "SELECT SUM(v) FROM t"), "9223372036854775808");
    assert_eq!(
        result_type(&e, "SELECT SUM(v) FROM t"),
        DataType::Decimal128(38, 0)
    );
    assert_eq!(
        one(&e, "SELECT SUM(v) FROM t GROUP BY v % 1"),
        "9223372036854775808"
    );

    let (_t, e) = engine(&[("t", vec![u64s(&[u64::MAX]), u64s(&[u64::MAX])])]);
    assert_eq!(one(&e, "SELECT SUM(v) FROM t"), "36893488147419103230");
}

#[test]
fn order_independent_refusal() {
    let (_t, e) = engine(&[(
        "t",
        vec![dec128(&[D38MAX]), dec128(&["1"]), dec128(&["-1"])],
    )]);
    assert_eq!(one(&e, "SELECT SUM(v) FROM t"), D38MAX);
    let (_t, e) = engine(&[("t", vec![dec128(&[D38MAX]), dec128(&["1"])])]);
    let m = refused(&e, "SELECT SUM(v) FROM t");
    assert!(m.contains("100000000000000000000000000000000000000"), "{m}");
}

// 2a-2c: a Decimal128(38,0) sum that leaves 38 digits, or i128, refuses; a partial that does
// while the total does not is harmless.
#[test]
fn decimal128_sums() {
    let (_t, e) = engine(&[("t", vec![dec128(&[D38MAX]), dec128(&[D38MAX])])]);
    refused(&e, "SELECT SUM(v) FROM t");
    let (_t, e) = engine(&[(
        "t",
        vec![dec128(&[D38MAX, D38MAX]), dec128(&[&format!("-{D38MAX}")])],
    )]);
    assert_eq!(one(&e, "SELECT SUM(v) FROM t"), D38MAX);
}

// 2d-2f.
#[test]
fn decimal256_sums() {
    let (_t, e) = engine(&[("t", vec![dec256(&[D76MAX]), dec256(&["1"])])]);
    refused(&e, "SELECT SUM(v) FROM t");
    let (_t, e) = engine(&[(
        "t",
        vec![dec256(&[D76MAX, D76MAX]), dec256(&[&format!("-{D76MAX}")])],
    )]);
    assert_eq!(one(&e, "SELECT SUM(v) FROM t"), D76MAX);
}

// 2g: DataFusion's AVG refuses when a partial overflows; ours only when the answer does. The
// answer is a DOUBLE, as DuckDB's is for every exact input.
#[test]
fn decimal_avg() {
    let (_t, e) = engine(&[("t", vec![dec128(&["1"]), dec128(&["2"])])]);
    assert_eq!(one(&e, "SELECT AVG(v) FROM t"), "1.5");
    let (_t, e) = engine(&[(
        "t",
        vec![
            dec128(&["99999999999999999999999999999999"]),
            dec128(&["99999999999999999999999999999999"]),
        ],
    )]);
    assert_eq!(one(&e, "SELECT AVG(v) FROM t"), "1e32");
}

// 1e-1h, 2i: scalar arithmetic, folded or on a column.
#[test]
fn scalars_refuse() {
    let (_t, e) = engine(&[("t", vec![i64s(&[i64::MAX, i64::MIN])])]);
    refused(&e, "SELECT 9223372036854775807 + 1");
    refused(&e, "SELECT 10000000000 * 10000000000");
    refused(&e, "SELECT v + 1 FROM t");
    refused(&e, "SELECT v * 2 FROM t");
    refused(&e, "SELECT v - (-1) FROM t");
    refused(&e, "SELECT -v FROM t");
    assert_eq!(
        one(&e, "SELECT v - 1 FROM t WHERE v > 0"),
        "9223372036854775806"
    );
    let (_t, e) = engine(&[("t", vec![dec128(&[D38MAX])])]);
    refused(&e, "SELECT v + 1 FROM t");
    assert_eq!(
        one(&e, "SELECT v - 1 FROM t"),
        "99999999999999999999999999999999999998"
    );
}

// 2k: a literal past u64 would be a float before any rule saw it.
#[test]
fn wide_literals_refused_at_the_surface() {
    let (_t, e) = engine(&[("t", vec![i64s(&[1])])]);
    match e.sql("SELECT 99999999999999999999999999999999999999 + 1") {
        Err(BurrmillError::NotAllowed(m)) => assert!(m.contains("CAST"), "{m}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(
        one(&e, "SELECT 18446744073709551615 FROM t"),
        "18446744073709551615"
    );
    assert_eq!(one(&e, "SELECT 1.5 FROM t"), "1.5");
}

#[test]
fn floats_pass_through() {
    let (_t, e) = engine(&[(
        "t",
        vec![col("v", Arc::new(Float64Array::from(vec![1.5, 2.0])))],
    )]);
    assert_eq!(one(&e, "SELECT SUM(v) FROM t"), "3.5");
    assert_eq!(one(&e, "SELECT v * 2 FROM t ORDER BY v LIMIT 1"), "3.0");
}

#[test]
fn unknown_exact_functions_are_refused() {
    let (_t, e) = engine(&[("t", vec![i64s(&[3])])]);
    let m = refused(&e, "SELECT v << 70 FROM t");
    assert!(m.contains("no checked form"), "{m}");
    let m = refused(&e, "SELECT factorial(v) FROM t");
    assert!(m.contains("factorial"), "{m}");
    assert_eq!(one(&e, "SELECT abs(v) FROM t"), "3");
    assert_eq!(one(&e, "SELECT count(*) FROM t"), "1");
}

#[test]
fn window_sums_are_checked() {
    let (_t, e) = engine(&[("t", vec![i64s(&[i64::MAX, 1])])]);
    let r = rows(&e, "SELECT SUM(v) OVER () FROM t");
    assert_eq!(r, vec![vec!["9223372036854775808".to_string()]; 2]);
    let r = rows(
        &e,
        "SELECT SUM(v) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
    );
    assert_eq!(
        r,
        vec![
            vec!["1".to_string()],
            vec!["9223372036854775808".to_string()]
        ]
    );
}

// 3a-3f: nuthatch's `_dec`. TRY_CAST drops what does not fit; the sum is made exact instead.
#[test]
fn dec_companion_sums_are_exact() {
    let (_t, e) = transfers(&[&[("a", "5"), ("b", "7")], &[("a", "10")]]);
    assert_eq!(one(&e, "SELECT SUM(value_dec) FROM transfer"), "22");
    assert_eq!(
        rows(
            &e,
            "SELECT party, SUM(value_dec) FROM transfer GROUP BY party ORDER BY party"
        ),
        vec![
            vec!["a".to_string(), "15".into()],
            vec!["b".to_string(), "7".into()]
        ]
    );
    assert_eq!(
        result_type(&e, "SELECT SUM(value_dec) FROM transfer"),
        DataType::Decimal128(38, 0)
    );

    // One party's value does not fit DECIMAL(38,0). DuckDB drops it and answers 5.
    let (_t, e) = transfers(&[&[("a", "5"), ("a", U256_MAX)]]);
    let total = "115792089237316195423570985008687907853269984665640564039457584007913129639940";
    let m = refused(&e, "SELECT SUM(value_dec) FROM transfer");
    assert!(m.contains(total), "{m}");
    assert_eq!(
        one(&e, "SELECT checked_sum_text(value) FROM transfer"),
        total
    );
    let m = refused(
        &e,
        "SELECT SUM(TRY_CAST(value AS DECIMAL(38,0))) FROM transfer",
    );
    assert!(m.contains(total), "{m}");
}

#[test]
fn dec_companion_through_subqueries_and_joins() {
    let (_t, e) = transfers(&[&[("a", "5"), ("b", "7")], &[("a", "10")]]);
    assert_eq!(
        one(
            &e,
            "SELECT SUM(x) FROM (SELECT value_dec AS x FROM transfer WHERE party = 'a') s"
        ),
        "15"
    );
    assert_eq!(
        rows(
            &e,
            "SELECT l.name, SUM(t.value_dec) FROM transfer t JOIN label l ON t.party = l.party \
             GROUP BY l.name ORDER BY l.name"
        ),
        vec![
            vec!["alice".to_string(), "15".into()],
            vec!["bob".to_string(), "7".into()]
        ]
    );
    assert_eq!(
        one(
            &e,
            "WITH c AS (SELECT party, value_dec FROM transfer) SELECT SUM(value_dec) FROM c"
        ),
        "22"
    );
}

#[test]
fn other_reads_of_dec_companions_in_aggregates_are_refused() {
    let (_t, e) = transfers(&[&[("a", "5"), ("b", "7")]]);
    refused(&e, "SELECT MAX(value_dec) FROM transfer");
    refused(&e, "SELECT SUM(value_dec * 2) FROM transfer");
    refused(&e, "SELECT SUM(value_dec / 1e18) FROM transfer");
    refused(&e, "SELECT SUM(value_dec) OVER () FROM transfer");
    // Reading the column itself is not an aggregate dropping rows: NULL is visible.
    assert_eq!(
        rows(
            &e,
            "SELECT lag(value_dec) OVER (ORDER BY party) FROM transfer"
        )
        .len(),
        2
    );
    assert_eq!(
        rows(&e, "SELECT value_dec FROM transfer ORDER BY party").len(),
        2
    );
    assert_eq!(
        one(&e, "SELECT count(*) FROM transfer WHERE value_dec > 6"),
        "1"
    );
}

// The signed fold nuthatch writes: credits and negated debits of a TRY_CAST, through UNION ALL.
#[test]
fn signed_fold_over_try_cast() {
    let (_t, e) = transfers(&[&[("a", "5"), ("b", "7")], &[("a", "10")]]);
    let fold = |cast: &str| {
        format!(
            "SELECT addr, SUM(d) FROM ( \
               SELECT party AS addr, {cast}(value AS DECIMAL(38,0)) AS d FROM transfer \
               UNION ALL \
               SELECT 'sink' AS addr, -{cast}(value AS DECIMAL(38,0)) AS d FROM transfer \
             ) GROUP BY addr ORDER BY addr"
        )
    };
    let want = vec![
        vec!["a".to_string(), "15".into()],
        vec!["b".to_string(), "7".into()],
        vec!["sink".to_string(), "-22".into()],
    ];
    assert_eq!(rows(&e, &fold("TRY_CAST")), want);
    assert_eq!(rows(&e, &fold("CAST")), want);
    assert_eq!(
        one(
            &e,
            "SELECT SUM(x) FROM (SELECT value_dec AS x FROM transfer UNION ALL SELECT -value_dec FROM transfer) u"
        ),
        "0"
    );
    assert_eq!(
        one(
            &e,
            "SELECT SUM(x) FROM (SELECT value_dec AS x FROM transfer UNION ALL SELECT CAST(1 AS DECIMAL(38,0))) u"
        ),
        "23"
    );

    // A uint256 credit and its debit: DuckDB drops both rows; the exact fold refuses both parties.
    let (_t, e) = transfers(&[&[("a", "5"), ("a", U256_MAX)]]);
    // Refused either way: by the checked sum, or, where the owned fold is substituted, on reading.
    let m = refused(&e, &fold("TRY_CAST"));
    assert!(
        m.contains("does not fit Decimal128(38, 0)")
            || m.contains("refuses it rather than dropping"),
        "{m}"
    );
}
