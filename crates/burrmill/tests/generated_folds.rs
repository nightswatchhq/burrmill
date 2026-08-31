//! Generated folds against a non-optimising reference (roadmap 2.1, 2.2).
//!
//! Nineteen hand-written refusals and three overflow tests are not a corpus. §3.7 asks for
//! differential testing, and the shape of it here is not quite the textbook one, for a reason worth
//! stating: NoREC and TLP exist to defeat an *optimiser*, and Burrmill has one plan shape and no
//! optimiser to defeat. What it does have is a great deal of machinery between the rows and the
//! answer - morsel splitting, a shared partitioned aggregate under locks, radix partitioning, arena
//! keys, a parallel sort - any of which could produce a plausible wrong number.
//!
//! So the oracle is a **non-optimising reference engine** in the most literal sense: the same
//! semantics implemented in fifteen lines with a `BTreeMap`, on one thread, straight from the
//! generated rows, with no Parquet and no parallelism anywhere near it. It is obviously correct in
//! the way the real one cannot be, and unlike DuckDB it survives DuckDB's removal in Q4.
//!
//! Three properties, each of which has a specific way the engine could be wrong:
//!
//! 1. **The answer matches the reference**, exactly, including refusing where the reference refuses.
//! 2. **The answer does not depend on how the rows were split into segments.** The union of any
//!    layout is the same table, so a layout that changes the answer is a defect and not a slower
//!    plan. This is the property the morsel splitter and the bimodal seal path can break.
//! 3. **The answer does not depend on the thread count.** The aggregate is shared and lock-guarded
//!    as of roadmap 1.2, and a lost update under contention would show up here and almost nowhere
//!    else.
//!
//! Failures print the seed. `BURRMILL_SEED=<n>` reruns exactly one case; `BURRMILL_CASES=<n>` runs
//! more of them than the default, which is sized to keep `cargo test` quick.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Burrmill, BurrmillError, Limits};

const SQL: &str = "SELECT addr, SUM(d) AS net FROM (\
                     SELECT \"to\" AS addr, TRY_CAST(\"value\" AS HUGEINT) AS d FROM t \
                     UNION ALL \
                     SELECT \"from\" AS addr, -TRY_CAST(\"value\" AS HUGEINT) AS d FROM t\
                   ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr";

/// splitmix64. A generator whose failures are reproducible from one `u64` is worth more than one
/// with better statistics and no seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }
}

/// One generated row, as text, exactly as a sealed segment holds it.
#[derive(Clone, Debug)]
struct Row {
    from: String,
    to: String,
    value: String,
}

/// How the `value` column is drawn.
///
/// **The boundary is the point of 2.2.** The old fixture topped out around 1e20 against an
/// `i128::MAX` of 1.7e38, so the overflow path was pinned by three hand-written tests and nothing
/// else; no amount of running the benchmark could ever have reached it. `Huge` draws values large
/// enough that a handful of them sum past the maximum, and `Adversarial` includes `i128::MIN`
/// itself, whose negation is the one value that cannot be represented - and the fold negates every
/// debit.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Values {
    Small,
    Huge,
    Adversarial,
}

fn gen_value(rng: &mut Rng, kind: Values) -> String {
    match kind {
        Values::Small => format!("{}", rng.next() % 1_000_000),
        // i128::MAX / 8, so a party seen a few times crosses the boundary rather than never
        // approaching it.
        Values::Huge => format!("{}", (rng.next() as u128 % 4) * (i128::MAX as u128 / 8) + 1),
        Values::Adversarial => match rng.below(10) {
            // The value whose negation does not exist. Every debit is negated, so this must be
            // refused rather than wrapped to itself.
            0 => i128::MIN.to_string(),
            1 => i128::MAX.to_string(),
            2 => "-1".into(),
            // TRY_CAST semantics: unparseable is NULL and SUM ignores NULLs, so the row is skipped.
            // Never substituted with zero, which is a different answer that looks plausible.
            3 => "not a number".into(),
            4 => "".into(),
            5 => " 7".into(),
            6 => "+7".into(),
            7 => "0".into(),
            8 => format!("{}", i128::MAX as u128 / 3),
            _ => format!("{}", rng.next() % 1_000),
        },
    }
}

/// The non-optimising reference: the admitted subset's semantics, on one thread, with a `BTreeMap`.
///
/// Deliberately dull. Every deviation from the real operator here is a bug in one of them, and the
/// dull one is much easier to be sure about.
fn reference(rows: &[Row]) -> Result<Vec<(String, i128)>, BurrmillError> {
    // Accumulate wider than `i128` so that a key is refused when its **answer** does not fit, never
    // because some partial sum did not. Written as an `i128` pair rather than a big-integer crate:
    // test inputs are at most sixty rows of magnitude below 2^127, so a 192-bit accumulator has
    // enormous headroom, and the arithmetic is short enough to check by eye.
    let mut acc: BTreeMap<String, (i64, i128)> = BTreeMap::new();
    let add = |acc: &mut BTreeMap<String, (i64, i128)>, k: &str, v: i128| {
        let (hi, lo) = acc.entry(k.to_string()).or_insert((0, 0));
        let (new_lo, carry) = (*lo as u128).overflowing_add(v as u128);
        *hi += (v >> 127) as i64 + carry as i64;
        *lo = new_lo as i128;
    };
    for r in rows {
        // TRY_CAST: unparseable becomes NULL, and SUM ignores NULLs. Skip, never substitute.
        // Trimmed, because DuckDB trims and " 7" is seven.
        let Ok(d) = r.value.trim().parse::<i128>() else { continue };
        // The one negation that does not exist. Refused rather than wrapped, and it is refused here
        // rather than at the sum because `-i128::MIN` is unrepresentable however wide the
        // accumulator is: the fold's expression negates the value itself.
        let minus_d = d
            .checked_neg()
            .ok_or_else(|| BurrmillError::Overflow(format!("reference negation of {d}")))?;
        add(&mut acc, &r.to, d);
        add(&mut acc, &r.from, minus_d);
    }
    let mut out = Vec::new();
    for (k, (hi, lo)) in acc {
        let u = lo as u128;
        let sum = match hi {
            0 if u <= i128::MAX as u128 => u as i128,
            -1 if u >= 1u128 << 127 => u as i128,
            _ => {
                return Err(BurrmillError::Overflow(format!("reference sum for {k} does not fit")))
            }
        };
        // HAVING SUM(d) <> 0, then canonical byte-wise ascending order, which a BTreeMap over
        // String already gives for these ASCII keys.
        if sum != 0 {
            out.push((k, sum));
        }
    }
    Ok(out)
}

fn write_segments(dir: &Path, rows: &[Row], splits: usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("from", DataType::Utf8, false),
        Field::new("to", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let per = rows.len().div_ceil(splits.max(1)).max(1);
    for (i, chunk) in rows.chunks(per).enumerate() {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from((0..chunk.len() as u64).collect::<Vec<_>>())),
                Arc::new(StringArray::from(
                    chunk.iter().map(|r| r.from.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    chunk.iter().map(|r| r.to.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    chunk.iter().map(|r| r.value.as_str()).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join(format!("t-{i:05}.parquet"))).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
}

fn fold(dir: &Path, threads: usize) -> Result<Vec<(String, i128)>, BurrmillError> {
    let db = Burrmill::open_segments("t", dir)?;
    let run = || {
        db.query(SQL, Limits::default())
            .map(|a| a.rows().iter().map(|(k, v)| (k.to_string(), v)).collect::<Vec<_>>())
    };
    rayon::ThreadPoolBuilder::new().num_threads(threads).build().unwrap().install(run)
}

fn gen_rows(rng: &mut Rng, kind: Values) -> Vec<Row> {
    let n_addrs = 1 + rng.below(12);
    let n_rows = 1 + rng.below(60);
    let addrs: Vec<String> = (0..n_addrs).map(|i| format!("0x{i:040x}")).collect();
    (0..n_rows)
        .map(|_| Row {
            from: addrs[rng.below(n_addrs)].clone(),
            to: addrs[rng.below(n_addrs)].clone(),
            value: gen_value(rng, kind),
        })
        .collect()
}

fn check_case(seed: u64, kind: Values) {
    let mut rng = Rng(seed);
    let rows = gen_rows(&mut rng, kind);
    let expected = reference(&rows);

    // Two different segment layouts of the identical rows, and two thread counts. Any of the four
    // runs disagreeing is a defect; agreeing on a wrong answer is what the reference is for.
    for (splits, threads) in [(1usize, 1usize), (1, 8), (3 + rng.below(9), 1), (3 + rng.below(9), 8)]
    {
        let dir = tempfile::tempdir().unwrap();
        write_segments(dir.path(), &rows, splits);
        let got = fold(dir.path(), threads);
        match (&expected, &got) {
            (Ok(want), Ok(have)) => assert_eq!(
                want, have,
                "seed={seed} kind={kind:?} splits={splits} threads={threads}: answer differs from \
                 the reference. Rerun with BURRMILL_SEED={seed}"
            ),
            (Err(BurrmillError::Overflow(_)), Err(BurrmillError::Overflow(_))) => {}
            (Err(w), Ok(have)) => panic!(
                "seed={seed} kind={kind:?} splits={splits} threads={threads}: the reference refused \
                 ({w}) but the fold returned {} rows. A wrapped sum is a wrong answer that looks \
                 like a balance, which is the one thing this engine must never do.",
                have.len()
            ),
            (Ok(want), Err(h)) => panic!(
                "seed={seed} kind={kind:?} splits={splits} threads={threads}: the fold refused ({h}) \
                 where the reference produced {} rows",
                want.len()
            ),
            (Err(w), Err(h)) => panic!(
                "seed={seed} kind={kind:?}: both refused but differently: reference {w}, fold {h}"
            ),
        }
    }
}

fn cases() -> u64 {
    std::env::var("BURRMILL_CASES").ok().and_then(|s| s.parse().ok()).unwrap_or(60)
}

fn run(kind: Values) {
    if let Some(seed) = std::env::var("BURRMILL_SEED").ok().and_then(|s| s.parse().ok()) {
        check_case(seed, kind);
        return;
    }
    for seed in 0..cases() {
        check_case(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ kind as u64, kind);
    }
}

#[test]
fn ordinary_values_match_the_reference() {
    run(Values::Small);
}

/// Values large enough that sums actually reach `i128::MAX`, which the benchmark fixture never did.
#[test]
fn values_near_the_boundary_match_the_reference_or_refuse_together() {
    run(Values::Huge);
}

/// `i128::MIN`, `i128::MAX`, unparseable text, empty strings, leading spaces and signs.
#[test]
fn adversarial_values_match_the_reference_or_refuse_together() {
    run(Values::Adversarial);
}

/// **A representable answer is now returned, not refused.** Roadmap 2.1b, decided and done.
///
/// Credit a party with `i128::MAX`, then `+1`, then `-1`. The true sum is `i128::MAX` and fits
/// exactly. The previous version refused it, because a checked `i128` accumulator that meets the
/// `+1` before the `-1` leaves the range on the way to an answer it could have represented - so
/// whether a query succeeded depended on the order rows happened to arrive in, which for a serving
/// engine means the same data answering on Tuesday and failing on Wednesday.
///
/// The aggregate now carries a high word for any entry that overflows, and refuses on the
/// **answer** rather than on a partial sum. It costs nothing for entries that never overflow, which
/// is all of them on real data.
///
/// DuckDB still refuses this case, in both directions - it is the same order dependence, and it is
/// why `burrmill-bench gen` counts a small number of order-dependent disagreements rather than
/// failing on them.
#[test]
fn a_representable_sum_is_returned_even_when_a_partial_sum_overflows() {
    let sink = "0x0000000000000000000000000000000000000000".to_string();
    let k = "0x0000000000000000000000000000000000000001".to_string();
    let rows = vec![
        Row { from: sink.clone(), to: k.clone(), value: i128::MAX.to_string() },
        Row { from: sink.clone(), to: k.clone(), value: "1".into() },
        Row { from: k.clone(), to: sink.clone(), value: "1".into() },
    ];
    let want = vec![(sink.clone(), -i128::MAX), (k.clone(), i128::MAX)];
    assert_eq!(reference(&rows).unwrap(), want);

    for splits in [1usize, 2, 3] {
        let dir = tempfile::tempdir().unwrap();
        write_segments(dir.path(), &rows, splits);
        assert_eq!(fold(dir.path(), 4).unwrap(), want, "splits={splits}");
    }
}

/// The boundary itself, hand-computed, in both directions.
#[test]
fn the_answer_decides_the_refusal_not_the_partial_sums() {
    let sink = "0x0000000000000000000000000000000000000000".to_string();
    let k = "0x0000000000000000000000000000000000000001".to_string();
    let row = |v: &str| Row { from: sink.clone(), to: k.clone(), value: v.to_string() };
    let big = (i128::MAX / 2 + 1).to_string();

    // MAX + 1 does not fit, whatever order it is reached in.
    for rows in [vec![row(&i128::MAX.to_string()), row("1")], vec![row("1"), row(&i128::MAX.to_string())]] {
        let dir = tempfile::tempdir().unwrap();
        write_segments(dir.path(), &rows, 2);
        assert!(matches!(fold(dir.path(), 4), Err(BurrmillError::Overflow(_))), "MAX + 1 must refuse");
    }

    // Two halves that each fit and together do not.
    let rows = vec![row(&big), row(&big)];
    let dir = tempfile::tempdir().unwrap();
    write_segments(dir.path(), &rows, 2);
    assert!(matches!(fold(dir.path(), 4), Err(BurrmillError::Overflow(_))));

    // Far past the range and back again. The intermediate leaves i128 twice; the answer is 5.
    let rows = vec![
        row(&i128::MAX.to_string()),
        row(&i128::MAX.to_string()),
        Row { from: k.clone(), to: sink.clone(), value: i128::MAX.to_string() },
        Row { from: k.clone(), to: sink.clone(), value: (i128::MAX - 5).to_string() },
    ];
    let dir = tempfile::tempdir().unwrap();
    write_segments(dir.path(), &rows, 4);
    assert_eq!(
        fold(dir.path(), 4).unwrap(),
        vec![(sink.clone(), -5i128), (k.clone(), 5i128)],
        "the answer is 5 however far the running total wandered"
    );
}

/// **An n-table fold, executed rather than merely planned** (roadmap 4.1a).
///
/// Experiment A4 found that every signed fold in a real workload reads *different* tables - a credit
/// and a debit are different events - and that the one-table-read-twice shape Burrmill was built for
/// occurs nowhere. This is the general form: three tables, two adding and one subtracting, written
/// the way the real views write it (`CAST`, `GROUP BY 1`, no `HAVING`, no `ORDER BY`).
#[test]
fn a_three_table_fold_gives_the_same_answer_as_the_reference() {
    let dir = tempfile::tempdir().unwrap();
    let mk = |name: &str, rows: &[(&str, u64)]| {
        let sub = dir.path().join(name);
        std::fs::create_dir_all(&sub).unwrap();
        let evs: Vec<Row> = rows
            .iter()
            .map(|(who, v)| Row {
                from: "0xsink".into(),
                to: (*who).into(),
                value: v.to_string(),
            })
            .collect();
        write_segments(&sub, &evs, 2);
        burrmill::SealedSegments::discover(name, &sub).unwrap()
    };

    let mut catalog = burrmill::Catalog::new();
    catalog.register(mk("deposited", &[("0xaa", 100), ("0xbb", 30), ("0xaa", 5)]));
    catalog.register(mk("rewarded", &[("0xaa", 7), ("0xcc", 11)]));
    catalog.register(mk("withdrawn", &[("0xaa", 40), ("0xbb", 30)]));
    let db = burrmill::Burrmill::with_threads(catalog, 4).unwrap();

    // aa: +100 +5 +7 -40 =  72
    // bb: +30            -30 =   0  -> no HAVING here, so it stays as a zero row
    // cc: +11                 =  11
    let sql = "SELECT who, SUM(v) AS net FROM (
                 SELECT \"to\" AS who,  CAST(\"value\" AS HUGEINT) AS v FROM deposited
                 UNION ALL
                 SELECT \"to\" AS who,  CAST(\"value\" AS HUGEINT) AS v FROM rewarded
                 UNION ALL
                 SELECT \"to\" AS who, -CAST(\"value\" AS HUGEINT) AS v FROM withdrawn
               ) GROUP BY 1";
    let plan = burrmill::plan::plan(sql).expect("the n-table fold must plan");
    let burrmill::Plan::SignedFold(f) = &plan;
    assert_eq!(f.branches.len(), 3);
    assert!(f.branches.iter().all(|b| b.strict_cast), "written with CAST, not TRY_CAST");
    assert!(!f.drop_zero, "no HAVING, so a party netting to zero is still a row");

    let a = db.query(sql, Limits::default()).expect("and must execute");
    let got: Vec<(String, i128)> = a.rows().iter().map(|(k, v)| (k.to_string(), v)).collect();
    assert_eq!(
        got,
        vec![("0xaa".into(), 72i128), ("0xbb".into(), 0i128), ("0xcc".into(), 11i128)]
    );
}

/// The same three tables with `HAVING SUM(v) <> 0` drops the party that nets out, and only that one.
#[test]
fn having_still_drops_the_zero_row_in_an_n_table_fold() {
    let dir = tempfile::tempdir().unwrap();
    for (name, rows) in [("credits", vec![("0xaa", 5u64), ("0xbb", 9)]), ("debits", vec![("0xbb", 9)])] {
        let sub = dir.path().join(name);
        std::fs::create_dir_all(&sub).unwrap();
        let evs: Vec<Row> = rows
            .iter()
            .map(|(w, v)| Row { from: "0xsink".into(), to: (*w).into(), value: v.to_string() })
            .collect();
        write_segments(&sub, &evs, 1);
    }
    let mut catalog = burrmill::Catalog::new();
    for name in ["credits", "debits"] {
        catalog.register(
            burrmill::SealedSegments::discover(name, &dir.path().join(name)).unwrap(),
        );
    }
    let db = burrmill::Burrmill::with_threads(catalog, 2).unwrap();
    let sql = "SELECT who, SUM(v) AS net FROM (
                 SELECT \"to\" AS who,  TRY_CAST(\"value\" AS HUGEINT) AS v FROM credits
                 UNION ALL
                 SELECT \"to\" AS who, -TRY_CAST(\"value\" AS HUGEINT) AS v FROM debits
               ) GROUP BY who HAVING SUM(v) <> 0";
    let a = db.query(sql, Limits::default()).unwrap();
    let got: Vec<(String, i128)> = a.rows().iter().map(|(k, v)| (k.to_string(), v)).collect();
    assert_eq!(got, vec![("0xaa".into(), 5i128)], "bb nets to zero and is dropped");
}
