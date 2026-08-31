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
    let mut acc: BTreeMap<String, i128> = BTreeMap::new();
    let add = |acc: &mut BTreeMap<String, i128>, k: &str, v: i128| -> Result<(), BurrmillError> {
        let e = acc.entry(k.to_string()).or_insert(0);
        *e = e
            .checked_add(v)
            .ok_or_else(|| BurrmillError::Overflow(format!("reference sum for {k}")))?;
        Ok(())
    };
    for r in rows {
        // TRY_CAST: unparseable becomes NULL, and SUM ignores NULLs. Skip, never substitute.
        let Ok(d) = r.value.parse::<i128>() else { continue };
        let minus_d = d
            .checked_neg()
            .ok_or_else(|| BurrmillError::Overflow(format!("reference negation of {d}")))?;
        add(&mut acc, &r.to, d)?;
        add(&mut acc, &r.from, minus_d)?;
    }
    // HAVING SUM(d) <> 0, then canonical byte-wise ascending order, which a BTreeMap over String
    // already gives for these ASCII keys.
    Ok(acc.into_iter().filter(|(_, v)| *v != 0).collect())
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

/// **A representable answer can still be refused, and that is worth pinning down.**
///
/// Credit a party with `i128::MAX`, then `+1`, then `-1`. The true sum is `i128::MAX` and fits
/// exactly. A checked accumulator that meets the `+1` before the `-1` overflows on the way to an
/// answer it could have represented, and refuses.
///
/// So "exact integer arithmetic, refuses on overflow" is a weaker guarantee than it reads: the
/// engine refuses when an intermediate **partial sum** leaves the range, not when the answer does,
/// and which partial sums occur depends on evaluation order. No wrong number is ever returned, which
/// is the part that matters, but some answerable queries are declined.
///
/// The generated corpus found DuckDB doing the mirror image of this - answering where Burrmill
/// refuses, and refusing where Burrmill answers - which is how the whole question surfaced.
///
/// This test asserts today's behaviour on purpose. Fixing it means accumulating wider than `i128`
/// so that refusal depends only on the answer, and **this test should then be inverted** rather than
/// deleted. Roadmap 2.1b carries the decision and its cost.
#[test]
fn a_representable_sum_is_refused_when_a_partial_sum_overflows() {
    let sink = "0x0000000000000000000000000000000000000000".to_string();
    let k = "0x0000000000000000000000000000000000000001".to_string();
    let rows = vec![
        Row { from: sink.clone(), to: k.clone(), value: i128::MAX.to_string() },
        Row { from: sink.clone(), to: k.clone(), value: "1".into() },
        Row { from: k.clone(), to: sink.clone(), value: "1".into() },
    ];

    // The reference refuses too, and for the same reason: it is the arithmetic that is
    // order-dependent, not this engine's parallelism.
    assert!(matches!(reference(&rows), Err(BurrmillError::Overflow(_))));

    for splits in [1usize, 2, 3] {
        let dir = tempfile::tempdir().unwrap();
        write_segments(dir.path(), &rows, splits);
        match fold(dir.path(), 4) {
            Err(BurrmillError::Overflow(_)) => {}
            other => panic!(
                "splits={splits}: expected today's false refusal, got {other:?}. If this is a \
                 deliberate fix to accumulate wider than i128, invert this test and say so."
            ),
        }
    }
}
