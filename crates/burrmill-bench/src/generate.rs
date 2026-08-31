//! The generated corpus, three ways (roadmap 2.1).
//!
//! `crates/burrmill/tests/generated_folds.rs` already checks the fold against a non-optimising
//! reference on every `cargo test`. That catches machinery bugs - morsel splitting, the shared
//! aggregate, the parallel sort - and it survives DuckDB's removal in Q4, which is why it lives in
//! the library and runs by default.
//!
//! **It cannot catch a misconception.** The reference implements this author's reading of what
//! `TRY_CAST` and `SUM` mean; if that reading is wrong, the reference and the engine agree with each
//! other and are both wrong together, and no amount of generation notices. So this runs the same
//! generated cases past DuckDB as well, which is an independent implementation of the standard and
//! the only thing here that is ground truth about the *semantics* rather than about the code.
//!
//! The generator is deliberately allowlist-constrained: it varies the data, not the query shape.
//! Burrmill admits one shape, and generating SQL it must refuse would be testing the parser, which
//! `tests/admitted_subset.rs` already does with nineteen hand-written refusals.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    pub fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }
}

#[derive(Clone, Debug)]
pub struct Row {
    pub from: String,
    pub to: String,
    pub value: String,
}

/// Values chosen to reach the boundary rather than to look plausible.
///
/// The benchmark fixture tops out around 1e20 against an `i128::MAX` of 1.7e38, so no amount of
/// running it could ever exercise the refusal path. `i128::MIN` is here because the fold negates
/// every debit and `-i128::MIN` is the one value that does not exist.
/// How one case draws its values.
///
/// **Chosen per case rather than per row, and the reason is a measurement.** With boundary values
/// mixed into every row, a case of twenty-odd rows almost always overflows somewhere: the first
/// version of this corpus ran 3,000 cases and 2,511 of them ended with *both* engines refusing, so
/// only 394 ever compared an actual answer. A corpus that spends 84% of its time agreeing that
/// something is impossible is not testing the fold.
#[derive(Clone, Copy, Debug)]
pub enum Mode {
    /// Values a real nest holds. This is where answers get compared.
    Ordinary,
    /// Ordinary with occasional oddities - signs, unparseable text, zero.
    Mixed,
    /// Deliberately near `i128::MAX`, to reach the refusal path the benchmark fixture never could.
    Boundary,
}

pub fn gen_mode(rng: &mut Rng) -> Mode {
    match rng.below(10) {
        0..=5 => Mode::Ordinary,
        6..=8 => Mode::Mixed,
        _ => Mode::Boundary,
    }
}

/// Values chosen to reach the boundary rather than to look plausible.
///
/// The benchmark fixture tops out around 1e20 against an `i128::MAX` of 1.7e38, so no amount of
/// running it could ever exercise the refusal path. `i128::MIN` is here because the fold negates
/// every debit and `-i128::MIN` is the one value that does not exist.
pub fn gen_value(rng: &mut Rng, mode: Mode) -> String {
    match mode {
        Mode::Ordinary => format!("{}", rng.next() % 1_000_000_000),
        Mode::Mixed => match rng.below(8) {
            0 => "0".into(),
            1 => "not a number".into(),
            2 => "".into(),
            // Deliberately **not** in the pool: "1e18", "7.0", "7.9", "1_000". DuckDB accepts all
            // four and rounds "7.9" to 8; Burrmill returns NULL. That is an undecided semantic
            // difference (roadmap 2.1a), not a bug to be papered over by omitting it from a test,
            // so it lives in `burrmill-bench cast` where it is printed every time anyone looks.
            // Generating it here would only mean a corpus that fails for a reason nobody has
            // chosen yet. Whitespace *is* generated, because that one is now reconciled.
            3 => " 7".into(),
            4 => "7 ".into(),
            5 => "+7".into(),
            6 => format!("-{}", rng.next() % 1_000_000),
            _ => format!("{}", rng.next() % 1_000_000),
        },
        Mode::Boundary => match rng.below(6) {
            0 => i128::MIN.to_string(),
            1 => i128::MAX.to_string(),
            2 => (i128::MAX as u128 / 3).to_string(),
            3 => ((i128::MAX as u128 / 8) * (1 + rng.next() as u128 % 4)).to_string(),
            4 => "-1".into(),
            _ => format!("{}", rng.next() % 1_000_000),
        },
    }
}

pub fn gen_rows(rng: &mut Rng) -> Vec<Row> {
    let mode = gen_mode(rng);
    let n_addrs = 1 + rng.below(10);
    let n_rows = 1 + rng.below(50);
    let addrs: Vec<String> = (0..n_addrs).map(|i| format!("0x{i:040x}")).collect();
    (0..n_rows)
        .map(|_| Row {
            from: addrs[rng.below(n_addrs)].clone(),
            to: addrs[rng.below(n_addrs)].clone(),
            value: gen_value(rng, mode),
        })
        .collect()
}

pub fn write_segments(dir: &Path, rows: &[Row], splits: usize) -> anyhow::Result<()> {
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
                Arc::new(StringArray::from(chunk.iter().map(|r| r.from.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(chunk.iter().map(|r| r.to.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(chunk.iter().map(|r| r.value.as_str()).collect::<Vec<_>>())),
            ],
        )?;
        let f = std::fs::File::create(dir.join(format!("t-{i:05}.parquet")))?;
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None)?;
        w.write(&batch)?;
        w.close()?;
    }
    Ok(())
}
