//! The parity corpus in `sqllogictest` format (roadmap 2.3).
//!
//! The generated corpus in `generated_folds.rs` says the fold agrees with a reference on thousands
//! of cases nobody chose. This says the opposite kind of thing: here are the exact answers to a
//! small number of cases somebody did choose, written down where a person can read them.
//!
//! **Why a standard format rather than a bespoke one.** DuckDB leaves the graph in Q4, and when it
//! does, every oracle that *is* DuckDB leaves with it. A `.slt` file is engine-agnostic: these same
//! files can be pointed at DuckDB's own runner today to confirm the expectations are the standard's
//! answers and not merely Burrmill's, and they keep working afterwards when there is nothing left to
//! compare against. Inventing a format would have thrown that away for no gain.
//!
//! The expected values are hand-computed from tables small enough to check on paper. A corpus whose
//! expectations were recorded from the engine under test is a regression test, which is worth
//! having, but it is not a parity oracle - it cannot tell you the engine was ever right.
//!
//! Every file is run at **three segment layouts**. The union of any layout is the same table, so an
//! answer that changes with the layout is a defect and not a slower plan, and getting that property
//! into the corpus costs one loop.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Burrmill, Catalog, SealedSegments};
use sqllogictest::{DBOutput, DefaultColumnType};

/// `(from, to, value)`, exactly as a sealed segment holds them - the value as text, because a
/// `uint256` does not fit anything narrower and nuthatch writes it as digits.
type Rows = &'static [(&'static str, &'static str, &'static str)];

const MAX: &str = "170141183460469231731687303715884105727";

/// The tables every `.slt` file may query, small enough to verify by hand.
///
/// | table | contents | net |
/// |---|---|---|
/// | `t` | aa→bb 100, bb→cc 30, aa→cc 5 | aa -105, bb +70, cc +35 |
/// | `zeros` | aa→bb 50, bb→aa 50 | everything nets to zero, `HAVING` drops it all |
/// | `nulls` | unparseable, empty, and `" 7"` | only the trimmed 7 survives |
/// | `boundary` | aa→bb `i128::MAX` | representable exactly, at the very edge |
/// | `overflow` | aa→bb `i128::MAX`, aa→bb 1 | refused, not wrapped |
const TABLES: &[(&str, Rows)] = &[
    ("t", &[("0xaa", "0xbb", "100"), ("0xbb", "0xcc", "30"), ("0xaa", "0xcc", "5")]),
    ("zeros", &[("0xaa", "0xbb", "50"), ("0xbb", "0xaa", "50")]),
    (
        "nulls",
        &[
            ("0xaa", "0xbb", "not a number"),
            ("0xaa", "0xbb", ""),
            // Trimmed since stage 2 found DuckDB casts this to 7 while `str::parse` refused it,
            // so the row was silently dropped and the balance came back short.
            ("0xaa", "0xbb", " 7"),
        ],
    ),
    ("boundary", &[("0xaa", "0xbb", MAX)]),
    ("overflow", &[("0xaa", "0xbb", MAX), ("0xaa", "0xbb", "1")]),
];

fn write_table(dir: &Path, rows: Rows, splits: usize) -> Vec<std::path::PathBuf> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("from", DataType::Utf8, false),
        Field::new("to", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let per = rows.len().div_ceil(splits.max(1)).max(1);
    let mut out = Vec::new();
    for (i, chunk) in rows.chunks(per).enumerate() {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from((0..chunk.len() as u64).collect::<Vec<_>>())),
                Arc::new(StringArray::from(chunk.iter().map(|r| r.0).collect::<Vec<_>>())),
                Arc::new(StringArray::from(chunk.iter().map(|r| r.1).collect::<Vec<_>>())),
                Arc::new(StringArray::from(chunk.iter().map(|r| r.2).collect::<Vec<_>>())),
            ],
        )
        .unwrap();
        let path = dir.join(format!("seg-{i:05}.parquet"));
        let f = std::fs::File::create(&path).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        out.push(path);
    }
    out
}

fn build(root: &Path, splits: usize) -> Burrmill {
    let mut catalog = Catalog::new();
    for (name, rows) in TABLES {
        let dir = root.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let files = write_table(&dir, rows, splits);
        catalog.register(SealedSegments::from_files(*name, files));
    }
    Burrmill::new(catalog)
}

struct Db(Burrmill);

impl sqllogictest::DB for Db {
    type Error = burrmill::BurrmillError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let answer = self.0.query(sql, burrmill::Limits::default())?;
        let rows = answer.rows();
        Ok(DBOutput::Rows {
            types: vec![DefaultColumnType::Text, DefaultColumnType::Integer],
            rows: (0..rows.len()).map(|i| vec![rows.key(i).to_string(), rows.sum(i).to_string()]).collect(),
        })
    }

    fn engine_name(&self) -> &str {
        "burrmill"
    }
}

#[test]
fn the_corpus_holds_at_every_segment_layout() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/slt");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "slt"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no .slt files in {}", dir.display());

    for splits in [1usize, 2, 7] {
        let root = tempfile::tempdir().unwrap();
        let db = build(root.path(), splits);
        for file in &files {
            let db = db.clone();
            let mut runner = sqllogictest::Runner::new(|| std::future::ready(Ok(Db(db.clone()))));
            runner.run_file(file).unwrap_or_else(|e| {
                panic!("{} failed at {splits} segment(s) per table:\n{e}", file.display())
            });
        }
    }
}
