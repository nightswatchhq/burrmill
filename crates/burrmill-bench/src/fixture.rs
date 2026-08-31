//! The fixture, shaped like a real nest rather than like a benchmark.
//!
//! Segment sizes on a live nest are **bimodal**, and the reason is that there are two seal paths:
//! backfill batches at 20,000 rows and cuts on a data-chosen block boundary, while the tip path
//! seals whatever just finalised - a few blocks carrying a handful of rows. #889 measured the result
//! on `horizon-nest`: 80% of segments under 20 KB, a 6.3 KB median, and the three largest being one
//! busy table's backfill.
//!
//! An even split across files is a different problem, and it would flatter whichever engine handles
//! uniform work best. That is precisely the confound a segment sweep exists to remove, so this
//! writer does not offer one.
//!
//! # Schema width (roadmap 1.1a)
//!
//! **The first version of this fixture was four columns wide and a real nest event is twelve.** That
//! is not a cosmetic difference: the fold reads three columns, so a four-column fixture makes
//! projection pushdown worth almost nothing, and the operator shipped for a whole slice with **no
//! projection at all** without a single measurement noticing. On the real nest it was decoding
//! fourteen columns to read three and running 2.2x DuckDB because of it.
//!
//! A fixture that cannot exhibit a defect cannot guard against it. This one now mirrors what
//! nuthatch actually seals - `_seq`, the contract address, both 66-character hex hashes, the block
//! triple, `log_index`, the table tag, and a second uint256-as-text column the fold does not read -
//! so the columns the query ignores cost something to ignore, exactly as they do in production.
//!
//! `NARROW=1` writes the old four-column schema. It exists so the projection win can be measured
//! rather than asserted, and for no other reason.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

pub struct FixtureSpec {
    pub rows: usize,
    pub segments: usize,
    /// Distinct parties. The high-cardinality gate turns this up: DataFusion's two-phase
    /// aggregation is known to grow memory with core count at high cardinality (#6937, #11680), and
    /// an owned operator that did the same under a 256 MB budget would not deserve to ship.
    pub addrs: usize,
    /// Write the old four-column schema instead of a nest-shaped twelve. Only for measuring what
    /// projection pushdown is worth; never the default, because it hides the thing it is measuring.
    pub narrow: bool,
}

pub fn write(dir: &Path, spec: &FixtureSpec) -> anyhow::Result<usize> {
    if spec.segments <= 1 {
        write_offset(&dir.join("t.parquet"), spec, spec.rows, 0)?;
        return Ok(1);
    }
    let small_count = (spec.segments * 4) / 5;
    let large_count = spec.segments - small_count;
    let small_total = (spec.rows / 20).min(spec.rows);
    let per_small = (small_total / small_count.max(1)).max(1);
    let small_total = per_small * small_count;
    let per_large = (spec.rows - small_total) / large_count.max(1);

    let mut written = 0usize;
    let mut emitted = 0usize;
    for i in 0..spec.segments {
        let n = if i < small_count { per_small } else { per_large };
        let n = if i == spec.segments - 1 { spec.rows - emitted } else { n };
        if n == 0 {
            continue;
        }
        write_offset(&dir.join(format!("t-{i:06}.parquet")), spec, n, emitted)?;
        emitted += n;
        written += 1;
    }
    Ok(written)
}

/// Rows `offset .. offset + rows` of one generated table.
///
/// The offset is what makes a segment sweep mean anything: the union of any layout is exactly the
/// rows a single file of the same total would hold, so **the fold must return an identical answer at
/// every segment count**. A layout that changes the answer is a defect, not a slower plan, and the
/// parity check catches it without needing a separate oracle.
fn write_offset(path: &Path, spec: &FixtureSpec, rows: usize, offset: usize) -> anyhow::Result<()> {
    let n_addr = spec.addrs.max(2);
    let addrs: Vec<String> = (0..n_addr).map(|i| format!("0x{i:040x}")).collect();
    let idx = |i: usize| i + offset;
    let from: Vec<&str> = (0..rows).map(|i| addrs[idx(i) % n_addr].as_str()).collect();
    // A stride coprime with most cardinalities, so credits and debits do not land on the same party
    // and cancel the fold into a trivially empty answer.
    let to: Vec<&str> = (0..rows).map(|i| addrs[(idx(i) * 7 + 3) % n_addr].as_str()).collect();
    // Values past i64 and nowhere near i128, which is the reason a 128-bit cast is in the query at
    // all. A fixture of small values would let a broken cast pass unnoticed.
    let value: Vec<String> = (0..rows)
        .map(|i| format!("{}", 1_000_000_000_000_000_000u128 * (idx(i) as u128 % 97 + 1)))
        .collect();
    let block: Vec<u64> = (0..rows).map(|i| idx(i) as u64 / 100).collect();

    if spec.narrow {
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("from", DataType::Utf8, false),
            Field::new("to", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(block)),
                Arc::new(StringArray::from(from)),
                Arc::new(StringArray::from(to)),
                Arc::new(StringArray::from(
                    value.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
            ],
        )?;
        return finish(path, schema, batch);
    }

    // The columns the fold never looks at, and which a real event always has. Two of them are
    // 66-character hex, which is why decoding what you did not ask for is expensive.
    let seq: Vec<u64> = (0..rows).map(|i| idx(i) as u64).collect();
    let log_index: Vec<u64> = (0..rows).map(|i| (idx(i) % 8) as u64).collect();
    let timestamp: Vec<u64> = (0..rows).map(|i| 1_700_000_000 + idx(i) as u64 / 100 * 12).collect();
    let block_hash: Vec<String> = (0..rows).map(|i| format!("0x{:064x}", idx(i) / 100)).collect();
    let tx_hash: Vec<String> = (0..rows).map(|i| format!("0x{:064x}", idx(i))).collect();
    // A second uint256-as-text the fold does not read. `shares` on the real staking table.
    let shares: Vec<String> =
        (0..rows).map(|i| format!("{}", 3_141_592_653_589u128 * (idx(i) as u128 % 89 + 1))).collect();
    let contract = "0xf55041e37e12cd407ad00ce2910b8269b01263b9";
    let table_tag = "staking__stake_delegated";

    let schema = Arc::new(Schema::new(vec![
        Field::new("_seq", DataType::UInt64, false),
        Field::new("address", DataType::Utf8, false),
        Field::new("block_hash", DataType::Utf8, false),
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("from", DataType::Utf8, false),
        Field::new("log_index", DataType::UInt64, false),
        Field::new("shares", DataType::Utf8, false),
        Field::new("table", DataType::Utf8, false),
        Field::new("to", DataType::Utf8, false),
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let str_col = |v: &[String]| -> Arc<StringArray> {
        Arc::new(StringArray::from(v.iter().map(|s| s.as_str()).collect::<Vec<_>>()))
    };
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(seq)),
            Arc::new(StringArray::from(vec![contract; rows])),
            str_col(&block_hash),
            Arc::new(UInt64Array::from(block)),
            Arc::new(UInt64Array::from(timestamp)),
            Arc::new(StringArray::from(from)),
            Arc::new(UInt64Array::from(log_index)),
            str_col(&shares),
            Arc::new(StringArray::from(vec![table_tag; rows])),
            Arc::new(StringArray::from(to)),
            str_col(&tx_hash),
            str_col(&value),
        ],
    )?;
    finish(path, schema, batch)
}

fn finish(path: &Path, schema: Arc<Schema>, batch: RecordBatch) -> anyhow::Result<()> {
    let file = std::fs::File::create(path)?;
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();
    let mut w = parquet::arrow::ArrowWriter::try_new(file, schema, Some(props))?;
    w.write(&batch)?;
    w.close()?;
    Ok(())
}
