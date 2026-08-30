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
            Arc::new(StringArray::from(value.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
        ],
    )?;
    let file = std::fs::File::create(path)?;
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();
    let mut w = parquet::arrow::ArrowWriter::try_new(file, schema, Some(props))?;
    w.write(&batch)?;
    w.close()?;
    Ok(())
}
