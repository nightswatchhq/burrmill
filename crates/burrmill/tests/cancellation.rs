//! The cancellation contract (RFC-0044 §3.5, roadmap 5.1).
//!
//! §3.5 makes a specific promise: **the delay between asking a query to stop and it stopping is
//! bounded by one morsel.** The RFC contrasts this with DataFusion, whose joins do not yield to
//! cancellation at all (#19358, with `make_cooperative` still only proposed in #19360), and the
//! point of owning an operator is that it picks its own yield points rather than hoping.
//!
//! The promise had never been tested. This file tests it, including on the path that roadmap 5.3
//! added underneath it: a query now waits at an admission gate before it reaches the fold, and a
//! yield point inside the fold says nothing about a query that has not got there yet.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{Burrmill, BurrmillError, CancelToken, Catalog, Limits, SealedSegments};

const SQL: &str = "SELECT addr, SUM(d) AS net FROM (\
                     SELECT \"to\" AS addr, TRY_CAST(\"value\" AS HUGEINT) AS d FROM t \
                     UNION ALL \
                     SELECT \"from\" AS addr, -TRY_CAST(\"value\" AS HUGEINT) AS d FROM t\
                   ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr";

/// A fixture big enough that a fold takes long enough to be interrupted in the middle of it.
fn fixture(dir: &Path, rows: usize, segments: usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("from", DataType::Utf8, false),
        Field::new("to", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let per = rows / segments;
    for s in 0..segments {
        let from: Vec<String> = (0..per).map(|i| format!("0x{:040x}", (s * per + i) % 90_000)).collect();
        let to: Vec<String> = (0..per).map(|i| format!("0x{:040x}", (s * per + i) * 7 % 90_000)).collect();
        let value: Vec<String> = (0..per).map(|i| (1_000_000 + i as u64 % 97).to_string()).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from((0..per as u64).collect::<Vec<_>>())),
                Arc::new(StringArray::from(from.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(to.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(value.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join(format!("t-{s:05}.parquet"))).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
}

fn open(dir: &Path, threads: usize) -> Burrmill {
    let mut cat = Catalog::new();
    cat.register(SealedSegments::discover("t", dir).unwrap());
    Burrmill::with_threads(cat, threads).unwrap()
}

/// **A query already folding stops within a morsel of being asked.**
#[test]
fn a_running_query_stops_promptly() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path(), 2_000_000, 100);
    let db = open(dir.path(), 4);

    // How long the fold takes uninterrupted, so "promptly" is measured against the real thing.
    let whole = Instant::now();
    db.query(SQL, Limits::default()).unwrap();
    let whole = whole.elapsed();
    assert!(whole > Duration::from_millis(60), "fixture too small to interrupt: {whole:?}");

    let token = CancelToken::new();
    let t = token.clone();
    let db2 = db.clone();
    let started = Instant::now();
    let h = std::thread::spawn(move || db2.query_with_cancel(SQL, Limits::default(), t));
    std::thread::sleep(whole / 4);
    let asked = Instant::now();
    token.cancel();
    let got = h.join().unwrap();
    let delay = asked.elapsed();

    assert!(
        matches!(got, Err(BurrmillError::Cancelled)),
        "a cancelled query must refuse, not answer: {:?}",
        got.map(|a| a.rows().len())
    );
    // One morsel of a hundred, run four ways: a small multiple of that is the bound the RFC means.
    // Generous against a busy CI box, and still an order of magnitude below "it ran to completion".
    assert!(
        delay < whole / 2,
        "stopping took {delay:?}, against a whole query of {whole:?}. The delay is supposed to be \
         bounded by one morsel, not by the rest of the scan"
    );
    assert!(started.elapsed() < whole * 2, "the query outlived its own uninterrupted runtime");
}

/// **A query still waiting for its turn stops too, and this is the one the gate broke.**
///
/// Roadmap 5.3 put an admission gate in front of the pool. A yield point inside the fold says
/// nothing about a query that has not reached the fold: with the gate full of long queries, a
/// cancelled newcomer would sit in the queue until admitted, and *then* notice. That turns a bound
/// of one morsel into a bound of however long everybody ahead of it takes.
#[test]
fn a_queued_query_stops_without_waiting_for_its_turn() {
    let dir = tempfile::tempdir().unwrap();
    fixture(dir.path(), 2_000_000, 100);
    // Width one, so the gate is certainly full while the first query runs.
    let db = open(dir.path(), 2).with_admission_width(1);

    let whole = Instant::now();
    db.query(SQL, Limits::default()).unwrap();
    let whole = whole.elapsed();

    // **One long query holds the gate for its whole run.** An earlier version looped the hog, which
    // released the turn between iterations and let the queued query in almost at once - so the test
    // passed without ever exercising a full gate. A test that cannot fail is not evidence.
    let hog_db = db.clone();
    let hog = std::thread::spawn(move || hog_db.query(SQL, Limits::default()));
    std::thread::sleep(whole / 5);

    // Queue behind it, then cancel while still queued.
    let token = CancelToken::new();
    let t = token.clone();
    let db2 = db.clone();
    let h = std::thread::spawn(move || db2.query_with_cancel(SQL, Limits::default(), t));
    std::thread::sleep(Duration::from_millis(5));
    let asked = Instant::now();
    token.cancel();
    let got = h.join().unwrap();
    let delay = asked.elapsed();
    let _ = hog.join();

    assert!(matches!(got, Err(BurrmillError::Cancelled)), "got {:?}", got.map(|a| a.rows().len()));
    assert!(
        delay < whole / 3,
        "a queued query took {delay:?} to notice it had been cancelled, against a whole query of \
         {whole:?}. It waited for a turn it was never going to use: the gate has to be a yield \
         point too, or the one-morsel bound only holds for queries that already started"
    );
}
