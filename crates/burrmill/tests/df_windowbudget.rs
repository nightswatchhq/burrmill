//! #47: a statement that holds its input in a window allocates within a stated factor of the pool
//! limit, or refuses. The live heap is counted by this binary's allocator, so the file holds one test.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            let now = LIVE.fetch_add(l.size(), Ordering::Relaxed) + l.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) };
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, l, new) };
        if !q.is_null() {
            if new >= l.size() {
                let now = LIVE.fetch_add(new - l.size(), Ordering::Relaxed) + new - l.size();
                PEAK.fetch_max(now, Ordering::Relaxed);
            } else {
                LIVE.fetch_sub(l.size() - new, Ordering::Relaxed);
            }
        }
        q
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

const ROWS: u64 = 400_000;

fn segments() -> Vec<(PathBuf, u64)> {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("df_windowbudget");
    std::fs::create_dir_all(&dir).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("who", DataType::Utf8, false),
        Field::new("amount", DataType::Utf8, false),
    ]));
    (0..8u64)
        .map(|n| {
            let path = dir.join(format!("t-{n:064x}.parquet"));
            if !path.exists() {
                let span = n * ROWS / 8..(n + 1) * ROWS / 8;
                let who: Vec<String> = span.clone().map(|i| format!("0x{i:040x}")).collect();
                let amount: Vec<String> = span
                    .clone()
                    .map(|i| (i * 7919 % 1_000_003).to_string())
                    .collect();
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt64Array::from(span.collect::<Vec<_>>())),
                        Arc::new(StringArray::from(who)),
                        Arc::new(StringArray::from(amount)),
                    ],
                )
                .unwrap();
                let tmp = dir.join(format!("t-{n}.{}.tmp", std::process::id()));
                let f = std::fs::File::create(&tmp).unwrap();
                let mut w = parquet::arrow::ArrowWriter::try_new(f, schema.clone(), None).unwrap();
                w.write(&batch).unwrap();
                w.close().unwrap();
                std::fs::rename(&tmp, &path).unwrap();
            }
            let len = std::fs::metadata(&path).unwrap().len();
            (path, len)
        })
        .collect()
}

fn engine(memory_bytes: usize) -> Engine {
    let mut e = Engine::open_empty_budgeted(burrmill::Budget {
        memory_bytes,
        threads: 8,
        spill: None,
    })
    .unwrap();
    let declared = vec![
        ("block_number".into(), "u64".into()),
        ("who".into(), "address".into()),
        ("amount".into(), "string".into()),
    ];
    e.register_facts("t", &declared, segments(), &[], (None, None))
        .unwrap();
    e
}

/// The live heap may reach this fraction of the pool limit: what the pool cannot see, decoded pages
/// and batches in flight, is a small part of it.
const FACTOR: f64 = 0.75;

/// Runs `sql` under `memory_bytes`, and returns its rows, or the refusal, with the most the heap held
/// above where it started.
fn measured(memory_bytes: usize, sql: &str) -> (Result<Vec<serde_json::Value>, String>, usize) {
    let e = engine(memory_bytes);
    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let got = e.sql(sql);
    let live = PEAK.load(Ordering::Relaxed) - base;
    let rows = got.map_err(|m| m.to_string()).map(|batches| {
        batches
            .iter()
            .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
            .collect()
    });
    (rows, live)
}

/// `indexer.delegators_page`'s shape: two windows over the whole of a grouped relation cross joined
/// with one row. DataFusion collects the grouped side and emits one batch per row of it, each of whose
/// strings sits in its own 8 KiB block, and the window held them all without a charge.
const DELEGATORS: &str = "WITH positions AS (SELECT block_number % 100000 AS k, max(who) AS w1, \
    min(who) AS w2, max(amount) AS a, sum(CAST(amount AS BIGINT)) AS s FROM t GROUP BY 1), \
    pool AS (SELECT count(*) AS n, max(m) AS m FROM \
             (SELECT amount, max(who) AS m FROM t GROUP BY amount) WHERE amount <> '') \
    SELECT p.k, p.w1, p.s, pool.n, COUNT(*) OVER () AS total, \
           COUNT(*) FILTER (WHERE p.k % 3 = 0) OVER () AS active \
    FROM positions p CROSS JOIN pool ORDER BY p.s DESC, p.k LIMIT 25";
/// A window over eight copies of the table, which it must hold, so the pool must be charged for it.
const WHOLE: &str = "SELECT who, amount, c.x, COUNT(*) OVER () AS total FROM t \
    CROSS JOIN (SELECT * FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8)) v(x)) c \
    ORDER BY amount DESC, who, c.x LIMIT 5";

#[test]
fn a_window_allocates_within_the_pool_limit() {
    let mut failed = Vec::new();
    let mut answers = Vec::new();
    for (name, sql, limit) in [
        ("delegators_page at 1 GiB", DELEGATORS, 1usize << 30),
        ("delegators_page at 2 GiB", DELEGATORS, 2 << 30),
        ("a window over eight tables at 256 MiB", WHOLE, 256 << 20),
    ] {
        let (got, live) = measured(limit, sql);
        eprintln!(
            "{name}: live {} MiB, {}",
            live >> 20,
            match &got {
                Ok(rows) => format!("{} rows", rows.len()),
                Err(m) => format!("refused: {}", m.lines().next().unwrap_or_default()),
            }
        );
        if live as f64 > FACTOR * limit as f64 {
            failed.push(format!(
                "{name}: live {live} past {FACTOR} of the limit {limit}"
            ));
        }
        if sql == DELEGATORS {
            answers.push(got.unwrap_or_else(|m| panic!("{name} refused: {m}")));
        }
    }
    assert!(failed.is_empty(), "{failed:#?}");
    let (counts, _) = measured(
        2 << 30,
        "WITH positions AS (SELECT block_number % 100000 AS k, sum(CAST(amount AS BIGINT)) AS s \
         FROM t GROUP BY 1) \
         SELECT count(*) AS total, count(*) FILTER (WHERE k % 3 = 0) AS active FROM positions",
    );
    let counts = &counts.unwrap()[0];
    assert_eq!(answers[0], answers[1]);
    assert_eq!(answers[0].len(), 25);
    for row in &answers[0] {
        assert_eq!(
            (&row["total"], &row["active"]),
            (&counts["total"], &counts["active"])
        );
    }
}
