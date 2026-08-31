//! The concurrency sweep (roadmap 5.2, RFC-0044 §3.5).
//!
//! **This is the claim the whole architecture rests on and it has never been measured here.**
//! Everything else in this repo is one query's latency and one query's memory. §3.5's argument is
//! about many clients at once: #986 measured DuckDB going from 40.3 qps to 39.6 between one client
//! and thirty-two while p99 went **29.5 ms to 7066 ms**, because it sits behind a single connection
//! mutex. Burrmill takes no global lock, so this ought to be the easiest headline in the project -
//! which is exactly why it gets measured rather than asserted.
//!
//! Three arms, because "DuckDB is slow under load" is only true of one way of embedding it and
//! saying so without the other would be the kind of flattering comparison this repo spends its time
//! catching:
//!
//! - **`duck_shared`** - one `Connection` behind a `Mutex`, which is what nuthatch actually does and
//!   what #986 measured. The mutex is the finding, not a handicap invented here.
//! - **`duck_multi`** - one database, one connection per client via `try_clone`. This is DuckDB's
//!   own concurrent model and the fair comparison. The first version opened a fresh
//!   `Connection::open_in_memory()` per client, which is a fresh **database** per client - four
//!   independent DuckDB instances with eight threads each, rather than one server with four
//!   clients. It read as DuckDB scaling beautifully and was a strawman in its favour, caught on the
//!   first run because four clients produced 221 qps against eight threads' worth of work.
//! - **`burrmill`** - one handle, cloned. It holds an `Arc<ThreadPool>` of `max_threads`, so every
//!   client shares **one** eight-thread pool however many clients there are. That is the serving
//!   model the thread budget was chosen for (roadmap 1.2c), and if the choice was wrong this is
//!   where it shows.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SQL: &str = "SELECT addr, SUM(d) AS net FROM (\
                     SELECT \"to\" AS addr, TRY_CAST(\"value\" AS HUGEINT) AS d FROM t \
                     UNION ALL \
                     SELECT \"from\" AS addr, -TRY_CAST(\"value\" AS HUGEINT) AS d FROM t\
                   ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr";

/// Latencies from one arm, in microseconds, **kept per client**.
struct Sample {
    /// One sorted vector per client.
    by_client: Vec<Vec<u64>>,
    lat: Vec<u64>,
    secs: f64,
    /// Queries completed by each client.
    per_client: Vec<usize>,
}

impl Sample {
    fn pct(&self, p: f64) -> f64 {
        Self::pct_of(&self.lat, p)
    }

    fn pct_of(v: &[u64], p: f64) -> f64 {
        if v.is_empty() {
            return f64::NAN;
        }
        v[((v.len() - 1) as f64 * p).round() as usize] as f64 / 1000.0
    }

    /// **The tail as a client actually experiences it.**
    ///
    /// Pooling every client's latencies weights by throughput, so a client that was starved
    /// contributes almost no samples and cannot move a percentile however badly it was treated. The
    /// mutex arm duly reported 82 qps at 32 clients with a 14 ms p99 - two numbers that cannot both
    /// be true - and the per-client counts said why: one client ran 191 queries and another ran
    /// **none**. This is the worst client's own p99, which is the number a p99 is supposed to be.
    fn worst_client_p99(&self) -> f64 {
        self.by_client
            .iter()
            .filter(|v| !v.is_empty())
            .map(|v| Self::pct_of(v, 0.99))
            .fold(f64::NAN, f64::max)
    }

    /// Slowest client's throughput as a fraction of the fastest's. One is perfect sharing; zero is a
    /// client that never got served at all.
    fn fairness(&self) -> f64 {
        let lo = self.per_client.iter().min().copied().unwrap_or(0) as f64;
        let hi = self.per_client.iter().max().copied().unwrap_or(0) as f64;
        if hi == 0.0 {
            return f64::NAN;
        }
        lo / hi
    }
    fn qps(&self) -> f64 {
        self.lat.len() as f64 / self.secs
    }
}

/// One client's work: a closure it calls in a loop.
///
/// Built by the caller **before** the threads start, because `duckdb::Connection` is `Send` but not
/// `Sync`: a client's connection has to be moved into its thread rather than shared into it.
type Runner = Box<dyn FnMut() -> anyhow::Result<usize> + Send>;

/// Run one thread per runner for `secs` and collect every latency.
fn drive(secs: f64, runners: Vec<Runner>) -> Sample {
    let stop = Arc::new(AtomicBool::new(false));
    let started = Instant::now();
    let mut all: Vec<u64> = Vec::new();
    let mut per_client: Vec<usize> = Vec::new();
    let mut by_client: Vec<Vec<u64>> = Vec::new();
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for mut run in runners {
            let stop = stop.clone();
            handles.push(scope.spawn(move || {
                let mut lat = Vec::new();
                // One untimed call first: the first query on a fresh DuckDB connection binds the
                // Parquet view, and charging that to the sweep would measure setup.
                let _ = run();
                while !stop.load(Ordering::Relaxed) {
                    let t = Instant::now();
                    let n = run().expect("a client query must not fail mid-sweep");
                    std::hint::black_box(n);
                    lat.push(t.elapsed().as_micros() as u64);
                }
                lat
            }));
        }
        // The driver sleeps rather than the clients, so every client is loaded for the same window.
        std::thread::sleep(Duration::from_secs_f64(secs));
        stop.store(true, Ordering::Relaxed);
        for h in handles {
            let mut lat = h.join().expect("client thread panicked");
            lat.sort_unstable();
            per_client.push(lat.len());
            all.extend(lat.iter().copied());
            by_client.push(lat);
        }
    });
    let elapsed = started.elapsed().as_secs_f64();
    all.sort_unstable();
    Sample { by_client, lat: all, secs: elapsed, per_client }
}

fn duck_conn(dir: &str) -> anyhow::Result<duckdb::Connection> {
    let conn = duckdb::Connection::open_in_memory()?;
    conn.execute_batch(&format!("SET threads TO {};", crate::oracles::thread_budget()))?;
    conn.execute_batch(&format!(
        "CREATE VIEW t AS SELECT * FROM read_parquet('{dir}/*.parquet');"
    ))?;
    Ok(conn)
}

fn duck_query(conn: &duckdb::Connection) -> anyhow::Result<usize> {
    let sql = SQL.replace("SUM(d) AS net", "SUM(d)::VARCHAR AS net");
    let mut stmt = conn.prepare(&sql)?;
    let mut n = 0usize;
    let mut rows = stmt.query([])?;
    while rows.next()?.is_some() {
        n += 1;
    }
    Ok(n)
}

pub fn run(dir: &str) -> anyhow::Result<()> {
    let secs: f64 = std::env::var("SECONDS").ok().and_then(|s| s.parse().ok()).unwrap_or(3.0);
    let counts: Vec<usize> = std::env::var("CLIENTS")
        .unwrap_or_else(|_| "1,2,4,8,16,32".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    // Parity first, untimed, as everywhere else in this repo: a throughput number from engines that
    // disagree is not fast-versus-slow, it is meaningless.
    let db = {
        let mut cat = burrmill::Catalog::new();
        cat.register(burrmill::SealedSegments::discover("t", std::path::Path::new(dir))?);
        burrmill::Burrmill::with_threads(cat, crate::oracles::thread_budget())?
    };
    let ours = db.query(SQL, burrmill::Limits::default())?.rows().len();
    let theirs = duck_query(&duck_conn(dir)?)?;
    anyhow::ensure!(
        ours == theirs,
        "PARITY FAILED before the sweep: Burrmill {ours} rows against DuckDB's {theirs}. No \
         throughput is reported."
    );
    println!("parity:  verified on {ours} parties, {secs}s per point, {} threads per query\n",
        crate::oracles::thread_budget());
    println!(
        "{:<8} {:<12} {:>8} {:>8} {:>10} {:>7}  queries per client: min..max",
        "clients", "engine", "qps", "p50_ms", "worstp99", "fair"
    );

    for &n in &counts {
        let shared = Arc::new(Mutex::new(duck_conn(dir)?));
        let runners: Vec<Runner> = (0..n)
            .map(|_| {
                let c = shared.clone();
                Box::new(move || duck_query(&c.lock().expect("poisoned"))) as Runner
            })
            .collect();
        report(n, "duck_shared", &drive(secs, runners));

        let base = duck_conn(dir)?;
        let mut runners: Vec<Runner> = Vec::with_capacity(n);
        for _ in 0..n {
            let conn = base.try_clone()?;
            runners.push(Box::new(move || duck_query(&conn)));
        }
        report(n, "duck_multi", &drive(secs, runners));

        let runners: Vec<Runner> = (0..n)
            .map(|_| {
                let db = db.clone();
                Box::new(move || {
                    Ok(db.query(SQL, burrmill::Limits::default()).map(|a| a.rows().len())?)
                }) as Runner
            })
            .collect();
        report(n, "burrmill", &drive(secs, runners));
        println!();
    }
    println!("peak_rss_mb={}", crate::rss_mb());
    Ok(())
}

fn report(clients: usize, engine: &str, s: &Sample) {
    let lo = s.per_client.iter().min().copied().unwrap_or(0);
    let hi = s.per_client.iter().max().copied().unwrap_or(0);
    println!(
        "{clients:<8} {engine:<12} {:>8.1} {:>8.1} {:>10.1} {:>7.2}  {lo}..{hi}{}",
        s.qps(),
        s.pct(0.50),
        s.worst_client_p99(),
        s.fairness(),
        if lo == 0 { "  STARVED" } else { "" }
    );
}
