//! RFC-0044 slice 1: the go/no-go.
//!
//! Reproduce nuthatch's #987 result in Burrmill proper - the owned operator at or under DuckDB's
//! latency on `net_balances`, at exact parity, across a segment sweep. If that does not reproduce,
//! the thesis is unproven and the honest outcome is to stop here cheaply.
//!
//! The discipline is RFC-0004's and it is not decoration:
//!
//! - **Parity first, untimed, before a single figure is printed.** An earlier version of the nuthatch
//!   gate emitted its RESULT line and *then* bailed on a parity failure, so an invalid comparison
//!   could be copied into a record before anyone read the error. Timings from engines that disagree
//!   are not fast-versus-slow, they are meaningless.
//! - **Run order is a confound.** Whichever engine goes first pays to warm the page cache and the
//!   second gets it free; the first such measurement was 3.9x apart for exactly that reason.
//!   `ORDER=burrmill_first` runs it the other way. A ratio that survives both orderings is about the
//!   engines.
//! - **Repeats inside one process**, median reported. Writing a ten-thousand-file fixture costs far
//!   more than the query, so a fresh process per repeat would be measuring the fixture writer.
//! - **`BREAK_PARITY=1`** drops a row from the candidate on purpose. The run must then refuse with no
//!   RESULT line. A guard nobody has watched refuse is not a guard.

mod fixture;
mod oracles;

use std::path::{Path, PathBuf};
use std::time::Instant;

use fixture::FixtureSpec;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

/// `is_ok()` alone is true for an **empty** value, so `FLAG=` would read as set. That exact bug
/// silently forced one ordering for a whole sweep, and it was found by the ordering control - which
/// is the point of having one.
fn env_flag(key: &str) -> bool {
    std::env::var(key).map(|v| !matches!(v.trim(), "" | "0" | "false")).unwrap_or(false)
}

/// **Peak** resident set size, not current.
///
/// This used to read `ps -o rss=` on macOS, which reports the process's RSS *right now*. Peak memory
/// in a fold happens while the aggregate and the answer are both live, and by the time the harness
/// asks, that moment has passed and the allocator has handed pages back. The Linux branch read
/// `VmHWM`, a true high-water mark, so the same code reported two different kinds of number
/// depending on the machine and the friendlier one came from the development laptop. That is the
/// third measurement defect found in a day and it is the same shape as the other two: the harness
/// flattering the thing it was built to check.
///
/// `getrusage` gives a genuine high-water mark on both. The unit does not agree between them -
/// bytes on macOS, kilobytes on Linux - which is a trap worth naming rather than a curiosity.
fn rss_mb() -> u64 {
    // SAFETY: `getrusage` writes a plain POD struct through the pointer and returns 0 on success.
    // Zeroed is a valid `rusage`, and nothing here retains the pointer.
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut ru) != 0 {
            return 0;
        }
        let max = ru.ru_maxrss as u64;
        if cfg!(target_os = "macos") {
            max >> 20
        } else {
            max >> 10
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match std::env::args().nth(1).as_deref() {
        Some("inspect") => inspect(),
        Some("explain") => explain(),
        Some("fold") => fold_only(),
        Some("nest") => nest(),
        _ => bench().await,
    }
}

/// Print a segment's schema. Needed before pointing the fold at a real nest, because a nest table's
/// columns are whatever the contract's event declared and guessing them is how you get a parity
/// failure that looks like an engine bug.
fn inspect() -> anyhow::Result<()> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let path = std::env::args().nth(2).ok_or_else(|| anyhow::anyhow!("usage: inspect <file|dir>"))?;
    let p = PathBuf::from(&path);
    let file = if p.is_dir() {
        std::fs::read_dir(&p)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .find(|f| f.extension().is_some_and(|x| x == "parquet"))
            .ok_or_else(|| anyhow::anyhow!("no parquet under {path}"))?
    } else {
        p
    };
    let b = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&file)?)?;
    let md = b.metadata();
    println!("file:       {}", file.display());
    println!("rows:       {}", md.file_metadata().num_rows());
    println!("row groups: {}", md.num_row_groups());
    println!("schema:");
    for f in b.schema().fields() {
        println!("  {:<28} {}", f.name(), f.data_type());
    }
    Ok(())
}

/// `EXPLAIN` against a directory of segments, without running anything.
fn explain() -> anyhow::Result<()> {
    let dir = std::env::args().nth(2).ok_or_else(|| anyhow::anyhow!("usage: explain <dir>"))?;
    let db = burrmill::Burrmill::open_segments("t", Path::new(&dir))?;
    let sql = std::env::args().nth(3).unwrap_or_else(|| {
        "SELECT addr, SUM(d) AS net FROM (\
           SELECT \"to\" AS addr, TRY_CAST(\"value\" AS HUGEINT) AS d FROM t \
           UNION ALL \
           SELECT \"from\" AS addr, -TRY_CAST(\"value\" AS HUGEINT) AS d FROM t\
         ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr"
            .into()
    });
    println!("{}", db.explain(&sql)?);
    Ok(())
}

async fn bench() -> anyhow::Result<()> {
    let spec = FixtureSpec {
        rows: env_usize("ROWS", 2_000_000),
        segments: env_usize("SEGMENTS", 1).max(1),
        addrs: env_usize("ADDRS", 512),
        // Nest-shaped by default. NARROW=1 writes the old four-column schema, which is only useful
        // for measuring what projection pushdown is worth - see fixture.rs.
        narrow: env_flag("NARROW"),
    };
    let repeats = env_usize("REPEATS", 1).max(1);
    let burrmill_first = matches!(std::env::var("ORDER").as_deref(), Ok("burrmill_first"));

    let tmp = tempfile::tempdir()?;
    let seg: PathBuf = match std::env::var("KEEP_FIXTURE") {
        Ok(d) => {
            std::fs::create_dir_all(&d)?;
            PathBuf::from(d)
        }
        Err(_) => tmp.path().to_path_buf(),
    };

    println!(
        "== RFC-0044 slice 1 gate: {} rows / {} segment(s) / {} distinct parties ==",
        spec.rows, spec.segments, spec.addrs
    );
    let t = Instant::now();
    let written = fixture::write(&seg, &spec)?;
    println!("fixture: {written} files in {:?} (rss {} MB)", t.elapsed(), rss_mb());

    // ---- parity, first, untimed ------------------------------------------------------------
    let (duck0, _) = oracles::duckdb(&seg)?;
    let (df0, _) = oracles::datafusion(&seg).await?;
    let (mut bm0, _, metrics) = oracles::burrmill(&seg)?;
    if env_flag("BREAK_PARITY") {
        bm0.pop();
    }
    for (label, got) in [("datafusion", &df0), ("burrmill", &bm0)] {
        if *got != duck0 {
            let first: Vec<String> = duck0
                .iter()
                .zip(got.iter())
                .filter(|(a, b)| a != b)
                .take(3)
                .map(|(a, b)| format!("duck {a:?} vs {label} {b:?}"))
                .collect();
            anyhow::bail!(
                "PARITY FAILED for {label}: {} rows against DuckDB's {}; first differences: \
                 {first:?}. No timing is reported - a comparison between engines that disagree is \
                 not a measurement.",
                got.len(),
                duck0.len()
            );
        }
    }
    println!(
        "parity:  verified on {} parties  (burrmill read {} rows across {} morsels, skipped {}; \
         plan {} ms, scan {} ms, merge {} ms)",
        duck0.len(),
        metrics.rows_read,
        metrics.morsels,
        metrics.rows_skipped,
        metrics.plan_ms,
        metrics.scan_ms,
        metrics.merge_ms
    );

    // ---- timings, interleaved --------------------------------------------------------------
    let mut d_all = Vec::new();
    let mut f_all = Vec::new();
    let mut b_all = Vec::new();
    for _ in 0..repeats {
        if burrmill_first {
            let (_, b) = (0, oracles::burrmill(&seg)?.1);
            let (_, f) = ((), oracles::datafusion(&seg).await?.1);
            let (_, d) = ((), oracles::duckdb(&seg)?.1);
            b_all.push(b);
            f_all.push(f);
            d_all.push(d);
        } else {
            d_all.push(oracles::duckdb(&seg)?.1);
            f_all.push(oracles::datafusion(&seg).await?.1);
            b_all.push(oracles::burrmill(&seg)?.1);
        }
    }
    d_all.sort_unstable();
    f_all.sort_unstable();
    b_all.sort_unstable();
    let dm = d_all[d_all.len() / 2];
    let fm = f_all[f_all.len() / 2];
    let bm = b_all[b_all.len() / 2];

    println!(
        "RESULT\trows={}\tsegments={}\taddrs={}\trepeats={repeats}\torder={}\tduck_ms={dm}\tdf_ms={fm}\tburrmill_ms={bm}\tdf_ratio={:.2}\tburrmill_ratio={:.2}\tparity=verified\tgroups={}\trss_mb={}",
        spec.rows,
        spec.segments,
        spec.addrs,
        if burrmill_first { "burrmill_first" } else { "duck_first" },
        fm as f64 / dm.max(1) as f64,
        bm as f64 / dm.max(1) as f64,
        duck0.len(),
        rss_mb()
    );
    println!("duck_all={d_all:?}");
    println!("df_all={f_all:?}");
    println!("burrmill_all={b_all:?}");
    Ok(())
}

/// Burrmill alone against an existing fixture, so peak RSS is the *operator's* and not a process
/// that has also linked DuckDB and instantiated a DataFusion session.
///
/// The combined harness reports its own footprint, and that number is honest about what it measures
/// but useless for the 256 MB gate: most of it is the two oracles. Reporting it as Burrmill's would
/// be the kind of flattering-by-accident measurement RFC-0004 exists to prevent - here it flatters
/// in the wrong direction, but a number that does not measure what it claims is no better for being
/// pessimistic.
fn fold_only() -> anyhow::Result<()> {
    let dir = std::env::args().nth(2).ok_or_else(|| anyhow::anyhow!("usage: fold <dir>"))?;
    let repeats = env_usize("REPEATS", 3).max(1);
    let db = burrmill::Burrmill::open_segments("t", Path::new(&dir))?;
    let sql = "SELECT addr, SUM(d) AS net FROM (\
                 SELECT \"to\" AS addr, TRY_CAST(\"value\" AS HUGEINT) AS d FROM t \
                 UNION ALL \
                 SELECT \"from\" AS addr, -TRY_CAST(\"value\" AS HUGEINT) AS d FROM t\
               ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr";

    let mut all = Vec::new();
    let mut rows = 0usize;
    let mut metrics = burrmill::FoldMetrics::default();
    for _ in 0..repeats {
        let t = Instant::now();
        // **Nothing is materialised here, and that is the point of this subcommand.** The RSS gate
        // is a claim about the operator, so the harness must not add a copy of the answer to the
        // thing it is measuring. `oracles::burrmill` collects into `Vec<(String, i128)>` because the
        // parity comparison needs owned rows; doing that here put a million fresh `String`s inside
        // the number and made the operator look 80 MB worse than it is. Same family of defect as
        // roadmap 1.1: a harness measuring itself.
        let answer = db.query(sql, burrmill::Limits::default())?;
        all.push(t.elapsed().as_millis());
        rows = answer.rows().len();
        metrics = answer.metrics();
    }
    all.sort_unstable();
    println!(
        "FOLD\tgroups={rows}\tmorsels={}\trows_read={}\tmedian_ms={}\tplan_ms={}\tscan_ms={}\tmerge_ms={}\tagg_mb={}\tall={all:?}\tpeak_rss_mb={}",
        metrics.morsels,
        metrics.rows_read,
        all[all.len() / 2],
        metrics.plan_ms,
        metrics.scan_ms,
        metrics.merge_ms,
        metrics.agg_bytes >> 20,
        rss_mb()
    );
    Ok(())
}

/// Run the fold against a **real nest's sealed segments, read-only**, head to head with DuckDB.
///
/// Nothing here writes to the nest, copies it, or opens anything but the Parquet files for reading.
/// A synthetic fixture can only prove the operator is fast on a table shaped the way its author
/// imagined; this proves it reads what nuthatch actually seals - the real bimodal size distribution,
/// the real twelve-column event schema, the real uint256-as-text values.
///
///     burrmill-bench nest <segments-dir> <table-prefix>
///     CREDIT=receiver DEBIT=payer VALUE=tokens burrmill-bench nest ... escrow__deposit
///
/// # Phase decomposition (roadmap 1.1)
///
/// The first version of this harness reported one number per engine and the real-nest ratios it
/// produced were not measuring the fold. DuckDB's time was flat at roughly 620 ms across tables
/// whose row counts differed fifty-twofold, which is not what a query engine's time does. The cause
/// is that a nest keeps every table's segments in **one** directory - 38,429 files in the nest this
/// was run against - and `read_parquet('<dir>/<prefix>-*.parquet')` enumerates and pattern-matches
/// all of them on every execution, while Burrmill's `open_nest_table` did its one `read_dir`
/// *outside* the timed region. The harness was charging DuckDB for catalog construction and giving
/// Burrmill the same work for free.
///
/// So both are now measured both ways, and the phases are printed rather than summed:
///
/// - `glob` - DuckDB's own `glob()` over the pattern, i.e. directory enumeration alone.
/// - `bind` / `exec` - `prepare()` against row iteration, splitting planning from execution.
/// - `list` - the same query with the file list materialised once and passed explicitly, which is
///   the catalog Burrmill is given.
/// - Burrmill's `plan` / `scan` / `merge` come from `FoldMetrics` and were always there.
///
/// A server answering a query holds a catalog; it does not re-stat the directory per request. That
/// makes `list` against Burrmill's existing figure the like-for-like comparison, and the glob
/// figure a measurement of how much a nest directory costs to enumerate.
fn nest() -> anyhow::Result<()> {
    let dir = std::env::args().nth(2).ok_or_else(|| anyhow::anyhow!("usage: nest <dir> <prefix>"))?;
    let prefix = std::env::args().nth(3).ok_or_else(|| anyhow::anyhow!("usage: nest <dir> <prefix>"))?;
    let credit = std::env::var("CREDIT").unwrap_or_else(|_| "receiver".into());
    let debit = std::env::var("DEBIT").unwrap_or_else(|_| "payer".into());
    let value = std::env::var("VALUE").unwrap_or_else(|_| "tokens".into());
    let repeats = env_usize("REPEATS", 5).max(1);

    let sql = format!(
        "SELECT addr, SUM(d) AS net FROM (\
           SELECT \"{credit}\" AS addr, TRY_CAST(\"{value}\" AS HUGEINT) AS d FROM t \
           UNION ALL \
           SELECT \"{debit}\" AS addr, -TRY_CAST(\"{value}\" AS HUGEINT) AS d FROM t\
         ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr"
    );

    let db = burrmill::Burrmill::open_nest_table("t", Path::new(&dir), &format!("{prefix}-"))?;
    println!("{}", db.explain(&sql)?);

    // DuckDB over the same files, selected by the same prefix. The glob is DuckDB's own way of
    // naming many segments, and it is the only way to keep the two engines reading identical bytes.
    let glob = format!("{dir}/{prefix}-*.parquet");
    // DuckDB's client cannot hand back a HUGEINT directly, so its projection casts to text and the
    // harness parses. Burrmill returns the i128 as an i128, which is the point.
    let duck_sql = sql.replace("SUM(d) AS net", "SUM(d)::VARCHAR AS net");

    // How big is the haystack the glob searches, as against the needles it returns? This is the
    // whole of the 1.1 question and it costs one `read_dir` to answer.
    let (dir_entries, prefix_files) = {
        let mut total = 0usize;
        let mut matched: Vec<PathBuf> = Vec::new();
        for e in std::fs::read_dir(&dir)?.flatten() {
            total += 1;
            let p = e.path();
            if p.extension().is_some_and(|x| x == "parquet")
                && p.file_name()
                    .and_then(|f| f.to_str())
                    .is_some_and(|f| f.starts_with(&format!("{prefix}-")))
            {
                matched.push(p);
            }
        }
        matched.sort();
        (total, matched)
    };

    // DuckDB reading the identical files, named explicitly instead of by pattern. This is the same
    // bytes and the same plan with the directory scan removed - the catalog handed over rather than
    // rediscovered.
    let file_list = prefix_files
        .iter()
        .map(|p| format!("'{}'", p.display()))
        .collect::<Vec<_>>()
        .join(",");
    let list_sql = duck_sql.clone();

    let run_duck = || -> anyhow::Result<(oracles::Rows, u128)> {
        let conn = duckdb::Connection::open_in_memory()?;
        conn.execute_batch(&format!("CREATE VIEW t AS SELECT * FROM read_parquet('{glob}');"))?;
        let t = std::time::Instant::now();
        let mut stmt = conn.prepare(&duck_sql)?;
        let mut out = Vec::new();
        let mut rows = stmt.query([])?;
        while let Some(r) = rows.next()? {
            let addr: String = r.get(0)?;
            let net: String = r.get(1)?;
            out.push((addr, net.parse::<i128>()?));
        }
        Ok((out, t.elapsed().as_millis()))
    };

    // The same run, split at `prepare()`. Binding a Parquet scan is where DuckDB expands the glob
    // and unifies the schema across every matched file; execution is where it reads rows.
    let run_duck_split = || -> anyhow::Result<(u128, u128)> {
        let conn = duckdb::Connection::open_in_memory()?;
        conn.execute_batch(&format!("CREATE VIEW t AS SELECT * FROM read_parquet('{glob}');"))?;
        let t = std::time::Instant::now();
        let mut stmt = conn.prepare(&duck_sql)?;
        let bind_ms = t.elapsed().as_millis();
        let t2 = std::time::Instant::now();
        let mut rows = stmt.query([])?;
        let mut n = 0u64;
        while let Some(r) = rows.next()? {
            let _: String = r.get(0)?;
            let _: String = r.get(1)?;
            n += 1;
        }
        std::hint::black_box(n);
        Ok((bind_ms, t2.elapsed().as_millis()))
    };

    // Directory enumeration alone, using DuckDB's own glob function so the number is DuckDB's cost
    // and not this harness's `read_dir`.
    let run_duck_glob = || -> anyhow::Result<u128> {
        let conn = duckdb::Connection::open_in_memory()?;
        let t = std::time::Instant::now();
        let mut stmt = conn.prepare(&format!("SELECT count(*) FROM glob('{glob}')"))?;
        let mut rows = stmt.query([])?;
        while let Some(r) = rows.next()? {
            let n: i64 = r.get(0)?;
            std::hint::black_box(n);
        }
        Ok(t.elapsed().as_millis())
    };

    // The catalog handed over: an explicit file list, no pattern to expand.
    let duck_over = |files: &[PathBuf]| -> anyhow::Result<u128> {
        let list = files
            .iter()
            .map(|p| format!("'{}'", p.display()))
            .collect::<Vec<_>>()
            .join(",");
        let conn = duckdb::Connection::open_in_memory()?;
        conn.execute_batch(&format!("CREATE VIEW t AS SELECT * FROM read_parquet([{list}]);"))?;
        let t = std::time::Instant::now();
        let mut stmt = conn.prepare(&duck_sql)?;
        let mut n = 0u64;
        let mut rows = stmt.query([])?;
        while let Some(r) = rows.next()? {
            let _: String = r.get(0)?;
            let _: String = r.get(1)?;
            n += 1;
        }
        std::hint::black_box(n);
        Ok(t.elapsed().as_millis())
    };

    let burrmill_over = |files: &[PathBuf]| -> anyhow::Result<u128> {
        let segs = burrmill::SealedSegments::from_files("t", files.to_vec());
        let mut cat = burrmill::Catalog::new();
        cat.register(segs);
        let db = burrmill::Burrmill::new(cat);
        let t = std::time::Instant::now();
        let a = db.query(&sql, burrmill::Limits::default())?;
        std::hint::black_box(a.rows().len());
        Ok(t.elapsed().as_millis())
    };

    let run_duck_list = || -> anyhow::Result<(oracles::Rows, u128)> {
        let conn = duckdb::Connection::open_in_memory()?;
        conn.execute_batch(&format!(
            "CREATE VIEW t AS SELECT * FROM read_parquet([{file_list}]);"
        ))?;
        let t = std::time::Instant::now();
        let mut stmt = conn.prepare(&list_sql)?;
        let mut out = Vec::new();
        let mut rows = stmt.query([])?;
        while let Some(r) = rows.next()? {
            let addr: String = r.get(0)?;
            let net: String = r.get(1)?;
            out.push((addr, net.parse::<i128>()?));
        }
        Ok((out, t.elapsed().as_millis()))
    };

    let run_burrmill = || -> anyhow::Result<(oracles::Rows, u128, burrmill::FoldMetrics)> {
        let t = std::time::Instant::now();
        let a = db.query(&sql, burrmill::Limits::default())?;
        let ms = t.elapsed().as_millis();
        Ok((a.rows().iter().map(|(k, v)| (k.to_string(), v)).collect(), ms, a.metrics()))
    };

    // Burrmill paying for its own catalog, which the previous harness did outside the timer.
    let run_burrmill_cold = || -> anyhow::Result<u128> {
        let t = std::time::Instant::now();
        let db = burrmill::Burrmill::open_nest_table("t", Path::new(&dir), &format!("{prefix}-"))?;
        let a = db.query(&sql, burrmill::Limits::default())?;
        std::hint::black_box(a.rows().len());
        Ok(t.elapsed().as_millis())
    };

    // Parity first, untimed, as everywhere else.
    let (d0, _) = run_duck()?;
    let (b0, _, metrics) = run_burrmill()?;
    if d0 != b0 {
        let first: Vec<String> = d0
            .iter()
            .zip(b0.iter())
            .filter(|(a, b)| a != b)
            .take(3)
            .map(|(a, b)| format!("duck {a:?} vs burrmill {b:?}"))
            .collect();
        anyhow::bail!(
            "PARITY FAILED on {prefix}: burrmill {} rows against DuckDB's {}; first differences: \
             {first:?}. No timing is reported.",
            b0.len(),
            d0.len()
        );
    }
    // The explicit-list variant reads the same files by another name, so it must agree too. If it
    // does not, the file list is not the set the glob matched and every figure below is void.
    let (l0, _) = run_duck_list()?;
    if l0 != d0 {
        anyhow::bail!(
            "PARITY FAILED between DuckDB's glob and its explicit file list on {prefix}: {} rows \
             against {}. The two are not reading the same segments; no timing is reported.",
            l0.len(),
            d0.len()
        );
    }
    println!(
        "parity:  verified on {} parties  ({} rows read across {} morsels, {} skipped)",
        d0.len(),
        metrics.rows_read,
        metrics.morsels,
        metrics.rows_skipped
    );
    println!(
        "catalog: {} entries in {dir}, {} match {prefix}-*.parquet  ({:.1}% of the directory)",
        dir_entries,
        prefix_files.len(),
        100.0 * prefix_files.len() as f64 / dir_entries.max(1) as f64
    );

    let mut ds = Vec::new();
    let mut bs = Vec::new();
    let mut binds = Vec::new();
    let mut execs = Vec::new();
    let mut globs = Vec::new();
    let mut lists = Vec::new();
    let mut colds = Vec::new();
    let mut plans = Vec::new();
    let mut scans = Vec::new();
    let mut merges = Vec::new();
    for _ in 0..repeats {
        ds.push(run_duck()?.1);
        let (b_ms, m) = {
            let (_, ms, m) = run_burrmill()?;
            (ms, m)
        };
        bs.push(b_ms);
        plans.push(m.plan_ms);
        scans.push(m.scan_ms);
        merges.push(m.merge_ms);
        let (bind, exec) = run_duck_split()?;
        binds.push(bind);
        execs.push(exec);
        globs.push(run_duck_glob()?);
        lists.push(run_duck_list()?.1);
        colds.push(run_burrmill_cold()?);
    }
    let median = |v: &mut Vec<u128>| {
        v.sort_unstable();
        v[v.len() / 2]
    };
    let (dm, bm) = (median(&mut ds), median(&mut bs));
    let (bind_m, exec_m) = (median(&mut binds), median(&mut execs));
    let (glob_m, list_m, cold_m) = (median(&mut globs), median(&mut lists), median(&mut colds));
    let (plan_m, scan_m, merge_m) =
        (median(&mut plans), median(&mut scans), median(&mut merges));
    // **The scale check (roadmap 1.1b).** Halve the input and see whether the time follows. An
    // engine whose time barely moves is not being measured on the query; it is being measured on
    // something fixed, and that is exactly how the real-nest ratios came to be a statement about a
    // 38,429-file directory scan. Fixed cost is estimated by linear extrapolation from two points -
    // crude, and enough to tell 5% fixed from 90%.
    let half = &prefix_files[..prefix_files.len() / 2];
    let (dh, bh) = (duck_over(half)?, burrmill_over(half)?);
    let (df, bf) = (duck_over(&prefix_files)?, burrmill_over(&prefix_files)?);
    let fixed = |t_half: u128, t_full: u128| -> u128 { (2 * t_half).saturating_sub(t_full) };
    let (duck_fixed, burr_fixed) = (fixed(dh, df), fixed(bh, bf));
    let frac = |f: u128, t: u128| -> f64 { f as f64 / t.max(1) as f64 };
    let (duck_frac, burr_frac) = (frac(duck_fixed, df), frac(burr_fixed, bf));
    println!(
        "SCALE\ttable={prefix}\tduck_half_ms={dh}\tduck_full_ms={df}\tduck_fixed_ms={duck_fixed}\tduck_fixed_pct={:.0}\tburr_half_ms={bh}\tburr_full_ms={bf}\tburr_fixed_ms={burr_fixed}\tburr_fixed_pct={:.0}",
        duck_frac * 100.0,
        burr_frac * 100.0
    );

    // **A ratio dominated by fixed cost is not printed as a number.** RFC-0004's discipline is to
    // make the wrong figure unavailable rather than merely discouraged: if more than half of either
    // engine's time is independent of how much data it read, there is no ratio here that means
    // "faster on this query", and a reader must not be able to copy one out.
    //
    // **The gate covers the number it publishes.** The first version of this check measured the
    // explicit-list path and then gated the glob-path ratio, which is a different quantity - the
    // same mistake in miniature as the one 1.1b exists to prevent, caught the first time it ran. The
    // headline `ratio` is therefore the like-for-like one, DuckDB handed the same catalog Burrmill
    // holds, and it is the one the scale check measured.
    let safe = |frac_d: f64, frac_b: f64, num: f64| -> String {
        if frac_d > 0.5 || frac_b > 0.5 {
            format!("UNSAFE_fixed_duck={:.0}pct_burr={:.0}pct", frac_d * 100.0, frac_b * 100.0)
        } else {
            format!("{num:.2}")
        }
    };
    let ratio_field = safe(duck_frac, burr_frac, bm as f64 / list_m.max(1) as f64);

    // The glob path's fixed cost is not estimated, it is measured: `glob()` plus bind, before a row
    // is read. On a nest whose segments directory holds tens of thousands of files this is most of
    // the number, which is why the published real-nest ratios were wrong for a whole slice.
    let glob_fixed_frac = (glob_m + bind_m) as f64 / dm.max(1) as f64;
    let glob_ratio_field = safe(glob_fixed_frac, burr_frac, bm as f64 / dm.max(1) as f64);
    if ratio_field.starts_with("UNSAFE") || glob_ratio_field.starts_with("UNSAFE") {
        eprintln!(
            "WARNING: {prefix}: a ratio was withheld because most of an engine's time does not vary \
             with input size. glob path: {:.0}% fixed. list path: DuckDB {:.0}%, Burrmill {:.0}%. \
             Use the PHASE and SCALE lines and say what the fixed cost is.",
            glob_fixed_frac * 100.0,
            duck_frac * 100.0,
            burr_frac * 100.0
        );
    }
    println!(
        "NEST\ttable={prefix}\tsegments={}\trows={}\tgroups={}\tduck_list_ms={list_m}\tduck_glob_ms={dm}\tburrmill_ms={bm}\tratio={ratio_field}\tglob_ratio={glob_ratio_field}\tparity=verified\trss_mb={}",
        metrics.morsels,
        metrics.rows_read,
        d0.len(),
        rss_mb()
    );
    println!(
        "PHASE\ttable={prefix}\tdir_entries={dir_entries}\tmatched={}\tduck_glob_ms={glob_m}\tduck_bind_ms={bind_m}\tduck_exec_ms={exec_m}\tduck_list_ms={list_m}\tburr_plan_ms={plan_m}\tburr_scan_ms={scan_m}\tburr_merge_ms={merge_m}\tburr_cold_ms={cold_m}\tlist_ratio={:.2}\tcold_ratio={:.2}",
        prefix_files.len(),
        bm as f64 / list_m.max(1) as f64,
        cold_m as f64 / dm.max(1) as f64
    );
    println!("duck_all={ds:?}\nburrmill_all={bs:?}\nduck_list_all={lists:?}\nduck_glob_all={globs:?}");
    Ok(())
}
