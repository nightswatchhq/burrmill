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

fn rss_mb() -> u64 {
    #[cfg(target_os = "macos")]
    {
        if let Ok(o) = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
        {
            if let Ok(s) = String::from_utf8(o.stdout) {
                if let Ok(kb) = s.trim().parse::<u64>() {
                    return kb / 1024;
                }
            }
        }
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
            for line in s.lines() {
                if let Some(v) = line.strip_prefix("VmHWM:") {
                    if let Some(kb) = v.split_whitespace().next().and_then(|k| k.parse::<u64>().ok())
                    {
                        return kb / 1024;
                    }
                }
            }
        }
    }
    0
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
    let mut all = Vec::new();
    let mut rows = 0usize;
    let mut metrics = burrmill::FoldMetrics::default();
    for _ in 0..repeats {
        let (r, ms, m) = oracles::burrmill(Path::new(&dir))?;
        rows = r.len();
        metrics = m;
        all.push(ms);
    }
    all.sort_unstable();
    println!(
        "FOLD\tgroups={rows}\tmorsels={}\trows_read={}\tmedian_ms={}\tplan_ms={}\tscan_ms={}\tmerge_ms={}\tall={all:?}\tpeak_rss_mb={}",
        metrics.morsels,
        metrics.rows_read,
        all[all.len() / 2],
        metrics.plan_ms,
        metrics.scan_ms,
        metrics.merge_ms,
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

    let run_burrmill = || -> anyhow::Result<(oracles::Rows, u128, burrmill::FoldMetrics)> {
        let t = std::time::Instant::now();
        let a = db.query(&sql, burrmill::Limits::default())?;
        let ms = t.elapsed().as_millis();
        Ok((a.rows().iter().map(|(k, v)| (k.to_string(), *v)).collect(), ms, a.metrics()))
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
    println!(
        "parity:  verified on {} parties  ({} rows read across {} morsels, {} skipped)",
        d0.len(),
        metrics.rows_read,
        metrics.morsels,
        metrics.rows_skipped
    );

    let mut ds = Vec::new();
    let mut bs = Vec::new();
    for _ in 0..repeats {
        ds.push(run_duck()?.1);
        bs.push(run_burrmill()?.1);
    }
    ds.sort_unstable();
    bs.sort_unstable();
    let (dm, bm) = (ds[ds.len() / 2], bs[bs.len() / 2]);
    println!(
        "NEST\ttable={prefix}\tsegments={}\trows={}\tgroups={}\tduck_ms={dm}\tburrmill_ms={bm}\tratio={:.2}\tparity=verified\trss_mb={}",
        metrics.morsels,
        metrics.rows_read,
        d0.len(),
        bm as f64 / dm.max(1) as f64,
        rss_mb()
    );
    println!("duck_all={ds:?}\nburrmill_all={bs:?}");
    Ok(())
}
