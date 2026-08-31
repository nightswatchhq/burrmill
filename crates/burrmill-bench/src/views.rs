//! The headline claim, against the queries an indexer actually runs (roadmap 4.2).
//!
//! The README says Burrmill is "faster than DuckDB on the queries an indexer actually runs". Every
//! measurement behind that has been a **synthetic** `net_balances` over a generated fixture. The real
//! authored views were not runnable until roadmap 4.1a-d generalised the fold, and nobody had gone
//! back to check the claim against them.
//!
//! This does. It extracts every fold-shaped sub-query from a nest's `views/*.sql`, runs each against
//! that nest's own sealed segments, and puts DuckDB on exactly the same files with exactly the same
//! thread budget. Parity first, as everywhere else: a timing between engines that disagree is not
//! fast-versus-slow.
//!
//! DuckDB is given the file list rather than a glob, because roadmap 1.1 established that globbing a
//! 38,429-file nest directory measures the directory and not the query.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Every distinct `<table>-` prefix in a nest's segments directory, and the files under it.
fn tables(dir: &Path) -> anyhow::Result<BTreeMap<String, Vec<PathBuf>>> {
    let mut out: BTreeMap<String, Vec<PathBuf>> = BTreeMap::new();
    for e in std::fs::read_dir(dir)?.flatten() {
        let p = e.path();
        if p.extension().is_some_and(|x| x == "parquet") {
            if let Some((table, _)) = p
                .file_name()
                .and_then(|f| f.to_str())
                .and_then(|f| f.strip_suffix(".parquet"))
                .and_then(|f| f.rsplit_once('-'))
            {
                out.entry(table.to_string()).or_default().push(p);
            }
        }
    }
    Ok(out)
}

pub fn run(segments: &str, views: &str) -> anyhow::Result<()> {
    let seg_dir = Path::new(segments);
    let by_table = tables(seg_dir)?;
    anyhow::ensure!(!by_table.is_empty(), "no segments in {segments}");

    // Every fold-shaped sub-query the workload contains, from the same extractor experiment A4 uses.
    let folds = crate::shapes::extract_folds(&[views.to_string()])?;
    anyhow::ensure!(!folds.is_empty(), "no folds found under {views}");

    println!(
        "{} tables in the nest, {} fold sub-plans in the views\n",
        by_table.len(),
        folds.len()
    );
    println!(
        "{:<52} {:>7} {:>9} {:>9} {:>7}  where it goes",
        "fold", "rows", "duck_ms", "burr_ms", "ratio"
    );

    let budget = crate::oracles::thread_budget();
    let (mut ran, mut wins) = (0usize, 0usize);
    for (i, sql) in folds.iter().enumerate() {
        // Only the tables this fold names, so a query over two tables does not pay to open thirty.
        let named: Vec<&String> = by_table.keys().filter(|t| sql.contains(t.as_str())).collect();
        if named.is_empty() {
            continue;
        }
        // A fold may name a table this nest has never sealed - `staking_legacy__stake_slashed` has
        // fired zero times here. Neither engine can run it, and saying "DuckDB refused" would blame
        // the wrong thing entirely.
        let missing: Vec<&str> = sql
            .split("FROM ")
            .skip(1)
            .filter_map(|s| s.split_whitespace().next())
            .map(|s| s.trim_matches(['"', ')', '(']))
            .filter(|s| s.contains("__") && !by_table.contains_key(*s))
            .collect();
        if !missing.is_empty() {
            println!("{:<52} not run: this nest has no {}", short(sql, i), missing[0]);
            continue;
        }

        let mut cat = burrmill::Catalog::new();
        for t in &named {
            cat.register(burrmill::SealedSegments::from_files(
                (*t).clone(),
                by_table[*t].clone(),
            ));
        }
        let db = match burrmill::Burrmill::with_threads(cat, budget) {
            Ok(db) => db,
            Err(e) => {
                println!("{:<52} skipped: {e}", short(sql, i));
                continue;
            }
        };

        let conn = duckdb::Connection::open_in_memory()?;
        conn.execute_batch(&format!("SET threads TO {budget};"))?;
        for t in &named {
            let list = by_table[*t]
                .iter()
                .map(|p| format!("'{}'", p.display()))
                .collect::<Vec<_>>()
                .join(",");
            conn.execute_batch(&format!(
                "CREATE VIEW \"{t}\" AS SELECT * FROM read_parquet([{list}]);"
            ))?;
        }

        // Burrmill returns i128 as i128; DuckDB's client cannot, so its projection casts to text.
        // Mechanical and applied to the sums only.
        let duck_sql = cast_sums(sql);
        let duck = |also_time: bool| -> anyhow::Result<(Vec<Vec<String>>, u128)> {
            let t = Instant::now();
            let mut stmt = conn.prepare(&duck_sql)?;
            let mut out = Vec::new();
            let mut rows = stmt.query([])?;
            while let Some(r) = rows.next()? {
                let mut row = Vec::new();
                let mut c = 0;
                while let Ok(v) = r.get::<_, String>(c) {
                    row.push(v);
                    c += 1;
                }
                out.push(row);
            }
            out.sort();
            Ok((out, if also_time { t.elapsed().as_millis() } else { 0 }))
        };

        let mut last_metrics = burrmill::FoldMetrics::default();
        let burr = |also_time: bool| -> anyhow::Result<(Vec<Vec<String>>, u128, burrmill::FoldMetrics)> {
            let t = Instant::now();
            let a = db.query(sql, burrmill::Limits::default())?;
            let ms = t.elapsed().as_millis();
            let metrics = a.metrics();
            let r = a.rows();
            let mut out = Vec::with_capacity(r.len());
            for i in 0..r.len() {
                let mut row: Vec<String> =
                    r.key_parts(i).map(|s| s.to_string()).collect();
                for j in 0..r.sum_arity() {
                    row.push(r.sum_at(i, j).to_string());
                }
                out.push(row);
            }
            out.sort();
            Ok((out, if also_time { ms } else { 0 }, metrics))
        };

        let (d0, _) = match duck(false) {
            Ok(v) => v,
            Err(e) => {
                println!("{:<52} DuckDB refused: {}", short(sql, i), first_line(&e.to_string()));
                continue;
            }
        };
        let (b0, _, _) = match burr(false) {
            Ok(v) => v,
            Err(e) => {
                println!("{:<52} Burrmill refused: {}", short(sql, i), first_line(&e.to_string()));
                continue;
            }
        };
        if d0 != b0 {
            println!(
                "{:<52} {:>7} PARITY FAILED: {} rows against {}",
                short(sql, i),
                d0.len(),
                b0.len(),
                d0.len()
            );
            continue;
        }

        let mut ds = Vec::new();
        let mut bs = Vec::new();
        for _ in 0..3 {
            ds.push(duck(true)?.1);
            let (_, ms, m) = burr(true)?;
            bs.push(ms);
            last_metrics = m;
        }
        ds.sort_unstable();
        bs.sort_unstable();
        let (dm, bm) = (ds[1], bs[1]);
        let ratio = bm as f64 / dm.max(1) as f64;
        ran += 1;
        if ratio <= 1.0 {
            wins += 1;
        }
        println!(
            "{:<52} {:>7} {:>9} {:>9} {:>7.2}  plan={} scan={} merge={} morsels={}",
            short(sql, i),
            d0.len(),
            dm,
            bm,
            ratio,
            last_metrics.plan_ms,
            last_metrics.scan_ms,
            last_metrics.merge_ms,
            last_metrics.morsels
        );
    }
    println!("\n{wins}/{ran} folds at or under 1.0x DuckDB, {} threads each", budget);
    Ok(())
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").chars().take(60).collect()
}

/// A short label: the tables the fold reads, which is what distinguishes them to a reader.
fn short(sql: &str, i: usize) -> String {
    let mut names: Vec<&str> = sql
        .split("FROM ")
        .skip(1)
        .filter_map(|s| s.split_whitespace().next())
        .map(|s| s.trim_matches(['"', ')', '(']))
        .filter(|s| s.contains("__") || s.contains('_'))
        .collect();
    names.sort();
    names.dedup();
    let label = names.join("+");
    let label = if label.len() > 48 { format!("{}…", &label[..47]) } else { label };
    if label.is_empty() {
        format!("fold #{i}")
    } else {
        label
    }
}

/// `SUM(x) AS y` -> `SUM(x)::VARCHAR AS y`, and bare trailing `SUM(x)` likewise.
fn cast_sums(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len() + 32);
    let mut rest = sql;
    while let Some(i) = rest.find("SUM(") {
        out.push_str(&rest[..i]);
        let after = &rest[i..];
        // find the matching close paren
        let mut depth = 0usize;
        let mut end = 0usize;
        for (j, c) in after.char_indices() {
            if c == '(' {
                depth += 1;
            } else if c == ')' {
                depth -= 1;
                if depth == 0 {
                    end = j + 1;
                    break;
                }
            }
        }
        out.push_str(&after[..end]);
        out.push_str("::VARCHAR");
        rest = &after[end..];
    }
    out.push_str(rest);
    out
}
