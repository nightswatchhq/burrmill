//! `engine-views <nest>` (gate 1): every authored view of a real nest through `burrmill::Engine`
//! against DuckDB set up as nuthatch sets it up, and the nest's pinned checks.
//!
//! DuckDB's base layer is nuthatch's: `read_parquet([...], union_by_name=true)` per table, `_dec`
//! as `TRY_CAST(... AS DECIMAL(38,0))` and `_overflow` beside it, then the views in file order.
//! Each view's whole contents are compared as a multiset of rows in nuthatch's JSON encoding.

use std::path::Path;
use std::time::Instant;

use serde_json::Value;

use crate::df_views::{load_nest, Nest};

fn duck(nest: &Nest) -> anyhow::Result<duckdb::Connection> {
    let conn = duckdb::Connection::open_in_memory()?;
    conn.execute_batch(&format!(
        "SET threads TO {}; SET TimeZone = 'UTC';",
        burrmill::Limits::default().max_threads
    ))?;
    for t in &nest.tables {
        let derived: String = t
            .wide
            .iter()
            .map(|c| {
                format!(
                    ", TRY_CAST(\"{c}\" AS DECIMAL(38,0)) AS \"{c}_dec\", \
                     (\"{c}\" IS NOT NULL AND TRY_CAST(\"{c}\" AS DECIMAL(38,0)) IS NULL) AS \"{c}_overflow\""
                )
            })
            .collect();
        if t.files.is_empty() {
            let cols: Vec<String> = t
                .schema
                .fields()
                .iter()
                .map(|f| {
                    let ty = if *f.data_type() == arrow::datatypes::DataType::UInt64 { "UBIGINT" } else { "VARCHAR" };
                    format!("\"{}\" {ty}", f.name())
                })
                .collect();
            conn.execute_batch(&format!("CREATE TABLE \"{}__raw\" ({});", t.name, cols.join(", ")))?;
            conn.execute_batch(&format!("CREATE VIEW \"{}\" AS SELECT *{derived} FROM \"{}__raw\";", t.name, t.name))?;
        } else {
            let list = t.files.iter().map(|(p, _)| format!("'{}'", p.display())).collect::<Vec<_>>().join(",");
            conn.execute_batch(&format!(
                "CREATE VIEW \"{}\" AS SELECT *{derived} FROM read_parquet([{list}], union_by_name=true);",
                t.name
            ))?;
        }
    }
    Ok(conn)
}

fn sorted_rows(v: Value) -> Vec<String> {
    let mut rows: Vec<String> = match v {
        Value::Array(a) => a.iter().map(|r| serde_json::to_string(r).unwrap()).collect(),
        _ => vec![],
    };
    rows.sort();
    rows
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").chars().take(220).collect()
}

pub fn run(root: &str) -> anyhow::Result<()> {
    let root = Path::new(root);
    let nest = load_nest(root)?;
    let conn = duck(&nest)?;
    let mut duck_ok = Vec::new();
    for v in &nest.views {
        match conn.execute_batch(&v.text) {
            Ok(()) => duck_ok.push(true),
            Err(e) => {
                println!("DUCK-FAIL {} ({}): {}", v.name, v.file, first_line(&e.to_string()));
                duck_ok.push(false);
            }
        }
    }

    let root_owned = root.to_path_buf();
    let views: Vec<(String, String)> = nest.views.iter().map(|v| (v.name.clone(), v.body.clone())).collect();
    let (engine, reg) = std::thread::spawn(move || -> anyhow::Result<_> {
        let mut e = burrmill::Engine::open_nest(&root_owned)?;
        let reg: Vec<Option<String>> = views
            .iter()
            .map(|(n, b)| e.register_view(n, b).err().map(|e| first_line(&e.to_string())))
            .collect();
        Ok((e, reg))
    })
    .join()
    .expect("engine thread")?;
    let engine = std::sync::Arc::new(engine);

    let (mut same, mut diff, mut fail) = (0, 0, 0);
    for (i, v) in nest.views.iter().enumerate() {
        if let Some(why) = &reg[i] {
            fail += 1;
            println!("REGISTER-FAIL {:<34} {}", v.name, why);
            continue;
        }
        if !duck_ok[i] {
            continue;
        }
        let sql = format!("SELECT * FROM \"{}\"", v.name);
        let t = Instant::now();
        let want = crate::encode_parity::nuthatch_rows(&conn, &sql).map(sorted_rows);
        let duck_ms = t.elapsed().as_millis();
        let e2 = std::sync::Arc::clone(&engine);
        let q = sql.clone();
        let t = Instant::now();
        let got = std::thread::spawn(move || -> Result<Vec<String>, String> {
            let batches = e2.sql(&q).map_err(|e| first_line(&e.to_string().replace('\n', " | ")))?;
            let mut rows = Vec::new();
            for b in &batches {
                rows.extend(burrmill::df::encode::rows(b).map_err(|e| e.to_string())?);
            }
            Ok(sorted_rows(Value::Array(rows)))
        })
        .join()
        .expect("engine thread");
        let bm_ms = t.elapsed().as_millis();
        match (want, got) {
            (Ok(w), Ok(g)) if w == g => {
                same += 1;
                // Warm medians when asked: the parity run above is each side's first, cold, query.
                let (duck_ms, bm_ms) = match std::env::var("TIMING").ok().and_then(|t| t.parse::<usize>().ok()) {
                    Some(n) if n > 0 => {
                        let median = |mut v: Vec<u128>| {
                            v.sort_unstable();
                            v[v.len() / 2]
                        };
                        let mut d = Vec::new();
                        for _ in 0..n {
                            let t = Instant::now();
                            let mut stmt = conn.prepare(&sql)?;
                            for b in stmt.query_arrow([])? {
                                std::hint::black_box(b.num_rows());
                            }
                            d.push(t.elapsed().as_millis());
                        }
                        let e3 = std::sync::Arc::clone(&engine);
                        let q = sql.clone();
                        let b = std::thread::spawn(move || {
                            (0..n)
                                .map(|_| {
                                    let t = Instant::now();
                                    let _ = e3.sql_for_each(&q, |b| {
                                        std::hint::black_box(b.num_rows());
                                        Ok(())
                                    });
                                    t.elapsed().as_millis()
                                })
                                .collect::<Vec<_>>()
                        })
                        .join()
                        .expect("engine thread");
                        if std::env::var("PROFILE").is_ok() {
                            let e4 = std::sync::Arc::clone(&engine);
                            let q = format!("EXPLAIN {sql}");
                            let plan_ms = std::thread::spawn(move || {
                                let mut v: Vec<u128> = (0..n)
                                    .map(|_| {
                                        let t = Instant::now();
                                        let _ = e4.sql(&q);
                                        t.elapsed().as_millis()
                                    })
                                    .collect();
                                v.sort_unstable();
                                v[v.len() / 2]
                            })
                            .join()
                            .expect("engine thread");
                            println!("      {} planning (EXPLAIN) median {plan_ms} ms", v.name);
                        }
                        (median(d), median(b))
                    }
                    _ => (duck_ms, bm_ms),
                };
                println!(
                    "SAME  {:<34} rows={:<7} duck_ms={duck_ms:<6} burrmill_ms={bm_ms:<6} ratio={:.2}",
                    v.name,
                    w.len(),
                    bm_ms as f64 / duck_ms.max(1) as f64
                );
            }
            (Ok(w), Ok(g)) => {
                diff += 1;
                let at = w.iter().zip(&g).position(|(a, b)| a != b).unwrap_or(w.len().min(g.len()));
                println!("DIFF  {:<34} rows duck={} burrmill={}", v.name, w.len(), g.len());
                println!("      duck     {}", w.get(at).map(|s| s.chars().take(300).collect::<String>()).unwrap_or_default());
                println!("      burrmill {}", g.get(at).map(|s| s.chars().take(300).collect::<String>()).unwrap_or_default());
            }
            (Ok(_), Err(e)) => {
                fail += 1;
                println!("FAIL  {:<34} {e}", v.name);
            }
            (Err(e), _) => println!("DUCK-QUERY-FAIL {:<28} {}", v.name, first_line(&e.to_string())),
        }
    }

    let mut checks = 0;
    let mut checks_ok = 0;
    if let Ok(dir) = std::fs::read_dir(root.join("checks")) {
        let mut files: Vec<_> = dir.flatten().map(|e| e.path()).filter(|p| p.extension().is_some_and(|x| x == "sql")).collect();
        files.sort();
        for f in files {
            let stem = f.file_stem().unwrap().to_string_lossy().to_string();
            let expected: Value = serde_json::from_str(&std::fs::read_to_string(root.join("checks/expected").join(format!("{stem}.json")))?)?;
            let sql: String = std::fs::read_to_string(&f)?
                .lines()
                .filter(|l| !l.trim_start().starts_with("--"))
                .collect::<Vec<_>>()
                .join("\n");
            let sql = sql.trim().trim_end_matches(';').to_string();
            let duck = crate::encode_parity::nuthatch_rows(&conn, &sql).map_err(|e| first_line(&e.to_string()));
            let e2 = std::sync::Arc::clone(&engine);
            let q = sql.clone();
            let bm = std::thread::spawn(move || -> Result<Value, String> {
                let batches = e2.sql(&q).map_err(|e| first_line(&e.to_string()))?;
                let mut rows = Vec::new();
                for b in &batches {
                    rows.extend(burrmill::df::encode::rows(b).map_err(|e| e.to_string())?);
                }
                Ok(Value::Array(rows))
            })
            .join()
            .expect("engine thread");
            checks += 1;
            let bm_ok = bm.as_ref().is_ok_and(|v| *v == expected);
            checks_ok += bm_ok as usize;
            println!(
                "CHECK {stem:<14} burrmill={} duckdb={}",
                if bm_ok { "expected".to_string() } else { format!("{bm:?}") },
                if duck.as_ref().is_ok_and(|v| *v == expected) { "expected".to_string() } else { format!("{duck:?}") }
            );
        }
    }
    println!(
        "VIEWS\tviews={}\tsame={same}\tdiffering={diff}\tfailing={fail}\tchecks={checks_ok}/{checks}",
        nest.views.len()
    );
    std::thread::spawn(move || drop(engine)).join().expect("drop engine");
    Ok(())
}
