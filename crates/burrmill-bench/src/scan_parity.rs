//! `scan-parity <nest>`: nuthatch's RFC-0048 admission count, DuckDB's `EXPLAIN (FORMAT JSON)` walk
//! (`physical_parquet_scans` in `engine_duck.rs` at nuthatch 711ae88, copied), against
//! `Engine::parquet_scans`, view by view.

use std::path::Path;

use serde_json::Value;

fn duck_scans(plan: &Value) -> Result<u64, String> {
    let nodes = plan.as_array().ok_or("not an operator list")?;
    let mut scans = 0u64;
    for node in nodes {
        let name = node.get("name").and_then(Value::as_str).ok_or("no name")?.trim();
        match name {
            "READ_PARQUET" | "SEQ_SCAN" | "COLUMN_DATA_SCAN" | "DUMMY_SCAN" | "EMPTY_RESULT" | "RANGE"
            | "GENERATE_SERIES" | "UNNEST" | "PROJECTION" | "FILTER" | "HASH_JOIN" | "CROSS_PRODUCT"
            | "PIECEWISE_MERGE_JOIN" | "HASH_GROUP_BY" | "PERFECT_HASH_GROUP_BY" | "UNGROUPED_AGGREGATE"
            | "SIMPLE_AGGREGATE" | "ORDER_BY" | "TOP_N" | "LIMIT" | "STREAMING_LIMIT" | "LIMIT_PERCENT"
            | "UNION" | "WINDOW" | "STREAMING_WINDOW" | "CTE" | "CTE_SCAN" | "RESERVOIR_SAMPLE"
            | "STREAMING_SAMPLE" => {}
            other => return Err(format!("cannot bound {other:?}")),
        }
        scans += duck_scans(node.get("children").ok_or("no children")?)? + u64::from(name == "READ_PARQUET");
    }
    Ok(scans)
}

pub fn run(root: &str) -> anyhow::Result<()> {
    let nest = crate::df_views::load_nest(Path::new(root))?;
    let conn = crate::engine_views::duck(&nest)?;
    for v in &nest.views {
        let _ = conn.execute_batch(&v.text);
    }
    // The engine blocks on a runtime of its own, so it cannot run on the bench's.
    let root_owned = Path::new(root).to_path_buf();
    let views: Vec<(String, String)> = nest.views.iter().map(|v| (v.name.clone(), v.body.clone())).collect();
    let burr: Vec<Result<u64, String>> = std::thread::spawn(move || -> anyhow::Result<_> {
        let mut engine = burrmill::Engine::open_nest(&root_owned)?;
        for (n, b) in &views {
            let _ = engine.register_view(n, b);
        }
        let out = views
            .iter()
            .map(|(n, _)| engine.parquet_scans(&format!("SELECT * FROM \"{n}\"")).map_err(|e| e.to_string()))
            .collect();
        std::thread::spawn(move || drop(engine)).join().expect("drop");
        Ok(out)
    })
    .join()
    .expect("engine thread")?;
    let (mut same, mut stricter, mut looser, mut both) = (0, 0, 0, 0);
    for (v, b) in nest.views.iter().zip(burr) {
        let sql = format!("SELECT * FROM \"{}\"", v.name);
        let d = conn
            .query_row(&format!("EXPLAIN (FORMAT JSON) {sql}"), [], |r| r.get::<_, String>(1))
            .map_err(|e| e.to_string())
            .and_then(|p| serde_json::from_str::<Value>(&p).map_err(|e| e.to_string()))
            .and_then(|p| duck_scans(&p));
        let tag = match (&d, &b) {
            (Ok(x), Ok(y)) if x == y => { same += 1; "SAME    " }
            (Ok(x), Ok(y)) if y > x => { stricter += 1; "MORE    " }
            (Ok(_), Ok(_)) => { looser += 1; "FEWER   " }
            (Ok(_), Err(_)) => { stricter += 1; "REFUSES " }
            (Err(_), Ok(_)) => { looser += 1; "ADMITS  " }
            (Err(_), Err(_)) => { both += 1; "BOTH-REF" }
        };
        println!("{tag} {:<36} duckdb={d:?} burrmill={b:?}", v.name);
    }
    println!("SCANS\tviews={}\tsame={same}\tstricter={stricter}\tlooser={looser}\tboth_refuse={both}", nest.views.len());
    Ok(())
}
