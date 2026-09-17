use std::path::Path;
use duckdb::Connection;

pub fn default_fixture_dir() -> String {
    format!("{}/../fixture", env!("CARGO_MANIFEST_DIR"))
}

/// Net balance per address: DuckDB, HUGEINT arithmetic, result cast to text at the edge.
pub fn net_balances(dir: &Path) -> Vec<(String, String)> {
    let conn = Connection::open_in_memory().expect("open");
    let sql = format!(
        "SELECT addr, CAST(SUM(d) AS VARCHAR) AS net FROM (SELECT \"to\" AS addr, CAST(\"value\" AS HUGEINT) AS d FROM read_parquet('{d}/*.parquet') UNION ALL SELECT \"from\" AS addr, -CAST(\"value\" AS HUGEINT) AS d FROM read_parquet('{d}/*.parquet')) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr",
        d = dir.display()
    );
    let mut stmt = conn.prepare(&sql).expect("prepare");
    stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
        .expect("query")
        .map(|r| r.expect("row"))
        .collect()
}
