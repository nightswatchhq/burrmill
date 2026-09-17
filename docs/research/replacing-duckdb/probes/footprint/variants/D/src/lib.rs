use std::path::Path;
use burrmill::{Burrmill, Limits};

pub fn default_fixture_dir() -> String {
    format!("{}/../fixture", env!("CARGO_MANIFEST_DIR"))
}

const SQL: &str = r#"SELECT addr, SUM(d) AS net FROM (SELECT "to" AS addr, TRY_CAST("value" AS HUGEINT) AS d FROM t UNION ALL SELECT "from" AS addr, -TRY_CAST("value" AS HUGEINT) AS d FROM t) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr"#;

/// Net balance per address: Burrmill, exact i128, result formatted at the edge.
pub fn net_balances(dir: &Path) -> Vec<(String, String)> {
    let db = Burrmill::open_segments("t", dir).expect("open");
    let a = db.query(SQL, Limits::default()).expect("query");
    a.rows().iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}
