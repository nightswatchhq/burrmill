//! The same consumer on `burrmill::Engine`, the crate that would replace DuckDB in nuthatch.

use std::path::Path;

use arrow::array::{Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;

/// nuthatch's segment naming, `table-<id>.parquet`: a link to the shared fixture.
pub fn default_fixture_dir() -> String {
    format!("{}/segments", env!("CARGO_MANIFEST_DIR"))
}

pub fn net_balances(dir: &Path) -> Vec<(String, String)> {
    let engine = burrmill::Engine::open_segments(dir).expect("open");
    let sql = "SELECT addr, CAST(SUM(d) AS VARCHAR) AS net FROM (SELECT \"to\" AS addr, CAST(\"value\" AS HUGEINT) AS d FROM t UNION ALL SELECT \"from\" AS addr, -CAST(\"value\" AS HUGEINT) AS d FROM t) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr";
    let mut out = Vec::new();
    for b in engine.sql(sql).expect("query") {
        let a = cast(b.column(0), &DataType::Utf8).expect("cast");
        let n = cast(b.column(1), &DataType::Utf8).expect("cast");
        let (a, n) = (
            a.as_any().downcast_ref::<StringArray>().unwrap(),
            n.as_any().downcast_ref::<StringArray>().unwrap(),
        );
        out.extend((0..b.num_rows()).map(|i| (a.value(i).to_string(), n.value(i).to_string())));
    }
    out
}
