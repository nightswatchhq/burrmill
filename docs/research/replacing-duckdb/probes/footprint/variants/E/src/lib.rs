//! Component crates only, no `datafusion` core. There is no SessionContext or physical planner
//! here, so this links the crates and measures their build cost; it does not run the query.
use std::path::Path;

pub fn default_fixture_dir() -> String {
    format!("{}/../fixture", env!("CARGO_MANIFEST_DIR"))
}

pub fn net_balances(_dir: &Path) -> Vec<(String, String)> {
    let _ = datafusion_sql::parser::DFParser::parse_sql("SELECT 1");
    let _ = datafusion_expr::lit(1i64);
    let _ = datafusion_physical_plan::displayable;
    let _ = datafusion_datasource_parquet::file_format::ParquetFormat::default();
    let _ = datafusion_functions::all_default_functions();
    let _ = datafusion_functions_aggregate::all_default_aggregate_functions();
    Vec::new()
}
