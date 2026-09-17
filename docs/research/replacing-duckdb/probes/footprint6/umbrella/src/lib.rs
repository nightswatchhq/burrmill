use std::path::Path;
use datafusion::prelude::*;
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;

pub fn default_fixture_dir() -> String {
    format!("{}/../fixture", env!("CARGO_MANIFEST_DIR"))
}

const SQL: &str = r#"SELECT addr, SUM(d) AS net FROM (SELECT "to" AS addr, CAST("value" AS DECIMAL(38,0)) AS d FROM t UNION ALL SELECT "from" AS addr, -CAST("value" AS DECIMAL(38,0)) AS d FROM t) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr"#;

pub fn net_balances(dir: &Path) -> Vec<(String, String)> {
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("rt");
    rt.block_on(async {
        let ctx = SessionContext::new();
        ctx.register_parquet("t", dir.to_str().unwrap(), ParquetReadOptions::default()).await.expect("register");
        let batches = ctx.sql(SQL).await.expect("plan").collect().await.expect("collect");
        let mut out = Vec::new();
        for b in batches {
            let k = cast(b.column(0), &DataType::Utf8).unwrap();
            let v = cast(b.column(1), &DataType::Utf8).unwrap();
            let k = k.as_any().downcast_ref::<StringArray>().unwrap();
            let v = v.as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..k.len() { out.push((k.value(i).to_string(), v.value(i).to_string())); }
        }
        out
    })
}
