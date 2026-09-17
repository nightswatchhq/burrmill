//! Data generation and query helpers shared by the experiments.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::*;

use crate::i320::I320;

pub const U256_MAX: &str = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
pub const P255: &str = "57896044618658097711785492504343953926634992332820282019728792003956564819968";
pub const P255M1: &str = "57896044618658097711785492504343953926634992332820282019728792003956564819967";
pub const D38MAX: &str = "99999999999999999999999999999999999999"; // 10^38 - 1
pub const D76MAX: &str = "9999999999999999999999999999999999999999999999999999999999999999999999999999"; // 10^76 - 1
pub const E76: &str = "10000000000000000000000000000000000000000000000000000000000000000000000000000"; // 10^76, 77 digits

/// Each batch becomes its own row group.
pub fn write_row_groups(path: &str, batches: &[RecordBatch]) {
    let p = Path::new(path);
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    let f = fs::File::create(p).unwrap();
    let mut w = ArrowWriter::try_new(f, batches[0].schema(), None).unwrap();
    for b in batches {
        w.write(b).unwrap();
        w.flush().unwrap();
    }
    w.close().unwrap();
}

pub fn write_files(dir: &str, files: &[RecordBatch]) {
    let _ = fs::remove_dir_all(dir);
    for (i, b) in files.iter().enumerate() {
        write_row_groups(&format!("{dir}/part{i}.parquet"), std::slice::from_ref(b));
    }
}

pub fn i64_col(vals: &[i64]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![("v", Arc::new(Int64Array::from(vals.to_vec())) as ArrayRef)]).unwrap()
}

pub fn dec128_col(vals: &[&str]) -> RecordBatch {
    let a = Decimal128Array::from_iter_values(vals.iter().map(|s| s.parse::<i128>().unwrap()))
        .with_precision_and_scale(38, 0)
        .unwrap();
    RecordBatch::try_from_iter(vec![("v", Arc::new(a) as ArrayRef)]).unwrap()
}

pub fn dec256_col(vals: &[&str]) -> RecordBatch {
    let a = Decimal256Array::from_iter_values(
        vals.iter().map(|s| I320::parse_canonical(s).unwrap().to_i256().expect("fits i256")),
    )
    .with_precision_and_scale(76, 0)
    .unwrap();
    RecordBatch::try_from_iter(vec![("v", Arc::new(a) as ArrayRef)]).unwrap()
}

pub fn text_cols(rows: &[(&str, &str)]) -> RecordBatch {
    let party = StringArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>());
    let amount = StringArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>());
    RecordBatch::try_from_iter(vec![
        ("party", Arc::new(party) as ArrayRef),
        ("amount", Arc::new(amount) as ArrayRef),
    ])
    .unwrap()
}

pub fn ctx(parts: usize) -> SessionContext {
    SessionContext::new_with_config(SessionConfig::new().with_target_partitions(parts).with_information_schema(true))
}

pub async fn reg(ctx: &SessionContext, name: &str, path: &str) {
    ctx.register_parquet(name, path, ParquetReadOptions::default()).await.unwrap();
}

fn trim(e: impl ToString) -> String {
    let s = e.to_string().replace('\n', " ");
    if s.len() > 230 {
        format!("{}...", &s[..230])
    } else {
        s
    }
}

/// Runs one statement and prints its outcome as one of OK / PLAN ERROR / EXEC ERROR.
pub async fn show(ctx: &SessionContext, label: &str, sql: &str) {
    match ctx.sql(sql).await {
        Err(e) => println!("[{label}] PLAN ERROR: {}", trim(e)),
        Ok(df) => match df.collect().await {
            Err(e) => println!("[{label}] EXEC ERROR: {}", trim(e)),
            Ok(b) => {
                let t = pretty_format_batches(&b).unwrap().to_string();
                let body: Vec<&str> = t.lines().collect();
                println!("[{label}] OK:\n{}", body.join("\n"));
            }
        },
    }
}

/// Prints the lines of the physical plan that show how the aggregate is split.
pub async fn explain_agg(ctx: &SessionContext, label: &str, sql: &str) {
    let df = ctx.sql(&format!("EXPLAIN {sql}")).await.unwrap();
    let b = df.collect().await.unwrap();
    let t = pretty_format_batches(&b).unwrap().to_string();
    println!("[{label}] physical plan:");
    for line in t.lines() {
        let l = line.trim_matches('|').trim_end();
        if l.contains("AggregateExec") || l.contains("RepartitionExec") || l.contains("DataSourceExec") || l.contains("Coalesce") {
            println!("    {}", l.trim_start_matches("physical_plan ").trim());
        }
    }
}

pub fn gen_int_data() {
    write_files("data/i64_over", &[i64_col(&[i64::MAX]), i64_col(&[1]), i64_col(&[0]), i64_col(&[0])]);
    write_files("data/i64_partial", &[i64_col(&[i64::MAX, 1]), i64_col(&[-2])]);
    write_files("data/i64_min", &[i64_col(&[i64::MIN])]);
    let _ = fs::remove_dir_all("data/i64_one");
    write_row_groups("data/i64_one/part0.parquet", &[i64_col(&[i64::MAX]), i64_col(&[1]), i64_col(&[0]), i64_col(&[0])]);
}

pub fn gen_dec_data() {
    write_files("data/d38_over", &[dec128_col(&[D38MAX]), dec128_col(&["1"])]);
    write_files("data/d38_i128", &[dec128_col(&[D38MAX]), dec128_col(&[D38MAX])]);
    write_files("data/d38_partial", &[dec128_col(&[D38MAX, D38MAX]), dec128_col(&[&format!("-{D38MAX}")])]);
    write_files("data/d76_over", &[dec256_col(&[D76MAX]), dec256_col(&["1"])]);
    write_files("data/d76_i256", &[dec256_col(&[D76MAX]), dec256_col(&[D76MAX]), dec256_col(&[D76MAX]), dec256_col(&[D76MAX]), dec256_col(&[D76MAX]), dec256_col(&[D76MAX])]);
    let neg = format!("-{D76MAX}");
    write_files("data/d76_partial", &[dec256_col(&[D76MAX; 6]), dec256_col(&[neg.as_str(); 5])]);
}

pub fn gen_text_data() {
    let d76 = D76MAX;
    let credits0 = text_cols(&[
        ("partial_over", d76), ("partial_over", d76), ("partial_over", d76),
        ("partial_over", d76), ("partial_over", d76), ("partial_over", d76),
        ("max", U256_MAX),
        ("p255m1", P255M1),
        ("fits76", d76),
        ("final_over", d76), ("final_over", d76),
        ("small", "1"),
    ]);
    let credits1 = text_cols(&[
        ("p255", P255M1), ("p255", "1"),
        ("final_over2", d76), ("final_over2", d76), ("final_over2", d76),
        ("final_over2", d76), ("final_over2", d76), ("final_over2", d76),
        ("small", "2"), ("small", "3"),
        ("neg", "5"),
    ]);
    let debits0 = text_cols(&[
        ("partial_over", d76), ("partial_over", d76), ("partial_over", d76),
        ("partial_over", d76), ("partial_over", d76),
        ("fits76", d76),
    ]);
    let debits1 = text_cols(&[("small", "4"), ("neg", "7")]);
    write_files("data/credits", &[credits0, credits1]);
    write_files("data/debits", &[debits0, debits1]);
    write_files("data/noncanon", &[text_cols(&[("a", "7"), ("a", " 7"), ("b", "007"), ("b", "+1"), ("c", "1e3"), ("c", "7.0")])]);
}

// The experiment 3 queries, shared with experiment 4.
pub const Q_UNION_CAST76: &str = "
WITH m AS (
  SELECT party, CAST(amount AS DECIMAL(76,0)) AS c, CAST('0' AS DECIMAL(76,0)) AS d FROM credits
  UNION ALL
  SELECT party, CAST('0' AS DECIMAL(76,0)), CAST(amount AS DECIMAL(76,0)) FROM debits
)
SELECT party, SUM(c) - SUM(d) AS balance FROM m GROUP BY party ORDER BY party";

pub const Q_UNION_TRYCAST76: &str = "
WITH m AS (
  SELECT party, TRY_CAST(amount AS DECIMAL(76,0)) AS c, CAST('0' AS DECIMAL(76,0)) AS d FROM credits
  UNION ALL
  SELECT party, CAST('0' AS DECIMAL(76,0)), TRY_CAST(amount AS DECIMAL(76,0)) FROM debits
)
SELECT party, SUM(c) - SUM(d) AS balance FROM m GROUP BY party ORDER BY party";

/// What nuthatch does today on DuckDB: TRY_CAST to DECIMAL(38,0) plus an overflow flag.
pub const Q_NUTHATCH38: &str = "
WITH m AS (
  SELECT party, TRY_CAST(amount AS DECIMAL(38,0)) AS c, CAST('0' AS DECIMAL(38,0)) AS d,
         CASE WHEN TRY_CAST(amount AS DECIMAL(38,0)) IS NULL THEN 1 ELSE 0 END AS ov FROM credits
  UNION ALL
  SELECT party, CAST('0' AS DECIMAL(38,0)), TRY_CAST(amount AS DECIMAL(38,0)),
         CASE WHEN TRY_CAST(amount AS DECIMAL(38,0)) IS NULL THEN 1 ELSE 0 END FROM debits
)
SELECT party, SUM(c) - SUM(d) AS balance, MAX(ov) AS _overflow FROM m GROUP BY party ORDER BY party";

pub const Q_SUM_TEXT: &str = "SELECT party, SUM(amount) AS s, arrow_typeof(SUM(amount)) AS t FROM credits GROUP BY party ORDER BY party";
