//! Experiment 4c: built-in Decimal128 SUM against checked_sum, 10M rows, 1M groups, 8 partitions.
//! `cost gen` writes the data; `cost builtin|checked|builtin-text|checked-text` runs one query.

use std::sync::Arc;
use std::time::Instant;

use burrmill_arith::checked::register_all;
use burrmill_arith::util::*;
use datafusion::arrow::array::*;
use datafusion::arrow::record_batch::RecordBatch;

const ROWS: usize = 10_000_000;
const GROUPS: i64 = 1_000_000;
const FILES: usize = 8;

fn gen() {
    let _ = std::fs::remove_dir_all("data/cost");
    let mut x: u64 = 0x9E3779B97F4A7C15;
    let per_file = ROWS / FILES;
    let per_batch = 125_000;
    for f in 0..FILES {
        let mut batches = Vec::new();
        for b in 0..per_file / per_batch {
            let base = (f * per_file + b * per_batch) as i64;
            let mut k = Vec::with_capacity(per_batch);
            let mut v = Vec::with_capacity(per_batch);
            let mut t = Vec::with_capacity(per_batch);
            for i in 0..per_batch as i64 {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                // values up to ~10^30 so that no sum of 10 of them nears 10^38
                let val = ((x as u128) * 54_210_108_624_275u128) as i128; // < 1.8e19 * 5.4e13 ~ 1e33
                k.push((base + i) % GROUPS);
                v.push(val);
                t.push(val.to_string());
            }
            let ka = Int64Array::from(k);
            let va = Decimal128Array::from_iter_values(v).with_precision_and_scale(38, 0).unwrap();
            let ta = StringArray::from(t);
            batches.push(
                RecordBatch::try_from_iter(vec![
                    ("k", Arc::new(ka) as ArrayRef),
                    ("v", Arc::new(va) as ArrayRef),
                    ("t", Arc::new(ta) as ArrayRef),
                ])
                .unwrap(),
            );
        }
        write_row_groups(&format!("data/cost/part{f}.parquet"), &batches);
    }
    println!("wrote {ROWS} rows in {FILES} files");
}

#[tokio::main]
async fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    if mode == "gen" {
        gen();
        return;
    }
    let c = ctx(8);
    register_all(&c);
    reg(&c, "t", "data/cost/").await;
    if mode == "verify" {
        let sql = "SELECT COUNT(*) AS groups, SUM(CASE WHEN a = b THEN 1 ELSE 0 END) AS equal, MIN(a = b) AS all_equal
                   FROM (SELECT k, SUM(v) a FROM t GROUP BY k) x
                   JOIN (SELECT k, checked_sum(v) b FROM t GROUP BY k) y ON x.k = y.k";
        let batches = c.sql(sql).await.unwrap().collect().await.unwrap();
        println!("{}", datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap());
        let sql = "SELECT COUNT(*) AS groups, SUM(CASE WHEN CAST(a AS VARCHAR) = b THEN 1 ELSE 0 END) AS equal
                   FROM (SELECT k, SUM(v) a FROM t GROUP BY k) x
                   JOIN (SELECT k, checked_sum_text(t) b FROM t GROUP BY k) y ON x.k = y.k";
        let batches = c.sql(sql).await.unwrap().collect().await.unwrap();
        println!("{}", datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap());
        return;
    }
    let sql = match mode.as_str() {
        "builtin" => "SELECT k, SUM(v) AS s FROM t GROUP BY k",
        "checked" => "SELECT k, checked_sum(v) AS s FROM t GROUP BY k",
        "builtin-text" => "SELECT k, SUM(CAST(t AS DECIMAL(38,0))) AS s FROM t GROUP BY k",
        "checked-text" => "SELECT k, checked_sum(t) AS s FROM t GROUP BY k",
        "checked-texttext" => "SELECT k, checked_sum_text(t) AS s FROM t GROUP BY k",
        "scan" => "SELECT k, COUNT(v) AS s FROM t GROUP BY k",
        _ => panic!("mode"),
    };
    let start = Instant::now();
    let batches = c.sql(sql).await.unwrap().collect().await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("{mode}: {rows} groups in {:.3}s", start.elapsed().as_secs_f64());
}
