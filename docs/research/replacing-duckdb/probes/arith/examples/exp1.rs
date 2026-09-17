//! Experiment 1: Int64 overflow in SUM (one file, several files, parallel),
//! scalar + - * at runtime and under constant folding, and CAST.

use burrmill_arith::util::*;

#[tokio::main]
async fn main() {
    gen_int_data();
    println!("=== 1. Integers (DataFusion {}) ===", datafusion::DATAFUSION_VERSION);
    println!("i64::MAX = {}, true SUM(i64_over) = 9223372036854775808, true SUM(i64_partial) = 9223372036854775806", i64::MAX);

    for parts in [1usize, 4] {
        let c = ctx(parts);
        for t in ["i64_over", "i64_partial", "i64_one", "i64_min"] {
            reg(&c, t, &format!("data/{t}/")).await;
        }
        println!("--- target_partitions = {parts} ---");
        show(&c, &format!("SUM 4 files, {parts}p"), "SELECT SUM(v) FROM i64_over").await;
        if parts > 1 {
            explain_agg(&c, "SUM 4 files", "SELECT SUM(v) FROM i64_over").await;
        }
        show(&c, &format!("SUM partial-over-final-fits 2 files, {parts}p"), "SELECT SUM(v) FROM i64_partial").await;
        show(&c, &format!("SUM 1 file 4 row groups, {parts}p"), "SELECT SUM(v) FROM i64_one").await;
        if parts > 1 {
            explain_agg(&c, "SUM 1 file", "SELECT SUM(v) FROM i64_one").await;
        }
        show(&c, &format!("SUM grouped, {parts}p"), "SELECT v > 0 AS k, SUM(v) FROM i64_over GROUP BY k ORDER BY k").await;
    }

    let c = ctx(4);
    for t in ["i64_over", "i64_min"] {
        reg(&c, t, &format!("data/{t}/")).await;
    }
    println!("--- scalar, constant folded ---");
    show(&c, "fold +", "SELECT 9223372036854775807 + 1").await;
    show(&c, "fold *", "SELECT 10000000000 * 10000000000").await;
    show(&c, "fold -", "SELECT -9223372036854775807 - 2").await;
    show(&c, "fold neg", "SELECT -(-9223372036854775807 - 1)").await;
    show(&c, "fold % (for contrast)", "SELECT (-9223372036854775807 - 1) % -1").await;
    show(&c, "fold / (for contrast)", "SELECT (-9223372036854775807 - 1) / -1").await;
    explain_agg(&c, "fold + is folded? (no plan lines means yes)", "SELECT 9223372036854775807 + 1").await;
    let df = c.sql("EXPLAIN SELECT 9223372036854775807 + 1").await.unwrap().collect().await.unwrap();
    let t = datafusion::arrow::util::pretty::pretty_format_batches(&df).unwrap().to_string();
    for l in t.lines().filter(|l| l.contains("Projection")) {
        println!("    {}", l.trim());
    }
    println!("--- scalar, runtime on a column ---");
    show(&c, "runtime v + 1", "SELECT v + 1 FROM i64_over WHERE v > 0").await;
    show(&c, "runtime v * 2", "SELECT v * 2 FROM i64_over WHERE v > 0").await;
    show(&c, "runtime v - (-1)", "SELECT v - (-1) FROM i64_over WHERE v > 0").await;
    show(&c, "runtime -v on MIN", "SELECT -v FROM i64_min").await;
    show(&c, "runtime abs(MIN)", "SELECT abs(v) FROM i64_min").await;
    show(&c, "runtime v * v", "SELECT v * v FROM i64_over WHERE v > 0").await;

    println!("--- CAST ---");
    show(&c, "CAST fold i64->i32", "SELECT CAST(9223372036854775807 AS INT)").await;
    show(&c, "TRY_CAST fold i64->i32", "SELECT TRY_CAST(9223372036854775807 AS INT)").await;
    show(&c, "arrow_cast fold i64->i32", "SELECT arrow_cast(9223372036854775807, 'Int32')").await;
    show(&c, "CAST runtime i64->i32", "SELECT CAST(v AS INT) FROM i64_over WHERE v > 0").await;
    show(&c, "TRY_CAST runtime i64->i32", "SELECT TRY_CAST(v AS INT) FROM i64_over WHERE v > 0").await;
    show(&c, "CAST text->i64 over", "SELECT CAST('9223372036854775808' AS BIGINT)").await;
    show(&c, "TRY_CAST text->i64 over", "SELECT TRY_CAST('9223372036854775808' AS BIGINT)").await;
    show(&c, "CAST literal 2^63 -> BIGINT", "SELECT CAST(9223372036854775808 AS BIGINT), arrow_typeof(9223372036854775808)").await;
    show(&c, "CAST 1e19 double -> BIGINT", "SELECT CAST(1e19 AS BIGINT)").await;
    show(&c, "CAST -1 -> BIGINT UNSIGNED", "SELECT CAST(-1 AS BIGINT UNSIGNED)").await;
    show(&c, "CAST runtime i64 -> i16", "SELECT CAST(v AS SMALLINT) FROM i64_over WHERE v > 0").await;
    show(&c, "config: any overflow knob?", "SELECT name FROM information_schema.df_settings WHERE name LIKE '%overflow%' OR name LIKE '%checked%' OR name LIKE '%wrap%'").await;
}
