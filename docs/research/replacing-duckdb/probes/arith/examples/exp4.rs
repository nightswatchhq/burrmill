//! Experiment 4: the checked UDAFs and the analyzer rule, against the cases of 1-3.

use std::sync::Arc;

use burrmill_arith::checked::register_all;
use burrmill_arith::rule::CheckedArithmetic;
use burrmill_arith::util::*;
use datafusion::prelude::SessionContext;

async fn checked_ctx(parts: usize) -> SessionContext {
    let c = ctx(parts);
    register_all(&c);
    c.add_analyzer_rule(Arc::new(CheckedArithmetic::default()));
    for t in [
        "i64_over", "i64_partial", "i64_one", "i64_min", "d38_over", "d38_i128", "d38_partial", "d76_over",
        "d76_i256", "d76_partial", "credits", "debits", "noncanon",
    ] {
        reg(&c, t, &format!("data/{t}/")).await;
    }
    c
}

#[tokio::main]
async fn main() {
    gen_int_data();
    gen_dec_data();
    gen_text_data();
    println!("=== 4. Checked prototype through the analyzer rule (DataFusion {}) ===", datafusion::DATAFUSION_VERSION);

    for parts in [1usize, 6] {
        let c = checked_ctx(parts).await;
        println!("--- rule on, target_partitions = {parts}: experiment 1 cases ---");
        show(&c, "SUM i64_over (true 2^63)", "SELECT SUM(v) FROM i64_over").await;
        show(&c, "SUM i64_partial (true MAX-1)", "SELECT SUM(v) FROM i64_partial").await;
        show(&c, "SUM i64_one", "SELECT SUM(v) FROM i64_one").await;
        show(&c, "SUM grouped", "SELECT v > 0 AS k, SUM(v) FROM i64_over GROUP BY k ORDER BY k").await;
        println!("--- rule on, target_partitions = {parts}: experiment 2 cases ---");
        for t in ["d38_over", "d38_i128", "d38_partial", "d76_over", "d76_i256", "d76_partial"] {
            show(&c, &format!("SUM {t}"), &format!("SELECT SUM(v) FROM {t}")).await;
        }
        for t in ["d38_i128", "d38_partial", "d76_i256", "d76_partial"] {
            show(&c, &format!("AVG {t}"), &format!("SELECT AVG(v) FROM {t}")).await;
        }
        println!("--- rule on, target_partitions = {parts}: experiment 3 cases ---");
        show(&c, "UNION ALL CAST 76", Q_UNION_CAST76).await;
        show(&c, "UNION ALL TRY_CAST 76", Q_UNION_TRYCAST76).await;
        show(&c, "nuthatch today 38", Q_NUTHATCH38).await;
        show(&c, "SUM over VARCHAR", Q_SUM_TEXT).await;
    }

    let c = checked_ctx(4).await;
    println!("--- rule on: scalars ---");
    show(&c, "fold +", "SELECT 9223372036854775807 + 1").await;
    show(&c, "fold *", "SELECT 10000000000 * 10000000000").await;
    show(&c, "fold neg", "SELECT -(-9223372036854775807 - 1)").await;
    show(&c, "fold ok", "SELECT 9223372036854775806 + 1, 3 * 4 - 5").await;
    show(&c, "runtime v + 1", "SELECT v + 1 FROM i64_over WHERE v > 0").await;
    show(&c, "runtime v * v", "SELECT v * v FROM i64_over WHERE v > 0").await;
    show(&c, "runtime -v MIN", "SELECT -v FROM i64_min").await;
    show(&c, "runtime v + 1 in WHERE", "SELECT v FROM i64_over WHERE v + 1 > 0").await;
    show(&c, "runtime ok", "SELECT v + 1, v * 2, v - 3 FROM i64_over WHERE v = 0 LIMIT 1").await;
    show(&c, "d38 a + b", "SELECT a + b FROM (SELECT v a, v b FROM d38_i128 LIMIT 1)").await;
    show(&c, "d38 a * 10 (39 digits, fits i128)", "SELECT a * 10 FROM (SELECT v a FROM d38_i128 LIMIT 1)").await;
    show(&c, "d38 a + 1", "SELECT a + 1 FROM (SELECT v a FROM d38_i128 LIMIT 1)").await;
    show(&c, "d38 a - 1 (fits)", "SELECT a - 1 FROM (SELECT v a FROM d38_i128 LIMIT 1)").await;
    show(&c, "d76 a + b", "SELECT a + b FROM (SELECT v a, v b FROM d76_i256 LIMIT 1)").await;
    show(&c, "d76 a * 6", "SELECT a * 6 FROM (SELECT v a FROM d76_i256 LIMIT 1)").await;
    show(&c, "fold d38 + 1", &format!("SELECT {D38MAX} + 1")).await;
    show(&c, "fold d38 * d38", &format!("SELECT {D38MAX} * {D38MAX}")).await;
    show(&c, "float untouched", "SELECT 1e308 * 10, 1.5 + 2.5").await;
    show(&c, "date arithmetic untouched", "SELECT DATE '2024-01-01' + INTERVAL '1 day'").await;

    println!("--- rule on: through a CTE and a join ---");
    show(&c, "CTE sum", "WITH s AS (SELECT party, SUM(CAST(amount AS DECIMAL(76,0))) AS t FROM credits WHERE party IN ('final_over','small') GROUP BY party) SELECT * FROM s ORDER BY party").await;
    show(&c, "CTE sum, fits", "WITH s AS (SELECT party, SUM(CAST(amount AS DECIMAL(76,0))) AS t FROM credits WHERE party IN ('fits76','small') GROUP BY party) SELECT party, t + 1 FROM s ORDER BY party").await;
    show(&c, "join sum - sum", "SELECT c.party, SUM(CAST(c.amount AS DECIMAL(76,0))) - SUM(CAST(d.amount AS DECIMAL(76,0))) AS bal FROM credits c JOIN debits d ON c.party = d.party WHERE c.party IN ('small','neg','fits76') GROUP BY c.party ORDER BY c.party").await;
    show(&c, "join sum in subquery", "SELECT party, bal FROM (SELECT c.party, SUM(CAST(c.amount AS DECIMAL(76,0))) AS bal FROM credits c JOIN debits d ON c.party = d.party WHERE c.party = 'partial_over' GROUP BY c.party) ORDER BY party").await;
    show(&c, "window sum", "SELECT v, SUM(v) OVER () FROM i64_over ORDER BY v").await;
    show(&c, "scalar subquery", "SELECT (SELECT SUM(v) FROM i64_over) AS s").await;
    show(&c, "HAVING", "SELECT party FROM credits GROUP BY party HAVING SUM(CAST(amount AS DECIMAL(76,0))) > 1 ORDER BY party").await;
    let df = c.sql("EXPLAIN SELECT c.party, SUM(CAST(c.amount AS DECIMAL(76,0))) - SUM(CAST(d.amount AS DECIMAL(76,0))) AS bal FROM credits c JOIN debits d ON c.party = d.party GROUP BY c.party").await.unwrap().collect().await.unwrap();
    let t = datafusion::arrow::util::pretty::pretty_format_batches(&df).unwrap().to_string();
    println!("[logical plan after rule]");
    for l in t.lines().take_while(|l| !l.contains("physical_plan")).filter(|l| l.contains("checked_")) {
        println!("    {}", l.trim_matches('|').trim());
    }

    println!("--- rule on: refusals ---");
    show(&c, "SUM DISTINCT", "SELECT SUM(DISTINCT v) FROM i64_over").await;
    show(&c, "SUM float", "SELECT SUM(CAST(v AS DOUBLE)) FROM i64_over").await;
    show(&c, "AVG float", "SELECT AVG(CAST(v AS DOUBLE)) FROM i64_over").await;

    println!("--- checked_sum / checked_sum_text called directly on VARCHAR (the Nuthatch fix) ---");
    let signed = "SELECT party, amount FROM credits UNION ALL SELECT party, CASE WHEN amount = '0' THEN '0' ELSE '-' || amount END FROM debits";
    for parts in [1usize, 4] {
        let c = checked_ctx(parts).await;
        show(&c, &format!("checked_sum(text) -> Decimal256, per party, {parts}p"),
            &format!("SELECT party, checked_sum(amount) AS bal FROM ({signed}) GROUP BY party ORDER BY party")).await;
        show(&c, &format!("checked_sum(text) excluding the two that cannot fit, {parts}p"),
            &format!("SELECT party, checked_sum(amount) AS bal, arrow_typeof(checked_sum(amount)) FROM ({signed}) WHERE party NOT IN ('max','p255','p255m1','final_over','final_over2') GROUP BY party ORDER BY party")).await;
        show(&c, &format!("checked_sum_text(text) -> exact text, all parties, {parts}p"),
            &format!("SELECT party, checked_sum_text(amount) AS bal FROM ({signed}) GROUP BY party ORDER BY party")).await;
        show(&c, &format!("checked_avg(text), {parts}p"),
            "SELECT party, checked_avg(amount) FROM credits WHERE party IN ('small','partial_over') GROUP BY party ORDER BY party").await;
    }
    let c = checked_ctx(4).await;
    show(&c, "checked_sum on non-canonical text", "SELECT party, checked_sum(amount) FROM noncanon GROUP BY party ORDER BY party").await;
    show(&c, "checked_sum on canonical subset", "SELECT party, checked_sum(amount) FROM noncanon WHERE amount = '7' GROUP BY party").await;
    show(&c, "checked_sum with FILTER", "SELECT checked_sum(v) FILTER (WHERE v < 9223372036854775807) FROM i64_over").await;
    show(&c, "checked_sum on Int64, fits", "SELECT checked_sum(v) FROM i64_partial").await;
    show(&c, "checked_sum on Int64, overflows", "SELECT checked_sum(v) FROM i64_over").await;
    show(&c, "checked_sum_text on Int64, overflows", "SELECT checked_sum_text(v) FROM i64_over").await;
    show(&c, "checked_sum_text on Decimal256 beyond i256", "SELECT checked_sum_text(v) FROM d76_i256").await;
    show(&c, "checked_sum on empty input", "SELECT checked_sum(v) FROM i64_over WHERE v = 5").await;
}

