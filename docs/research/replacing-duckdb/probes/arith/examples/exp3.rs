//! Experiment 3: the Nuthatch pattern. VARCHAR decimal amounts in two tables,
//! credits minus debits per party over a UNION ALL.

use burrmill_arith::util::*;

pub const EXPECTED: &str = "\
  true balances:
    fits76       0
    final_over   2*(10^76-1)  = 19999...98 (77 digits, fits i256, exceeds DECIMAL(76,0))
    final_over2  6*(10^76-1)  = 59999...94 (77 digits, exceeds i256::MAX)
    max          uint256 max  = 115792089237316195423570985008687907853269984665640564039457584007913129639935
    neg          -2
    p255         2^255 (exceeds i256::MAX by one)
    p255m1       2^255-1 (= i256::MAX, 77 digits, exceeds DECIMAL(76,0))
    partial_over 10^76-1 (credits partial is 6*(10^76-1) > i256::MAX; final fits)
    small        2";


#[tokio::main]
async fn main() {
    gen_text_data();
    println!("=== 3. Nuthatch pattern (DataFusion {}) ===", datafusion::DATAFUSION_VERSION);
    println!("{EXPECTED}");
    for parts in [1usize, 4] {
        let c = ctx(parts);
        reg(&c, "credits", "data/credits/").await;
        reg(&c, "debits", "data/debits/").await;
        println!("--- target_partitions = {parts} ---");
        show(&c, &format!("UNION ALL, CAST DECIMAL(76,0), {parts}p"), Q_UNION_CAST76).await;
        show(&c, &format!("UNION ALL, TRY_CAST DECIMAL(76,0), {parts}p"), Q_UNION_TRYCAST76).await;
        show(&c, &format!("nuthatch today: TRY_CAST DECIMAL(38,0) + _overflow, {parts}p"), Q_NUTHATCH38).await;
        if parts > 1 {
            explain_agg(&c, "UNION ALL TRY_CAST 76", Q_UNION_TRYCAST76).await;
        }
    }
    let c = ctx(4);
    reg(&c, "credits", "data/credits/").await;
    show(&c, "SUM directly over VARCHAR", Q_SUM_TEXT).await;
    show(&c, "CAST DECIMAL(76,0) without the union, small parties only",
        "SELECT party, SUM(CAST(amount AS DECIMAL(76,0))) FROM credits WHERE party IN ('small','neg','fits76') GROUP BY party ORDER BY party").await;
}
