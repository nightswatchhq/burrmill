//! Experiment 2: Decimal128(38,0) and Decimal256(76,0): SUM, AVG, arithmetic, casts.

use burrmill_arith::util::*;

#[tokio::main]
async fn main() {
    gen_dec_data();
    println!("=== 2. Decimals (DataFusion {}) ===", datafusion::DATAFUSION_VERSION);
    println!("D38MAX = 10^38-1 (38 digits); i128::MAX = {} (39 digits)", i128::MAX);
    println!("D76MAX = 10^76-1 (76 digits); i256::MAX = 2^255-1 = {P255M1} (77 digits)");
    println!("true sums: d38_over=10^38 (39 digits), d38_i128=2*(10^38-1)=199..98 (39 digits, > i128::MAX)");
    println!("           d38_partial=10^38-1 (fits; partial in file 0 is 2*(10^38-1))");
    println!("           d76_over=10^76 (77 digits, fits i256), d76_i256=6*(10^76-1)=599..94 (77 digits, > i256::MAX)");
    println!("           d76_partial=10^76-1 (fits; partial in file 0 is 6*(10^76-1), > i256::MAX)");

    for parts in [1usize, 6] {
        let c = ctx(parts);
        for t in ["d38_over", "d38_i128", "d38_partial", "d76_over", "d76_i256", "d76_partial"] {
            reg(&c, t, &format!("data/{t}/")).await;
        }
        println!("--- target_partitions = {parts} ---");
        for t in ["d38_over", "d38_i128", "d38_partial", "d76_over", "d76_i256", "d76_partial"] {
            show(&c, &format!("SUM {t} {parts}p"), &format!("SELECT SUM(v), arrow_typeof(SUM(v)) FROM {t}")).await;
        }
        if parts > 1 {
            explain_agg(&c, "SUM d76_i256", "SELECT SUM(v) FROM d76_i256").await;
        }
        for t in ["d38_i128", "d38_partial", "d76_i256", "d76_partial"] {
            show(&c, &format!("AVG {t} {parts}p"), &format!("SELECT AVG(v), arrow_typeof(AVG(v)) FROM {t}")).await;
        }
    }

    let c = ctx(4);
    for t in ["d38_i128", "d76_i256"] {
        reg(&c, t, &format!("data/{t}/")).await;
    }
    println!("--- arithmetic, runtime on columns (a = b = 10^38-1, Decimal128(38,0)) ---");
    show(&c, "d38 a + b", "SELECT a + b, arrow_typeof(a + b) FROM (SELECT v a, v b FROM d38_i128 LIMIT 1)").await;
    show(&c, "d38 a - (-b)", "SELECT a - (-b) FROM (SELECT v a, v b FROM d38_i128 LIMIT 1)").await;
    show(&c, "d38 a * b", "SELECT a * b, arrow_typeof(a * b) FROM (SELECT v a, v b FROM d38_i128 LIMIT 1)").await;
    show(&c, "d38 a * 10", "SELECT a * 10, arrow_typeof(a * 10) FROM (SELECT v a FROM d38_i128 LIMIT 1)").await;
    show(&c, "d38 a + 1", "SELECT a + 1, arrow_typeof(a + 1) FROM (SELECT v a FROM d38_i128 LIMIT 1)").await;
    println!("--- arithmetic, runtime on columns (a = b = 10^76-1, Decimal256(76,0)) ---");
    show(&c, "d76 a + b", "SELECT a + b, arrow_typeof(a + b) FROM (SELECT v a, v b FROM d76_i256 LIMIT 1)").await;
    show(&c, "d76 a * 6", "SELECT a * 6 FROM (SELECT v a FROM d76_i256 LIMIT 1)").await;
    show(&c, "d76 a * b", "SELECT a * b FROM (SELECT v a, v b FROM d76_i256 LIMIT 1)").await;
    println!("--- arithmetic, constant folded ---");
    show(&c, "fold typeof 38-digit literal", &format!("SELECT arrow_typeof({D38MAX})")).await;
    show(&c, "fold typeof 39-digit literal", &format!("SELECT arrow_typeof({D38MAX}9), {D38MAX}9")).await;
    show(&c, "fold typeof uint256 max literal", &format!("SELECT arrow_typeof({U256_MAX}), {U256_MAX}")).await;
    show(&c, "fold d38 + 1", &format!("SELECT {D38MAX} + 1")).await;
    show(&c, "fold d38 + d38", &format!("SELECT {D38MAX} + {D38MAX}")).await;
    show(&c, "fold d38 * d38", &format!("SELECT {D38MAX} * {D38MAX}")).await;
    show(&c, "fold d76 + 1 via CAST", &format!("SELECT CAST('{D76MAX}' AS DECIMAL(76,0)) + 1")).await;
    show(&c, "fold d76 * 6 via CAST", &format!("SELECT CAST('{D76MAX}' AS DECIMAL(76,0)) * 6")).await;

    println!("--- CAST text -> DECIMAL(76,0) ---");
    let cases = [
        ("76 digits 10^76-1", D76MAX),
        ("77 digits 2^255-1 (i256::MAX)", P255M1),
        ("77 digits 2^255 (i256::MAX+1)", P255),
        ("77 digits 10^76", E76),
        ("78 digits uint256 max", U256_MAX),
    ];
    for (label, v) in cases {
        show(&c, &format!("CAST {label}"), &format!("SELECT CAST('{v}' AS DECIMAL(76,0))")).await;
        show(&c, &format!("TRY_CAST {label}"), &format!("SELECT TRY_CAST('{v}' AS DECIMAL(76,0))")).await;
        show(&c, &format!("arrow_cast {label}"), &format!("SELECT arrow_cast('{v}', 'Decimal256(76, 0)')")).await;
    }
    println!("--- CAST text -> DECIMAL(38,0) ---");
    show(&c, "CAST 38 digits", &format!("SELECT CAST('{D38MAX}' AS DECIMAL(38,0))")).await;
    show(&c, "CAST 39 digits", &format!("SELECT CAST('{D38MAX}9' AS DECIMAL(38,0))")).await;
    show(&c, "TRY_CAST 39 digits", &format!("SELECT TRY_CAST('{D38MAX}9' AS DECIMAL(38,0))")).await;
    show(&c, "CAST i128::MAX (39 digits)", &format!("SELECT CAST('{}' AS DECIMAL(38,0))", i128::MAX)).await;
    show(&c, "CAST uint256 max", &format!("SELECT CAST('{U256_MAX}' AS DECIMAL(38,0))")).await;
    println!("--- CAST decimal -> narrower ---");
    show(&c, "CAST d256 -> DECIMAL(38,0) runtime", "SELECT CAST(v AS DECIMAL(38,0)) FROM d76_i256 LIMIT 1").await;
    show(&c, "TRY_CAST d256 -> DECIMAL(38,0) runtime", "SELECT TRY_CAST(v AS DECIMAL(38,0)) FROM d76_i256 LIMIT 1").await;
    show(&c, "CAST d128 -> BIGINT runtime", "SELECT CAST(v AS BIGINT) FROM d38_i128 LIMIT 1").await;
    show(&c, "CAST d128 -> DECIMAL(20,0) runtime", "SELECT CAST(v AS DECIMAL(20,0)) FROM d38_i128 LIMIT 1").await;
}
