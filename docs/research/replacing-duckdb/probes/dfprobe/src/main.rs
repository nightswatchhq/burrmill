use datafusion::prelude::*;
use datafusion::execution::context::SQLOptions;
async fn run(ctx: &SessionContext, q: &str) {
    print!("{q}\n  => ");
    match ctx.sql(q).await {
        Ok(df) => match df.collect().await {
            Ok(b) => println!("{}", datafusion::arrow::util::pretty::pretty_format_batches(&b).unwrap()),
            Err(e) => println!("EXEC ERR: {e}"),
        },
        Err(e) => println!("PLAN ERR: {e}"),
    }
}
#[tokio::main]
async fn main() {
    let ctx = SessionContext::new();
    let n78 = "115792089237316195423570985008687907853269984665640564039457584007913129639935"; // 2^256-1, 78 digits
    let n76 = "9999999999999999999999999999999999999999999999999999999999999999999999999999"; // 76 nines
    let n38 = "99999999999999999999999999999999999999";
    let qs = vec![
        "SELECT 10000000000 * 10000000000".to_string(),
        "SELECT 9223372036854775807 + 1".to_string(),
        "SELECT a + b FROM (VALUES (9223372036854775807, 1)) t(a,b)".to_string(),
        "SELECT arrow_cast(-2147483648,'Int32') % arrow_cast(-1,'Int32')".to_string(),
        "SELECT a + b FROM (VALUES (arrow_cast(-2147483648,'Int32'), arrow_cast(-1,'Int32'))) t(a,b)".to_string(),
        "SELECT -a FROM (VALUES (arrow_cast(-2147483648,'Int32'))) t(a)".to_string(),
        format!("SELECT CAST('{n78}' AS DECIMAL(76,0))"),
        format!("SELECT TRY_CAST('{n78}' AS DECIMAL(76,0))"),
        format!("SELECT CAST(v AS DECIMAL(76,0)) FROM (VALUES ('{n78}')) t(v)"),
        format!("SELECT TRY_CAST(v AS DECIMAL(76,0)) FROM (VALUES ('{n78}')) t(v)"),
        format!("SELECT CAST(v AS DECIMAL(76,0)) FROM (VALUES ('{n76}')) t(v)"),
        format!("SELECT CAST(v AS DECIMAL(38,0)) FROM (VALUES ('{n38}9')) t(v)"),
        format!("SELECT TRY_CAST(v AS DECIMAL(38,0)) FROM (VALUES ('{n38}9')) t(v)"),
        "SELECT CAST(v AS DECIMAL(38,0)) FROM (VALUES ('7.9'), (' 7 '), ('1e18'), ('1_000')) t(v)".to_string(),
        "SELECT TRY_CAST(v AS DECIMAL(38,0)) FROM (VALUES ('7.9'), (' 7 '), ('1e18'), ('1_000'), ('abc')) t(v)".to_string(),
        "SELECT SUM(v) FROM (VALUES (9223372036854775807), (1)) t(v)".to_string(),
        "SELECT k, SUM(v) FROM (VALUES (1, 9223372036854775807), (1, 1)) t(k, v) GROUP BY k".to_string(),
        format!("SELECT SUM(CAST(v AS DECIMAL(38,0))) FROM (VALUES ('{n38}'), ('1')) t(v)"),
        format!("SELECT k, SUM(CAST(v AS DECIMAL(38,0))) FROM (VALUES (1,'{n38}'), (1,'1')) t(k,v) GROUP BY k"),
        format!("SELECT SUM(CAST(v AS DECIMAL(76,0))) FROM (VALUES ('{n76}'), ('1')) t(v)"),
        format!("SELECT k, SUM(CAST(v AS DECIMAL(76,0))) FROM (VALUES (1,'{n76}'), (1,'1')) t(k,v) GROUP BY k"),
        format!("SELECT AVG(CAST(v AS DECIMAL(76,0))) FROM (VALUES ('{n76}'), ('{n76}')) t(v)"),
        format!("SELECT CAST(v AS DECIMAL(76,0)) * CAST(v AS DECIMAL(76,0)) FROM (VALUES ('{n38}')) t(v)"),
        format!("SELECT CAST(v AS DECIMAL(76,0)) + CAST(v AS DECIMAL(76,0)) FROM (VALUES ('{n76}')) t(v)"),
        format!("SELECT CAST(v AS DECIMAL(76,0)) - CAST(w AS DECIMAL(76,0)) FROM (VALUES ('-{n76}', '{n76}')) t(v, w)"),
        "SELECT '1' + 1, 7 / 2, 7 // 2".to_string(),
        "SELECT * FROM (VALUES (1),(NULL),(2)) t(v) ORDER BY v".to_string(),
        "SELECT * FROM (VALUES (1),(NULL),(2)) t(v) ORDER BY v DESC".to_string(),
        "SELECT x FROM (VALUES (1,2)) t(x,y) GROUP BY ALL".to_string(),
        "SELECT * EXCLUDE (y) FROM (VALUES (1,2)) t(x,y)".to_string(),
        "SELECT x, row_number() OVER (ORDER BY x) rn FROM (VALUES (1),(2)) t(x) QUALIFY rn = 1".to_string(),
        "SELECT * FROM read_parquet('/etc/hosts')".to_string(),
        "SELECT * FROM '/etc/hosts'".to_string(),
        "CREATE EXTERNAL TABLE pw STORED AS CSV LOCATION '/etc/hosts'".to_string(),
        "SELECT count(*) > 0 FROM pw".to_string(),
        "CREATE VIEW vw AS SELECT 1 AS one".to_string(),
        "COPY (SELECT 1) TO '/tmp/dfprobe_copy_test.csv'".to_string(),
        "SET datafusion.execution.target_partitions = 1".to_string(),
        "INSERT INTO pw VALUES ('x')".to_string(),
        "SET datafusion.sql_parser.dialect = 'duckdb'".to_string(),
        "SELECT x FROM (VALUES (1,2)) t(x,y) GROUP BY ALL".to_string(),
        "SELECT * EXCLUDE (y) FROM (VALUES (1,2)) t(x,y)".to_string(),
        "FROM (VALUES (1,2)) t(x,y) SELECT x".to_string(),
        "SELECT 7 // 2".to_string(),
        "SELECT x::INT FROM (VALUES ('3')) t(x)".to_string(),
    ];
    for q in &qs { run(&ctx, q).await; }
    println!("\n==== SQLOptions locked ====");
    let opts = SQLOptions::new().with_allow_ddl(false).with_allow_dml(false).with_allow_statements(false);
    for q in ["CREATE EXTERNAL TABLE pw2 STORED AS CSV LOCATION '/etc/hosts'", "COPY (SELECT 1) TO '/tmp/dfprobe_copy_test2.csv'", "SET datafusion.execution.batch_size = 1", "CREATE VIEW vw2 AS SELECT 1", "SELECT * FROM '/etc/hosts'", "INSERT INTO pw VALUES ('x')", "SELECT 1"] {
        print!("{q}\n  => ");
        match ctx.sql_with_options(q, opts).await { Ok(_) => println!("ALLOWED"), Err(e) => println!("REFUSED: {e}") }
    }
    println!("\n==== registered table functions ====");
    let st = ctx.state();
    let mut names: Vec<_> = st.table_functions().keys().cloned().collect(); names.sort();
    println!("{names:?}");
    println!("udf count {} udaf count {} udwf count {}", st.scalar_functions().len(), st.aggregate_functions().len(), st.window_functions().len());
}
