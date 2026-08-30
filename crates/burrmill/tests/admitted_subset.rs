//! The allowlist, exercised at the point it is enforced: the parsed shape.
//!
//! A boundary nobody has watched refuse is not a boundary. Each of these is a query that must be
//! turned away, and the test says why rather than only that.

use burrmill::{plan, BurrmillError};

const NET_BALANCES: &str = r#"
SELECT addr, SUM(d) AS net FROM (
  SELECT "to" AS addr, TRY_CAST("value" AS HUGEINT) AS d FROM t
  UNION ALL
  SELECT "from" AS addr, -TRY_CAST("value" AS HUGEINT) AS d FROM t
) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr
"#;

fn refused(sql: &str) -> String {
    match plan::plan(sql) {
        Err(BurrmillError::NotAllowed(m)) => m,
        Err(other) => panic!("expected NotAllowed, got {other:?}"),
        Ok(p) => panic!("expected a refusal, got a plan: {}", p.describe()),
    }
}

#[test]
fn the_hot_path_shape_is_recognised() {
    let plan::Plan::SignedFold(f) = plan::plan(NET_BALANCES).expect("net_balances must plan");
    assert_eq!(f.table, "t");
    assert_eq!(f.credit_col, "to");
    assert_eq!(f.debit_col, "from");
    assert_eq!(f.value_col, "value");
    assert!(f.drop_zero, "HAVING SUM(d) <> 0 is part of the answer");
}

/// DataFusion's spelling of the same 128-bit width. Both dialects plan to the same operator, because
/// Burrmill owns the semantics and neither engine's vocabulary is authoritative here.
#[test]
fn decimal_38_0_plans_the_same_as_hugeint() {
    let df = NET_BALANCES.replace("HUGEINT", "DECIMAL(38,0)");
    assert_eq!(plan::plan(NET_BALANCES).unwrap(), plan::plan(&df).unwrap());
}

/// **The filesystem boundary.** This is the class that gave DuckDB CVE-2024-41672, where `sniff_csv`
/// kept reading the filesystem with `enable_external_access=false` set, and it is the mechanism
/// behind Grafana's CVE-2024-9264 at CVSS 9.9. Burrmill does not deny the function - there is no
/// place in the admitted grammar for a table function at all, so the path has nowhere to go.
#[test]
fn a_path_cannot_be_named() {
    let sql = NET_BALANCES.replace("FROM t", "FROM read_parquet('/etc/passwd')");
    let why = refused(&sql);
    assert!(why.contains("table functions"), "{why}");

    let unregistered = "SELECT addr, SUM(d) AS net FROM (\
        SELECT a AS addr, TRY_CAST(v AS HUGEINT) AS d FROM \"/etc/passwd\" UNION ALL \
        SELECT b AS addr, -TRY_CAST(v AS HUGEINT) AS d FROM \"/etc/passwd\") \
        GROUP BY addr";
    // It plans - a quoted string is a perfectly ordinary table name - and then dies at the catalog,
    // which is the layer that owns the allowlist. Two independent refusals, not one.
    let planned = plan::plan(unregistered).expect("a quoted name is still just a name");
    let burrmill::Plan::SignedFold(f) = &planned;
    assert_eq!(f.table, "/etc/passwd");
    let db = burrmill::Burrmill::new(burrmill::Catalog::new());
    assert!(matches!(
        db.query(unregistered, burrmill::Limits::default()),
        Err(BurrmillError::NotAllowed(_))
    ));
}

#[test]
fn writes_are_not_expressible() {
    for sql in [
        "COPY (SELECT 1) TO '/tmp/x.csv'",
        "INSERT INTO t VALUES (1)",
        "DROP TABLE t",
        "ATTACH '/etc/passwd' AS p",
    ] {
        assert!(
            matches!(plan::plan(sql), Err(BurrmillError::NotAllowed(_)) | Err(BurrmillError::Parse(_))),
            "{sql} must not plan"
        );
    }
}

#[test]
fn plain_cast_is_refused_because_it_changes_the_answer() {
    let sql = NET_BALANCES.replace("TRY_CAST", "CAST");
    let why = refused(&sql);
    assert!(why.contains("TRY_CAST"), "{why}");
}

/// Deduplicating would silently drop a party's second identical transfer. Same query text, different
/// answer, no error - exactly the failure this project refuses to ship.
#[test]
fn union_must_be_union_all() {
    let sql = NET_BALANCES.replace("UNION ALL", "UNION");
    assert!(refused(&sql).contains("UNION ALL"));
}

#[test]
fn a_narrower_cast_is_refused() {
    let sql = NET_BALANCES.replace("HUGEINT", "BIGINT");
    assert!(refused(&sql).contains("128-bit"));
}

/// A different ORDER BY is refused rather than silently overruled. Canonical ordering is applied
/// unconditionally, so honouring the request is impossible and ignoring it would be a lie.
#[test]
fn a_non_canonical_order_is_refused_not_ignored() {
    assert!(refused(&NET_BALANCES.replace("ORDER BY addr", "ORDER BY net DESC")).contains("canonically"));
    assert!(refused(&NET_BALANCES.replace("ORDER BY addr", "ORDER BY addr DESC")).contains("ascending"));
}

#[test]
fn clauses_the_shape_does_not_implement_are_refused_not_dropped() {
    assert!(refused(&NET_BALANCES.replace("SELECT addr,", "SELECT DISTINCT addr,")).contains("DISTINCT"));
    // Regression: this planned and dropped the LIMIT, which would have handed the caller more
    // rows than they asked for with nothing to indicate it.
    assert!(refused(&format!("{NET_BALANCES} LIMIT 10")).contains("LIMIT"));
    assert!(refused(&format!("WITH x AS (SELECT 1) {NET_BALANCES}")).contains("common table"));
    assert!(refused(&NET_BALANCES.replace("SUM(d)", "SUM(DISTINCT d)")).contains("DISTINCT"));
    assert!(refused(&NET_BALANCES.replace("SUM(d) AS net", "AVG(d) AS net")).contains("SUM"));
    assert!(refused("SELECT * FROM t").contains("SELECT *"));
}

#[test]
fn joins_are_not_admitted() {
    let sql = NET_BALANCES.replace(") GROUP BY addr", ") JOIN other ON true GROUP BY addr");
    assert!(refused(&sql).contains("join"));
}

#[test]
fn one_statement_per_query() {
    let why = refused(&format!("{NET_BALANCES}; {NET_BALANCES}"));
    assert!(why.contains("one statement"), "{why}");
}
