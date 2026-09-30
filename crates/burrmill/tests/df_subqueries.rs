//! Scalar subqueries that find no row.

use burrmill::Engine;
use serde_json::{Value, json};

fn rows(sql: &str) -> Vec<Value> {
    let engine = Engine::open_empty().unwrap();
    let got = engine.sql(sql).unwrap_or_else(|e| panic!("{sql}\n{e}"));
    got.iter().flat_map(|b| burrmill::df::encode::rows(b).unwrap()).collect()
}

#[test]
fn an_empty_scalar_subquery_is_null_even_over_a_non_nullable_column() {
    assert_eq!(
        rows("WITH s AS (SELECT true AS p WHERE false) SELECT coalesce((SELECT p FROM s), false) AS x"),
        vec![json!({"x": false})]
    );
    assert_eq!(
        rows("WITH s AS (SELECT true AS p WHERE false), n AS (SELECT CAST(NULL AS BOOLEAN) AS q) \
              SELECT (SELECT p FROM s) IS NULL AS x, (SELECT p FROM s), (SELECT q FROM n)"),
        vec![json!({"x": true, "p": null, "q": null})]
    );
    let sql = "WITH a(v) AS (VALUES (1)), \
               s AS (SELECT coalesce((SELECT true FROM a WHERE v > 5 ORDER BY v DESC LIMIT 1), false) AS p \
                     WHERE EXISTS (SELECT 1 FROM a WHERE v > 5)) \
               SELECT coalesce((SELECT p FROM s), false) AS x, coalesce((SELECT true FROM a WHERE v > 5), false) AS y";
    assert_eq!(rows(sql), vec![json!({"x": false, "y": false})]);
}
