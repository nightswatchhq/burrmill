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

const POOLS: &str = "WITH pool(id, liquidity) AS (VALUES ('0xaaa', 5), ('0xbbb', 6), ('0xbbb', 8)), \
                     swap(id, pool) AS (VALUES ('s1', '0xaaa'), ('s2', '0xaaa'), ('s3', '0xaaa'), ('s4', '0xbbb'), ('s5', '0xccc'))";

/// A to-one lookup, as nuthatch's GraphQL compiles a relation: not aggregated, one row or none.
#[test]
fn a_correlated_lookup_that_is_not_aggregated_takes_its_one_row() {
    assert_eq!(
        rows(&format!(
            "{POOLS} SELECT c.id, (SELECT p.liquidity FROM pool p WHERE p.id = c.pool) AS l \
             FROM swap c WHERE c.pool <> '0xbbb' ORDER BY 1"
        )),
        vec![
            json!({"id": "s1", "l": 5}),
            json!({"id": "s2", "l": 5}),
            json!({"id": "s3", "l": 5}),
            json!({"id": "s5", "l": null}),
        ]
    );
    let nested = format!(
        "{POOLS} SELECT b.id, coalesce((SELECT to_json(list(t.s)) FROM (SELECT struct_pack(\"id\" := c.id, \
         \"pool\" := (SELECT struct_pack(\"id\" := p.id, \"liquidity\" := p.liquidity) FROM pool p WHERE p.id = c.pool)) AS s \
         FROM swap c WHERE c.pool = b.id ORDER BY c.id LIMIT 1 OFFSET 1) t), '[]') AS l FROM pool b WHERE b.id = '0xaaa'"
    );
    assert_eq!(
        rows(&nested),
        vec![json!({"id": "0xaaa", "l": "[{\"id\":\"s2\",\"pool\":{\"id\":\"0xaaa\",\"liquidity\":5}}]"})]
    );
}

#[test]
fn a_lookup_finding_two_rows_fails_as_duckdb_does() {
    let sql = format!("{POOLS} SELECT c.id, (SELECT p.liquidity FROM pool p WHERE p.id = c.pool) AS l FROM swap c");
    let e = Engine::open_empty().unwrap().sql(&sql).expect_err("0xbbb has two rows").to_string();
    assert!(e.contains("More than one row returned by a subquery"), "{e}");
}
