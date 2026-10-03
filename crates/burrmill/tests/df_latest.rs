//! "The latest matching row per outer row": a correlated `ORDER BY … LIMIT k` with a range in the
//! correlation, as a scalar subquery and as a lateral join. DataFusion 55 decorrelates neither.

use burrmill::Engine;
use serde_json::{Value, json};

/// Creations `c` and decisions `d`: a decision counts for a creation when it is for the same id and
/// comes after it. `c` repeats a row, and id 3 has no decision after it.
const DATA: &str = "WITH c(id, b) AS (VALUES (1, 10), (1, 10), (1, 30), (2, 10), (3, 50)), \
                    d(id, b, v) AS (VALUES (1, 5, 'early'), (1, 20, 'mid'), (1, 40, 'late'), \
                    (2, 15, 'two'), (3, 40, 'before'))";

fn rows(sql: &str) -> Vec<Value> {
    let engine = Engine::open_empty().unwrap();
    let got = engine.sql(sql).unwrap_or_else(|e| panic!("{sql}\n{e}"));
    got.iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect()
}

#[test]
fn a_scalar_subquery_takes_the_latest_later_row_per_outer_row() {
    let sql = format!(
        "{DATA} SELECT c.id, c.b, (SELECT d.v FROM d WHERE d.id = c.id AND d.b > c.b ORDER BY d.b DESC LIMIT 1) AS v \
         FROM c ORDER BY c.id, c.b"
    );
    assert_eq!(
        rows(&sql),
        vec![
            json!({"id": 1, "b": 10, "v": "late"}),
            json!({"id": 1, "b": 10, "v": "late"}),
            json!({"id": 1, "b": 30, "v": "late"}),
            json!({"id": 2, "b": 10, "v": "two"}),
            json!({"id": 3, "b": 50, "v": null}),
        ]
    );
}

#[test]
fn a_left_lateral_join_keeps_an_outer_row_with_no_match() {
    let sql = format!(
        "{DATA} SELECT c.id, c.b, x.v FROM c LEFT JOIN LATERAL \
         (SELECT d.v FROM d WHERE d.id = c.id AND d.b > c.b ORDER BY d.b ASC LIMIT 1) x ON true \
         ORDER BY c.id, c.b"
    );
    assert_eq!(
        rows(&sql),
        vec![
            json!({"id": 1, "b": 10, "v": "mid"}),
            json!({"id": 1, "b": 10, "v": "mid"}),
            json!({"id": 1, "b": 30, "v": "late"}),
            json!({"id": 2, "b": 10, "v": "two"}),
            json!({"id": 3, "b": 50, "v": null}),
        ]
    );
}

#[test]
fn a_lateral_join_with_a_limit_of_two_and_a_cross_join_drops_the_unmatched() {
    let sql = format!(
        "{DATA} SELECT c.id, c.b, x.v FROM c CROSS JOIN LATERAL \
         (SELECT d.v, d.b FROM d WHERE d.id = c.id AND d.b > c.b ORDER BY d.b DESC LIMIT 2) x \
         ORDER BY c.id, c.b, x.b"
    );
    assert_eq!(
        rows(&sql),
        vec![
            json!({"id": 1, "b": 10, "v": "mid"}),
            json!({"id": 1, "b": 10, "v": "mid"}),
            json!({"id": 1, "b": 10, "v": "late"}),
            json!({"id": 1, "b": 10, "v": "late"}),
            json!({"id": 1, "b": 30, "v": "late"}),
            json!({"id": 2, "b": 10, "v": "two"}),
        ]
    );
}

/// A lateral with no relation of its own names a value computed from the outer row, one row per
/// outer row: the network nest's reward fold binds its per-event reward this way.
#[test]
fn a_lateral_binding_is_a_column_of_the_outer_row() {
    let sql = format!(
        "{DATA} SELECT c.id, c.b, r.x, r.y FROM c CROSS JOIN LATERAL \
         (SELECT CASE WHEN c.b > 20 THEN c.b * 2 ELSE 0 END AS x, c.id + 100 AS y) r ORDER BY c.id, c.b"
    );
    assert_eq!(
        rows(&sql),
        vec![
            json!({"id": 1, "b": 10, "x": 0, "y": 101}),
            json!({"id": 1, "b": 10, "x": 0, "y": 101}),
            json!({"id": 1, "b": 30, "x": 60, "y": 101}),
            json!({"id": 2, "b": 10, "x": 0, "y": 102}),
            json!({"id": 3, "b": 50, "x": 100, "y": 103}),
        ]
    );
}
