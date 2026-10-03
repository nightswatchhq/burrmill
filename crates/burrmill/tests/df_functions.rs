//! The functions the engine registers, audited (roadmap 6.2's allowlist question).
//!
//! DuckDB needed an allowlist because its own functions read files, environment and network
//! (`read_csv`, `getenv`, `glob`, the `duckdb_*` catalogue). This lists what DataFusion's
//! registry, as the engine builds it, puts in reach, and fails if anything of that kind appears.

use burrmill::Engine;

#[test]
fn nothing_registered_reaches_outside_the_query() {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("x", arrow::datatypes::DataType::Utf8, true),
    ]));
    let b = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![std::sync::Arc::new(arrow::array::StringArray::from(vec![
            "a",
        ]))],
    )
    .unwrap();
    let f = std::fs::File::create(segs.join(format!("t-{:064x}.parquet", 1))).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&b).unwrap();
    w.close().unwrap();
    let e = Engine::open_segments(&segs).unwrap();
    let names = e.function_names();
    if std::env::var("PRINT").is_ok() {
        println!("{}", names.join("\n"));
    }
    let suspicious = [
        "read", "file", "glob", "env", "shell", "exec", "http", "url", "load", "import", "copy",
        "attach", "path", "dir", "sys", "setting", "catalog", "query", "sql",
    ];
    let hits: Vec<&String> = names
        .iter()
        .filter(|n| suspicious.iter().any(|s| n.contains(s)))
        .collect();
    assert!(hits.is_empty(), "functions to examine: {hits:?}");
    for sql in [
        "SELECT input_file_name() FROM t",
        "SELECT file_row_index() FROM t",
    ] {
        let err = e.sql(sql).unwrap_err().to_string();
        assert!(
            !err.contains(segs.to_str().unwrap()),
            "leaks the path: {err}"
        );
    }
}

/// `error(text)` as DuckDB has it: the statement fails with the text wherever a row reaches it, and
/// a `CASE` that does not take its branch never does.
#[test]
fn error_raises_only_where_it_is_reached() {
    let engine = Engine::open_empty().unwrap();
    let err = engine
        .sql("SELECT error('the tripwire') AS e")
        .map(|_| ())
        .unwrap_err()
        .to_string();
    assert!(err.contains("the tripwire"), "{err}");
    let err = engine
        .sql("SELECT CASE WHEN x > 1 THEN error('two is too many') ELSE x END AS y FROM (VALUES (1), (2)) t(x)")
        .map(|_| ())
        .unwrap_err()
        .to_string();
    assert!(err.contains("two is too many"), "{err}");
    let got = engine
        .sql("SELECT CASE WHEN x > 5 THEN error('never') ELSE x END AS y FROM (VALUES (1), (2)) t(x)")
        .unwrap();
    assert_eq!(got.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

/// DuckDB's `first` and `last` aggregates, as nuthatch's port emitter folds an overlay: the value in
/// the given order, a NULL included, with `FILTER` to pass one over.
#[test]
fn first_and_last_are_duckdbs_aggregates() {
    let engine = Engine::open_empty().unwrap();
    let got = engine
        .sql(
            "SELECT last(x ORDER BY y) AS a, first(x ORDER BY y) AS b, \
             last(x ORDER BY y) FILTER (WHERE x IS NOT NULL) AS c \
             FROM (VALUES (1, 5), (NULL, 9), (3, 2), (4, 6)) t(x, y)",
        )
        .unwrap();
    let rows: Vec<_> = got
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect();
    assert_eq!(rows, vec![serde_json::json!({"a": null, "b": 3, "c": 4})]);
}

/// DuckDB's `BIGNUM`, as the network nest's views write it: token amounts cast from text, added,
/// negated and summed, exactly, and read back as text. It is `DECIMAL(38,0)` here, the nest's
/// 38-digit line, so a value past it refuses rather than being guessed at.
#[test]
fn bignum_is_the_38_digit_decimal() {
    let engine = Engine::open_empty().unwrap();
    let one = |sql: &str| -> serde_json::Value {
        let got = engine.sql(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        got.iter()
            .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
            .next()
            .unwrap()
    };
    assert_eq!(
        one(
            "SELECT CAST(CAST('9000000000000000000000000000' AS BIGNUM) + CAST('1000000000000000000' AS BIGNUM) AS VARCHAR) AS x"
        ),
        serde_json::json!({"x": "9000000001000000000000000000"})
    );
    assert_eq!(
        one("SELECT CAST(CAST(0 AS BIGNUM) - CAST('7' AS BIGNUM) AS VARCHAR) AS x"),
        serde_json::json!({"x": "-7"})
    );
    assert_eq!(
        one(
            "SELECT CAST(sum(CAST(v AS BIGNUM)) AS VARCHAR) AS x FROM (VALUES ('5000000000000000000000000000'), ('5000000000000000000000000000')) t(v)"
        ),
        serde_json::json!({"x": "10000000000000000000000000000"})
    );
    let refused = engine
        .sql("SELECT CAST('12345678901234567890123456789012345678901234567890' AS BIGNUM) AS x")
        .expect_err("a value past 38 digits refuses")
        .to_string();
    assert!(refused.contains("to DECIMAL(38,0)"), "{refused}");
}

/// `AS MATERIALIZED` is only a hint to DuckDB's planner; the answer is the CTE's either way.
#[test]
fn a_materialized_cte_answers_as_a_plain_one() {
    let engine = Engine::open_empty().unwrap();
    for sql in [
        "WITH t AS MATERIALIZED (SELECT 1 AS n) SELECT n FROM t",
        "WITH t AS NOT MATERIALIZED (SELECT 1 AS n) SELECT n FROM t",
        "WITH RECURSIVE t(n) AS MATERIALIZED (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT max(n) AS n FROM t",
    ] {
        let got = engine.sql(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(got.iter().map(|b| b.num_rows()).sum::<usize>(), 1, "{sql}");
    }
    let got = engine.sql("SELECT 'AS MATERIALIZED (' AS n").unwrap();
    let rows: Vec<_> = got
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect();
    assert_eq!(
        rows,
        vec![serde_json::json!({"n": "AS MATERIALIZED ("})],
        "a string is left as written"
    );
}

/// What the network nest's controller and escrow views call: a whole-string regex match, and `hex`
/// of a byte value as DuckDB prints it, upper case with no leading zeros.
#[test]
fn regexp_full_match_and_hex_as_duckdb_has_them() {
    let engine = Engine::open_empty().unwrap();
    let got = engine
        .sql(
            "SELECT regexp_full_match('0x' || repeat('0', 24) || repeat('ab', 20), '0x0{24}[0-9a-fA-F]{40}') AS whole, \
             regexp_full_match('x0x', '0x') AS part, hex(10) AS a, hex(255) AS b, hex(0) AS c, \
             lpad(hex(7 & 255), 2, '0') AS d",
        )
        .unwrap();
    let rows: Vec<_> = got
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect();
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"whole": true, "part": false, "a": "A", "b": "FF", "c": "0", "d": "07"})
        ]
    );
}

/// DuckDB allows an unaliased expression to repeat inside a CTE, whose column list names it; the
/// recursive fold the network nest's indexer view seeds this way.
#[test]
fn a_repeated_expression_inside_a_cte_is_not_a_clash() {
    let engine = Engine::open_empty().unwrap();
    let got = engine
        .sql(
            "WITH RECURSIVE f(a, b, c, d) AS (SELECT DISTINCT 1, CAST(0 AS BIGNUM), CAST(0 AS BIGNUM), 0 \
             UNION ALL SELECT a + 1, b, c, d FROM f WHERE a < 3) SELECT count(*) AS n FROM f",
        )
        .unwrap_or_else(|e| panic!("{e}"));
    let rows: Vec<_> = got
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect();
    assert_eq!(rows, vec![serde_json::json!({"n": 3})]);
}

/// Shifts as DuckDB 1.5 does them: `>>` is 0 past the width, `<<` refuses anything shifted out,
/// and a literal takes the other side's type.
#[test]
fn shifts_are_duckdbs() {
    let engine = Engine::open_empty().unwrap();
    let one = |sql: &str| -> serde_json::Value {
        let got = engine.sql(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        got.iter()
            .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
            .next()
            .unwrap()
    };
    assert_eq!(
        one(
            "SELECT CAST(1000 AS BIGINT) >> 64 AS a, CAST(-1000 AS BIGINT) >> 3 AS b, CAST(1000 AS UBIGINT) >> -1 AS c, \
             (CAST(66051 AS UBIGINT) >> 8) & 255 AS d, CAST(3 AS BIGINT) << 2 AS e"
        ),
        serde_json::json!({"a": 0, "b": -125, "c": 0, "d": 2, "e": 12})
    );
    for sql in [
        "SELECT CAST(1 AS INTEGER) << 31 AS a",
        "SELECT CAST(1 AS BIGINT) << 64 AS a",
        "SELECT CAST(-1 AS BIGINT) << 2 AS a",
    ] {
        assert!(engine.sql(sql).is_err(), "{sql} refuses");
    }
}

/// `information_schema` as nuthatch's `.tables` and `.schema` read it, current after a view is
/// defined past the first read.
#[test]
fn information_schema_follows_the_views() {
    let mut engine = Engine::open_empty().unwrap();
    let rows = |engine: &Engine, sql: &str| -> Vec<serde_json::Value> {
        let got = engine.sql(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        got.iter()
            .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
            .collect()
    };
    let tables = "SELECT table_name FROM information_schema.tables ORDER BY 1";
    engine.register_view("a", "SELECT 1 AS x").unwrap();
    assert_eq!(
        rows(&engine, tables),
        vec![serde_json::json!({"table_name": "a"})]
    );
    engine
        .register_view("b", "SELECT 'y' AS y, 2 AS z")
        .unwrap();
    assert_eq!(
        rows(&engine, tables),
        vec![
            serde_json::json!({"table_name": "a"}),
            serde_json::json!({"table_name": "b"})
        ]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'b' ORDER BY ordinal_position"
        ),
        vec![
            serde_json::json!({"column_name": "y", "data_type": "VARCHAR"}),
            serde_json::json!({"column_name": "z", "data_type": "BIGINT"}),
        ]
    );
}

/// A statement that cannot plan is told apart from one that failed reading, as DuckDB's binder
/// errors are: nuthatch's integrity sweep reads damaged data into the second and only the second.
#[test]
fn a_statement_that_does_not_plan_is_a_plan_error() {
    use burrmill::BurrmillError;
    let mut engine = Engine::open_empty().unwrap();
    engine.register_view("t", "SELECT 1 AS x").unwrap();
    for sql in [
        "SELECT no_such_column FROM t",
        "SELECT x + 'a' FROM t",
        "SELECT * FROM t WHERE x < 'a'",
    ] {
        let e = engine.sql(sql).expect_err(sql);
        assert!(matches!(e, BurrmillError::Plan(_)), "{sql}: {e:?}");
    }
    let e = engine
        .sql("SELECT error('at a row') FROM t")
        .expect_err("raised at a row");
    assert!(matches!(e, BurrmillError::Substrate(_)), "{e:?}");
}

fn one_row(engine: &Engine, sql: &str) -> Vec<serde_json::Value> {
    let got = engine.sql(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    got.iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect()
}

/// #17: LIKE has no escape character unless `ESCAPE` names one, and then any one character may be
/// it, as DuckDB 1.5 answers each of these.
#[test]
fn like_escapes_only_with_an_escape_clause() {
    let engine = Engine::open_empty().unwrap();
    assert_eq!(
        one_row(
            &engine,
            r#"SELECT 'ab' LIKE 'a\b' AS a, 'a\b' LIKE 'a\\b' AS b, 'a\b' LIKE 'a\b' AS c, 'a%' LIKE 'a$%' ESCAPE '$' AS d, 'ab' LIKE 'a$%' ESCAPE '$' AS e, 'a$' LIKE 'a$$' ESCAPE '$' AS f, 'a_c' LIKE 'a\_c' ESCAPE '\' AS g, 'abc' LIKE 'a\_c' ESCAPE '\' AS h"#
        ),
        vec![
            serde_json::json!({"a": false, "b": false, "c": true, "d": true, "e": false, "f": true, "g": true, "h": false})
        ]
    );
    assert_eq!(
        one_row(
            &engine,
            r#"SELECT 'A\B' ILIKE 'a\b' AS a, 'AB' ILIKE 'a\b' AS b, 'A%' ILIKE 'a#%' ESCAPE '#' AS c, 'a\b' NOT LIKE 'a\b' AS d, 'a' LIKE 'a' ESCAPE '' AS e, '%' LIKE '%%' ESCAPE '%' AS f, 'x' LIKE '%%' ESCAPE '%' AS g, 'ab' LIKE 'a$b' ESCAPE '$' AS h"#
        ),
        vec![
            serde_json::json!({"a": true, "b": false, "c": true, "d": false, "e": true, "f": true, "g": false, "h": true})
        ]
    );
    assert_eq!(
        one_row(
            &engine,
            r#"SELECT s LIKE p AS m FROM (VALUES ('a\b', 'a\b'), ('ab', 'a\b'), ('a\b', 'a\\b'), ('x', NULL)) t(s, p)"#
        ),
        [true, false, false]
            .map(|m| serde_json::json!({ "m": m }))
            .into_iter()
            .chain([serde_json::json!({ "m": null })])
            .collect::<Vec<_>>()
    );
    for (sql, why) in [
        (
            r#"SELECT 'a$' LIKE 'a$' ESCAPE '$' AS a"#,
            "must not end with escape character",
        ),
        (
            r#"SELECT 'a\' LIKE 'a\' ESCAPE '\' AS a"#,
            "must not end with escape character",
        ),
        (
            r#"SELECT s LIKE p ESCAPE '$' AS m FROM (VALUES ('a%', 'a$%')) t(s, p)"#,
            "is not supported here",
        ),
    ] {
        let e = engine.sql(sql).expect_err(sql).to_string();
        assert!(e.contains(why), "{sql}: {e}");
    }
}

/// #18: text to DECIMAL(p,0) and HUGEINT reads every spelling DuckDB 1.5 reads: an exponent,
/// underscores between digits, a fraction rounded half away from zero; NULL from `TRY_CAST` and a
/// refusal from `CAST` for what it does not.
#[test]
fn text_to_decimal_reads_duckdbs_spellings() {
    let engine = Engine::open_empty().unwrap();
    assert_eq!(
        one_row(
            &engine,
            "SELECT TRY_CAST('1e3' AS DECIMAL(38,0)) a, TRY_CAST('1.5e18' AS HUGEINT) b, TRY_CAST('1_000' AS HUGEINT) c, \
             TRY_CAST(' 12 ' AS HUGEINT) d, TRY_CAST('+7' AS DECIMAL(38,0)) e, TRY_CAST('1.5' AS DECIMAL(38,0)) f, \
             TRY_CAST('-2.5' AS HUGEINT) g, TRY_CAST('0x10' AS HUGEINT) h, TRY_CAST('abc' AS HUGEINT) i, \
             TRY_CAST('' AS DECIMAL(38,0)) j"
        ),
        vec![
            serde_json::json!({"a": "1000", "b": "1500000000000000000", "c": "1000", "d": "12", "e": "7",
            "f": "2", "g": "-3", "h": null, "i": null, "j": null})
        ]
    );
    assert_eq!(
        one_row(
            &engine,
            "SELECT TRY_CAST('1e38' AS DECIMAL(38,0)) a, TRY_CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) b, \
             TRY_CAST('99999999999999999999999999999999999999.5' AS DECIMAL(38,0)) c, TRY_CAST('123' AS DECIMAL(2,0)) d, \
             TRY_CAST('1e2' AS DECIMAL(3,0)) e, TRY_CAST('1_000' AS DECIMAL(38,0)) f, TRY_CAST('2.5' AS HUGEINT) g, \
             TRY_CAST('1e-1' AS DECIMAL(38,0)) h, TRY_CAST('5e-1' AS HUGEINT) i, TRY_CAST('1E3' AS DECIMAL(38,0)) j, \
             TRY_CAST('1__0' AS HUGEINT) k, TRY_CAST('inf' AS HUGEINT) l"
        ),
        vec![
            serde_json::json!({"a": null, "b": "99999999999999999999999999999999999999", "c": null, "d": null,
            "e": "100", "f": "1000", "g": "3", "h": "0", "i": "1", "j": "1000", "k": null, "l": null})
        ]
    );
    assert_eq!(
        one_row(
            &engine,
            "SELECT CAST('1e3' AS DECIMAL(38,0)) a, CAST('1_000' AS HUGEINT) b, CAST(' 7 ' AS HUGEINT) c, \
             CAST('1.5' AS DECIMAL(38,0)) d"
        ),
        vec![serde_json::json!({"a": "1000", "b": "1000", "c": "7", "d": "2"})]
    );
    assert_eq!(
        one_row(
            &engine,
            "SELECT TRY_CAST('1__0' AS BIGINT) a, TRY_CAST('1_0.5_5' AS HUGEINT) b, TRY_CAST('1_0e1_0' AS HUGEINT) c, \
             TRY_CAST('1.0_0' AS DECIMAL(38,0)) d, TRY_CAST('1e_1' AS HUGEINT) e"
        ),
        vec![serde_json::json!({"a": null, "b": "11", "c": "100000000000", "d": "1", "e": null})]
    );
    for sql in [
        "SELECT CAST('abc' AS HUGEINT) a",
        "SELECT CAST('1e39' AS DECIMAL(38,0)) a",
    ] {
        let e = engine.sql(sql).expect_err(sql).to_string();
        assert!(e.contains("Could not convert string"), "{sql}: {e}");
    }
}
