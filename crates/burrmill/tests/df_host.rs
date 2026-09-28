//! A host registering tables itself: segment lists, hot rows, declared columns and a window, as
//! nuthatch's shadow will (RFC-0044 Amendment 2, phase 2b).

use std::sync::Arc;

use arrow::array::{StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;
use serde_json::{Value, json};

fn segment(dir: &std::path::Path, n: u64, rows: &[(u64, &str, &str)]) -> (std::path::PathBuf, u64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("who", DataType::Utf8, false),
        Field::new("amount", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let path = dir.join(format!("t-{n:064x}.parquet"));
    let f = std::fs::File::create(&path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let len = std::fs::metadata(&path).unwrap().len();
    (path, len)
}

fn rows(engine: &Engine, sql: &str) -> Vec<Value> {
    engine
        .sql(sql)
        .unwrap()
        .iter()
        .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
        .collect()
}

fn declared() -> Vec<(String, String)> {
    vec![
        ("block_number".into(), "u64".into()),
        ("who".into(), "address".into()),
        ("amount".into(), "word32".into()),
        ("memo".into(), "string".into()),
    ]
}

#[test]
fn sealed_and_hot_union_with_declared_columns_and_derived_decimals() {
    let tmp = tempfile::tempdir().unwrap();
    let a = segment(tmp.path(), 1, &[(10, "x", "5"), (11, "y", "7")]);
    let b = segment(
        tmp.path(),
        2,
        &[(12, "x", "99999999999999999999999999999999999999999")],
    );
    let hot = vec![
        json!({"block_number": 20, "who": "z", "amount": "1", "memo": "late"}),
        json!({"block_number": 21, "who": "x", "amount": null}),
    ];
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![a, b], &hot, (None, None))
        .unwrap();
    assert!(engine.has_table("t"));
    assert!(engine.has_table("T"));
    assert!(!engine.has_table("t__raw"));

    // `sum` over `UBIGINT` is `HUGEINT`, which nuthatch's JSON carries as text.
    let got = rows(
        &engine,
        "SELECT count(*) AS n, sum(block_number) AS s FROM t",
    );
    assert_eq!(got, vec![json!({"n": 5, "s": "74"})]);

    // The declared column no segment carries is NULL there and read from hot where it has it.
    let got = rows(
        &engine,
        "SELECT block_number, memo FROM t WHERE memo IS NOT NULL ORDER BY block_number",
    );
    assert_eq!(got, vec![json!({"block_number": 20, "memo": "late"})]);

    // `_dec` and `_overflow` beside the wide column, across sealed and hot alike.
    let got = rows(
        &engine,
        "SELECT block_number, amount_dec, amount_overflow FROM t ORDER BY block_number",
    );
    assert_eq!(
        got,
        vec![
            json!({"block_number": 10, "amount_dec": "5", "amount_overflow": false}),
            json!({"block_number": 11, "amount_dec": "7", "amount_overflow": false}),
            json!({"block_number": 12, "amount_dec": null, "amount_overflow": true}),
            json!({"block_number": 20, "amount_dec": "1", "amount_overflow": false}),
            json!({"block_number": 21, "amount_dec": null, "amount_overflow": false}),
        ]
    );
}

#[test]
fn a_window_bounds_the_rows_and_registration_replaces() {
    let tmp = tempfile::tempdir().unwrap();
    let a = segment(
        tmp.path(),
        1,
        &[(10, "x", "5"), (11, "y", "7"), (12, "x", "9")],
    );
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![a.clone()], &[], (Some(10), Some(11)))
        .unwrap();
    let got = rows(&engine, "SELECT block_number FROM t ORDER BY 1");
    assert_eq!(got, vec![json!({"block_number": 11})]);

    engine
        .register_facts("t", &declared(), vec![a], &[], (None, None))
        .unwrap();
    let got = rows(&engine, "SELECT count(*) AS n FROM t");
    assert_eq!(got, vec![json!({"n": 3})]);

    // Declared and never sealed: empty, typed, and its derived columns exist.
    engine
        .register_facts("u", &declared(), vec![], &[], (None, None))
        .unwrap();
    let got = rows(&engine, "SELECT count(*) AS n, sum(amount_dec) AS s FROM u");
    assert_eq!(got, vec![json!({"n": 0, "s": null})]);
}

/// A statement stopped from another thread ends as `Cancelled` at its next batch, and the engine
/// answers the next statement as if nothing happened.
#[test]
fn a_cancel_from_another_thread_stops_the_statement_and_not_the_engine() {
    // A cross join of a sealed table with itself, summed: nothing reaches the output until the
    // join has run to the end, so only a cancel seen by the scan can stop it in time.
    let tmp = tempfile::tempdir().unwrap();
    let many: Vec<(u64, &str, &str)> = (0..8_000u64).map(|i| (i, "x", "1")).collect();
    let seg = segment(tmp.path(), 1, &many);
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![seg], &[], (None, None))
        .unwrap();
    let token = engine.cancel_token();
    let stopper = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(150));
        token.cancel();
    });
    let started = std::time::Instant::now();
    let r = engine.sql("SELECT sum(a.block_number * b.block_number) AS s FROM t a, t b");
    stopper.join().unwrap();
    assert!(
        matches!(r, Err(burrmill::BurrmillError::Cancelled)),
        "{:?}",
        r.map(|_| ())
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(20));
    assert_eq!(rows(&engine, "SELECT 1 AS one"), vec![json!({"one": 1})]);
}

#[test]
fn rows_become_a_text_table() {
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_rows(
            "labels",
            &[
                json!({"address": "0xab", "label": "exchange"}),
                json!({"address": "0xcd", "label": null}),
            ],
        )
        .unwrap();
    let got = rows(
        &engine,
        "SELECT address, label FROM labels ORDER BY address",
    );
    assert_eq!(
        got,
        vec![
            json!({"address": "0xab", "label": "exchange"}),
            json!({"address": "0xcd", "label": null}),
        ]
    );
}

#[test]
fn a_bounded_engine_refuses_what_an_unbounded_one_answers() {
    let tmp = tempfile::tempdir().unwrap();
    let who: Vec<String> = (0..200_000).map(|i| format!("0x{i:040x}")).collect();
    let data: Vec<(u64, &str, &str)> = who.iter().map(|w| (1, w.as_str(), "1")).collect();
    let seg = segment(tmp.path(), 1, &data);
    let sql = "SELECT who, count(*) AS n FROM t GROUP BY who ORDER BY who";

    let mut free = Engine::open_empty().unwrap();
    free.register_facts("t", &declared(), vec![seg.clone()], &[], (None, None))
        .unwrap();
    assert_eq!(rows(&free, sql).len(), 200_000);

    let mut bounded = Engine::open_empty_within(4 << 20).unwrap();
    bounded
        .register_facts("t", &declared(), vec![seg], &[], (None, None))
        .unwrap();
    let err = bounded.sql(sql).unwrap_err().to_string();
    assert!(err.contains("Resources exhausted"), "{err}");
    assert_eq!(
        rows(&bounded, "SELECT count(*) AS n FROM t"),
        vec![json!({"n": 200000})]
    );
}

#[test]
fn ordered_list_per_group_across_segments() {
    let tmp = tempfile::tempdir().unwrap();
    let a = segment(
        tmp.path(),
        1,
        &[(5, "x", "1"), (2, "y", "7"), (9, "x", "3")],
    );
    let b = segment(
        tmp.path(),
        2,
        &[(1, "x", "4"), (7, "y", "8"), (3, "z", "0")],
    );
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![a, b], &[], (None, None))
        .unwrap();
    let got = rows(
        &engine,
        "SELECT who, CAST(list(amount ORDER BY block_number DESC) AS VARCHAR) AS l, \
         CAST(list(amount ORDER BY block_number) FILTER (WHERE amount <> '0') AS VARCHAR) AS f \
         FROM t GROUP BY who ORDER BY who",
    );
    assert_eq!(
        got,
        vec![
            json!({"who": "x", "l": "[3, 1, 4]", "f": "[4, 1, 3]"}),
            json!({"who": "y", "l": "[8, 7]", "f": "[7, 8]"}),
            json!({"who": "z", "l": "[0]", "f": null}),
        ]
    );
}

#[test]
fn ordered_list_when_partial_aggregation_is_skipped() {
    let tmp = tempfile::tempdir().unwrap();
    // Pairs for the first 30,000 rows, singles after: 285,000 groups, enough to skip.
    let who: Vec<String> = (0..300_000u64)
        .map(|i| format!("0x{:040x}", if i < 30_000 { i / 2 } else { i }))
        .collect();
    let data: Vec<(u64, &str, &str)> = who
        .iter()
        .enumerate()
        .map(|(i, w)| {
            (
                300_000 - i as u64,
                w.as_str(),
                if i % 2 == 0 { "a" } else { "b" },
            )
        })
        .collect();
    let seg = segment(tmp.path(), 1, &data);
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![seg], &[], (None, None))
        .unwrap();
    let got = rows(
        &engine,
        "SELECT CAST(l AS VARCHAR) AS l, count(*) AS n FROM \
         (SELECT who, list(amount ORDER BY block_number) AS l FROM t GROUP BY who) GROUP BY 1 \
         ORDER BY 1",
    );
    assert_eq!(
        got,
        vec![
            json!({"l": "[a]", "n": 135000}),
            json!({"l": "[b, a]", "n": 15000}),
            json!({"l": "[b]", "n": 135000}),
        ]
    );
}

#[test]
fn last_value_ignore_nulls_carries_the_last_non_null() {
    let tmp = tempfile::tempdir().unwrap();
    let seg = segment(
        tmp.path(),
        1,
        &[
            (1, "a", "0"),
            (2, "a", "5"),
            (3, "a", "0"),
            (4, "a", "7"),
            (5, "b", "0"),
            (6, "b", "0"),
        ],
    );
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![seg], &[], (None, None))
        .unwrap();
    let got = rows(
        &engine,
        "SELECT who, block_number, last_value(CASE WHEN amount <> '0' THEN amount END IGNORE NULLS) \
         OVER (PARTITION BY who ORDER BY block_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS l \
         FROM t ORDER BY who, block_number",
    );
    assert_eq!(
        got,
        vec![
            json!({"who": "a", "block_number": 1, "l": null}),
            json!({"who": "a", "block_number": 2, "l": "5"}),
            json!({"who": "a", "block_number": 3, "l": "5"}),
            json!({"who": "a", "block_number": 4, "l": "7"}),
            json!({"who": "b", "block_number": 5, "l": null}),
            json!({"who": "b", "block_number": 6, "l": null}),
        ]
    );
    // An exact type goes through the checked rule, which refuses aggregates it does not know.
    let got = rows(
        &engine,
        "SELECT last_value(CASE WHEN block_number % 2 = 0 THEN block_number END IGNORE NULLS) \
         OVER (ORDER BY block_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS l \
         FROM t ORDER BY block_number",
    );
    let got: Vec<_> = got.iter().map(|r| r["l"].clone()).collect();
    assert_eq!(
        got,
        vec![
            json!(null),
            json!(2),
            json!(2),
            json!(4),
            json!(4),
            json!(6)
        ]
    );
}

#[test]
fn a_sort_over_budget_spills_into_the_given_directory_only() {
    let tmp = tempfile::tempdir().unwrap();
    let who: Vec<String> = (0..1_000_000u64)
        .map(|i| format!("0x{:040x}", (i * 7919) % 1_000_000))
        .collect();
    let data: Vec<(u64, &str, &str)> = who.iter().map(|w| (1, w.as_str(), "1")).collect();
    let seg = segment(tmp.path(), 1, &data);
    let sql = "SELECT who FROM t ORDER BY who";
    let budget = |spill| burrmill::Budget {
        memory_bytes: 64 << 20,
        threads: 2,
        spill,
    };

    let mut refused = Engine::open_empty_budgeted(budget(None)).unwrap();
    refused
        .register_facts("t", &declared(), vec![seg.clone()], &[], (None, None))
        .unwrap();
    assert!(refused.sql(sql).is_err());

    let spill = tempfile::tempdir().unwrap();
    let mut spilling =
        Engine::open_empty_budgeted(budget(Some((spill.path().to_path_buf(), 1 << 30)))).unwrap();
    spilling
        .register_facts("t", &declared(), vec![seg], &[], (None, None))
        .unwrap();
    let mut seen = 0usize;
    let mut last = String::new();
    let mut wrote_there = false;
    spilling
        .sql_for_each(sql, |b| {
            wrote_there |= std::fs::read_dir(spill.path()).unwrap().next().is_some();
            for r in burrmill::df::encode::rows(&b).unwrap() {
                let w = r["who"].as_str().unwrap().to_string();
                assert!(w >= last);
                last = w;
                seen += 1;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(seen, 1_000_000);
    assert!(wrote_there, "nothing was spilled to the directory");
}

#[test]
fn a_host_text_function_propagates_null_refuses_on_error_and_stays_behind_a_case_guard() {
    let tmp = tempfile::tempdir().unwrap();
    let seg = segment(tmp.path(), 1, &[(1, "0x", "7"), (2, "0x2a", "5")]);
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![seg], &[], (None, None))
        .unwrap();
    let hex: burrmill::df::TextFunction = std::sync::Arc::new(|v: &[&str]| {
        let h = v[0]
            .strip_prefix("0x")
            .filter(|h| !h.is_empty())
            .ok_or("empty word")?;
        u64::from_str_radix(h, 16)
            .map(|n| n.to_string())
            .map_err(|e| e.to_string())
    });
    let cat: burrmill::df::TextFunction = std::sync::Arc::new(|v: &[&str]| Ok(v.join("|")));
    engine.register_text_function("t_hex", 1, hex);
    engine.register_text_function("t_cat", 3, cat);

    assert!(engine.sql("SELECT t_hex(who) FROM t").is_err());
    assert_eq!(
        rows(
            &engine,
            "SELECT CASE WHEN who = '0x' THEN '-' ELSE t_hex(who) END AS v, \
             t_cat(who, amount, 'k') AS c, t_hex(NULL) AS n FROM t ORDER BY block_number"
        ),
        vec![
            json!({"v": "-", "c": "0x|7|k", "n": null}),
            json!({"v": "42", "c": "0x2a|5|k", "n": null}),
        ]
    );
}

#[test]
fn parquet_scans_count_reads_of_segments_and_refuse_what_rescans() {
    let tmp = tempfile::tempdir().unwrap();
    let a = segment(tmp.path(), 1, &[(1, "x", "1"), (2, "y", "2")]);
    let b = segment(tmp.path(), 2, &[(3, "x", "3")]);
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![a, b], &[], (None, None))
        .unwrap();
    engine
        .register_rows("labels", &[json!({"who": "x", "name": "ex"})])
        .unwrap();
    let n = |sql: &str| engine.parquet_scans(sql);
    assert_eq!(n("SELECT who FROM t").unwrap(), 1);
    assert_eq!(
        n("SELECT count(*) FROM t a JOIN t b ON a.who = b.who").unwrap(),
        2
    );
    assert_eq!(n("SELECT name FROM labels").unwrap(), 0);
    assert_eq!(
        n("SELECT who FROM t ORDER BY block_number LIMIT 1").unwrap(),
        1
    );
    assert_eq!(
        n("SELECT who, name FROM t JOIN labels USING (who)").unwrap(),
        1
    );
    assert!(n("SELECT count(*) FROM t a JOIN t b ON a.block_number < b.block_number").is_err());
    assert!(
        n("WITH RECURSIVE r AS (SELECT 1 AS k UNION ALL SELECT k + 1 FROM r WHERE k < 3) SELECT * FROM r")
            .is_err()
    );
}

#[test]
fn host_tables_hold_results_roll_back_and_round_trip_through_parquet() {
    let tmp = tempfile::tempdir().unwrap();
    let seg = segment(
        tmp.path(),
        1,
        &[(1, "x", "1"), (2, "y", "2"), (3, "x", "3")],
    );
    let mut e = Engine::open_empty().unwrap();
    e.register_facts("t", &declared(), vec![seg], &[], (None, None))
        .unwrap();
    let count = |e: &Engine, rel: &str| {
        rows(e, &format!("SELECT count(*) AS n FROM {rel}"))[0]["n"].clone()
    };

    assert_eq!(
        e.create_table_as("f", "SELECT who, count(*) AS n FROM t GROUP BY who")
            .unwrap(),
        2
    );
    e.begin().unwrap();
    e.create_table_as("f", "SELECT * FROM f WHERE false")
        .unwrap();
    e.create_table_as("g", "SELECT 1 AS k").unwrap();
    assert_eq!(count(&e, "f"), json!(0));
    e.rollback().unwrap();
    assert_eq!(count(&e, "f"), json!(2));
    assert!(e.sql("SELECT * FROM g").is_err());
    e.begin().unwrap();
    e.create_table_as("f", "SELECT who, n + 1 AS n FROM f")
        .unwrap();
    e.commit().unwrap();
    assert!(e.commit().is_err());
    assert_eq!(rows(&e, "SELECT sum(n) AS s FROM f")[0]["s"], json!("5"));

    let path = tmp.path().join("ck.parquet");
    assert_eq!(
        e.write_parquet("SELECT * FROM f ORDER BY ALL", &path)
            .unwrap(),
        2
    );
    assert_eq!(e.load_parquet("ck", &path).unwrap(), 2);
    assert_eq!(
        rows(&e, "SELECT * FROM ck ORDER BY who"),
        rows(&e, "SELECT * FROM f ORDER BY who")
    );
    assert!(e.drop_relation("ck"));
    assert!(!e.drop_relation("ck"));

    assert_eq!(
        e.describe(
            "SELECT CAST(NULL AS UBIGINT) AS a, CAST(NULL AS INT) AS b, CAST(NULL AS VARCHAR) AS c, \
             CAST(NULL AS DECIMAL(20,2)) AS d, CAST(NULL AS BOOLEAN) AS e, CAST(NULL AS BIGINT) AS f \
             WHERE false"
        )
        .unwrap()
        .into_iter()
        .map(|(_, t)| t)
        .collect::<Vec<_>>(),
        vec!["UBIGINT", "INTEGER", "VARCHAR", "DECIMAL(20,2)", "BOOLEAN", "BIGINT"]
    );

    let ipc = e.sql_ipc("SELECT * FROM f ORDER BY who").unwrap();
    let back: Vec<_> = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(ipc), None)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(back.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

#[test]
fn a_repeated_name_in_any_branch_of_a_union_is_answered_as_duckdb_does() {
    let tmp = tempfile::tempdir().unwrap();
    let seg = segment(tmp.path(), 1, &[(1, "x", "5"), (2, "y", "7")]);
    let mut engine = Engine::open_empty().unwrap();
    engine
        .register_facts("t", &declared(), vec![seg], &[], (None, None))
        .unwrap();
    let names = |sql: &str| -> Vec<String> {
        engine.sql(sql).unwrap()[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    };
    assert_eq!(
        names(
            "SELECT who, block_number, block_number FROM t UNION ALL SELECT who, block_number, block_number FROM t"
        ),
        vec!["who", "block_number", "block_number"]
    );
    assert_eq!(
        names("SELECT who AS a, amount AS b FROM t UNION ALL SELECT who, who FROM t"),
        vec!["a", "b"]
    );
    assert_eq!(
        rows(
            &engine,
            "SELECT count(*) AS n, count(DISTINCT b) AS d FROM \
             (SELECT who AS a, amount AS b FROM t UNION ALL SELECT who, who FROM t) s"
        ),
        vec![json!({"n": 4, "d": 4})]
    );
}
