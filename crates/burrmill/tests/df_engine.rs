//! NestCatalog + lockdown, behind the `datafusion` feature (roadmap 6.1 / 6.2).

use std::sync::Arc;

use arrow::array::{Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::{BurrmillError, Engine};

fn write_table(dir: &std::path::Path, table: &str, rows: &[(&str, &str, &str)]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("from", DataType::Utf8, false),
        Field::new("to", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("_seq", DataType::UInt64, false),
    ]));
    let n = rows.len() as u64;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())),
            Arc::new(StringArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())),
            Arc::new(StringArray::from(rows.iter().map(|r| r.2).collect::<Vec<_>>())),
            Arc::new(UInt64Array::from((0..n).collect::<Vec<_>>())),
        ],
    )
    .unwrap();
    let hash = "bb04b072d5ecb39489f65ddbb5dac50d78c2ad8a407ebb721fdc6ae5c9f916bc0";
    let f = std::fs::File::create(dir.join(format!("{table}-{hash}.parquet"))).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn nest_with_transfer() -> (tempfile::TempDir, Engine) {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    write_table(
        &segs,
        "token__transfer",
        &[
            ("0xa", "0xb", "10"),
            ("0xb", "0xc", "4"),
            ("0xa", "0xc", "1"),
        ],
    );
    let engine = Engine::open_segments(&segs).unwrap();
    (tmp, engine)
}

#[test]
fn signed_fold_over_explicit_files() {
    let (_tmp, engine) = nest_with_transfer();
    assert_eq!(engine.tables(), vec!["token__transfer".to_string()]);
    let batches = engine
        .sql(
            r#"SELECT addr, SUM(d) AS net FROM (
                 SELECT "to" AS addr, CAST("value" AS DECIMAL(38,0)) AS d FROM token__transfer
                 UNION ALL
                 SELECT "from" AS addr, -CAST("value" AS DECIMAL(38,0)) AS d FROM token__transfer
               ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr"#,
        )
        .unwrap();
    let mut rows = Vec::new();
    for b in &batches {
        let k = arrow::compute::cast(b.column(0), &DataType::Utf8).unwrap();
        let k = k.as_any().downcast_ref::<StringArray>().unwrap();
        let v = arrow::compute::cast(b.column(1), &DataType::Utf8).unwrap();
        let v = v.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..k.len() {
            rows.push((k.value(i).to_string(), v.value(i).to_string()));
        }
    }
    assert_eq!(
        rows,
        vec![
            ("0xa".into(), "-11".into()),
            ("0xb".into(), "6".into()),
            ("0xc".into(), "5".into()),
        ]
    );
}

#[test]
fn declared_unsealed_table_is_empty_not_missing() {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    write_table(&segs, "token__transfer", &[("0xa", "0xb", "1")]);
    std::fs::write(
        tmp.path().join("schema.json"),
        r#"{"tables":[{"table":"token__approval","columns":[{"name":"owner","storage":"text"},{"name":"block_number","storage":"u64"}]}]}"#,
    )
    .unwrap();
    let engine = Engine::open_nest(tmp.path()).unwrap();
    assert!(engine.tables().iter().any(|t| t == "token__approval"));
    let batches = engine
        .sql(r#"SELECT count(*) FROM token__approval"#)
        .unwrap();
    let col = batches[0].column(0);
    let n = if let Some(a) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
        a.value(0)
    } else if let Some(a) = col.as_any().downcast_ref::<arrow::array::UInt64Array>() {
        a.value(0) as i64
    } else {
        panic!("count type: {:?}", batches[0].schema());
    };
    assert_eq!(n, 0);
}

#[test]
fn word_columns_get_dec_companions() {
    let tmp = tempfile::tempdir().unwrap();
    let segs = tmp.path().join("segments");
    std::fs::create_dir(&segs).unwrap();
    write_table(&segs, "token__transfer", &[("0xa", "0xb", "10")]);
    std::fs::write(
        tmp.path().join("schema.json"),
        r#"{"tables":[{"table":"token__transfer","columns":[
            {"name":"from","storage":"text"},
            {"name":"to","storage":"text"},
            {"name":"value","storage":"word16"}
        ]}]}"#,
    )
    .unwrap();
    let engine = Engine::open_nest(tmp.path()).unwrap();
    let batches = engine
        .sql(r#"SELECT "value_dec" FROM token__transfer"#)
        .unwrap();
    let v = arrow::compute::cast(batches[0].column(0), &DataType::Utf8).unwrap();
    let v = v.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(v.value(0), "10");
}

#[test]
fn unknown_table_is_refused() {
    let (_tmp, engine) = nest_with_transfer();
    let err = engine.sql(r#"SELECT 1 FROM nosuch"#).unwrap_err();
    match err {
        BurrmillError::NoSegments(m) | BurrmillError::Substrate(m) | BurrmillError::NotAllowed(m) => {
            assert!(m.contains("nosuch") || m.contains("no table"), "{m}");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn copy_is_refused() {
    let (_tmp, engine) = nest_with_transfer();
    let err = engine
        .sql("COPY (SELECT 1) TO '/tmp/out.csv'")
        .unwrap_err();
    match err {
        BurrmillError::NotAllowed(_) => {}
        other => panic!("expected NotAllowed, got {other:?}"),
    }
}

#[test]
fn create_external_table_is_refused() {
    let (_tmp, engine) = nest_with_transfer();
    let err = engine
        .sql("CREATE EXTERNAL TABLE pw STORED AS CSV LOCATION '/etc/passwd'")
        .unwrap_err();
    match err {
        BurrmillError::NotAllowed(_) => {}
        other => panic!("expected NotAllowed, got {other:?}"),
    }
}

#[test]
fn read_csv_table_function_is_refused() {
    let (_tmp, engine) = nest_with_transfer();
    let err = engine
        .sql("SELECT * FROM read_csv('/etc/passwd')")
        .unwrap_err();
    match err {
        BurrmillError::NotAllowed(_) | BurrmillError::Parse(_) | BurrmillError::Substrate(_) => {}
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[test]
fn replacement_scan_is_refused() {
    let (_tmp, engine) = nest_with_transfer();
    let err = engine.sql("SELECT * FROM '/etc/passwd'").unwrap_err();
    match err {
        BurrmillError::NotAllowed(_) | BurrmillError::NoSegments(_) | BurrmillError::Substrate(_) | BurrmillError::Parse(_) => {}
        other => panic!("expected a refusal, got {other:?}"),
    }
}

// nuthatch's `segment_vanished` keys on the segment path plus the OS's own wording.
#[test]
fn a_vanished_segment_names_its_path_and_the_os_error() {
    let (tmp, engine) = nest_with_transfer();
    let segs = tmp.path().join("segments");
    let file = std::fs::read_dir(&segs).unwrap().next().unwrap().unwrap().path();
    std::fs::remove_file(&file).unwrap();
    let err = engine.sql("SELECT count(*) FROM token__transfer").unwrap_err().to_string();
    assert!(err.contains("No such file or directory"), "{err}");
    assert!(err.contains(segs.to_str().unwrap()), "{err}");
}

#[test]
fn queries_opening_with_a_parenthesis_or_a_comment_are_queries() {
    let (_tmp, engine) = nest_with_transfer();
    for sql in [
        "(SELECT 1 AS a) UNION ALL (SELECT 2)",
        "-- a note\nSELECT count(*) FROM token__transfer",
        "/* note */ SELECT 1",
        "  (\n (SELECT 1))",
    ] {
        engine.sql(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert!(engine.sql("/* x */ COPY (SELECT 1) TO '/tmp/y'").is_err());
}

#[test]
fn doubles_round_to_hugeint_half_to_even_and_to_decimal_half_away() {
    let (_tmp, engine) = nest_with_transfer();
    // DuckDB 1.5's answers.
    let sql = "SELECT CAST(2.5::DOUBLE AS HUGEINT) AS a, CAST(3.5::DOUBLE AS HUGEINT) AS b, \
               CAST(-2.5::DOUBLE AS HUGEINT) AS c, CAST(2.5::DOUBLE AS DECIMAL(38,0)) AS d, \
               TRY_CAST(2106471783529098.5::DOUBLE AS HUGEINT) AS e, CAST('7' AS HUGEINT) AS f, \
               CAST(CAST('47582028310819253533' AS HUGEINT) AS DOUBLE) AS g";
    let rows = burrmill::df::encode::rows(&engine.sql(sql).unwrap()[0]).unwrap();
    assert_eq!(
        serde_json::to_string(&rows).unwrap(),
        r#"[{"a":"2","b":"4","c":"-2","d":"3","e":"2106471783529098","f":"7","g":4.758202831081926e+19}]"#
    );
}

#[test]
fn is_distinct_from_binds_tighter_than_and_or() {
    let (_tmp, engine) = nest_with_transfer();
    // sqlparser alone reads this as `1 IS DISTINCT FROM (2 AND 3 IS DISTINCT FROM (3 OR ...))`.
    let sql = "SELECT 1 IS DISTINCT FROM 2 AND 3 IS DISTINCT FROM 3 OR 4 IS NOT DISTINCT FROM 4 AS v";
    let rows = burrmill::df::encode::rows(&engine.sql(sql).unwrap()[0]).unwrap();
    assert_eq!(serde_json::to_string(&rows).unwrap(), r#"[{"v":true}]"#);
}

#[test]
fn substring_plans_without_the_umbrella_crate() {
    let (_tmp, engine) = nest_with_transfer();
    let sql = "SELECT substr('abcdef', 2, 3) AS a, SUBSTRING('abcdef' FROM 2 FOR 3) AS b";
    let rows = burrmill::df::encode::rows(&engine.sql(sql).unwrap()[0]).unwrap();
    assert_eq!(serde_json::to_string(&rows).unwrap(), r#"[{"a":"bcd","b":"bcd"}]"#);
}

#[test]
fn a_subquery_repeating_a_name_keeps_both_columns_as_duckdb_names_them() {
    let (_tmp, engine) = nest_with_transfer();
    // Unaliased, DataFusion passed both `from` columns through under one name and the encoder
    // kept one; DuckDB calls the second `from_1`.
    let sql = "SELECT * FROM (SELECT * FROM token__transfer a JOIN token__transfer b ON a.\"to\" = b.\"to\" \
               WHERE a._seq = 0)";
    let rows = burrmill::df::encode::rows(&engine.sql(sql).unwrap()[0]).unwrap();
    assert_eq!(
        serde_json::to_string(&rows).unwrap(),
        r#"[{"from":"0xa","to":"0xb","value":"10","_seq":0,"from_1":"0xa","to_1":"0xb","value_1":"10","_seq_1":0}]"#
    );
    let sql = "WITH j AS (SELECT \"from\", \"from\", value || 'x' FROM token__transfer) \
               SELECT from_1, \"(\"\"value\"\" || 'x')\" FROM j ORDER BY 1, 2";
    let rows = burrmill::df::encode::rows(&engine.sql(sql).unwrap()[0]).unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn a_small_table_is_not_fanned_out() {
    let (_tmp, engine) = nest_with_transfer();
    let sql = "SELECT \"from\", count(*) AS n FROM token__transfer GROUP BY 1 ORDER BY 1";
    let plan: String = engine
        .sql(&format!("EXPLAIN {sql}"))
        .unwrap()
        .iter()
        .map(|b| arrow::util::pretty::pretty_format_batches(std::slice::from_ref(b)).unwrap().to_string())
        .collect();
    assert!(!plan.contains("RoundRobinBatch"), "{plan}");
    let rows = burrmill::df::encode::rows(&engine.sql(sql).unwrap()[0]).unwrap();
    assert_eq!(serde_json::to_string(&rows).unwrap(), r#"[{"from":"0xa","n":2},{"from":"0xb","n":1}]"#);
}

/// A host function that refuses some input, as `nuthatch_abi_tuple` refuses a malformed payload.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Strict(datafusion_expr::Signature);

impl datafusion_expr::ScalarUDFImpl for Strict {
    fn name(&self) -> &str {
        "host_strict"
    }
    fn signature(&self) -> &datafusion_expr::Signature {
        &self.0
    }
    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> datafusion_common::Result<datafusion_expr::ColumnarValue> {
        let a = datafusion_expr::ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let a = a.as_any().downcast_ref::<StringArray>().unwrap();
        let mut out = Vec::new();
        for i in 0..a.len() {
            if a.value(i).starts_with("0xb") {
                return datafusion_common::exec_err!("host_strict refuses {}", a.value(i));
            }
            out.push(format!("ok:{}", a.value(i)));
        }
        Ok(datafusion_expr::ColumnarValue::Array(Arc::new(StringArray::from(out))))
    }
}

#[test]
fn a_host_function_under_try_is_null_where_it_fails() {
    let (_tmp, mut engine) = nest_with_transfer();
    let sig = datafusion_expr::Signature::exact(vec![DataType::Utf8], datafusion_expr::Volatility::Immutable);
    engine.register_scalar_udf(Arc::new(datafusion_expr::ScalarUDF::from(Strict(sig))));
    assert!(engine.sql("SELECT host_strict(\"from\") FROM token__transfer").is_err());
    let sql = "SELECT \"from\", TRY(host_strict(\"from\")) AS v FROM token__transfer ORDER BY _seq";
    let rows = burrmill::df::encode::rows(&engine.sql(sql).unwrap()[0]).unwrap();
    assert_eq!(
        serde_json::to_string(&rows).unwrap(),
        r#"[{"from":"0xa","v":"ok:0xa"},{"from":"0xb","v":null},{"from":"0xa","v":"ok:0xa"}]"#
    );
}
