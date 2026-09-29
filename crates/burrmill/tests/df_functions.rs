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
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
        "x",
        arrow::datatypes::DataType::Utf8,
        true,
    )]));
    let b = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![std::sync::Arc::new(arrow::array::StringArray::from(vec!["a"]))],
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
    let hits: Vec<&String> =
        names.iter().filter(|n| suspicious.iter().any(|s| n.contains(s))).collect();
    assert!(hits.is_empty(), "functions to examine: {hits:?}");
    for sql in ["SELECT input_file_name() FROM t", "SELECT file_row_index() FROM t"] {
        let err = e.sql(sql).unwrap_err().to_string();
        assert!(!err.contains(segs.to_str().unwrap()), "leaks the path: {err}");
    }
}



/// `error(text)` as DuckDB has it: the statement fails with the text wherever a row reaches it, and
/// a `CASE` that does not take its branch never does.
#[test]
fn error_raises_only_where_it_is_reached() {
    let engine = Engine::open_empty().unwrap();
    let err = engine.sql("SELECT error('the tripwire') AS e").map(|_| ()).unwrap_err().to_string();
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
