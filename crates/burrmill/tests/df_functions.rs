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


