//! A hash join builds from its input gathered and compacted, not from 128-row batches.
//!
//! Under a budget each batch's text columns kept a buffer of their own, and every batch the join put
//! out referenced all of its build side's, which DataFusion walks per batch for its `output_bytes`
//! metric: most of BetSwirl's `bets` statement (nuthatch #1951).

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use burrmill::Engine;

const ROWS: u64 = 20_000;

fn engine() -> (tempfile::TempDir, Engine) {
    let tmp = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("who", DataType::Utf8, false),
        Field::new("memo", DataType::Utf8, false),
    ]));
    let who: Vec<String> = (0..ROWS).map(|i| format!("0x{i:040x}")).collect();
    let memo: Vec<String> = (0..ROWS).map(|i| format!("memo number {i:020}")).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from((0..ROWS).collect::<Vec<_>>())),
            Arc::new(StringArray::from(who)),
            Arc::new(StringArray::from(memo)),
        ],
    )
    .unwrap();
    let path: PathBuf = tmp.path().join(format!("t-{:064x}.parquet", 1));
    let f = std::fs::File::create(&path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let len = std::fs::metadata(&path).unwrap().len();
    let mut e = Engine::open_empty_budgeted(burrmill::Budget {
        memory_bytes: 1 << 30,
        threads: 2,
        spill: None,
    })
    .unwrap();
    let declared = vec![
        ("block_number".into(), "u64".into()),
        ("who".into(), "address".into()),
        ("memo".into(), "string".into()),
    ];
    e.register_facts("t", &declared, vec![(path, len)], &[], (None, None))
        .unwrap();
    (tmp, e)
}

fn plan(e: &Engine, sql: &str) -> Vec<String> {
    e.sql(&format!("EXPLAIN {sql}"))
        .unwrap()
        .iter()
        .flat_map(|b| {
            let c = arrow::compute::cast(b.column(1), &DataType::Utf8).unwrap();
            let s = c.as_any().downcast_ref::<StringArray>().unwrap();
            (0..s.len())
                .map(|i| s.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .flat_map(|s| s.lines().map(str::to_string).collect::<Vec<_>>())
        .collect()
}

#[test]
fn a_hash_join_builds_from_its_input_gathered_and_compacted() {
    let (_t, e) = engine();
    let sql = "SELECT count(*) AS n, max(a.memo) AS m FROM t a JOIN t b ON a.who = b.who";
    let n = e.sql(sql).unwrap();
    let rows = n[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(rows, ROWS as i64);
    let lines = plan(&e, sql);
    let depth = |l: &str| l.len() - l.trim_start().len();
    let joins: Vec<usize> = (0..lines.len())
        .filter(|&i| lines[i].contains("HashJoinExec"))
        .collect();
    assert!(!joins.is_empty(), "{}", lines.join("\n"));
    for i in joins {
        // The build side is the join's first child: the next line one level deeper.
        let child = &lines[i + 1];
        assert!(depth(child) > depth(&lines[i]), "{}", lines.join("\n"));
        assert!(
            child.contains("GatherExec: compacted"),
            "build side not gathered:\n{}",
            lines.join("\n")
        );
    }
}
