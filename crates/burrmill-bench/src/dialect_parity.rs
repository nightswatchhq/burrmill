//! `dialect-parity` (roadmap 6.5): the same SQL through DuckDB and `burrmill::Engine`, each result
//! encoded as nuthatch encodes it, compared byte for byte.
//!
//! DuckDB reads the fixture through views built as nuthatch builds them (`_dec` by `TRY_CAST`);
//! Burrmill opens the same directory as a nest. A difference is either the dialect layer's to close
//! or listed in `KNOWN` with the reason it stands.

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::Value;

const CORPUS: &[&str] = &[
    "SELECT count(*) FROM transfer",
    "SELECT sum(value_dec) FROM transfer",
    "SELECT sum(block_number) FROM transfer",
    "SELECT avg(block_number) FROM transfer",
    "SELECT block_number / 100 AS q FROM transfer ORDER BY 1",
    "SELECT block_number // 100 AS b, count(*) AS n FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT CAST(value AS HUGEINT) * 2 AS v FROM transfer ORDER BY 1",
    "SELECT -CAST(value AS HUGEINT) AS v FROM transfer ORDER BY 1",
    "SELECT \"from\", sum(CAST(value AS HUGEINT)) AS s FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT 7 / 2 AS a, 7 // 2 AS b, -7 // 2 AS c, 7 % -2 AS d, 1 / 0 AS e, 1 // 0 AS f, 0.5 / 2 AS g",
    "SELECT to_timestamp(block_timestamp) AS t FROM transfer ORDER BY 1 LIMIT 2",
    "SELECT date_trunc('day', to_timestamp(block_timestamp)) AS d, count(*) AS n FROM transfer GROUP BY 1 ORDER BY 1",
    "SELECT extract(year FROM to_timestamp(block_timestamp)) AS y FROM transfer LIMIT 1",
    "SELECT round(CAST(value AS DOUBLE) / 1e18, 4) AS r FROM transfer ORDER BY 1",
    "SELECT lower(\"to\") AS t FROM transfer ORDER BY 1",
    "SELECT count(*) AS n FROM transfer WHERE enabled = 'true'",
    "SELECT count(*) AS n FROM transfer WHERE value = 10",
    "SELECT count(*) AS n FROM transfer WHERE value IN (10, 4)",
    "SELECT count(*) AS n FROM transfer WHERE \"tokensRewards\" = 10",
    "SELECT count(*) AS n FROM transfer WHERE \"tokensRewards\" IN (5, 7, 10)",
    "SELECT count(*) AS n FROM transfer WHERE \"tokensRewards\" > 9",
    "SELECT count(*) AS n FROM transfer WHERE \"tokensRewards\" BETWEEN 1 AND 9",
    "SELECT count(*) AS n FROM transfer WHERE 9 < \"tokensRewards\"",
    "SELECT count(*) AS n FROM transfer WHERE CAST(\"tokensRewards\" AS INTEGER) > 9",
    "SELECT count(*) AS n FROM transfer WHERE enabled = true",
    "SELECT count(*) AS n FROM transfer WHERE enabled AND true",
    "SELECT count(*) AS n FROM transfer WHERE NOT enabled",
    "SELECT \"tokensRewards\" <> 5 AS ne FROM transfer ORDER BY 1",
    "SELECT CAST(value AS HUGEINT), block_number // 2, NOT (block_number > 2) FROM transfer ORDER BY 2",
    "SELECT \"Value\", VALUE, \"TOKENSREWARDS\", t.\"FROM\", \"Value\" || 'x', sum(Block_Number) OVER () FROM Transfer t ORDER BY 1",
    "SELECT count(*) FROM TRANSFER T WHERE t.ENABLED = 'true'",
    "SELECT table_name FROM information_schema.tables WHERE NOT starts_with(table_name, '__hot_') ORDER BY table_name",
    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'transfer' ORDER BY ordinal_position",
    "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables ORDER BY 3",
    "SELECT column_name, ordinal_position, is_nullable FROM information_schema.columns WHERE table_name = 'label' ORDER BY 2",
    "SELECT *, value_dec, \"from\" FROM transfer ORDER BY block_number, log_index",
    "SELECT 1 AS a, 2 AS a, block_number AS a FROM transfer ORDER BY 3",
    "SELECT block_number - 1 AS a, COALESCE(block_number - log_index, 0) AS b, CASE WHEN log_index = 0 THEN 0 ELSE block_number END AS c, block_number * 2 + 1 AS d FROM transfer ORDER BY 1",
    "SELECT t.\"from\", CAST(t.block_number / 10 AS BIGINT) AS block_number FROM transfer t ORDER BY t.block_number DESC",
    "SELECT CAST(0.5::DOUBLE AS BIGINT) a, CAST(1.5::DOUBLE AS BIGINT) b, CAST(2.5::DOUBLE AS BIGINT) c, CAST(-0.5::DOUBLE AS BIGINT) d, CAST(-1.5::DOUBLE AS BIGINT) e, CAST(0.6::DOUBLE AS BIGINT) f, CAST(2.5::DECIMAL(3,1) AS BIGINT) l, CAST(-2.5::DECIMAL(3,1) AS BIGINT) m, CAST(2.4::DECIMAL(3,1) AS BIGINT) n",
    "SELECT CAST(CAST(value AS DOUBLE) / 3 AS BIGINT) AS q, CAST(block_number / 4 AS INTEGER) AS r FROM transfer WHERE block_number < 100 ORDER BY 1, 2",
    "SELECT k FROM (SELECT block_number * 100000 + log_index AS k FROM transfer UNION ALL SELECT block_number * 100000 + log_index FROM transfer) ORDER BY 1 LIMIT 3",
    "SELECT CAST('0x1F' AS BIGINT) a, CAST(' 0x1f' AS BIGINT) b, CAST('0b101' AS BIGINT) c, CAST('0x1_f' AS INTEGER) d, CAST('0x7fffffffffffffff' AS BIGINT) e, CAST('42' AS BIGINT) f",
    "SELECT CAST('0xffffffffffffffff' AS BIGINT)",
    "SELECT CAST('-0x1f' AS BIGINT)",
    "SELECT decode(from_hex('68c3a96c6c6f')) a, decode(from_hex('')) b, from_hex('abc') c, from_hex('4142') d",
    "SELECT decode(from_hex('ff'))",
    "SELECT from_hex('zz')",
    "SELECT CAST(('0x' || substr('000000000000000000000000000000000000000000000000000000000000002a', 49, 16)) AS BIGINT) AS n",
    "SELECT max(\"from\") AS m FROM transfer",
    "SELECT (block_number, log_index) > (2, 0) AS gt FROM transfer ORDER BY block_number, log_index",
    "SELECT CAST(block_number AS UBIGINT) AS b FROM transfer ORDER BY 1 LIMIT 1",
    "SELECT \"tokensRewards\" AS r FROM transfer ORDER BY 1",
    "SELECT t.\"to\", l.name FROM transfer t LEFT JOIN label l ON l.addr = t.\"to\" ORDER BY 1, 2",
    "SELECT count(DISTINCT \"from\") AS n FROM transfer",
    "SELECT \"from\" || ':' || CAST(block_number AS VARCHAR) AS k FROM transfer ORDER BY 1",
    "SELECT 1 = true AS a, 2 = true AS b, 0 = false AS c, 1 < true AS d, 2 > false AS e, 1.5 = true AS f",
    "SELECT block_number FROM transfer WHERE (block_number = 1) = true OR log_index = true ORDER BY 1",
    "SELECT true IN (1, 2) AS a, 1 IN (true, false) AS b, 2 IN (true) AS c",
    "SELECT 1 IS DISTINCT FROM 2 AND 3 IS DISTINCT FROM 3 OR 4 IS NOT DISTINCT FROM 4 AS v",
    "SELECT block_number FROM transfer WHERE \"from\" IS DISTINCT FROM '0xa' AND log_index = 0 OR \"to\" IS NOT DISTINCT FROM '0xe' ORDER BY 1",
    "SELECT CAST(2.5::DOUBLE AS HUGEINT) AS a, CAST(-3.5::DOUBLE AS HUGEINT) AS b, CAST(2.5::DOUBLE AS DECIMAL(38,0)) AS c",
    "SELECT CAST(CAST('47582028310819253533' AS HUGEINT) AS DOUBLE) AS a, CAST(CAST('9791626625542365.709860864' AS DECIMAL(38,9)) AS DOUBLE) AS b",
    "SELECT CAST(value AS HUGEINT)::DOUBLE / 3 AS d FROM transfer ORDER BY 1",
    "SELECT 1.5 AS a, 1.5 + 1 AS b, 1.5 * 2.25 AS c, 0.5 AS d, -2.50 AS e, 100.0 / 3 AS f, 1e3 AS g, 1.5e2 AS h, 0.001 AS i",
    "SELECT 1.5 - 0.25 AS a, 10.5 % 3 AS b, block_number * 1.5 AS c, block_number + 0.25 AS d FROM transfer ORDER BY 1, 3",
    "SELECT round(2.345, 2) AS a, CAST(1.25 AS DOUBLE) AS b, 1.10 = 1.1 AS c, 12345678901234567890.5 AS d",
    "SELECT round(9.995, 2) AS a, round(-2.345, 2) AS b, round(2.345) AS c, round(2.345, 5) AS d, round(1234.5, -2) AS e, round(2.5) AS f, round(-2.5) AS g",
    "SELECT round(CAST(value AS HUGEINT) / 7, 3) AS r FROM transfer ORDER BY 1",
    "SELECT * FROM (SELECT block_number, block_number FROM transfer) ORDER BY 1",
    "SELECT count(*) AS n FROM (SELECT t.\"to\", l.addr AS \"to\" FROM transfer t JOIN label l ON l.addr = t.\"to\")",
    "SELECT * FROM (SELECT t.\"from\", l.* FROM transfer t JOIN label l ON l.addr = t.\"from\") ORDER BY 1, 2, 3",
    "WITH x AS (SELECT block_number, block_number + 0 FROM transfer) SELECT * FROM x ORDER BY 1",
    "SELECT * FROM (SELECT l.addr, l.* FROM label l) ORDER BY 1",
    "SELECT block_number FROM (SELECT block_number, block_number FROM transfer) ORDER BY 1",
    "SELECT * FROM (SELECT * FROM label a JOIN label b ON a.addr = b.addr) ORDER BY 1",
    "SELECT count(*) AS n FROM (SELECT * FROM transfer x JOIN transfer y ON x.block_number = y.block_number)",
    "WITH j AS (SELECT * FROM label a JOIN label b ON a.addr = b.addr) SELECT addr_1, name_1 FROM j ORDER BY 1",
    "SELECT block_number FROM transfer WHERE block_number > 1.5 AND block_number < 150.0 ORDER BY 1",
    "SELECT sum(block_number * 0.5) AS s, avg(block_number) + 0.5 AS a FROM transfer",
    "SELECT list_reduce([1, 2, 3], lambda a, x: a * 10 + x) AS a, list_reduce([1.5, 2.25], lambda a, x: a + x) AS b",
    "SELECT \"from\", list_reduce(list(block_number ORDER BY block_number, log_index), lambda a, x: a * 1000 + x) AS r FROM transfer GROUP BY 1 ORDER BY 1",
];

/// Differences that stand, and why.
const KNOWN: &[(&str, &str)] = &[];

fn fixture(root: &std::path::Path) -> anyhow::Result<()> {
    let segs = root.join("segments");
    std::fs::create_dir_all(&segs)?;
    let s = |v: &[&str]| Arc::new(StringArray::from(v.to_vec())) as ArrayRef;
    let u = |v: &[u64]| Arc::new(UInt64Array::from(v.to_vec())) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("log_index", DataType::UInt64, false),
        Field::new("block_timestamp", DataType::UInt64, false),
        Field::new("from", DataType::Utf8, true),
        Field::new("to", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("enabled", DataType::Utf8, true),
        Field::new("tokensRewards", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            u(&[1, 2, 2, 3, 150, 301]),
            u(&[0, 0, 1, 0, 2, 0]),
            u(&[1700000000, 1700000012, 1700000012, 1700086400, 1700172800, 1703980800]),
            s(&["0xa", "0xb", "0xA", "0xc", "0xa", "0xd"]),
            s(&["0xb", "0xc", "0xc", "0xa", "0xe", "0xa"]),
            s(&["10", "4", "1", "010", "250000000000000000000", "7"]),
            s(&["true", "false", "true", "true", "false", "true"]),
            s(&["5", "6", "7", "8", "9", "10"]),
        ],
    )?;
    let f = std::fs::File::create(segs.join(format!("transfer-{:064x}.parquet", 1)))?;
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None)?;
    w.write(&batch)?;
    w.close()?;
    let lschema = Arc::new(Schema::new(vec![
        Field::new("addr", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let labels = RecordBatch::try_new(lschema.clone(), vec![s(&["0xa", "0xc"]), s(&["alice", "carol"])])?;
    let f = std::fs::File::create(segs.join(format!("label-{:064x}.parquet", 2)))?;
    let mut w = parquet::arrow::ArrowWriter::try_new(f, lschema, None)?;
    w.write(&labels)?;
    w.close()?;
    std::fs::write(
        root.join("schema.json"),
        r#"{"tables":[{"table":"transfer","columns":[{"name":"from","storage":"text"},{"name":"to","storage":"text"},{"name":"value","storage":"word32"},{"name":"tokensRewards","storage":"word32"}]}]}"#,
    )?;
    Ok(())
}

pub fn run() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    fixture(tmp.path())?;
    let segs = tmp.path().join("segments");
    let duck = duckdb::Connection::open_in_memory()?;
    // nuthatch sets no zone, so DuckDB follows the host's; hosted servers run in UTC, and so does
    // Burrmill, deterministically.
    duck.execute_batch("SET TimeZone = 'UTC';")?;
    duck.execute_batch(&format!(
        "CREATE VIEW transfer AS SELECT *, TRY_CAST(\"value\" AS DECIMAL(38,0)) AS \"value_dec\", \
         (\"value\" IS NOT NULL AND TRY_CAST(\"value\" AS DECIMAL(38,0)) IS NULL) AS \"value_overflow\", \
         TRY_CAST(\"tokensRewards\" AS DECIMAL(38,0)) AS \"tokensRewards_dec\", \
         (\"tokensRewards\" IS NOT NULL AND TRY_CAST(\"tokensRewards\" AS DECIMAL(38,0)) IS NULL) AS \"tokensRewards_overflow\" \
         FROM read_parquet('{0}/transfer-*.parquet');
         CREATE VIEW label AS SELECT * FROM read_parquet('{0}/label-*.parquet');",
        segs.display()
    ))?;
    let root = tmp.path().to_path_buf();
    let engine = std::thread::spawn(move || burrmill::Engine::open_nest(&root)).join().expect("open")?;
    let engine = Arc::new(engine);
    let mut failed = 0;
    for sql in CORPUS {
        let want = match crate::encode_parity::nuthatch_rows(&duck, sql) {
            Ok(v) => serde_json::to_string(&v)?,
            Err(e) => format!("ERROR {}", e.to_string().lines().next().unwrap_or("")),
        };
        let e2 = Arc::clone(&engine);
        let q = sql.to_string();
        let got = std::thread::spawn(move || -> String {
            match e2.sql(&q) {
                Ok(bs) => {
                    let mut rows = Vec::new();
                    for b in &bs {
                        match burrmill::df::encode::rows(b) {
                            Ok(r) => rows.extend(r),
                            Err(e) => return format!("ERROR {e}"),
                        }
                    }
                    serde_json::to_string(&Value::Array(rows)).unwrap()
                }
                Err(e) => format!("ERROR {}", e.to_string().replace('\n', " | ")),
            }
        })
        .join()
        .expect("engine thread");
        let known = KNOWN.iter().find(|(k, _)| k == sql).map(|(_, why)| *why);
        let both_refuse = want.starts_with("ERROR") && got.starts_with("ERROR");
        let tag = match (want == got || both_refuse, known) {
            (true, _) if both_refuse => "BOTH-REFUSE".to_string(),
            (true, _) => "SAME ".to_string(),
            (false, Some(why)) => format!("KNOWN ({why})"),
            (false, None) => {
                failed += 1;
                "DIFF ".to_string()
            }
        };
        println!("{tag}  {sql}");
        if want != got && !both_refuse {
            println!("    duckdb   {}", want.chars().take(300).collect::<String>());
            println!("    burrmill {}", got.chars().take(300).collect::<String>());
        }
    }
    println!("DIALECT\tcases={}\tdiffering={failed}", CORPUS.len());
    std::thread::spawn(move || drop(engine)).join().expect("drop engine");
    anyhow::ensure!(failed == 0, "{failed} dialect differences");
    Ok(())
}

/// `duck-names <sql>`: DuckDB's own column names for a statement over an empty `t`, one per line.
pub fn duck_names(sql: &str) -> anyhow::Result<()> {
    let duck = duckdb::Connection::open_in_memory()?;
    duck.execute_batch(
        "CREATE TABLE t(\"from\" VARCHAR, \"to\" VARCHAR, \"value\" VARCHAR, block_number UBIGINT, \
         log_index UBIGINT, \"tokensRewards\" VARCHAR);",
    )?;
    let mut stmt = duck.prepare(sql)?;
    let rows = stmt.query([])?;
    for n in rows.as_ref().map(|s| s.column_names()).unwrap_or_default() {
        println!("{n}");
    }
    Ok(())
}

/// `duck-keywords`: DuckDB's keyword list with categories, for the naming printer's quoting rule.
pub fn duck_keywords() -> anyhow::Result<()> {
    let duck = duckdb::Connection::open_in_memory()?;
    let mut stmt = duck.prepare("SELECT keyword_name, keyword_category FROM duckdb_keywords() ORDER BY 1")?;
    let mut rows = stmt.query([])?;
    while let Some(r) = rows.next()? {
        let (k, c): (String, String) = (r.get(0)?, r.get(1)?);
        println!("{k}\t{c}");
    }
    Ok(())
}

/// `duck-eval <sql>`: DuckDB's answer as nuthatch's JSON, for probing semantics.
pub fn duck_eval(sql: &str) -> anyhow::Result<()> {
    let duck = duckdb::Connection::open_in_memory()?;
    println!("{}", serde_json::to_string(&crate::encode_parity::nuthatch_rows(&duck, sql)?)?);
    Ok(())
}
