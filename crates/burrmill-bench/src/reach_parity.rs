//! `reach-parity` (roadmap 6.7): `burrmill::inspect::reach` against nuthatch's own walk over
//! DuckDB's `json_serialize_sql`, statement by statement.
//!
//! nuthatch's side is `reject_unknown_table_refs` and `walk_table_refs` from `analytics.rs` at
//! 5513498, copied. A statement DuckDB cannot serialise is `open` on nuthatch's side (it falls
//! through to other guards); Burrmill refusing it is stricter, and reported as such.

use std::collections::BTreeSet;

use serde_json::Value;

const ALLOWED_TABLE_FNS: &[&str] = &["generate_series", "range", "unnest"];

/// nuthatch `walk_table_refs`, verbatim.
fn walk_table_refs(v: &Value, f: &mut impl FnMut(&str, &str)) {
    match v {
        Value::Object(map) => {
            if let Some(Value::String(t)) = map.get("type") {
                if t == "TABLE_FUNCTION" {
                    if let Some(name) = map
                        .get("function")
                        .and_then(|fun| fun.get("function_name"))
                        .and_then(Value::as_str)
                    {
                        f("TABLE_FUNCTION", name);
                    }
                } else if t == "BASE_TABLE" {
                    if let Some(name) = map.get("table_name").and_then(Value::as_str) {
                        f("BASE_TABLE", name);
                        if let Some(schema) = map.get("schema_name").and_then(Value::as_str) {
                            if !schema.is_empty() && schema != "main" {
                                f("QUALIFIED_SCHEMA", schema);
                            }
                        }
                    }
                }
            }
            for child in map.values() {
                walk_table_refs(child, f);
            }
        }
        Value::Array(items) => {
            for child in items {
                walk_table_refs(child, f);
            }
        }
        _ => {}
    }
}

#[derive(Debug, PartialEq)]
enum Verdict {
    /// DuckDB could not serialise it; nuthatch's walk does not decide.
    Open,
    Refused,
    Reach(BTreeSet<String>, bool),
}

/// nuthatch `reject_unknown_table_refs`, its decisions only.
fn nuthatch(conn: &duckdb::Connection, sql: &str) -> Verdict {
    let literal = format!("'{}'", sql.replace('\'', "''"));
    let Ok(ast) = conn.query_row(&format!("SELECT json_serialize_sql({literal})"), [], |r| {
        r.get::<_, String>(0)
    }) else {
        return Verdict::Open;
    };
    let Ok(v) = serde_json::from_str::<Value>(&ast) else {
        return Verdict::Open;
    };
    if v.get("error").and_then(Value::as_bool) == Some(true) {
        return Verdict::Open;
    }
    let mut referenced = BTreeSet::new();
    let mut surveys = false;
    let mut bad = false;
    walk_table_refs(&v, &mut |kind, name| {
        if bad {
            return;
        }
        match kind {
            "TABLE_FUNCTION" => {
                let f = name.to_ascii_lowercase();
                if !ALLOWED_TABLE_FNS.contains(&f.as_str()) {
                    bad = true;
                }
                if f.starts_with("duckdb_") {
                    surveys = true;
                }
            }
            "QUALIFIED_SCHEMA" => surveys = true,
            "BASE_TABLE"
                if name.is_empty()
                    || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') =>
            {
                bad = true
            }
            "BASE_TABLE" => {
                referenced.insert(name.to_ascii_lowercase());
            }
            _ => {}
        }
    });
    if bad {
        Verdict::Refused
    } else {
        Verdict::Reach(referenced, surveys)
    }
}

fn burrmill(sql: &str) -> Verdict {
    match burrmill::inspect::reach(sql) {
        Ok(r) => Verdict::Reach(r.tables, r.surveys),
        Err(_) => Verdict::Refused,
    }
}

const CORPUS: &[&str] = &[
    "SELECT count(*) FROM transfer",
    "SELECT * FROM Transfer t JOIN label l ON l.addr = t.\"to\"",
    "WITH c AS (SELECT * FROM transfer) SELECT * FROM c",
    "WITH c AS (SELECT * FROM transfer), d AS (SELECT * FROM c) SELECT * FROM d JOIN mint USING (addr)",
    "SELECT * FROM transfer WHERE x IN (SELECT y FROM burn) AND EXISTS (SELECT 1 FROM mint)",
    "SELECT (SELECT max(v) FROM mint) AS m FROM transfer",
    "SELECT * FROM transfer UNION ALL SELECT * FROM mint EXCEPT SELECT * FROM burn",
    "SELECT * FROM (SELECT * FROM (SELECT * FROM deep) a) b",
    "SELECT * FROM range(10)",
    "SELECT * FROM generate_series(1, 3) g(x)",
    "SELECT * FROM unnest([1, 2]) u(x)",
    "SELECT * FROM transfer, unnest([1,2]) u(x)",
    "SELECT * FROM read_csv('/etc/passwd')",
    "SELECT * FROM \"read_csv\"('/etc/passwd')",
    "SELECT * FROM READ_CSV_AUTO('/etc/passwd')",
    "SELECT * FROM read_parquet(['a.parquet'])",
    "SELECT * FROM glob('/*')",
    "SELECT * FROM duckdb_tables()",
    "SELECT * FROM duckdb_settings()",
    "SELECT * FROM '/etc/passwd'",
    "SELECT * FROM 'data.parquet'",
    "SELECT * FROM \"weird-name\"",
    "SELECT * FROM \"/etc/passwd\"",
    "SELECT table_name FROM information_schema.tables",
    "SELECT * FROM main.transfer",
    "SELECT * FROM memory.main.transfer",
    "SELECT * FROM pg_catalog.pg_class",
    "SELECT 'FROM read_csv(x)' AS s FROM transfer",
    "SELECT 1",
    "SELECT * FROM transfer -- FROM read_csv('/x')",
    "SELECT * FROM transfer /* FROM '/etc/passwd' */",
    "SELECT * FROM transfer t1, transfer t2",
    "SELECT * FROM transfer ASOF JOIN label l ON t.a = l.a AND t.k >= l.k",
    "SELECT * FROM transfer POSITIONAL JOIN mint",
    "FROM transfer SELECT *",
    "SELECT * FROM transfer QUALIFY row_number() OVER () = 1",
    "SELECT 1; SELECT 2",
    "COPY (SELECT 1) TO '/tmp/x'",
    "ATTACH '/tmp/x.db'",
    "INSTALL httpfs",
    "PIVOT transfer ON addr USING sum(v)",
    "SELECT * FROM (VALUES (1), (2)) v(x)",
    "SELECT * FROM transfer AS OF 1",
    "SELECT * FROM transfer TABLESAMPLE 10%",
    "SELECT * FROM lateral_fn() l",
    "SELECT * FROM LATERAL (SELECT * FROM mint) m",
];

pub fn run() -> anyhow::Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    let (mut same, mut stricter, mut looser) = (0, 0, 0);
    for sql in CORPUS {
        let (n, b) = (nuthatch(&conn, sql), burrmill(sql));
        let tag = match (&n, &b) {
            _ if n == b => {
                same += 1;
                "SAME    "
            }
            (Verdict::Open, Verdict::Refused) => {
                stricter += 1;
                "STRICTER"
            }
            (Verdict::Reach(..), Verdict::Refused) => {
                stricter += 1;
                "STRICTER"
            }
            _ => {
                looser += 1;
                "DIFF    "
            }
        };
        println!("{tag} {sql}");
        if n != b {
            println!("         nuthatch {n:?}\n         burrmill {b:?}");
        }
    }
    println!(
        "REACH\tstatements={}\tsame={same}\tstricter={stricter}\tdiffering={looser}",
        CORPUS.len()
    );
    anyhow::ensure!(
        looser == 0,
        "{looser} statements where Burrmill is not at least as strict"
    );
    Ok(())
}

/// nuthatch `walk_base_table_refs` (analytics.rs at 711ae88), verbatim.
fn walk_base_table_refs(
    value: &Value,
    outer: &std::collections::BTreeSet<String>,
    out: &mut std::collections::BTreeSet<String>,
) {
    match value {
        Value::Object(map) => {
            let mut scope = outer.clone();
            let ctes = map
                .get("cte_map")
                .and_then(|v| v.get("map"))
                .and_then(Value::as_array);
            if let Some(ctes) = ctes {
                // Definitions see earlier siblings, not later ones. A nonrecursive definition may
                // read a physical table with its own name; only bind its name after visiting it.
                for cte in ctes {
                    let name = cte
                        .get("key")
                        .and_then(Value::as_str)
                        .map(str::to_ascii_lowercase);
                    let recursive = cte
                        .pointer("/value/query/node/type")
                        .and_then(Value::as_str)
                        == Some("RECURSIVE_CTE_NODE");
                    if recursive {
                        if let Some(name) = &name {
                            scope.insert(name.clone());
                        }
                    }
                    walk_base_table_refs(cte, &scope, out);
                    if let Some(name) = name {
                        scope.insert(name);
                    }
                }
            }
            if map.get("type").and_then(Value::as_str) == Some("BASE_TABLE") {
                if let Some(name) = map.get("table_name").and_then(Value::as_str) {
                    let qualified = ["schema_name", "catalog_name"].iter().any(|key| {
                        map.get(*key)
                            .and_then(Value::as_str)
                            .is_some_and(|s| !s.is_empty())
                    });
                    let name = name.to_ascii_lowercase();
                    if qualified || !scope.contains(&name) {
                        out.insert(name);
                    }
                }
            }
            for (key, child) in map {
                if key != "cte_map" || ctes.is_none() {
                    walk_base_table_refs(child, &scope, out);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                walk_base_table_refs(value, outer, out);
            }
        }
        _ => {}
    }
}

/// DuckDB's `BASE_TABLE` and `TABLE_FUNCTION` names, lowercased, as nuthatch's `table_refs_in` reads them.
fn duck_refs(conn: &duckdb::Connection, sql: &str) -> Option<(BTreeSet<String>, BTreeSet<String>)> {
    let literal = format!("'{}'", sql.replace('\'', "''"));
    let ast: String = conn
        .query_row(&format!("SELECT json_serialize_sql({literal})"), [], |r| {
            r.get(0)
        })
        .ok()?;
    let v = serde_json::from_str::<Value>(&ast).ok()?;
    if v.get("error").and_then(Value::as_bool) == Some(true) {
        return None;
    }
    let (mut tables, mut functions) = (BTreeSet::new(), BTreeSet::new());
    walk_base_table_refs(&v, &Default::default(), &mut tables);
    walk_table_refs(&v, &mut |kind, name| {
        if kind == "TABLE_FUNCTION" {
            functions.insert(name.to_ascii_lowercase());
        }
    });
    Some((tables, functions))
}

/// Where CTE scope decides what is a table.
const SCOPES: &[&str] = &[
    "WITH c AS (SELECT * FROM t) SELECT * FROM c",
    "WITH t AS (SELECT * FROM t) SELECT * FROM t",
    "WITH a AS (SELECT * FROM b), b AS (SELECT * FROM x) SELECT * FROM a",
    "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT * FROM r",
    "WITH c AS (SELECT 1) SELECT * FROM main.c, (SELECT * FROM c) s",
    "SELECT * FROM (WITH c AS (SELECT 1) SELECT * FROM c) s, c",
    "WITH c AS (SELECT 1) SELECT * FROM c WHERE EXISTS (WITH d AS (SELECT * FROM c) SELECT * FROM d, e)",
    "SELECT * FROM t WHERE x IN (WITH t AS (SELECT 1 AS x) SELECT x FROM t)",
];

/// `refs-parity [nest]`: `burrmill::inspect::refs` against DuckDB's table and table-function nodes,
/// over the reach corpus and, given a nest, every authored view body.
pub fn run_refs(nest: Option<&str>) -> anyhow::Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    let mut statements: Vec<(String, String)> = CORPUS
        .iter()
        .chain(SCOPES)
        .map(|s| ("corpus".to_string(), s.to_string()))
        .collect();
    if let Some(root) = nest {
        let n = crate::df_views::load_nest(std::path::Path::new(root))?;
        statements.extend(n.views.iter().map(|v| (v.name.clone(), v.body.clone())));
    }
    let (mut same, mut differ) = (0, 0);
    for (from, sql) in &statements {
        let d = duck_refs(&conn, sql);
        let b = burrmill::inspect::refs(sql)
            .and_then(|r| Some((burrmill::inspect::base_tables(sql)?, r.functions)));
        if d == b {
            same += 1;
        } else {
            differ += 1;
            println!(
                "DIFF {from}: {}\n     duckdb   {d:?}\n     burrmill {b:?}",
                sql.chars().take(120).collect::<String>()
            );
        }
    }
    println!(
        "REFS\tstatements={}\tsame={same}\tdiffering={differ}",
        statements.len()
    );
    Ok(())
}
