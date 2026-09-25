//! `fuzz`: generated SQL on DuckDB and on `Engine`, answers compared (the differential corpus owed
//! since slice 1).
//!
//! Queries are drawn from a typed grammar over two small tables whose data is chosen to be awkward:
//! mixed-case and NULL text, negative and missing numbers, integer strings past 64 bits and strings
//! that are not numbers, repeated join keys. Every query is deterministic: rows are compared as a
//! multiset unless the query orders by every column it returns, and nothing sums a DOUBLE.
//!
//! Outcomes: `same`, both refuse, Burrmill alone refuses (stricter, allowed), DuckDB alone refuses
//! (looser, reported), and different answers (reported with the seed that reproduces them:
//! `SEED=<n> CASES=1 PRINT=1 burrmill-bench fuzz`).

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::Value;

use crate::generate::Rng;

fn write(dir: &std::path::Path, name: &str, schema: Arc<Schema>, cols: Vec<ArrayRef>) -> anyhow::Result<()> {
    let batch = RecordBatch::try_new(schema.clone(), cols)?;
    let f = std::fs::File::create(dir.join(format!("{name}-{:064x}.parquet", 1)))?;
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None)?;
    w.write(&batch)?;
    w.close()?;
    Ok(())
}

fn fixture(dir: &std::path::Path) -> anyhow::Result<()> {
    let mut r = Rng(7);
    let addrs = [Some("0xa"), Some("0xA"), Some("0xb"), Some("0xc"), Some("0xd"), None];
    let values = [
        Some("10"), Some("4"), Some("0"), Some("-5"), Some("010"), Some("7"), Some("123456789"),
        Some("250000000000000000000"), Some("abc"), Some(""), None,
    ];
    let n = 120;
    let pick = |r: &mut Rng, xs: &[Option<&'static str>]| xs[r.below(xs.len())];
    let (mut bn, mut li, mut from, mut to, mut value, mut amount, mut flag, mut kind) =
        (vec![], vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
    for i in 0..n {
        bn.push(1 + (i / 3) as u64);
        li.push((i % 3) as u64);
        from.push(pick(&mut r, &addrs));
        to.push(pick(&mut r, &addrs));
        value.push(pick(&mut r, &values));
        amount.push(if r.below(8) == 0 { None } else { Some(r.below(200) as i64 - 60) });
        flag.push(pick(&mut r, &[Some("true"), Some("false"), None]));
        kind.push(pick(&mut r, &[Some("in"), Some("out")]));
    }
    let s = |v: Vec<Option<&str>>| Arc::new(StringArray::from(v)) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("log_index", DataType::UInt64, false),
        Field::new("from", DataType::Utf8, true),
        Field::new("to", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
        Field::new("flag", DataType::Utf8, true),
        Field::new("kind", DataType::Utf8, true),
    ]));
    write(
        dir,
        "ev",
        schema,
        vec![
            Arc::new(UInt64Array::from(bn)),
            Arc::new(UInt64Array::from(li)),
            s(from),
            s(to),
            s(value),
            Arc::new(Int64Array::from(amount)),
            s(flag),
            s(kind),
        ],
    )?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("addr", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("weight", DataType::Int64, true),
    ]));
    write(
        dir,
        "lbl",
        schema,
        vec![
            s(vec![Some("0xa"), Some("0xb"), Some("0xb"), Some("0xC"), None, Some("0xe")]),
            s(vec![Some("alice"), Some("bob"), Some("Bob"), None, Some("nobody"), Some("eve")]),
            Arc::new(Int64Array::from(vec![Some(3), Some(-1), None, Some(10), Some(0), Some(7)])),
        ],
    )?;
    Ok(())
}

/// What the current query's FROM clause offers.
struct Scope {
    joined: bool,
}

struct Gen<'a> {
    r: &'a mut Rng,
}

impl Gen<'_> {
    fn one<'s>(&mut self, xs: &[&'s str]) -> &'s str {
        xs[self.r.below(xs.len())]
    }
    fn chance(&mut self, n: usize) -> bool {
        self.r.below(n) == 0
    }

    fn int(&mut self, sc: &Scope, d: usize) -> String {
        let leaf = d == 0 || self.chance(3);
        if leaf {
            let mut cols = vec!["e.block_number", "e.log_index", "e.amount"];
            if sc.joined {
                cols.push("l.weight");
            }
            return match self.r.below(4) {
                0 => format!("{}", self.r.below(120) as i64 - 20),
                _ => self.one(&cols).to_string(),
            };
        }
        match self.r.below(12) {
            0 => format!("({} + {})", self.int(sc, d - 1), self.int(sc, d - 1)),
            1 => format!("({} - {})", self.int(sc, d - 1), self.int(sc, d - 1)),
            2 => format!("({} * {})", self.int(sc, d - 1), self.int(sc, d - 1)),
            3 => format!("({} // {})", self.int(sc, d - 1), 1 + self.r.below(7)),
            4 => format!("({} % {})", self.int(sc, d - 1), 1 + self.r.below(7)),
            5 => format!("abs({})", self.int(sc, d - 1)),
            6 => format!(
                "CASE WHEN {} THEN {} ELSE {} END",
                self.pred(sc, d - 1),
                self.int(sc, d - 1),
                self.int(sc, d - 1)
            ),
            7 => format!("COALESCE({}, {})", self.int(sc, d - 1), self.int(sc, d - 1)),
            8 => format!("TRY_CAST({} AS BIGINT)", self.text(sc, d - 1)),
            9 => format!("length({})", self.text(sc, d - 1)),
            10 => format!("CAST({} AS BIGINT)", self.int(sc, d - 1)),
            _ => format!("greatest({}, {})", self.int(sc, d - 1), self.int(sc, d - 1)),
        }
    }

    fn text(&mut self, sc: &Scope, d: usize) -> String {
        if d == 0 || self.chance(3) {
            let mut cols = vec!["e.\"from\"", "e.\"to\"", "e.value", "e.flag", "e.kind"];
            if sc.joined {
                cols.extend(["l.addr", "l.name"]);
            }
            return match self.r.below(5) {
                0 => format!("'{}'", self.one(&["0xa", "0xA", "in", "", "x y", "Bob", "10"])),
                _ => self.one(&cols).to_string(),
            };
        }
        match self.r.below(8) {
            0 => format!("lower({})", self.text(sc, d - 1)),
            1 => format!("upper({})", self.text(sc, d - 1)),
            2 => format!("({} || {})", self.text(sc, d - 1), self.text(sc, d - 1)),
            3 => format!("substr({}, {}, {})", self.text(sc, d - 1), 1 + self.r.below(3), self.r.below(4)),
            4 => format!("COALESCE({}, 'z')", self.text(sc, d - 1)),
            5 => format!("CAST({} AS VARCHAR)", self.int(sc, d - 1)),
            6 => format!(
                "CASE WHEN {} THEN {} ELSE {} END",
                self.pred(sc, d - 1),
                self.text(sc, d - 1),
                self.text(sc, d - 1)
            ),
            _ => format!("replace({}, '0x', '')", self.text(sc, d - 1)),
        }
    }

    fn dec(&mut self, sc: &Scope, d: usize) -> String {
        match self.r.below(5) {
            0 => "TRY_CAST(e.value AS HUGEINT)".into(),
            1 => format!("CAST({} AS HUGEINT)", self.int(sc, d.saturating_sub(1))),
            2 => format!("({} * {})", self.dec(sc, d.saturating_sub(1)), self.one(&["1.5", "0.25", "2", "-3"])),
            3 => format!("({} + {})", self.dec(sc, d.saturating_sub(1)), self.dec(sc, d.saturating_sub(1))),
            _ => format!("TRY_CAST(e.value AS DECIMAL(20,2))"),
        }
    }

    fn pred(&mut self, sc: &Scope, d: usize) -> String {
        let d = d.min(3);
        match self.r.below(if d == 0 { 7 } else { 13 }) {
            0 => format!("{} {} {}", self.int(sc, d), self.one(&["=", "<>", "<", ">", "<=", ">="]), self.int(sc, d)),
            1 => format!("{} {} {}", self.text(sc, d), self.one(&["=", "<>", "<", ">="]), self.text(sc, d)),
            2 => format!("{} LIKE '{}'", self.text(sc, d), self.one(&["0x%", "%a", "_x%", "%"])),
            3 => format!("{} IS {}NULL", self.text(sc, d), self.one(&["", "NOT "])),
            4 => format!("{} IN ({}, {}, {})", self.int(sc, d), self.r.below(10), self.r.below(30), self.r.below(60)),
            5 => format!("{} BETWEEN {} AND {}", self.int(sc, d), self.r.below(20) as i64 - 5, self.r.below(80)),
            6 => format!("e.flag = '{}'", self.one(&["true", "false"])),
            7 => format!("NOT ({})", self.pred(sc, d - 1)),
            8 => format!("({} AND {})", self.pred(sc, d - 1), self.pred(sc, d - 1)),
            9 => format!("({} OR {})", self.pred(sc, d - 1), self.pred(sc, d - 1)),
            10 => format!("{} IS {}DISTINCT FROM {}", self.text(sc, d), self.one(&["", "NOT "]), self.text(sc, d)),
            11 => format!("e.\"to\" IN (SELECT addr FROM lbl WHERE weight > {})", self.r.below(5) as i64 - 2),
            _ => format!(
                "{}EXISTS (SELECT 1 FROM lbl x WHERE x.addr = e.\"from\")",
                self.one(&["", "NOT "])
            ),
        }
    }

    fn any(&mut self, sc: &Scope, d: usize) -> String {
        match self.r.below(5) {
            0 | 1 => self.int(sc, d),
            2 => self.text(sc, d),
            3 => self.dec(sc, d),
            _ => self.pred(sc, d),
        }
    }

    fn from(&mut self, sc: &Scope) -> String {
        if sc.joined {
            let kind = self.one(&["JOIN", "LEFT JOIN"]);
            let on = self.one(&["l.addr = e.\"to\"", "l.addr = e.\"from\"", "lower(l.addr) = lower(e.\"to\")"]);
            format!("ev e {kind} lbl l ON {on}")
        } else {
            "ev e".into()
        }
    }

    /// A query and whether its row order is part of the answer.
    fn query(&mut self) -> (String, bool) {
        let sc = Scope { joined: self.chance(3) };
        let from = self.from(&sc);
        let filter = if self.chance(2) { format!(" WHERE {}", self.pred(&sc, 2)) } else { String::new() };
        match self.r.below(8) {
            // Projection.
            0 | 1 => {
                let n = 1 + self.r.below(3);
                let items: Vec<String> = (0..n).map(|i| format!("{} AS c{i}", self.any(&sc, 3))).collect();
                let distinct = if self.chance(4) { "DISTINCT " } else { "" };
                let base = format!("SELECT {distinct}{} FROM {from}{filter}", items.join(", "));
                if self.chance(3) {
                    let order: Vec<String> = (1..=n).map(|i| i.to_string()).collect();
                    (format!("{base} ORDER BY {} LIMIT {}", order.join(", "), 1 + self.r.below(20)), true)
                } else {
                    (base, false)
                }
            }
            // Grouped.
            2 | 3 => {
                let key = if self.chance(2) { self.text(&sc, 1) } else { self.int(&sc, 1) };
                let aggs = [
                    "count(*)".to_string(),
                    format!("count({})", self.text(&sc, 1)),
                    format!("sum({})", self.int(&sc, 2)),
                    format!("sum({})", self.dec(&sc, 2)),
                    format!("min({})", self.text(&sc, 1)),
                    format!("max({})", self.int(&sc, 2)),
                    format!("count(DISTINCT {})", self.text(&sc, 1)),
                    format!("avg({})", self.int(&sc, 1)),
                ];
                let a = aggs[self.r.below(aggs.len())].clone();
                let b = aggs[self.r.below(aggs.len())].clone();
                let having = if self.chance(3) { format!(" HAVING count(*) > {}", self.r.below(4)) } else { String::new() };
                (format!("SELECT {key} AS k, {a} AS a, {b} AS b FROM {from}{filter} GROUP BY 1{having}"), false)
            }
            // Derived table or CTE over a grouped or projected inner query.
            4 => {
                let inner = format!(
                    "SELECT e.\"from\" AS f, {} AS v, {} AS w FROM {from}{filter}",
                    self.int(&sc, 2),
                    self.text(&sc, 1)
                );
                if self.chance(2) {
                    (format!("SELECT f, sum(v) AS s, count(*) AS n FROM ({inner}) s GROUP BY f"), false)
                } else {
                    (format!("WITH s AS ({inner}) SELECT * FROM s WHERE v {} {}", self.one(&[">", "<", "="]), self.r.below(40)), false)
                }
            }
            // Windows, ordered by the unique key.
            5 => {
                let part = self.one(&["e.\"from\"", "e.kind", "e.flag", "e.block_number % 4"]);
                let f = self.one(&[
                    "row_number()",
                    "sum(e.amount)",
                    "count(*)",
                    "lag(e.amount)",
                    "first_value(e.value)",
                    "max(e.amount)",
                ]);
                (
                    format!(
                        "SELECT e.block_number, e.log_index, {f} OVER (PARTITION BY {part} ORDER BY e.block_number, e.log_index) AS w FROM {from}{filter}"
                    ),
                    false,
                )
            }
            // Set operations.
            6 => {
                let op = self.one(&["UNION ALL", "UNION", "EXCEPT", "INTERSECT"]);
                let a = self.int(&sc, 2);
                let b = self.int(&sc, 2);
                let p = self.pred(&sc, 1);
                (format!("SELECT {a} AS x FROM {from}{filter} {op} SELECT {b} FROM {from} WHERE {p}"), false)
            }
            // Scalar subquery and whole-table aggregates.
            _ => (
                format!(
                    "SELECT count(*) AS n, (SELECT count(*) FROM lbl WHERE weight > {}) AS m, sum({}) AS s FROM {from}{filter}",
                    self.r.below(5),
                    self.int(&sc, 2)
                ),
                false,
            ),
        }
    }
}

fn duck_rows(conn: &duckdb::Connection, sql: &str) -> Result<Vec<String>, String> {
    match crate::encode_parity::nuthatch_rows(conn, sql) {
        Ok(Value::Array(rows)) => Ok(rows.iter().map(|r| r.to_string()).collect()),
        Ok(v) => Ok(vec![v.to_string()]),
        Err(e) => Err(e.to_string().lines().next().unwrap_or("").to_string()),
    }
}

fn burrmill_rows(e: &burrmill::Engine, sql: &str) -> Result<Vec<String>, String> {
    let batches = e.sql(sql).map_err(|e| e.to_string().replace('\n', " | "))?;
    let mut out = Vec::new();
    for b in &batches {
        for r in burrmill::df::encode::rows(b).map_err(|e| e.to_string())? {
            out.push(r.to_string());
        }
    }
    Ok(out)
}

pub fn run() -> anyhow::Result<()> {
    let seed = crate::env_usize("SEED", 1) as u64;
    let cases = crate::env_usize("CASES", 2000);
    let print = crate::env_flag("PRINT");
    let tmp = tempfile::tempdir()?;
    let segs = tmp.path().join("segments");
    std::fs::create_dir_all(&segs)?;
    fixture(&segs)?;
    let duck = duckdb::Connection::open_in_memory()?;
    duck.execute_batch("SET TimeZone = 'UTC';")?;
    duck.execute_batch(&format!(
        "CREATE VIEW ev AS SELECT * FROM read_parquet('{0}/ev-*.parquet');
         CREATE VIEW lbl AS SELECT * FROM read_parquet('{0}/lbl-*.parquet');",
        segs.display()
    ))?;
    let s2 = segs.clone();
    let engine = std::thread::spawn(move || burrmill::Engine::open_segments(&s2)).join().expect("open")?;
    let engine = Arc::new(engine);

    let (mut same, mut both, mut stricter, mut looser, mut differ) = (0, 0, 0, 0, 0);
    let mut stricter_why: std::collections::BTreeMap<String, usize> = Default::default();
    for i in 0..cases {
        let case_seed = seed + i as u64;
        let mut r = Rng(case_seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let (sql, ordered) = Gen { r: &mut r }.query();
        let want = duck_rows(&duck, &sql);
        let e2 = Arc::clone(&engine);
        let q = sql.clone();
        let got = std::thread::spawn(move || burrmill_rows(&e2, &q)).join().expect("engine thread");
        let tag = match (&want, &got) {
            (Ok(w), Ok(g)) => {
                let (mut w, mut g) = (w.clone(), g.clone());
                if !ordered {
                    w.sort();
                    g.sort();
                }
                if w == g {
                    same += 1;
                    "SAME"
                } else {
                    differ += 1;
                    "DIFF"
                }
            }
            (Err(_), Err(_)) => {
                both += 1;
                "BOTH-REFUSE"
            }
            (Ok(_), Err(e)) => {
                stricter += 1;
                let key: String = e.chars().take(90).collect();
                *stricter_why.entry(key).or_default() += 1;
                "STRICTER"
            }
            (Err(_), Ok(_)) => {
                looser += 1;
                "LOOSER"
            }
        };
        if print || matches!(tag, "DIFF" | "LOOSER") {
            println!("{tag} seed={case_seed}  {sql}");
            let clip = |r: &Result<Vec<String>, String>| -> String {
                match r {
                    Ok(v) => v.join(",").chars().take(400).collect(),
                    Err(e) => format!("ERROR {e}").chars().take(400).collect(),
                }
            };
            if tag != "SAME" {
                println!("    duckdb   {}", clip(&want));
                println!("    burrmill {}", clip(&got));
            }
        }
    }
    println!("STRICTER reasons:");
    let mut why: Vec<_> = stricter_why.into_iter().collect();
    why.sort_by(|a, b| b.1.cmp(&a.1));
    for (k, n) in why.iter().take(12) {
        println!("  {n:>5}  {k}");
    }
    println!(
        "FUZZ\tseed={seed}\tcases={cases}\tsame={same}\tboth_refuse={both}\tstricter={stricter}\tlooser={looser}\tdiffer={differ}"
    );
    std::thread::spawn(move || drop(engine)).join().expect("drop engine");
    Ok(())
}
