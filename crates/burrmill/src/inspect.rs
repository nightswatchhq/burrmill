//! What a statement reaches, from sqlparser's AST (roadmap 6.7).
//!
//! nuthatch asks DuckDB's parser (`json_serialize_sql`) which tables and table functions a `/sql`
//! statement touches, refuses anything unrecognised, and bounds its integrity sweep by the tables
//! reached (`reject_unknown_table_refs`, `walk_table_refs`). This answers the same questions by the
//! same rules, checked against that walker by `burrmill-bench reach-parity`:
//!
//! - a table function must be `generate_series`, `range` or `unnest`;
//! - a base table must be named `[A-Za-z0-9_]+`, which is what stops a quoted path in table position
//!   (DuckDB's replacement scan) reading a file;
//! - a schema other than `main` marks the statement as surveying the catalogue;
//! - the tables reached are returned lowercased, CTE names included, as DuckDB's walk includes them.
//!
//! It is stricter where nuthatch's is open. An unparseable statement is an error, not a pass; so is
//! more than one statement, and anything that is not a query.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sqlparser::ast::{ObjectName, ObjectNamePart, Statement, TableFactor, Visit, Visitor};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;

use crate::error::{BurrmillError, Result};

/// nuthatch's `ALLOWED_TABLE_FNS`.
pub const ALLOWED_TABLE_FNS: &[&str] = &["generate_series", "range", "unnest"];

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Reach {
    /// Base tables and CTE names the statement names, lowercased.
    pub tables: BTreeSet<String>,
    /// It names a schema other than `main`, so it asks about the catalogue.
    pub surveys: bool,
}

fn refused(why: String) -> BurrmillError {
    BurrmillError::NotAllowed(format!(
        "{why} - the SQL surface serves this nest's tables and views only"
    ))
}

fn parts(name: &ObjectName) -> Vec<String> {
    name.0
        .iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => i.value.clone(),
            other => other.to_string(),
        })
        .collect()
}

struct Walk {
    reach: Reach,
    bad: Option<String>,
}

impl Walk {
    fn table_function(&mut self, name: &ObjectName) {
        let f = parts(name).last().cloned().unwrap_or_default();
        let lower = f.to_ascii_lowercase();
        if !ALLOWED_TABLE_FNS.contains(&lower.as_str()) {
            self.bad = Some(format!("table function `{f}` is not permitted here"));
        }
    }
}

impl Visitor for Walk {
    type Break = ();

    fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<()> {
        if self.bad.is_some() {
            return ControlFlow::Break(());
        }
        match t {
            TableFactor::Table { name, args: Some(_), .. } => self.table_function(name),
            TableFactor::Function { name, .. } => self.table_function(name),
            TableFactor::Table { name, .. } => {
                let p = parts(name);
                let table = p.last().cloned().unwrap_or_default();
                if table.is_empty() || !table.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                {
                    self.bad = Some(format!(
                        "`{table}` is not a table name - a quoted path in table position reads a file"
                    ));
                } else {
                    self.reach.tables.insert(table.to_ascii_lowercase());
                }
                if p.len() >= 2 && !p[p.len() - 2].eq_ignore_ascii_case("main") {
                    self.reach.surveys = true;
                }
            }
            TableFactor::UNNEST { .. }
            | TableFactor::Derived { .. }
            | TableFactor::NestedJoin { .. }
            | TableFactor::Pivot { .. }
            | TableFactor::Unpivot { .. } => {}
            other => {
                self.bad = Some(format!("`{other}` is not a table this surface serves"));
            }
        }
        if self.bad.is_some() { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }
    }
}

/// The tables `sql` reaches, or why it is refused.
pub fn reach(sql: &str) -> Result<Reach> {
    let stmts = Parser::parse_sql(&DuckDbDialect {}, sql)
        .map_err(|e| BurrmillError::Parse(format!("Parser Error: {e}")))?;
    let [stmt] = stmts.as_slice() else {
        return Err(refused(format!("{} statements where one is allowed", stmts.len())));
    };
    if !matches!(stmt, Statement::Query(_)) {
        return Err(refused("only SELECT/WITH queries are allowed".into()));
    }
    let mut w = Walk { reach: Reach::default(), bad: None };
    let _ = stmt.visit(&mut w);
    match w.bad {
        Some(why) => Err(refused(why)),
        None => Ok(w.reach),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tables(sql: &str) -> Vec<String> {
        reach(sql).unwrap().tables.into_iter().collect()
    }

    #[test]
    fn reaches_through_joins_ctes_and_subqueries() {
        assert_eq!(
            tables(
                "WITH c AS (SELECT * FROM Transfer) SELECT * FROM c JOIN label l ON true \
                 WHERE EXISTS (SELECT 1 FROM mint) AND x IN (SELECT y FROM burn)"
            ),
            vec!["burn", "c", "label", "mint", "transfer"]
        );
    }

    #[test]
    fn refuses_files_and_foreign_table_functions() {
        for sql in [
            "SELECT * FROM read_csv('/etc/passwd')",
            "SELECT * FROM \"read_csv\"('/etc/passwd')",
            "SELECT * FROM '/etc/passwd'",
            "SELECT * FROM duckdb_tables()",
            "SELECT 1; SELECT 2",
            "COPY (SELECT 1) TO '/tmp/x'",
        ] {
            assert!(reach(sql).is_err(), "{sql}");
        }
        assert!(reach("SELECT * FROM range(3)").is_ok());
    }

    #[test]
    fn a_schema_other_than_main_surveys() {
        assert!(reach("SELECT * FROM information_schema.tables").unwrap().surveys);
        assert!(!reach("SELECT * FROM main.t").unwrap().surveys);
    }
}
