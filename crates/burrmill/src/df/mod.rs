//! DataFusion-hosted SQL over a nest, component crates plus an owned planner (roadmap 6.0 C).
//!
//! Off unless the `datafusion` feature is enabled. The default `burrmill` graph stays free of it.

use std::path::Path;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion_catalog::view::ViewTable;
use datafusion_catalog::{MemTable, Session, TableProvider};
use datafusion_physical_plan::collect;
use datafusion_sql::parser::{DFParser, Statement as DfStatement};
use datafusion_sql::planner::SqlToRel;
use sqlparser::ast::Statement as SqlStatement;

use crate::error::{BurrmillError, Result};
use crate::limits::Limits;

mod catalog;
mod checked;
mod fold;
mod rule;
mod session;
mod wide;

#[path = "generated/schema_equivalence.rs"]
mod schema_equivalence;
#[path = "generated/physical_planner.rs"]
mod physical_planner;

use catalog::{apply_schema_json, discover_tables, NestTable, SegmentTable};
use session::MiniSession;

/// Concrete engine: SQL in, RecordBatches out. Generics stay inside this crate.
pub struct Engine {
    rt: tokio::runtime::Runtime,
    session: MiniSession,
}

impl Engine {
    /// Open every table under a `segments/` directory.
    pub fn open_segments(segments: &Path) -> Result<Self> {
        let tables = discover_tables(segments)?;
        if tables.is_empty() {
            return Err(BurrmillError::NoSegments(format!(
                "no parquet tables under {}",
                segments.display()
            )));
        }
        Self::from_tables(tables, Limits::default().max_threads)
    }

    /// Open a nest root (`segments/` plus optional `schema.json` for unsealed and `_dec`).
    pub fn open_nest(root: &Path) -> Result<Self> {
        let mut tables = discover_tables(&root.join("segments"))?;
        let schema = root.join("schema.json");
        if schema.is_file() {
            apply_schema_json(&mut tables, &schema)?;
        }
        if tables.is_empty() {
            return Err(BurrmillError::NoSegments(format!(
                "no tables under {}",
                root.display()
            )));
        }
        Self::from_tables(tables, Limits::default().max_threads)
    }

    fn from_tables(tables: Vec<NestTable>, threads: usize) -> Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| BurrmillError::Substrate(e.to_string()))?;
        let mut fold_tables = std::collections::HashMap::new();
        for t in tables.iter().filter(|t| !t.files.is_empty()) {
            let name = if t.wide.is_empty() { t.name.clone() } else { format!("{}__raw", t.name) };
            let files = t.files.iter().map(|(p, _)| p.clone());
            fold_tables.insert(name.clone(), crate::segment::SealedSegments::from_files(name, files));
        }
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads.max(1))
            .thread_name(|i| format!("burrmill-fold-{i}"))
            .build()
            .map_err(|e| BurrmillError::Substrate(e.to_string()))?;
        let fold = fold::FoldTables { tables: Arc::new(fold_tables), pool: Arc::new(pool) };
        let mut session = MiniSession::new(threads, fold).map_err(df_err)?;
        let groups = threads.max(1);
        for t in &tables {
            let provider: Arc<dyn TableProvider> = if t.files.is_empty() {
                Arc::new(MemTable::try_new(t.schema.clone(), vec![vec![]]).map_err(df_err)?)
            } else {
                Arc::new(SegmentTable::new(t, groups)?)
            };
            if t.wide.is_empty() {
                session.register_table(&t.name, provider);
            } else {
                let raw = format!("{}__raw", t.name);
                session.register_table(&raw, provider);
                let dec: String = t
                    .wide
                    .iter()
                    .map(|c| format!(", TRY_CAST(\"{c}\" AS DECIMAL(38,0)) AS \"{c}_dec\""))
                    .collect();
                let sql = format!("SELECT *{dec} FROM \"{raw}\"");
                let logical = plan_query(&session, &sql)?;
                session.register_table(&t.name, Arc::new(ViewTable::new(logical, Some(sql))));
            }
        }
        Ok(Self { rt, session })
    }

    pub fn tables(&self) -> Vec<String> {
        self.session.table_names()
    }

    /// Run a query. DDL, DML, COPY, and table functions are refused before they plan.
    pub fn sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        refuse_non_query(sql)?;
        let logical = plan_query(&self.session, sql)?;
        self.rt.block_on(async {
            let physical = self.session.create_physical_plan(&logical).await.map_err(df_err)?;
            collect(physical, self.session.task_ctx()).await.map_err(df_err)
        })
    }
}

fn plan_query(session: &MiniSession, sql: &str) -> Result<datafusion_expr::LogicalPlan> {
    let stmts = DFParser::parse_sql(sql).map_err(df_err)?;
    let Some(stmt) = stmts.into_iter().next() else {
        return Err(BurrmillError::Parse("empty statement".into()));
    };
    refuse_df_statement(&stmt)?;
    refuse_wide_literals(&stmt)?;
    let planner = SqlToRel::new(session);
    planner.statement_to_plan(stmt).map_err(df_err)
}

fn refuse_non_query(sql: &str) -> Result<()> {
    let trimmed = sql.trim_start();
    let head = trimmed
        .chars()
        .take_while(|c| c.is_ascii_alphabetic() || *c == '_')
        .collect::<String>()
        .to_ascii_uppercase();
    match head.as_str() {
        "SELECT" | "WITH" | "VALUES" | "EXPLAIN" => Ok(()),
        other => Err(BurrmillError::NotAllowed(format!(
            "only SELECT/WITH is admitted, not `{other}`"
        ))),
    }
}

fn refuse_df_statement(stmt: &DfStatement) -> Result<()> {
    match stmt {
        DfStatement::Statement(s) => match s.as_ref() {
            SqlStatement::Query(_) => Ok(()),
            SqlStatement::Explain { .. } => Ok(()),
            other => Err(BurrmillError::NotAllowed(format!(
                "statement not admitted: {}",
                stmt_kind(other)
            ))),
        },
        DfStatement::CreateExternalTable(_) => Err(BurrmillError::NotAllowed(
            "CREATE EXTERNAL TABLE is not in the grammar we expose".into(),
        )),
        DfStatement::CopyTo(_) => {
            Err(BurrmillError::NotAllowed("COPY is not in the grammar we expose".into()))
        }
        DfStatement::Explain(_) => Ok(()),
        DfStatement::Reset(_) => {
            Err(BurrmillError::NotAllowed("RESET is not in the grammar we expose".into()))
        }
    }
}

/// An integer literal past u64 parses as Float64 before any plan rule can see it, and a float
/// cannot hold it exactly.
fn refuse_wide_literals(stmt: &DfStatement) -> Result<()> {
    use sqlparser::ast::{Expr as SqlExpr, Value, visit_expressions};
    use std::ops::ControlFlow;
    let DfStatement::Statement(s) = stmt else { return Ok(()) };
    let found = visit_expressions(s.as_ref(), |e| {
        if let SqlExpr::Value(v) = e
            && let Value::Number(n, _) = &v.value
            && n.bytes().all(|b| b.is_ascii_digit())
            && n.parse::<u64>().is_err()
        {
            return ControlFlow::Break(n.clone());
        }
        ControlFlow::Continue(())
    });
    match found {
        ControlFlow::Break(n) => Err(BurrmillError::NotAllowed(format!(
            "integer literal {n} is wider than 64 bits and would be read as a float; \
             write CAST('{n}' AS DECIMAL(38,0))"
        ))),
        ControlFlow::Continue(()) => Ok(()),
    }
}

fn stmt_kind(s: &SqlStatement) -> &'static str {
    match s {
        SqlStatement::Insert { .. } => "INSERT",
        SqlStatement::CreateTable { .. } => "CREATE TABLE",
        SqlStatement::CreateView { .. } => "CREATE VIEW",
        SqlStatement::Copy { .. } => "COPY",
        SqlStatement::Delete { .. } => "DELETE",
        SqlStatement::Update { .. } => "UPDATE",
        SqlStatement::Drop { .. } => "DROP",
        _ => "non-query",
    }
}

fn df_err(e: datafusion_common::DataFusionError) -> BurrmillError {
    let s = e.to_string();
    if s.contains("not yet implemented") || s.contains("Table Functions are not supported") {
        BurrmillError::NotAllowed(s)
    } else if s.contains("no table") {
        BurrmillError::NoSegments(s)
    } else {
        BurrmillError::Substrate(s)
    }
}
