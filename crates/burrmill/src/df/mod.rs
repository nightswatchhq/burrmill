//! DataFusion-hosted SQL over a nest, component crates plus an owned planner (roadmap 6.0 C).
//!
//! Off unless the `datafusion` feature is enabled. The default `burrmill` graph stays free of it.

use std::path::Path;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion_catalog::view::ViewTable;
use datafusion_catalog::{MemTable, Session, TableProvider};
use datafusion_physical_plan::collect;
use datafusion_sql::parser::Statement as DfStatement;
use datafusion_sql::planner::SqlToRel;
use sqlparser::ast::Statement as SqlStatement;

use crate::error::{BurrmillError, Result};
use crate::limits::Limits;

mod catalog;
pub mod encode;
mod errors;
mod checked;
mod dialect;
mod distinct;
mod doubles;
mod fastcast;
mod fold;
mod names;
mod rangejoin;
mod lists;
mod rule;
mod session;
mod sharing;
mod topn;
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
    /// First-come-first-served admission, as on the owned path (roadmap 5.3). Without it, 32
    /// clients sharing one runtime starved one of them outright (roadmap 6.8).
    gate: crate::gate::Gate,
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
                    .map(|c| {
                        format!(
                            ", TRY_CAST(\"{c}\" AS DECIMAL(38,0)) AS \"{c}_dec\", \
                             (\"{c}\" IS NOT NULL AND TRY_CAST(\"{c}\" AS DECIMAL(38,0)) IS NULL) \
                             AS \"{c}_overflow\""
                        )
                    })
                    .collect();
                let sql = format!("SELECT *{dec} FROM \"{raw}\"");
                let logical = plan_query(&session, &sql)?;
                session.register_table(&t.name, Arc::new(ViewTable::new(logical, Some(sql))));
            }
        }
        session
            .build_information_schema(|n| n.ends_with("__raw"))
            .map_err(df_err)?;
        let gate = crate::gate::Gate::new(crate::default_width(threads.max(1)));
        Ok(Self { rt, session, gate })
    }

    /// Define a view over the nest's tables and earlier views, as nuthatch defines its authored
    /// ones: `body` is the query after `AS`.
    pub fn register_view(&mut self, name: &str, body: &str) -> Result<()> {
        let logical = plan_query(&self.session, body)?;
        self.session
            .register_table(name, Arc::new(ViewTable::new(logical, Some(body.to_string()))));
        self.session
            .build_information_schema(|n| n.ends_with("__raw"))
            .map_err(df_err)
    }

    /// Every scalar, aggregate and window function a statement can call, sorted.
    pub fn function_names(&self) -> Vec<String> {
        self.session.function_names()
    }

    pub fn tables(&self) -> Vec<String> {
        self.session.table_names()
    }

    /// Run a query. DDL, DML, COPY, and table functions are refused before they plan.
    pub fn sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        // Parsed first, so malformed SQL is a syntax error as DuckDB reports it, not a refusal.
        let logical = plan_query(&self.session, sql)?;
        refuse_non_query(sql)?;
        let _pass = self.gate.enter();
        self.rt.block_on(async {
            let physical = self.session.create_physical_plan(&logical).await.map_err(df_err)?;
            let batches = collect(physical, self.session.task_ctx()).await.map_err(df_err)?;
            Ok(batches.into_iter().map(dialect::strip_dup_suffix).collect())
        })
    }
}

impl Engine {
    /// Run a query and hand each batch to `f` as it is produced, holding none of them.
    ///
    /// `sql` collects; a million-row answer is then a million rows live at once, which is the
    /// caller's memory and not the engine's. An encoder writing to a socket wants this instead.
    pub fn sql_for_each(
        &self,
        sql: &str,
        mut f: impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<()> {
        use futures::StreamExt;
        // Parsed first, so malformed SQL is a syntax error as DuckDB reports it, not a refusal.
        let logical = plan_query(&self.session, sql)?;
        refuse_non_query(sql)?;
        let _pass = self.gate.enter();
        self.rt.block_on(async {
            let physical = self.session.create_physical_plan(&logical).await.map_err(df_err)?;
            let mut stream = datafusion_physical_plan::execute_stream(physical, self.session.task_ctx())
                .map_err(df_err)?;
            while let Some(b) = stream.next().await {
                f(dialect::strip_dup_suffix(b.map_err(df_err)?))?;
            }
            Ok(())
        })
    }
}

fn plan_query(session: &MiniSession, sql: &str) -> Result<datafusion_expr::LogicalPlan> {
    let (stmt, names) = dialect::parse(sql, &session.known_names())?;
    refuse_df_statement(&stmt)?;
    refuse_wide_literals(&stmt)?;
    let planner = SqlToRel::new(session);
    let plan = planner.statement_to_plan(stmt).map_err(df_err)?;
    rename(plan, &names)
}

/// DuckDB's default names on the unaliased columns, by position, and the private suffixes of
/// `dialect` taken off wherever the plain name is unique. A name that stays repeated keeps its
/// suffix until the result batches, which may repeat names where a plan may not.
fn rename(plan: datafusion_expr::LogicalPlan, names: &[Option<String>]) -> Result<datafusion_expr::LogicalPlan> {
    use datafusion_expr::{Expr, LogicalPlanBuilder};
    let schema = plan.schema().clone();
    let n = schema.fields().len();
    let by_position = names.len() == n;
    let suffixed = schema.fields().iter().any(|f| f.name().contains(dialect::DUP));
    if !suffixed && (!by_position || names.iter().all(Option::is_none)) {
        return Ok(plan);
    }
    let wanted: Vec<String> = (0..n)
        .map(|i| match names.get(i).filter(|_| by_position) {
            Some(Some(name)) => name.clone(),
            _ => schema.field(i).name().clone(),
        })
        .collect();
    let base = |s: &str| s.split_once(dialect::DUP).map_or(s, |(b, _)| b).to_string();
    let finals: Vec<String> = wanted
        .iter()
        .map(|w| {
            let b = base(w);
            let clash = wanted.iter().filter(|o| base(o) == b).count() > 1;
            if clash { w.clone() } else { b }
        })
        .collect();
    let mut seen = std::collections::HashSet::new();
    if !finals.iter().all(|f| seen.insert(f.as_str())) {
        return Ok(plan);
    }
    if (0..n).all(|i| finals[i] == *schema.field(i).name()) {
        return Ok(plan);
    }
    let exprs: Vec<Expr> = (0..n)
        .map(|i| {
            let col = Expr::Column(datafusion_common::Column::from(schema.qualified_field(i)));
            if finals[i] == *schema.field(i).name() { col } else { col.alias(finals[i].as_str()) }
        })
        .collect();
    LogicalPlanBuilder::from(plan).project(exprs).and_then(|b| b.build()).map_err(df_err)
}

/// The first keyword, past whitespace, opening parentheses and comments: `(SELECT ...) UNION ...`
/// and a statement opening with `-- note` are queries too.
fn leading_keyword(sql: &str) -> &str {
    let mut s = sql;
    loop {
        let t = s.trim_start_matches(|c: char| c.is_whitespace() || c == '(');
        if let Some(rest) = t.strip_prefix("--") {
            s = rest.split_once('\n').map_or("", |(_, r)| r);
        } else if let Some(rest) = t.strip_prefix("/*") {
            s = rest.split_once("*/").map_or("", |(_, r)| r);
        } else {
            return t;
        }
    }
}

fn refuse_non_query(sql: &str) -> Result<()> {
    let trimmed = leading_keyword(sql);
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
    let s = errors::restate(e.to_string());
    if s.contains("not yet implemented") || s.contains("Table Functions are not supported") {
        BurrmillError::NotAllowed(s)
    } else if s.contains("no table") {
        BurrmillError::NoSegments(s)
    } else {
        BurrmillError::Substrate(s)
    }
}
