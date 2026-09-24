//! Every authored view, whole, on the engine Burrmill intends to rent.
//!
//! `views` runs the fold sub-plans Burrmill owns. This runs **every statement** in a nest's
//! `views/*.sql` on DataFusion and on DuckDB, and asks the questions that decide whether renting
//! is viable: does it parse, does it plan, does it agree with DuckDB, and where does the time go
//! when it does not keep up.
//!
//! The rules are the ones the rest of the harness lives by. Both engines get an explicit file list
//! (roadmap 1.1: a glob over a 38,429-file directory measures the directory), the same thread
//! budget, and a footer cache - DuckDB's is off by default and worth about 9% on the curation
//! fold, so `views` leaving it off flattered Burrmill. Parity is checked before any timing, and a
//! statement without it is reported with the difference and never timed.
//!
//! Tables the nest declares in `schema.json` but has not sealed are registered **empty** in both
//! engines, which is what nuthatch itself serves for them. A view over an unsealed table is still
//! a statement the engine has to plan.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::{DataType, Decimal128Type, Field, Float32Type, Float64Type, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DFSchema;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl, PartitionedFile,
};
use datafusion::datasource::physical_plan::parquet::{
    transform_schema_to_view, CachedParquetFileReaderFactory,
};
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, ParquetFileMetrics, ParquetFileReaderFactory, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::{MemTable, TableType, ViewTable};
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{utils::conjunction, Expr, LogicalPlan, TableProviderFilterPushDown};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::*;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::ParquetMetaData;
use sqlparser::ast::{self as sq, VisitMut, VisitorMut};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;

const REPEATS: usize = 3;

// ---------------------------------------------------------------------------------------------
// The nest

pub(crate) struct Table {
    pub(crate) name: String,
    pub(crate) schema: SchemaRef,
    /// Path and size, from one `read_dir` pass. Empty for a declared table nothing has sealed.
    pub(crate) files: Vec<(PathBuf, u64)>,
    /// `word16`/`word32` columns, which nuthatch gives a `{col}_dec` companion at query time.
    pub(crate) wide: Vec<String>,
}

pub(crate) struct View {
    pub(crate) name: String,
    pub(crate) file: String,
    /// The statement as authored, for DuckDB and for the parity record.
    pub(crate) text: String,
    /// The body after `AS`, which is what a serving path runs.
    pub(crate) body: String,
}

pub(crate) struct Nest {
    pub(crate) tables: Vec<Table>,
    pub(crate) views: Vec<View>,
    /// Whether any view reads a `_dec` companion, so the base layer must synthesise them.
    pub(crate) wants_dec: bool,
}

pub(crate) fn load_nest(root: &Path) -> anyhow::Result<Nest> {
    let mut by_table: BTreeMap<String, Vec<(PathBuf, u64)>> = BTreeMap::new();
    for e in std::fs::read_dir(root.join("segments"))?.flatten() {
        let p = e.path();
        let Some(stem) = p.file_name().and_then(|f| f.to_str()).and_then(|f| f.strip_suffix(".parquet")) else {
            continue;
        };
        if let Some((table, _)) = stem.rsplit_once('-') {
            let size = e.metadata()?.len();
            by_table.entry(table.to_string()).or_default().push((p, size));
        }
    }
    anyhow::ensure!(!by_table.is_empty(), "no segments under {}", root.display());

    let raw = std::fs::read_to_string(root.join("schema.json"))?;
    let doc: serde_json::Value = serde_json::from_str(&raw)?;
    let mut declared: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    for t in doc["tables"].as_array().into_iter().flatten() {
        let Some(name) = t["table"].as_str() else { continue };
        let cols = t["columns"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|c| Some((c["name"].as_str()?.to_string(), c["storage"].as_str()?.to_string())))
            .collect();
        declared.insert(name.to_string(), cols);
    }

    let mut tables = Vec::new();
    let names: BTreeSet<&String> = by_table.keys().chain(declared.keys()).collect();
    for name in names {
        let wide = declared
            .get(name)
            .map(|cols| {
                cols.iter()
                    .filter(|(_, s)| s == "word16" || s == "word32")
                    .map(|(n, _)| n.clone())
                    .collect()
            })
            .unwrap_or_default();
        let (schema, files) = match by_table.get(name) {
            Some(files) => {
                let f = std::fs::File::open(&files[0].0)?;
                let meta = ArrowReaderMetadata::load(&f, ArrowReaderOptions::new())?;
                (Arc::new(transform_schema_to_view(meta.schema())), files.clone())
            }
            // Unsealed: the layout every sealed table on this nest has. u64 stays u64; the rest is
            // text, including bools and the 256-bit words - nuthatch seals them as strings.
            None => {
                let mut fields: Vec<Field> = declared[name]
                    .iter()
                    .map(|(n, s)| match s.as_str() {
                        "u64" => Field::new(n, DataType::UInt64, false),
                        _ => Field::new(n, DataType::Utf8View, true),
                    })
                    .collect();
                fields.push(Field::new("table", DataType::Utf8View, true));
                (Arc::new(Schema::new(fields)), Vec::new())
            }
        };
        tables.push(Table { name: name.clone(), schema, files, wide });
    }

    let mut vfiles: Vec<PathBuf> = std::fs::read_dir(root.join("views"))?
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "sql"))
        .collect();
    vfiles.sort();
    let mut views = Vec::new();
    for f in &vfiles {
        let text = std::fs::read_to_string(f)?;
        let short = f.file_name().unwrap().to_string_lossy().to_string();
        for (name, text, body) in split_views(&text) {
            views.push(View { name, file: short.clone(), text, body });
        }
    }
    anyhow::ensure!(!views.is_empty(), "no CREATE VIEW statements under {}", root.display());
    let wants_dec = views.iter().any(|v| v.body.contains("_dec"));
    Ok(Nest { tables, views, wants_dec })
}

/// Each `CREATE VIEW` in a file, split on the line it starts, comment lines dropped so a `;` in
/// prose cannot end a statement early.
fn split_views(text: &str) -> Vec<(String, String, String)> {
    let mut chunks: Vec<String> = Vec::new();
    for line in text.lines() {
        if line.trim_start().starts_with("--") {
            continue;
        }
        if line.to_ascii_uppercase().starts_with("CREATE VIEW") {
            chunks.push(String::new());
        }
        if let Some(c) = chunks.last_mut() {
            c.push_str(line);
            c.push('\n');
        }
    }
    chunks
        .into_iter()
        .filter_map(|c| {
            let stmt = c.trim().trim_end_matches(';').trim().to_string();
            let rest = stmt["CREATE VIEW".len()..].trim_start();
            let name: String = rest.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
            let rest = rest[name.len()..].trim_start();
            let body = rest.get(..2).filter(|k| k.eq_ignore_ascii_case("AS")).map(|_| rest[2..].trim().to_string())?;
            Some((name, stmt, body))
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// The dialect

/// The DuckDB constructs the views use that DataFusion has no spelling for, each replaced by the
/// smallest equivalent that returns the same answer. Every rule is named in the output.
#[derive(Default)]
struct Rewriter {
    used: BTreeSet<&'static str>,
}

fn decimal38() -> sq::DataType {
    sq::DataType::Decimal(sq::ExactNumberInfo::PrecisionAndScale(38, 0))
}

fn cast(expr: sq::Expr, data_type: sq::DataType) -> sq::Expr {
    sq::Expr::Cast { kind: sq::CastKind::Cast, expr: Box::new(expr), data_type, array: false, format: None }
}

fn binop(l: sq::Expr, op: sq::BinaryOperator, r: sq::Expr) -> sq::Expr {
    sq::Expr::BinaryOp { left: Box::new(l), op, right: Box::new(r) }
}

/// `(a1, a2) op (b1, b2)` spelled out lexicographically.
fn tuple_cmp(a: &[sq::Expr], b: &[sq::Expr], op: &sq::BinaryOperator) -> sq::Expr {
    use sq::BinaryOperator::*;
    let strict = match op {
        Gt | GtEq => Gt,
        _ => Lt,
    };
    if a.len() == 1 {
        return binop(a[0].clone(), op.clone(), b[0].clone());
    }
    let head = binop(a[0].clone(), strict, b[0].clone());
    let eq = binop(a[0].clone(), Eq, b[0].clone());
    let tail = tuple_cmp(&a[1..], &b[1..], op);
    binop(head, Or, sq::Expr::Nested(Box::new(binop(eq, And, tail))))
}

impl VisitorMut for Rewriter {
    type Break = ();

    fn pre_visit_expr(&mut self, e: &mut sq::Expr) -> ControlFlow<()> {
        match e {
            sq::Expr::Cast { data_type, .. } if matches!(data_type, sq::DataType::HugeInt) => {
                *data_type = decimal38();
                self.used.insert("hugeint");
            }
            // `a // b` is truncating division. DataFusion's decimal `/` carries four extra digits
            // and a cast back would round them, so divide only the exact multiple.
            sq::Expr::BinaryOp { left, op: sq::BinaryOperator::DuckIntegerDivide, right } => {
                use sq::BinaryOperator::*;
                let (a, b) = (*left.clone(), *right.clone());
                let rem = binop(a.clone(), Modulo, b.clone());
                let exact = sq::Expr::Nested(Box::new(binop(a, Minus, rem)));
                *e = cast(binop(exact, Divide, b), decimal38());
                self.used.insert("intdiv");
            }
            sq::Expr::BinaryOp { left, op, right } => {
                use sq::BinaryOperator::*;
                if let (sq::Expr::Tuple(a), sq::Expr::Tuple(b)) = (left.as_ref(), right.as_ref()) {
                    if matches!(op, Gt | Lt | GtEq | LtEq) && a.len() == b.len() && !a.is_empty() {
                        *e = tuple_cmp(a, b, op);
                        self.used.insert("tuple-cmp");
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

enum Translated {
    Ok { stmt: sq::Statement, rules: Vec<&'static str> },
    ParseFail(String),
}

fn translate(text: &str) -> Translated {
    let parsed = match Parser::parse_sql(&DuckDbDialect {}, text) {
        Ok(p) => p,
        Err(e) => {
            let mut msg = first_line(&e.to_string());
            // Name the construct the parser tripped on, since its message will not.
            let up = text.to_ascii_uppercase();
            for (needle, what) in [
                ("found: FOR", "list comprehension"),
                ("MATCH_CONDITION", "ASOF JOIN"),
                ("Expected: JOIN, found: LEFT", "ASOF JOIN"),
            ] {
                if msg.contains(needle) {
                    msg = format!("{what}: {msg}");
                    return Translated::ParseFail(msg);
                }
            }
            if up.contains(" FOR C IN ") {
                msg = format!("list comprehension: {msg}");
            }
            return Translated::ParseFail(msg);
        }
    };
    let Some(sq::Statement::CreateView(cv)) = parsed.into_iter().next() else {
        return Translated::ParseFail("not a CREATE VIEW".into());
    };
    let mut query = cv.query;
    let mut rw = Rewriter::default();
    let _ = query.visit(&mut rw);
    Translated::Ok { stmt: sq::Statement::Query(query), rules: rw.used.into_iter().collect() }
}

// ---------------------------------------------------------------------------------------------
// DataFusion, three ways of handing it the same files

#[derive(Clone, Copy, Debug, PartialEq)]
enum Provider {
    /// `ListingTable` over one URL per file: DataFusion's own path, minus the directory glob.
    Listing,
    /// A provider that already knows every file's size, so the scan starts with no listing.
    Files { groups: usize },
    /// The same, with footers served pre-parsed from Burrmill's morsel cache.
    Morsels { groups: usize },
}

#[derive(Clone, Copy, Debug)]
struct DfConfig {
    provider: Provider,
    collect_statistics: bool,
    metadata_cache_bytes: usize,
}

impl DfConfig {
    fn label(&self) -> String {
        let p = match self.provider {
            Provider::Listing => "listing".to_string(),
            Provider::Files { groups } => format!("files/{groups}g"),
            Provider::Morsels { groups } => format!("morsels/{groups}g"),
        };
        let cache = match self.metadata_cache_bytes {
            0 => "off".to_string(),
            b if b >= 1 << 30 => format!("{}G", b >> 30),
            b => format!("{}M", b >> 20),
        };
        format!("{p:<12} stats={:<3} cache={cache:<4}", if self.collect_statistics { "on" } else { "off" })
    }
}

/// Footers parsed once, handed to DataFusion's reader instead of read from the file.
#[derive(Debug)]
struct PreparsedFactory {
    store: Arc<dyn ObjectStore>,
    footers: Arc<HashMap<String, Arc<ParquetMetaData>>>,
}

struct PreparsedReader {
    store: Arc<dyn ObjectStore>,
    file: PartitionedFile,
    footer: Arc<ParquetMetaData>,
    metrics: ParquetFileMetrics,
}

impl AsyncFileReader for PreparsedReader {
    fn get_bytes(&mut self, range: std::ops::Range<u64>) -> BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
        self.metrics.bytes_scanned.add((range.end - range.start) as usize);
        self.store
            .get_range(&self.file.object_meta.location, range)
            .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<bytes::Bytes>>> {
        self.metrics.bytes_scanned.add(ranges.iter().map(|r| (r.end - r.start) as usize).sum());
        async move {
            self.store
                .get_ranges(&self.file.object_meta.location, &ranges)
                .await
                .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let m = self.footer.clone();
        async move { Ok(m) }.boxed()
    }
}

impl ParquetFileReaderFactory for PreparsedFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        partitioned_file: PartitionedFile,
        _metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion::error::Result<Box<dyn AsyncFileReader + Send>> {
        let key = partitioned_file.object_meta.location.to_string();
        let footer = self
            .footers
            .get(&key)
            .cloned()
            .ok_or_else(|| datafusion::error::DataFusionError::Internal(format!("no pre-parsed footer for {key}")))?;
        let metrics = ParquetFileMetrics::new(partition_index, &key, metrics);
        Ok(Box::new(PreparsedReader { store: self.store.clone(), file: partitioned_file, footer, metrics }))
    }
}

/// A table that is exactly a list of sealed files, sizes known, nothing to list.
#[derive(Debug)]
struct SegmentTable {
    pub(crate) schema: SchemaRef,
    files: Vec<PartitionedFile>,
    groups: usize,
    footers: Option<Arc<HashMap<String, Arc<ParquetMetaData>>>>,
}

#[async_trait::async_trait]
impl TableProvider for SegmentTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let url = ObjectStoreUrl::local_filesystem();
        let store = state.runtime_env().object_store(&url)?;
        let opts = TableParquetOptions { global: state.config_options().execution.parquet.clone(), ..Default::default() };
        let mut source = ParquetSource::new(self.schema.clone()).with_table_parquet_options(opts);
        if let Some(pred) = conjunction(filters.iter().cloned()) {
            let df_schema = DFSchema::try_from(self.schema.as_ref().clone())?;
            source = source.with_predicate(state.create_physical_expr(pred, &df_schema)?);
        }
        let factory: Arc<dyn ParquetFileReaderFactory> = match &self.footers {
            Some(f) => Arc::new(PreparsedFactory { store, footers: f.clone() }),
            None => Arc::new(CachedParquetFileReaderFactory::new(
                store,
                state.runtime_env().cache_manager.get_file_metadata_cache(),
            )),
        };
        source = source.with_parquet_file_reader_factory(factory);

        let n = self.groups.min(self.files.len()).max(1);
        let mut groups: Vec<Vec<PartitionedFile>> = vec![Vec::new(); n];
        for (i, f) in self.files.iter().enumerate() {
            groups[i % n].push(f.clone());
        }
        let cfg = FileScanConfigBuilder::new(url, Arc::new(source))
            .with_file_groups(groups.into_iter().map(FileGroup::new).collect())
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .build();
        Ok(DataSourceExec::from_data_source(cfg))
    }
}

struct Df {
    ctx: SessionContext,
    register_ms: u128,
}

async fn df_session(cfg: &DfConfig, nest: &Nest, budget: usize) -> anyhow::Result<Df> {
    let t = Instant::now();
    let config = SessionConfig::new()
        .with_target_partitions(budget)
        .with_collect_statistics(cfg.collect_statistics)
        // Solidity parameters are camelCase and unquoted in the views; DuckDB matches them
        // case-insensitively and DataFusion would lowercase them.
        .set_bool("datafusion.sql_parser.enable_ident_normalization", false);
    let runtime = RuntimeEnvBuilder::new()
        .with_cache_manager(CacheManagerConfig::default().with_metadata_cache_limit(cfg.metadata_cache_bytes))
        .build_arc()?;
    let state = SessionStateBuilder::new().with_config(config).with_runtime_env(runtime).with_default_features().build();
    let ctx = SessionContext::new_with_state(state);

    for t in &nest.tables {
        let raw = if nest.wants_dec && !t.wide.is_empty() { format!("{}__raw", t.name) } else { t.name.clone() };
        if t.files.is_empty() {
            ctx.register_table(&raw, Arc::new(MemTable::try_new(t.schema.clone(), vec![vec![]])?))?;
        } else {
            match cfg.provider {
                Provider::Listing => {
                    let urls = t
                        .files
                        .iter()
                        .map(|(p, _)| ListingTableUrl::parse(p.to_string_lossy()))
                        .collect::<Result<Vec<_>, _>>()?;
                    let lc = ListingTableConfig::new_with_multi_paths(urls)
                        .with_listing_options(ListingOptions::new(Arc::new(ParquetFormat::default())))
                        .with_schema(t.schema.clone());
                    ctx.register_table(&raw, Arc::new(ListingTable::try_new(lc)?))?;
                }
                Provider::Files { groups } | Provider::Morsels { groups } => {
                    let files = t
                        .files
                        .iter()
                        .map(|(p, size)| {
                            Ok(PartitionedFile::new(object_store::path::Path::from_filesystem_path(p)?.to_string(), *size))
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?;
                    let footers = match cfg.provider {
                        Provider::Morsels { .. } => {
                            let segs = burrmill::SealedSegments::from_files(t.name.clone(), t.files.iter().map(|(p, _)| p.clone()));
                            let mut map = HashMap::new();
                            for m in segs.morsels()?.iter() {
                                let key = object_store::path::Path::from_filesystem_path(&*m.path)?.to_string();
                                map.entry(key).or_insert_with(|| m.meta.metadata().clone());
                            }
                            Some(Arc::new(map))
                        }
                        _ => None,
                    };
                    ctx.register_table(&raw, Arc::new(SegmentTable { schema: t.schema.clone(), files, groups, footers }))?;
                }
            }
        }
        if nest.wants_dec && !t.wide.is_empty() {
            let dec: Vec<String> = t.wide.iter().map(|c| format!(", TRY_CAST(\"{c}\" AS DECIMAL(38,0)) AS \"{c}_dec\"")).collect();
            ctx.sql(&format!("CREATE VIEW \"{}\" AS SELECT *{} FROM \"{raw}\"", t.name, dec.join("")))
                .await?
                .collect()
                .await?;
        }
    }
    Ok(Df { ctx, register_ms: t.elapsed().as_millis() })
}

fn duck_session(nest: &Nest, budget: usize) -> anyhow::Result<duckdb::Connection> {
    let conn = duckdb::Connection::open_in_memory()?;
    conn.execute_batch(&format!("SET threads TO {budget}; SET parquet_metadata_cache = true;"))?;
    for t in &nest.tables {
        let dec: Vec<String> = if nest.wants_dec {
            t.wide.iter().map(|c| format!(", TRY_CAST(\"{c}\" AS HUGEINT) AS \"{c}_dec\"")).collect()
        } else {
            Vec::new()
        };
        if t.files.is_empty() {
            let cols: Vec<String> = t
                .schema
                .fields()
                .iter()
                .map(|f| format!("\"{}\" {}", f.name(), if *f.data_type() == DataType::UInt64 { "UBIGINT" } else { "VARCHAR" }))
                .collect();
            conn.execute_batch(&format!("CREATE TABLE \"{}__raw\" ({});", t.name, cols.join(", ")))?;
            conn.execute_batch(&format!("CREATE VIEW \"{}\" AS SELECT *{} FROM \"{}__raw\";", t.name, dec.join(""), t.name))?;
        } else {
            let list = t.files.iter().map(|(p, _)| format!("'{}'", p.display())).collect::<Vec<_>>().join(",");
            conn.execute_batch(&format!(
                "CREATE VIEW \"{}\" AS SELECT *{} FROM read_parquet([{list}]);",
                t.name,
                dec.join("")
            ))?;
        }
    }
    Ok(conn)
}

// ---------------------------------------------------------------------------------------------
// Running and rendering

/// One value as canonical text, so the comparison is about data and not about two libraries'
/// formatting. Floats go through `f64` on both sides; a 128-bit integer is its digits.
fn duck_cell(v: &duckdb::types::Value) -> String {
    use duckdb::types::Value as V;
    match v {
        V::Null => "NULL".into(),
        V::Boolean(b) => b.to_string(),
        V::Text(t) => t.clone(),
        V::TinyInt(n) => n.to_string(),
        V::SmallInt(n) => n.to_string(),
        V::Int(n) => n.to_string(),
        V::BigInt(n) => n.to_string(),
        V::HugeInt(n) => n.to_string(),
        V::UTinyInt(n) => n.to_string(),
        V::USmallInt(n) => n.to_string(),
        V::UInt(n) => n.to_string(),
        V::UBigInt(n) => n.to_string(),
        V::Float(f) => (*f as f64).to_string(),
        V::Double(f) => f.to_string(),
        V::Decimal(d) => d.normalize().to_string(),
        other => format!("{other:?}"),
    }
}

fn duck_rows(conn: &duckdb::Connection, sql: &str) -> anyhow::Result<Vec<String>> {
    let mut stmt = conn.prepare(sql)?;
    let mut q = stmt.query([])?;
    let mut out = Vec::new();
    while let Some(r) = q.next()? {
        let mut cells = Vec::new();
        let mut i = 0;
        while let Ok(v) = r.get::<_, duckdb::types::Value>(i) {
            cells.push(duck_cell(&v));
            i += 1;
        }
        out.push(cells.join("\u{1f}"));
    }
    out.sort();
    Ok(out)
}

/// The timed DuckDB call. Arrow out, like `df_run`'s `collect`: `duck_rows` boxes, formats and sorts
/// every cell, which charged 600k rows of client work to DuckDB alone.
fn duck_arrow(conn: &duckdb::Connection, sql: &str) -> anyhow::Result<usize> {
    let mut stmt = conn.prepare(sql)?;
    Ok(stmt.query_arrow([])?.map(|b| b.num_rows()).sum())
}

fn df_rows(batches: &[RecordBatch]) -> anyhow::Result<Vec<String>> {
    let mut out = Vec::new();
    for b in batches {
        let cols: Vec<Vec<String>> = b
            .columns()
            .iter()
            .map(|c| -> anyhow::Result<Vec<String>> {
                let mut v = Vec::with_capacity(c.len());
                for i in 0..c.len() {
                    if c.is_null(i) {
                        v.push("NULL".into());
                        continue;
                    }
                    v.push(match c.data_type() {
                        DataType::Decimal128(_, 0) => c.as_primitive::<Decimal128Type>().value(i).to_string(),
                        DataType::Float64 => c.as_primitive::<Float64Type>().value(i).to_string(),
                        DataType::Float32 => (c.as_primitive::<Float32Type>().value(i) as f64).to_string(),
                        _ => datafusion::arrow::util::display::array_value_to_string(c, i)?,
                    });
                }
                Ok(v)
            })
            .collect::<anyhow::Result<_>>()?;
        for i in 0..b.num_rows() {
            out.push(cols.iter().map(|c| c[i].as_str()).collect::<Vec<_>>().join("\u{1f}"));
        }
    }
    out.sort();
    Ok(out)
}

/// Where a DataFusion run's time went. Wall for the two planning phases; the rest are the plan's
/// own metrics summed over partitions, and since file opens are async the sums overlap and can
/// exceed the wall total several times over. They rank costs; they do not add up to the total.
#[derive(Default, Clone, Copy)]
struct Breakdown {
    plan: Duration,
    physical: Duration,
    exec: Duration,
    meta: Duration,
    open: Duration,
    scan: Duration,
    agg_join: Duration,
    other: Duration,
    bytes: usize,
}

impl Breakdown {
    fn total(&self) -> Duration {
        self.plan + self.physical + self.exec
    }
    fn dominant(&self) -> &'static str {
        // Planning is wall time and the metrics are CPU sums; compare each against the wall it
        // could at most have taken.
        let cpu = |d: Duration| d.as_secs_f64().min(self.exec.as_secs_f64());
        let mut best = ("plan", self.plan.as_secs_f64());
        for (name, v) in [
            ("listing", self.physical.as_secs_f64()),
            ("footers", cpu(self.meta)),
            ("open", cpu(self.open - self.meta.min(self.open))),
            ("scan", cpu(self.scan)),
            ("agg/join", cpu(self.agg_join)),
            ("other", cpu(self.other)),
        ] {
            if v > best.1 {
                best = (name, v);
            }
        }
        best.0
    }
}

fn walk_metrics(plan: &Arc<dyn ExecutionPlan>, b: &mut Breakdown) {
    if let Some(m) = plan.metrics() {
        let m = m.aggregate_by_name();
        let ns = |n: &str| Duration::from_nanos(m.sum_by_name(n).map(|v| v.as_usize() as u64).unwrap_or(0));
        if plan.name() == "DataSourceExec" {
            b.meta += ns("metadata_load_time");
            b.open += ns("time_elapsed_opening");
            b.scan += ns("time_elapsed_scanning_total");
            b.bytes += m.sum_by_name("bytes_scanned").map(|v| v.as_usize()).unwrap_or(0);
        } else {
            let ec = Duration::from_nanos(m.elapsed_compute().unwrap_or(0) as u64);
            if plan.name().contains("Aggregate") || plan.name().contains("Join") {
                b.agg_join += ec;
            } else {
                b.other += ec;
            }
        }
    }
    for c in plan.children() {
        walk_metrics(c, b);
    }
}

enum DfErr {
    Plan(String),
    Exec(String),
}

async fn df_run(ctx: &SessionContext, stmt: &sq::Statement) -> Result<(LogicalPlan, Vec<RecordBatch>, Breakdown), DfErr> {
    let state = ctx.state();
    let mut b = Breakdown::default();
    let t = Instant::now();
    let df_stmt = datafusion::sql::parser::Statement::Statement(Box::new(stmt.clone()));
    let logical = state.statement_to_plan(df_stmt).await.map_err(|e| DfErr::Plan(e.to_string()))?;
    b.plan = t.elapsed();
    let t = Instant::now();
    let physical = state.create_physical_plan(&logical).await.map_err(|e| DfErr::Plan(e.to_string()))?;
    b.physical = t.elapsed();
    let t = Instant::now();
    let batches = collect(physical.clone(), state.task_ctx()).await.map_err(|e| DfErr::Exec(e.to_string()))?;
    b.exec = t.elapsed();
    walk_metrics(&physical, &mut b);
    Ok((logical, batches, b))
}

fn median(mut v: Vec<u128>) -> u128 {
    v.sort_unstable();
    v[v.len() / 2]
}

fn first_line(s: &str) -> String {
    let l = s.lines().next().unwrap_or("");
    let l = l.strip_prefix("Error during planning: ").unwrap_or(l);
    l.chars().take(90).collect()
}

/// A view that ran on DataFusion with parity, kept so the diagnosis can run it again.
struct Runnable {
    pub(crate) name: String,
    stmt: sq::Statement,
    duck_ms: u128,
    df_ms: u128,
}

// ---------------------------------------------------------------------------------------------
// The run

pub async fn run(nest_dir: &str) -> anyhow::Result<()> {
    let root = Path::new(nest_dir);
    let nest = load_nest(root)?;
    let budget = crate::oracles::thread_budget();
    let sealed = nest.tables.iter().filter(|t| !t.files.is_empty()).count();
    let segments: usize = nest.tables.iter().map(|t| t.files.len()).sum();
    println!(
        "{}: {} statements in {} view files, {} tables ({} sealed over {} segments, {} declared and empty){}",
        root.display(),
        nest.views.len(),
        nest.views.iter().map(|v| &v.file).collect::<BTreeSet<_>>().len(),
        nest.tables.len(),
        sealed,
        segments,
        nest.tables.len() - sealed,
        if nest.wants_dec { ", _dec companions synthesised" } else { "" }
    );

    // The deliberate DataFusion configuration. Statistics are off because DuckDB collects none at
    // plan time either and a fair scan is the question; the footer cache is sized so that every
    // segment on the nest fits, because a cache that evicts under the working set is the same as
    // no cache with extra steps. The diagnosis below runs DataFusion's own defaults for comparison.
    let main_cfg = DfConfig { provider: Provider::Listing, collect_statistics: false, metadata_cache_bytes: 1 << 30 };
    let duck_version: String = duckdb::Connection::open_in_memory()?
        .query_row("SELECT library_version FROM pragma_version()", [], |r| r.get(0))?;
    println!(
        "duckdb {duck_version}: threads={budget} parquet_metadata_cache=true, explicit file lists\ndatafusion {}: target_partitions={budget} \
         collect_statistics={} metadata_cache={}MiB ({}) enable_ident_normalization=false schema_force_view_types=true\n",
        datafusion::DATAFUSION_VERSION,
        main_cfg.collect_statistics,
        main_cfg.metadata_cache_bytes >> 20,
        main_cfg.label().trim()
    );

    let duck = duck_session(&nest, budget)?;
    let df = df_session(&main_cfg, &nest, budget).await?;
    println!("registered in {} ms\n", df.register_ms);

    println!(
        "{:<34} {:<10} {:<20} {:>7} {:>8} {:>8} {:>6}  {:<8} plan/list wall, footers/open/scan/agg+join/other summed ms",
        "statement", "outcome", "rewrites", "rows", "duck_ms", "df_ms", "ratio", "dominant"
    );

    let mut runnable: Vec<Runnable> = Vec::new();
    let mut failed: BTreeSet<String> = BTreeSet::new();
    let mut rule_counts: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut outcomes: BTreeMap<String, usize> = BTreeMap::new();
    fn note(outcomes: &mut BTreeMap<String, usize>, name: &str, outcome: &str, rules: &str, detail: String) {
        *outcomes.entry(outcome.to_string()).or_default() += 1;
        println!("{:<34} {:<10} {:<20} {}", name, outcome, rules, detail);
    }

    for v in &nest.views {
        // DuckDB first, untimed; it is also the oracle, so a view it refuses is not a comparison.
        let d_rows = match duck_rows(&duck, &v.body) {
            Ok(r) => r,
            Err(e) => {
                note(&mut outcomes, &v.name, "duck fail", "", first_line(&e.to_string()));
                failed.insert(v.name.clone());
                continue;
            }
        };
        duck.execute_batch(&v.text)?;

        let (stmt, rules) = match translate(&v.text) {
            Translated::Ok { stmt, rules } => (stmt, rules),
            Translated::ParseFail(e) => {
                note(&mut outcomes, &v.name, "parse fail", "", e);
                failed.insert(v.name.clone());
                continue;
            }
        };
        for r in &rules {
            *rule_counts.entry(r).or_default() += 1;
        }
        let rules_s = if rules.is_empty() { "-".to_string() } else { rules.join(",") };

        let (logical, batches, _) = match df_run(&df.ctx, &stmt).await {
            Ok(x) => x,
            Err(e) => {
                let (kind, msg) = match e {
                    DfErr::Plan(m) => ("plan fail", m),
                    DfErr::Exec(m) => ("exec fail", m),
                };
                // A dependent of a view that already failed is that failure again, not a new one.
                let cascade = failed.iter().find(|f| msg.contains(f.as_str())).cloned();
                match cascade {
                    Some(parent) => note(&mut outcomes, &v.name, "cascade", &rules_s, format!("via {parent}")),
                    None => note(&mut outcomes, &v.name, kind, &rules_s, first_line(&msg)),
                }
                failed.insert(v.name.clone());
                continue;
            }
        };
        let f_rows = df_rows(&batches)?;
        if d_rows != f_rows {
            let diff = d_rows
                .iter()
                .zip(f_rows.iter())
                .find(|(a, b)| a != b)
                .map(|(a, b)| format!("duck[{}] df[{}]", a.replace('\u{1f}', "|"), b.replace('\u{1f}', "|")))
                .unwrap_or_default();
            note(&mut outcomes, &v.name, "mismatch", &rules_s, format!("{} rows vs {}: {}", d_rows.len(), f_rows.len(), diff));
            failed.insert(v.name.clone());
            continue;
        }
        df.ctx.register_table(&v.name, Arc::new(ViewTable::new(logical, None)))?;

        // Parity held, so time it: both warmed by the parity pass, three interleaved repeats.
        let (mut ds, mut fs) = (Vec::new(), Vec::new());
        let mut last = Breakdown::default();
        for _ in 0..REPEATS {
            let t = Instant::now();
            let n = duck_arrow(&duck, &v.body)?;
            ds.push(t.elapsed().as_millis());
            anyhow::ensure!(n == d_rows.len(), "{}: DuckDB returned {n} rows timed, {} at parity", v.name, d_rows.len());
            let (_, _, b) = df_run(&df.ctx, &stmt).await.map_err(|e| match e {
                DfErr::Plan(m) | DfErr::Exec(m) => anyhow::anyhow!("{}: repeat failed: {m}", v.name),
            })?;
            fs.push(b.total().as_millis());
            last = b;
        }
        let (dm, fm) = (median(ds), median(fs));
        *outcomes.entry("parity".into()).or_default() += 1;
        println!(
            "{:<34} {:<10} {:<20} {:>7} {:>8} {:>8} {:>6.2}  {:<8} {}/{}/{}/{}/{}/{}/{}",
            v.name,
            "parity",
            rules_s,
            d_rows.len(),
            dm,
            fm,
            fm as f64 / dm.max(1) as f64,
            last.dominant(),
            last.plan.as_millis(),
            last.physical.as_millis(),
            last.meta.as_millis(),
            last.open.as_millis(),
            last.scan.as_millis(),
            last.agg_join.as_millis(),
            last.other.as_millis(),
        );
        runnable.push(Runnable { name: v.name.clone(), stmt, duck_ms: dm, df_ms: fm });
    }

    let (sd, sf): (u128, u128) = runnable.iter().fold((0, 0), |(a, b), r| (a + r.duck_ms, b + r.df_ms));
    println!(
        "\n{}/{} statements run on DataFusion with parity; time-weighted ratio {:.2} ({} ms DataFusion against {} ms DuckDB)",
        runnable.len(),
        nest.views.len(),
        sf as f64 / sd.max(1) as f64,
        sf,
        sd
    );
    println!(
        "outcomes: {}",
        outcomes.iter().map(|(k, n)| format!("{k}={n}")).collect::<Vec<_>>().join(" ")
    );
    println!(
        "rewrite rules: {}",
        if rule_counts.is_empty() {
            "none".to_string()
        } else {
            rule_counts.iter().map(|(k, n)| format!("{k}={n}")).collect::<Vec<_>>().join(" ")
        }
    );
    println!("  hugeint    CAST(x AS HUGEINT) -> CAST(x AS DECIMAL(38,0)), the same 128-bit width");
    println!("  intdiv     a // b -> CAST((a - a % b) / b AS DECIMAL(38,0)), exact where decimal division would round");
    println!("  tuple-cmp  (a, b) > (c, d) -> a > c OR (a = c AND b > d)");

    if runnable.is_empty() {
        return Ok(());
    }
    drop(df);
    diagnose(&nest, &runnable, budget).await
}

/// The worst ratios, re-run under every remedy on the list, against DataFusion's own defaults.
async fn diagnose(nest: &Nest, runnable: &[Runnable], budget: usize) -> anyhow::Result<()> {
    let worst_n = std::env::var("DF_WORST").ok().and_then(|s| s.parse().ok()).unwrap_or(6);
    // A statement DuckDB answers in a millisecond has a ratio made of timer noise.
    let mut worst: Vec<&Runnable> = runnable.iter().filter(|r| r.duck_ms >= 20).collect();
    worst.sort_by(|a, b| {
        let ra = a.df_ms as f64 / a.duck_ms.max(1) as f64;
        let rb = b.df_ms as f64 / b.duck_ms.max(1) as f64;
        rb.partial_cmp(&ra).unwrap()
    });
    worst.truncate(worst_n);

    if worst.is_empty() {
        return Ok(());
    }
    println!(
        "\nDIAGNOSIS: the {} worst ratios (DuckDB >= 20 ms) under each remedy, median of {REPEATS} after a warm-up, df_ms per statement",
        worst.len()
    );
    println!("  DataFusion's defaults first (ListingTable, statistics on, 50 MiB footer cache); each row changes one thing.");
    let configs = [
        DfConfig { provider: Provider::Listing, collect_statistics: true, metadata_cache_bytes: 50 << 20 },
        DfConfig { provider: Provider::Listing, collect_statistics: false, metadata_cache_bytes: 50 << 20 },
        DfConfig { provider: Provider::Listing, collect_statistics: false, metadata_cache_bytes: 0 },
        DfConfig { provider: Provider::Listing, collect_statistics: false, metadata_cache_bytes: 1 << 30 },
        DfConfig { provider: Provider::Listing, collect_statistics: true, metadata_cache_bytes: 1 << 30 },
        DfConfig { provider: Provider::Files { groups: budget }, collect_statistics: false, metadata_cache_bytes: 1 << 30 },
        DfConfig { provider: Provider::Files { groups: 1 }, collect_statistics: false, metadata_cache_bytes: 1 << 30 },
        DfConfig { provider: Provider::Files { groups: budget }, collect_statistics: false, metadata_cache_bytes: 0 },
        DfConfig { provider: Provider::Morsels { groups: budget }, collect_statistics: false, metadata_cache_bytes: 0 },
    ];

    let mut header = format!("{:<34} {:>7}", "configuration", "reg_ms");
    for w in &worst {
        header.push_str(&format!(" {:>21}", w.name.chars().take(21).collect::<String>()));
    }
    header.push_str(&format!(" {:>7} {:>6}", "sum_ms", "ratio"));
    println!("{header}");
    let mut duck_line = format!("{:<34} {:>7}", "duckdb", "");
    for w in &worst {
        duck_line.push_str(&format!(" {:>21}", w.duck_ms));
    }
    duck_line.push_str(&format!(" {:>7} {:>6}", worst.iter().map(|w| w.duck_ms).sum::<u128>(), "1.00"));
    println!("{duck_line}");

    let mut baseline: Option<u128> = None;
    for cfg in &configs {
        let df = match df_session(cfg, nest, budget).await {
            Ok(d) => d,
            Err(e) => {
                println!("{:<34} failed to register: {}", cfg.label(), first_line(&e.to_string()));
                continue;
            }
        };
        // Views are re-registered in order so dependents resolve, exactly as in the main pass.
        for r in runnable {
            match df_run(&df.ctx, &r.stmt).await {
                Ok((logical, _, _)) => df.ctx.register_table(&r.name, Arc::new(ViewTable::new(logical, None)))?,
                Err(DfErr::Plan(m) | DfErr::Exec(m)) => anyhow::bail!("{}: {} under {}", r.name, first_line(&m), cfg.label()),
            };
        }
        let mut line = format!("{:<34} {:>7}", cfg.label(), df.register_ms);
        let mut sum = 0u128;
        let mut detail = String::new();
        for w in &worst {
            let mut samples = Vec::new();
            let mut last = Breakdown::default();
            for _ in 0..REPEATS {
                let (_, _, b) = df_run(&df.ctx, &w.stmt).await.map_err(|e| match e {
                    DfErr::Plan(m) | DfErr::Exec(m) => anyhow::anyhow!("{}: {m}", w.name),
                })?;
                samples.push(b.total().as_millis());
                last = b;
            }
            let m = median(samples);
            sum += m;
            line.push_str(&format!(" {:>21}", m));
            detail.push_str(&format!(
                " {:>21}",
                format!(
                    "{}/{}/{}/{}/{}",
                    last.plan.as_millis() + last.physical.as_millis(),
                    last.meta.as_millis(),
                    last.open.as_millis(),
                    last.scan.as_millis(),
                    last.agg_join.as_millis() + last.other.as_millis()
                )
            ));
        }
        let base = *baseline.get_or_insert(sum);
        line.push_str(&format!(" {:>7} {:>6.2}", sum, sum as f64 / base.max(1) as f64));
        println!("{line}");
        println!("{:<34} {:>7}{detail}", "  plan+list/footers/open/scan/rest", "");
    }
    println!("\nratio is against the first row; plan+list is wall, the rest are per-file elapsed summed over {budget} partitions and overlap under async I/O");
    Ok(())
}
