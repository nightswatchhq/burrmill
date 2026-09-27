//! Tables a host registers itself, one at a time, on an engine opened empty.
//!
//! `open_nest` reads a directory. Nuthatch's `/sql` binds each table from a segment list it has
//! already filtered (the sealed watermark, segments failing verification, a historical window) and
//! its unsealed rows, and it rebinds on every statement against a cached session. Its shadow needs
//! the same, so these take exactly those inputs and replace whatever the name held before.
//!
//! A table registered this way is not in the fold substitution's table map, which is fixed when the
//! engine opens; its aggregates run as DataFusion plans them, to the same answer.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringViewArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_catalog::default_table_source::provider_as_source;
use datafusion_catalog::view::ViewTable;
use datafusion_catalog::{MemTable, TableProvider};
use datafusion_expr::LogicalPlanBuilder;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use serde_json::Value;

use super::catalog::{NestTable, SegmentTable, view_schema};
use super::{Engine, df_err, plan_query};
use crate::error::Result;
use crate::limits::Limits;

/// The four counter columns nuthatch seals as `UBIGINT`; every other column is canonical text.
fn counter(name: &str) -> bool {
    matches!(
        name,
        "block_number" | "log_index" | "_seq" | "block_timestamp"
    )
}

fn hidden(name: &str) -> bool {
    name.ends_with("__raw") || name.ends_with("__hot") || name.ends_with("__union")
}

impl Engine {
    /// No tables until the host registers them.
    pub fn open_empty() -> Result<Self> {
        Self::from_tables(Vec::new(), Limits::default().max_threads)
    }

    /// Define `name` over `files`, unioned by name with `hot` (unsealed rows as nuthatch keeps
    /// them: JSON objects, counters as numbers, everything else text), every column in `declared`
    /// present and NULL where no input carries it, `_dec` and `_overflow` beside each `word16` or
    /// `word32` column, and only rows with `after < block_number <= through` when a bound is given.
    /// Replaces an earlier `name`.
    pub fn register_facts(
        &mut self,
        name: &str,
        declared: &[(String, String)],
        files: Vec<(PathBuf, u64)>,
        hot: &[Value],
        window: (Option<u64>, Option<u64>),
    ) -> Result<()> {
        let raw = format!("{name}__raw");
        let raw_schema: SchemaRef = if files.is_empty() {
            Arc::new(Schema::new(
                declared
                    .iter()
                    .map(|(c, _)| Field::new(c, column_type(c), true))
                    .collect::<Vec<_>>(),
            ))
        } else {
            let f = std::fs::File::open(&files[0].0)?;
            let meta = ArrowReaderMetadata::load(&f, ArrowReaderOptions::new())?;
            Arc::new(view_schema(meta.schema()))
        };
        let raw_provider: Arc<dyn TableProvider> = if files.is_empty() {
            Arc::new(MemTable::try_new(raw_schema.clone(), vec![vec![]]).map_err(df_err)?)
        } else {
            Arc::new(SegmentTable::new(
                &NestTable {
                    name: raw.clone(),
                    schema: raw_schema.clone(),
                    files,
                    wide: Vec::new(),
                },
                self.threads,
                self.cancel.clone(),
            )?)
        };
        self.session.register_table(&raw, Arc::clone(&raw_provider));

        let mut present: Vec<String> = raw_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let mut source = raw;
        if !hot.is_empty() {
            let hot_name = format!("{name}__hot");
            let (schema, batch) = json_batch(hot, true)?;
            present.extend(schema.fields().iter().map(|f| f.name().clone()));
            let hot_provider: Arc<dyn TableProvider> =
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).map_err(df_err)?);
            self.session
                .register_table(&hot_name, Arc::clone(&hot_provider));
            let plan =
                LogicalPlanBuilder::scan(source.as_str(), provider_as_source(raw_provider), None)
                    .and_then(|b| {
                        b.union_by_name(
                            LogicalPlanBuilder::scan(
                                hot_name.as_str(),
                                provider_as_source(hot_provider),
                                None,
                            )?
                            .build()?,
                        )
                    })
                    .and_then(|b| b.build())
                    .map_err(df_err)?;
            source = format!("{name}__union");
            self.session
                .register_table(&source, Arc::new(ViewTable::new(plan, None)));
        }

        let stubs: String = declared
            .iter()
            .filter(|(c, _)| !present.contains(c))
            .map(|(c, _)| format!(", CAST(NULL AS {}) AS \"{c}\"", sql_type(c)))
            .collect();
        let dec: String = declared
            .iter()
            .filter(|(_, s)| s == "word16" || s == "word32")
            .map(|(c, _)| {
                format!(
                    ", TRY_CAST(\"{c}\" AS DECIMAL(38,0)) AS \"{c}_dec\", \
                     (\"{c}\" IS NOT NULL AND TRY_CAST(\"{c}\" AS DECIMAL(38,0)) IS NULL) \
                     AS \"{c}_overflow\""
                )
            })
            .collect();
        let bound = match window {
            (None, None) => String::new(),
            (Some(a), None) => format!(" WHERE block_number > {a}"),
            (None, Some(t)) => format!(" WHERE block_number <= {t}"),
            (Some(a), Some(t)) => {
                format!(" WHERE block_number > {a} AND block_number <= {t}")
            }
        };
        let sql = format!("SELECT *{dec} FROM (SELECT *{stubs} FROM \"{source}\"){bound}");
        let logical = plan_query(&self.session, &sql)?;
        self.session
            .register_table(name, Arc::new(ViewTable::new(logical, Some(sql))));
        self.session
            .build_information_schema(hidden)
            .map_err(df_err)
    }

    /// A table of text columns from JSON rows, for the small side inputs a nest carries beside its
    /// segments (label snapshots). Replaces an earlier `name`.
    pub fn register_rows(&mut self, name: &str, rows: &[Value]) -> Result<()> {
        let (schema, batch) = json_batch(rows, false)?;
        self.session.register_table(
            name,
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).map_err(df_err)?),
        );
        self.session
            .build_information_schema(hidden)
            .map_err(df_err)
    }

    /// Whether `name` is a table or view the host or the nest defined.
    pub fn has_table(&self, name: &str) -> bool {
        let lower = name.to_ascii_lowercase();
        self.visible_tables()
            .iter()
            .any(|t| t.to_ascii_lowercase() == lower)
    }

    /// Every table and view except the parts a registration is built from, sorted.
    pub fn visible_tables(&self) -> Vec<String> {
        self.session
            .table_names()
            .into_iter()
            .filter(|t| !hidden(t))
            .collect()
    }
}

fn column_type(name: &str) -> DataType {
    if counter(name) {
        DataType::UInt64
    } else {
        DataType::Utf8View
    }
}

fn sql_type(name: &str) -> &'static str {
    if counter(name) { "UBIGINT" } else { "VARCHAR" }
}

/// One batch from JSON objects: the columns are the sorted union of the keys. With `typed`, the
/// counter columns are `UInt64` (0 where absent, as nuthatch seals them); every other column is
/// text, the string itself or the JSON value rendered, NULL where absent or null.
fn json_batch(rows: &[Value], typed: bool) -> Result<(SchemaRef, RecordBatch)> {
    let mut columns: std::collections::BTreeSet<String> = Default::default();
    for r in rows {
        if let Some(o) = r.as_object() {
            columns.extend(o.keys().cloned());
        }
    }
    if columns.is_empty() {
        return Err(crate::BurrmillError::NotAllowed(
            "rows have no columns".into(),
        ));
    }
    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for c in &columns {
        if typed && counter(c) {
            fields.push(Field::new(c, DataType::UInt64, false));
            arrays.push(Arc::new(UInt64Array::from(
                rows.iter()
                    .map(|r| r.get(c).and_then(Value::as_u64).unwrap_or(0))
                    .collect::<Vec<_>>(),
            )));
        } else {
            fields.push(Field::new(c, DataType::Utf8View, true));
            arrays.push(Arc::new(StringViewArray::from(
                rows.iter()
                    .map(|r| match r.get(c) {
                        None | Some(Value::Null) => None,
                        Some(Value::String(s)) => Some(s.clone()),
                        Some(other) => Some(other.to_string()),
                    })
                    .collect::<Vec<Option<String>>>(),
            )));
        }
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| crate::BurrmillError::Substrate(e.to_string()))?;
    Ok((schema, batch))
}
