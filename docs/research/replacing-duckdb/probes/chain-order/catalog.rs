//! Nest tables as DataFusion providers: explicit file lists, sizes known, empty if unsealed.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::{DFSchema, Result as DFResult};
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::utils::conjunction;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::ExecutionPlan;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};

pub(super) fn view_schema(schema: &Schema) -> Schema {
    let fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                Arc::new(Field::new(f.name(), DataType::Utf8View, f.is_nullable()))
            }
            DataType::Binary | DataType::LargeBinary => {
                Arc::new(Field::new(f.name(), DataType::BinaryView, f.is_nullable()))
            }
            _ => Arc::clone(f),
        })
        .collect();
    Schema::new(fields)
}

/// One nest table: a known list of sealed files, or empty if declared and not yet sealed.
#[derive(Debug, Clone)]
pub struct NestTable {
    pub name: String,
    pub schema: SchemaRef,
    pub files: Vec<(PathBuf, u64)>,
    pub wide: Vec<String>,
}

/// Discover `<table>-<hash>.parquet` under `segments/`, last hyphen splitting table from hash.
pub fn discover_tables(segments: &Path) -> crate::Result<Vec<NestTable>> {
    let mut by_table: HashMap<String, Vec<(PathBuf, u64)>> = HashMap::new();
    for e in std::fs::read_dir(segments)?.flatten() {
        let p = e.path();
        let Some(stem) = p
            .file_name()
            .and_then(|f| f.to_str())
            .and_then(|f| f.strip_suffix(".parquet"))
        else {
            continue;
        };
        let Some((table, _)) = stem.rsplit_once('-') else {
            continue;
        };
        let size = e.metadata()?.len();
        by_table
            .entry(table.to_string())
            .or_default()
            .push((p, size));
    }
    let mut tables = Vec::new();
    for (name, mut files) in by_table {
        files.sort_by(|a, b| a.0.cmp(&b.0));
        let f = std::fs::File::open(&files[0].0)?;
        let meta = ArrowReaderMetadata::load(&f, ArrowReaderOptions::new())?;
        let schema = Arc::new(view_schema(meta.schema()));
        tables.push(NestTable {
            name,
            schema,
            files,
            wide: Vec::new(),
        });
    }
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(tables)
}

/// Apply `schema.json` declarations: unsealed tables become empty, `word16`/`word32` get `_dec`.
pub fn apply_schema_json(tables: &mut Vec<NestTable>, schema_json: &Path) -> crate::Result<()> {
    let raw = std::fs::read_to_string(schema_json)?;
    let doc: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|e| crate::BurrmillError::Substrate(format!("schema.json: {e}")))?;
    let Some(arr) = doc["tables"].as_array() else {
        return Ok(());
    };
    let mut declared: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for t in arr {
        let Some(name) = t["table"].as_str() else {
            continue;
        };
        let cols = t["columns"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|c| {
                Some((
                    c["name"].as_str()?.to_string(),
                    c["storage"].as_str()?.to_string(),
                ))
            })
            .collect();
        declared.insert(name.to_string(), cols);
    }
    for (name, cols) in &declared {
        let wide: Vec<String> = cols
            .iter()
            .filter(|(_, s)| *s == "word16" || *s == "word32")
            .map(|(n, _)| n.clone())
            .collect();
        if let Some(t) = tables.iter_mut().find(|t| t.name == *name) {
            t.wide = wide;
            continue;
        }
        let mut fields: Vec<Field> = cols
            .iter()
            .map(|(n, s)| match s.as_str() {
                "u64" => Field::new(n, DataType::UInt64, false),
                _ => Field::new(n, DataType::Utf8View, true),
            })
            .collect();
        fields.push(Field::new("table", DataType::Utf8View, true));
        tables.push(NestTable {
            name: name.clone(),
            schema: Arc::new(Schema::new(fields)),
            files: Vec::new(),
            wide,
        });
    }
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(())
}

#[derive(Debug)]
pub struct SegmentTable {
    schema: SchemaRef,
    files: Vec<PartitionedFile>,
    groups: usize,
    /// The engine's token; every scan of this table stops at it.
    cancel: crate::CancelToken,
    ordered: bool,
}

impl SegmentTable {
    pub fn new(
        table: &NestTable,
        groups: usize,
        cancel: crate::CancelToken,
    ) -> crate::Result<Self> {
        let files = table
            .files
            .iter()
            .map(|(p, size)| {
                let loc = object_store::path::Path::from_filesystem_path(p)
                    .map_err(|e| crate::BurrmillError::Substrate(e.to_string()))?;
                Ok(PartitionedFile::new(loc.to_string(), *size))
            })
            .collect::<crate::Result<Vec<_>>>()?;
        let ordered = std::env::var_os("BM_ORDERED").is_some() && table.schema.index_of("block_number").is_ok() && table.schema.index_of("log_index").is_ok();
        let mut files = files;
        if ordered {
            let mut keyed = Vec::with_capacity(files.len());
            let range = |md: &parquet::file::metadata::ParquetMetaData, name: &str| -> Option<(u64, u64)> {
                let ci = md.file_metadata().schema_descr().columns().iter().position(|c| c.name() == name)?;
                let mut lo = u64::MAX;
                let mut hi = 0u64;
                for rg in md.row_groups() {
                    match rg.column(ci).statistics()? {
                        parquet::file::statistics::Statistics::Int64(v) => {
                            lo = lo.min(*v.min_opt()? as u64);
                            hi = hi.max(*v.max_opt()? as u64);
                        }
                        _ => return None,
                    }
                }
                Some((lo, hi))
            };
            for (f, (p, _)) in files.into_iter().zip(table.files.iter()) {
                let file = std::fs::File::open(p)?;
                let meta = parquet::arrow::arrow_reader::ArrowReaderMetadata::load(&file, Default::default())
                    .map_err(|e| crate::BurrmillError::Substrate(e.to_string()))?;
                let md = meta.metadata();
                let bn = range(md, "block_number");
                let li = range(md, "log_index");
                let lo = bn.map(|r| r.0).unwrap_or(u64::MAX);
                let mut f = f;
                if let (Some(bn), Some(li)) = (bn, li) {
                    use datafusion_common::stats::Precision;
                    use datafusion_common::{ColumnStatistics, ScalarValue, Statistics};
                    let mut cols = vec![ColumnStatistics::new_unknown(); table.schema.fields().len()];
                    for (name, (a, b)) in [("block_number", bn), ("log_index", li)] {
                        let i = table.schema.index_of(name).expect("checked above");
                        cols[i] = ColumnStatistics::new_unknown()
                            .with_min_value(Precision::Exact(ScalarValue::UInt64(Some(a))))
                            .with_max_value(Precision::Exact(ScalarValue::UInt64(Some(b))));
                    }
                    let st = Statistics { num_rows: Precision::Absent, total_byte_size: Precision::Absent, column_statistics: cols };
                    f = f.with_statistics(std::sync::Arc::new(st));
                }
                keyed.push((lo, f));
            }
            keyed.sort_by_key(|(lo, _)| *lo);
            files = keyed.into_iter().map(|(_, f)| f).collect();
        }
        Ok(Self {
            schema: table.schema.clone(),
            files,
            groups: groups.max(1),
            cancel,
            ordered,
        })
    }
}

#[async_trait]
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
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let url = ObjectStoreUrl::local_filesystem();
        let mut opts = datafusion_common::config::TableParquetOptions {
            global: state.config_options().execution.parquet.clone(),
            ..Default::default()
        };
        opts.global.pushdown_filters = true;
        let mut source = ParquetSource::new(self.schema.clone()).with_table_parquet_options(opts);
        if let Some(pred) = conjunction(filters.iter().cloned()) {
            let df_schema = DFSchema::try_from(self.schema.as_ref().clone())?;
            source = source.with_predicate(state.create_physical_expr(pred, &df_schema)?);
        }

        // An ordered table is one stream: split into runs, an ordered merge above would hold every
        // run but the first while it drains that one.
        let n = if self.ordered { 1 } else { self.groups.min(self.files.len().max(1)) };
        let mut groups: Vec<Vec<PartitionedFile>> = vec![Vec::new(); n];
        if self.ordered {
            groups[0] = self.files.clone();
        } else {
            for (i, f) in self.files.iter().enumerate() {
                groups[i % n].push(f.clone());
            }
        }
        let mut ordering = Vec::new();
        if self.ordered {
            use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr, expressions::col};
            let opts = arrow::compute::SortOptions { descending: false, nulls_first: false };
            let lex = LexOrdering::new(vec![
                PhysicalSortExpr::new(col("block_number", &self.schema)?, opts),
                PhysicalSortExpr::new(col("log_index", &self.schema)?, opts),
            ]);
            ordering.extend(lex);
        }
        let file_source: Arc<dyn FileSource> = Arc::new(source);
        let cfg = FileScanConfigBuilder::new(url, file_source)
            .with_output_ordering(ordering)
            .with_file_groups(groups.into_iter().map(FileGroup::new).collect())
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .build();
        Ok(super::cancel::CancelExec::wrap(
            DataSourceExec::from_data_source(cfg),
            self.cancel.clone(),
        ))
    }
}
