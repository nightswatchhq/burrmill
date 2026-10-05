//! `ComputedKeyFilters`: a hash join keeps its dynamic filter only where the scan reads the keys
//! as stored columns.
//!
//! DataFusion 55 evaluates a join's filter at the probe scan for every row it reads, and a
//! partitioned join's filter names each key four times: routed by the repartition hash, then
//! bounded below, above, and looked up in the build's table. Over a stored column that is a few
//! comparisons, and a selective build pays for it. Over a key computed at the scan, a cast or text
//! parsed as a number, the computation runs four times a row before the join runs it again, and no
//! statistic covers it, so no row group is ever skipped for it. nuthatch's QoS views join on such
//! keys, every probe row matches, and the filter doubled six of their statements.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{Column, DynamicFilterPhysicalExpr};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::joins::HashJoinExec;
use datafusion_session::PhysicalOptimizerRule;

#[derive(Debug, Default)]
pub struct ComputedKeyFilters;

/// The dynamic filters some operator below a join holds over a key that is not a column there.
fn computed(plan: &Arc<dyn ExecutionPlan>) -> Result<HashSet<u64>> {
    let mut ids = HashSet::new();
    plan.apply(|node| {
        node.apply_expressions(&mut |root| {
            root.apply(|e| {
                if let Some(f) = e.downcast_ref::<DynamicFilterPhysicalExpr>()
                    && f.children().iter().any(|k| !k.is::<Column>())
                    && let Some(id) = e.expression_id()
                {
                    ids.insert(id);
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(ids)
}

impl PhysicalOptimizerRule for ComputedKeyFilters {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ids = computed(&plan)?;
        if ids.is_empty() {
            return Ok(plan);
        }
        let mut dropped = HashSet::new();
        let plan = plan
            .transform_up(|p| {
                let Some(j) = p.downcast_ref::<HashJoinExec>() else {
                    return Ok(Transformed::no(p));
                };
                let produced: Vec<u64> = p
                    .dynamic_expressions_produced()
                    .iter()
                    .filter_map(|f| f.expression_id())
                    .collect();
                if !produced.iter().any(|id| ids.contains(id)) {
                    return Ok(Transformed::no(p));
                }
                dropped.extend(produced);
                // Without its filter the join never fills the scans' copies, which stay `true`.
                Ok(Transformed::yes(j.builder().reset_state().build_exec()?))
            })?
            .data;
        // Finished, so a scan stops watching them: one still in progress gets a row-group pruner
        // in every file the scan opens.
        plan.apply(|node| {
            node.apply_expressions(&mut |root| {
                root.apply(|e| {
                    if let Some(f) = e.downcast_ref::<DynamicFilterPhysicalExpr>()
                        && e.expression_id().is_some_and(|id| dropped.contains(&id))
                    {
                        f.mark_complete();
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(plan)
    }

    fn name(&self) -> &str {
        "computed_key_filters"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_catalog::Session;
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion_datasource::file_scan_config::FileScanConfig;
    use datafusion_datasource::source::DataSourceExec;
    use datafusion_physical_expr::expressions::DynamicFilterTracking;
    use datafusion_physical_plan::ExecutionPlan;

    use crate::df::Engine;

    fn segment(dir: &std::path::Path, name: &str, n: Vec<i64>) -> (std::path::PathBuf, u64) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("n_text", DataType::Utf8, false),
        ]));
        let text: Vec<String> = n.iter().map(|x| x.to_string()).collect();
        let columns: Vec<arrow::array::ArrayRef> = vec![
            Arc::new(Int64Array::from(n)),
            Arc::new(StringArray::from(text)),
        ];
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let path = dir.join(format!("{name}-{:064x}.parquet", 0));
        let f = std::fs::File::create(&path).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let len = std::fs::metadata(&path).unwrap().len();
        (path, len)
    }

    fn plan(sql: &str) -> Arc<dyn ExecutionPlan> {
        let tmp = tempfile::tempdir().unwrap();
        let mut e = Engine::open_empty_budgeted(crate::Budget {
            memory_bytes: 256 << 20,
            threads: 4,
            spill: None,
        })
        .unwrap();
        let rows = segment(tmp.path(), "rows", (0..20_000).collect());
        let keys = segment(tmp.path(), "keys", (0..20_000).step_by(10).collect());
        e.register_facts("rows", &[], vec![rows], &[], (None, None))
            .unwrap();
        e.register_facts("keys", &[], vec![keys], &[], (None, None))
            .unwrap();
        let logical = super::super::plan_query(&e.session, sql).unwrap();
        e.runtime()
            .block_on(e.session.create_physical_plan(&logical))
            .unwrap()
    }

    /// Whether some scan in `plan` waits on a dynamic filter that could still change.
    fn scan_watches(plan: &Arc<dyn ExecutionPlan>) -> bool {
        let mut watching = false;
        plan.apply(|p| {
            if let Some(scan) = p.downcast_ref::<DataSourceExec>()
                && let Some(config) = scan.data_source().downcast_ref::<FileScanConfig>()
                && let Some(predicate) = config.file_source().filter()
            {
                watching |= matches!(
                    DynamicFilterTracking::classify(&predicate),
                    DynamicFilterTracking::Watching(_)
                );
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        watching
    }

    /// A scan watching a filter nobody will fill keeps a row-group pruner per file it opens.
    #[test]
    fn a_dropped_filter_is_finished_at_the_scan() {
        let computed = "SELECT count(*) FROM rows r JOIN keys k ON k.n = CAST(r.n_text AS BIGINT)";
        assert!(!scan_watches(&plan(computed)));
        let column = "SELECT count(*) FROM rows r JOIN keys k ON k.n = r.n";
        assert!(scan_watches(&plan(column)));
    }
}
