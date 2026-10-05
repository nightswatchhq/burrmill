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
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{Column, DynamicFilterPhysicalExpr};
use datafusion_physical_expr::utils::{conjunction, split_conjunction};
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
        // Off the scans too: a predicate left holding only those still builds a row filter, and the
        // reader decodes the filter's columns apart from the rest for every row.
        let plan = plan
            .transform_up(|p| {
                let Some(scan) = p.downcast_ref::<DataSourceExec>() else {
                    return Ok(Transformed::no(p));
                };
                let Some(config) = scan.data_source().downcast_ref::<FileScanConfig>() else {
                    return Ok(Transformed::no(p));
                };
                let Some(source) = config.file_source().downcast_ref::<ParquetSource>() else {
                    return Ok(Transformed::no(p));
                };
                let Some(predicate) = source.filter() else {
                    return Ok(Transformed::no(p));
                };
                let all = split_conjunction(&predicate);
                let kept: Vec<_> = all
                    .iter()
                    .filter(|c| {
                        !(c.is::<DynamicFilterPhysicalExpr>()
                            && c.expression_id().is_some_and(|id| dropped.contains(&id)))
                    })
                    .map(|c| Arc::clone(c))
                    .collect();
                if kept.len() == all.len() {
                    return Ok(Transformed::no(p));
                }
                // `true` alone would still be evaluated row by row; nothing is left to push down.
                let source = match kept.is_empty() {
                    true => source
                        .with_predicate(conjunction(kept))
                        .with_pushdown_filters(false),
                    false => source.with_predicate(conjunction(kept)),
                };
                let config = FileScanConfigBuilder::from(config.clone())
                    .with_source(Arc::new(source))
                    .build();
                Ok(Transformed::yes(
                    DataSourceExec::from_data_source(config) as _
                ))
            })?
            .data;
        Ok(plan)
    }

    fn name(&self) -> &str {
        "computed_key_filters"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
