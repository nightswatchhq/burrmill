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
        plan.transform_up(|p| {
            let Some(j) = p.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(p));
            };
            let held = p
                .dynamic_expressions_produced()
                .iter()
                .any(|f| f.expression_id().is_some_and(|id| ids.contains(&id)));
            if !held {
                return Ok(Transformed::no(p));
            }
            // Without its filter the join never fills the scans' copies, which stay `true`.
            Ok(Transformed::yes(j.builder().reset_state().build_exec()?))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "computed_key_filters"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
