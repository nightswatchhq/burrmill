//! Compacts the view columns of every input to a sort-preserving merge, and of the side a join
//! collects, under a memory budget.
//!
//! A string view keeps the buffer its bytes live in, and a sort's merge of spilled runs emits rows
//! whose views point into every 128 KiB read chunk they came from. DataFusion 55's
//! `SortPreservingMergeExec` charges each input batch for all of those chunks, so 128 rows of
//! 42-byte addresses cost 15 MB in the pool and a two-thread `ORDER BY` refused under 64 MiB
//! where the same sort over `Utf8` spilled and finished. Copying the rows out first charges what
//! they hold.
//!
//! A join's collected side meets it by another route: `RepartitionExec` coalesces through arrow's
//! `BatchCoalescer`, whose copy buffer doubles per batch to 1 MiB and stays there, so each 128-row
//! batch of addresses arrives in a 1 MiB buffer and a hash join is charged that much per batch.

use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::execution_plan::replace_children_if_necessary;
use datafusion_physical_plan::joins::{CrossJoinExec, HashJoinExec, NestedLoopJoinExec};
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_session::PhysicalOptimizerRule;
use futures::StreamExt;

use super::rangejoin::RangeJoinExec;

#[derive(Debug)]
pub(super) struct CompactViewsExec {
    inner: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for CompactViewsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CompactViewsExec")
    }
}

impl ExecutionPlan for CompactViewsExec {
    fn name(&self) -> &str {
        "CompactViewsExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            inner: children.pop().expect("one child"),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.inner.execute(partition, ctx)?;
        let schema = stream.schema();
        let compacted = stream.map(|item| item.and_then(compact));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, compacted)))
    }
}

fn compact(batch: RecordBatch) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| match c.data_type() {
            DataType::Utf8View => Arc::new(c.as_string_view().gc()) as ArrayRef,
            DataType::BinaryView => Arc::new(c.as_binary_view().gc()),
            _ => Arc::clone(c),
        })
        .collect();
    RecordBatch::try_new(batch.schema(), columns).map_err(DataFusionError::from)
}

fn has_views(p: &Arc<dyn ExecutionPlan>) -> bool {
    p.schema()
        .fields()
        .iter()
        .any(|f| matches!(f.data_type(), DataType::Utf8View | DataType::BinaryView))
}

/// Whether `p` holds its `i`th input whole before it emits: every input of a merge, the side a
/// join collects.
fn holds(p: &Arc<dyn ExecutionPlan>, i: usize) -> bool {
    p.downcast_ref::<SortPreservingMergeExec>().is_some()
        || (i == 0
            && (p.downcast_ref::<HashJoinExec>().is_some()
                || p.downcast_ref::<NestedLoopJoinExec>().is_some()
                || p.downcast_ref::<CrossJoinExec>().is_some()
                || p.downcast_ref::<RangeJoinExec>().is_some()))
}

/// Puts [`CompactViewsExec`] under each input a merge or a join holds that has view columns.
#[derive(Debug)]
pub(super) struct CompactHeldViews;

impl PhysicalOptimizerRule for CompactHeldViews {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| {
            let wrap = |i: usize, c: &Arc<dyn ExecutionPlan>| holds(&p, i) && has_views(c);
            if !p
                .children()
                .into_iter()
                .enumerate()
                .any(|(i, c)| wrap(i, c))
            {
                return Ok(Transformed::no(p));
            }
            let children = p
                .children()
                .into_iter()
                .enumerate()
                .map(|(i, c)| {
                    if wrap(i, c) {
                        Arc::new(CompactViewsExec {
                            inner: Arc::clone(c),
                        }) as Arc<dyn ExecutionPlan>
                    } else {
                        Arc::clone(c)
                    }
                })
                .collect();
            Ok(Transformed::yes(replace_children_if_necessary(
                p, children,
            )?))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "compact_held_views"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
