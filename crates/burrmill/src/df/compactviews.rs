//! Compacts the view columns of every operator output that can carry more buffer than rows, under
//! a memory budget, so nothing downstream holds a batch charged for bytes it does not use.
//!
//! A string view keeps the buffer its bytes live in, and the pool charges a batch for every buffer
//! it references. Three producers make that far larger than the rows at a 128-row batch. A Parquet
//! scan's batches point into whole decoded pages, about 1 MiB of addresses per column. Arrow's
//! `BatchCoalescer`, behind `RepartitionExec`, `FilterExec` and the joins' output, copies into a
//! buffer that doubles per batch to 1 MiB and stays there, so 5 KB of addresses arrive in 1 MiB;
//! a hash join's build side and a sort's input held thousands of them and refused at 1.8 GB. A
//! sort's merge of spilled runs emits views into every 128 KiB read chunk they came from, which cost
//! a two-thread `ORDER BY` 15 MB per 128 rows. Copying the rows out charges what they hold.

use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::source::DataSourceExec;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::async_func::AsyncFuncExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::{
    HashJoinExec, NestedLoopJoinExec, PiecewiseMergeJoinExec, SortMergeJoinExec,
};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_session::PhysicalOptimizerRule;
use futures::StreamExt;

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

/// Whether `p` builds its output batches in a `BatchCoalescer`, reads them out of whole pages, or a
/// batch it emits may point into a spilled run's read chunks.
fn bloats(p: &Arc<dyn ExecutionPlan>) -> bool {
    p.downcast_ref::<RepartitionExec>().is_some()
        || p.downcast_ref::<FilterExec>().is_some()
        || p.downcast_ref::<HashJoinExec>().is_some()
        || p.downcast_ref::<NestedLoopJoinExec>().is_some()
        || p.downcast_ref::<SortMergeJoinExec>().is_some()
        || p.downcast_ref::<PiecewiseMergeJoinExec>().is_some()
        || p.downcast_ref::<AsyncFuncExec>().is_some()
        || p.downcast_ref::<SortExec>().is_some()
        || p.downcast_ref::<DataSourceExec>().is_some()
}

/// Puts [`CompactViewsExec`] over every operator that [`bloats`] and emits view columns.
#[derive(Debug)]
pub(super) struct CompactViews;

impl PhysicalOptimizerRule for CompactViews {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| {
            if !bloats(&p) || !has_views(&p) {
                return Ok(Transformed::no(p));
            }
            Ok(Transformed::yes(
                Arc::new(CompactViewsExec { inner: p }) as Arc<dyn ExecutionPlan>
            ))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "compact_views"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
