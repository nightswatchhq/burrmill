//! A window over its whole input charges the pool for what it holds, and is not handed a cross
//! join's output one row at a time (#47).
//!
//! DataFusion's `WindowAggExec` keeps every input batch and concatenates them before it computes,
//! without a reservation, so nothing bounds it under a budget. A cross join collects its left input
//! and emits one batch per left row and right batch, which with a one-row right side is one batch
//! per row, each of whose strings is copied into a fresh 8 KiB block: `indexer.delegators_page` held
//! 1.8 GB in a window that way under a 1 GB pool.

use std::fmt;
use std::sync::Arc;

use arrow::compute::BatchCoalescer;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::joins::CrossJoinExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::windows::WindowAggExec;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, replace_children_if_necessary,
};
use datafusion_session::PhysicalOptimizerRule;
use futures::StreamExt;

/// A `WindowAggExec` whose input is charged to a reservation that lives as long as its output.
#[derive(Debug)]
pub(super) struct ChargedWindowExec {
    window: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for ChargedWindowExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ChargedWindowExec")
    }
}

impl ExecutionPlan for ChargedWindowExec {
    fn name(&self) -> &str {
        "ChargedWindowExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.window.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.window]
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
            window: children.pop().expect("one child"),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // The window drops its input stream before it concatenates, so the charge is held here.
        let reservation = Arc::new(
            MemoryConsumer::new(format!("WindowAggExec[{partition}]")).register(ctx.memory_pool()),
        );
        let input = Arc::new(ChargeExec {
            inner: Arc::clone(self.window.children()[0]),
            reservation: Arc::clone(&reservation),
        });
        let window = replace_children_if_necessary(Arc::clone(&self.window), vec![input])?;
        let stream = window.execute(partition, ctx)?;
        let schema = stream.schema();
        let held = stream.map(move |item| {
            let _ = &reservation;
            item
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, held)))
    }
}

/// Charges each batch that passes twice: once as the window keeps it, once as it is concatenated.
#[derive(Debug)]
struct ChargeExec {
    inner: Arc<dyn ExecutionPlan>,
    reservation: Arc<MemoryReservation>,
}

impl DisplayAs for ChargeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ChargeExec")
    }
}

impl ExecutionPlan for ChargeExec {
    fn name(&self) -> &str {
        "ChargeExec"
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
            reservation: Arc::clone(&self.reservation),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.inner.execute(partition, ctx)?;
        let schema = stream.schema();
        let reservation = Arc::clone(&self.reservation);
        let charged = stream.map(move |item| {
            item.and_then(|batch| {
                reservation.try_grow(2 * batch.get_array_memory_size())?;
                Ok(batch)
            })
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, charged)))
    }
}

/// A cross join's output gathered into batches of the session's batch size.
#[derive(Debug)]
pub(super) struct CoalesceExec {
    inner: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for CoalesceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CoalesceExec")
    }
}

impl ExecutionPlan for CoalesceExec {
    fn name(&self) -> &str {
        "CoalesceExec"
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
        let size = ctx.session_config().batch_size();
        let stream = self.inner.execute(partition, ctx)?;
        let schema = stream.schema();
        let coalescer = BatchCoalescer::new(Arc::clone(&schema), size);
        let out = futures::stream::unfold(
            (stream, coalescer, false),
            |(mut stream, mut c, mut done)| async move {
                loop {
                    if let Some(batch) = c.next_completed_batch() {
                        return Some((Ok(batch), (stream, c, done)));
                    }
                    if done {
                        return None;
                    }
                    let pushed = match stream.next().await {
                        Some(Ok(batch)) => c.push_batch(batch),
                        Some(Err(e)) => return Some((Err(e), (stream, c, true))),
                        None => {
                            done = true;
                            c.finish_buffered_batch()
                        }
                    };
                    if let Err(e) = pushed {
                        return Some((Err(DataFusionError::from(e)), (stream, c, true)));
                    }
                }
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, out)))
    }
}

/// Puts [`ChargedWindowExec`] over every `WindowAggExec` and [`CoalesceExec`] over every cross join.
#[derive(Debug)]
pub(super) struct HeldInput;

impl PhysicalOptimizerRule for HeldInput {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| {
            if p.downcast_ref::<WindowAggExec>().is_some() {
                return Ok(Transformed::yes(
                    Arc::new(ChargedWindowExec { window: p }) as Arc<dyn ExecutionPlan>
                ));
            }
            if p.downcast_ref::<CrossJoinExec>().is_some() {
                return Ok(Transformed::yes(
                    Arc::new(CoalesceExec { inner: p }) as Arc<dyn ExecutionPlan>
                ));
            }
            Ok(Transformed::no(p))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "held_input"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
