//! A scan that stops when the engine's token is cancelled.
//!
//! DataFusion's operators do not yield inside themselves (apache/datafusion#19358): an aggregate
//! over a join produces nothing until it has consumed everything, so a check between output
//! batches sees the cancel only at the end. Every nest table is read through this wrapper, so a
//! cancelled statement dries up at its source within one scan batch, and whatever is above it fails
//! with the input.

use std::fmt;
use std::sync::Arc;

use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;

use crate::CancelToken;

#[derive(Debug)]
pub(super) struct CancelExec {
    inner: Arc<dyn ExecutionPlan>,
    token: CancelToken,
}

impl CancelExec {
    pub(super) fn wrap(
        inner: Arc<dyn ExecutionPlan>,
        token: CancelToken,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self { inner, token })
    }
}

impl DisplayAs for CancelExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CancelExec")
    }
}

impl ExecutionPlan for CancelExec {
    fn name(&self) -> &str {
        "CancelExec"
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
            token: self.token.clone(),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.inner.execute(partition, ctx)?;
        let schema = stream.schema();
        let token = self.token.clone();
        let checked = stream.map(move |item| {
            if token.is_cancelled() {
                return Err(DataFusionError::Execution("cancelled".into()));
            }
            item
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, checked)))
    }
}
