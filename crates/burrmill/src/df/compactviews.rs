//! Keeps every operator under a memory budget charged for the view columns' rows, not for the
//! buffers they point into.
//!
//! A string view keeps the buffer its bytes live in, and the pool charges a batch for every buffer
//! it references, which at a 128-row batch can be far more than the rows. A Parquet scan's batches
//! point into whole decoded pages; Arrow's `BatchCoalescer`, behind `RepartitionExec`, `FilterExec`
//! and the joins' output, copies into a buffer that doubles per batch to 1 MiB and stays there, so
//! 5 KB of addresses arrive in 1 MiB. A hash join's build side and a sort's input held thousands of
//! them and refused at 1.8 GB. Copying the rows out of such an output charges what they hold.
//!
//! Two operators charge inside themselves, where a copy at their output comes too late: a hash
//! repartition queues its coalesced batches against the pool, and a sort charges each run once per
//! batch it slices the run into, so a sort #40 ran in 18 MB as `Utf8` refused at 1 GiB as views.
//! They run over the same columns as `Utf8`, with views restored above them.

use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::source::DataSourceExec;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion_physical_plan::async_func::AsyncFuncExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::{
    HashJoinExec, NestedLoopJoinExec, PiecewiseMergeJoinExec, SortMergeJoinExec,
};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    replace_children_if_necessary,
};
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

/// Whether `p` builds its output batches in a `BatchCoalescer`, or reads them out of whole pages.
fn bloats(p: &Arc<dyn ExecutionPlan>) -> bool {
    p.downcast_ref::<RepartitionExec>().is_some()
        || p.downcast_ref::<FilterExec>().is_some()
        || p.downcast_ref::<HashJoinExec>().is_some()
        || p.downcast_ref::<NestedLoopJoinExec>().is_some()
        || p.downcast_ref::<SortMergeJoinExec>().is_some()
        || p.downcast_ref::<PiecewiseMergeJoinExec>().is_some()
        || p.downcast_ref::<AsyncFuncExec>().is_some()
        || p.downcast_ref::<DataSourceExec>().is_some()
}

/// Casts each column whose type differs from `props`' schema to that type. Under a hash repartition
/// or a sort it turns views into offsets, and over one turns them back with the operator's own
/// properties, so the operator charges what its rows hold.
#[derive(Debug)]
struct CastViewsExec {
    inner: Arc<dyn ExecutionPlan>,
    props: Arc<PlanProperties>,
}

impl DisplayAs for CastViewsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CastViewsExec")
    }
}

impl ExecutionPlan for CastViewsExec {
    fn name(&self) -> &str {
        "CastViewsExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
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
            props: Arc::clone(&self.props),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.inner.execute(partition, ctx)?;
        let schema = self.schema();
        let target = Arc::clone(&schema);
        let cast = stream.map(move |item| {
            item.and_then(|batch| {
                let columns = batch
                    .columns()
                    .iter()
                    .zip(target.fields())
                    .map(|(c, f)| match c.data_type() == f.data_type() {
                        true => Ok(Arc::clone(c)),
                        false => arrow::compute::cast(c, f.data_type()),
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                RecordBatch::try_new(Arc::clone(&target), columns).map_err(DataFusionError::from)
            })
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, cast)))
    }
}

/// Whether `p` charges view batches by buffers its own output shares between them: a hash
/// repartition's `BatchCoalescer` copies each partition's rows into a buffer that grows to 1 MiB and
/// queues it charged at that, and a sort slices each sorted run into batches that all keep the run.
/// Compacting the output comes too late for either, since the charge is made inside.
fn charges_inside(p: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(r) = p.downcast_ref::<RepartitionExec>() {
        return matches!(r.partitioning(), Partitioning::Hash(..)) && !r.preserve_order();
    }
    p.downcast_ref::<SortExec>().is_some()
}

/// `t` with its views, a list's included, as offsets; `None` if it holds none. An aggregate's list
/// state is the one nesting #40 found charged for buffers.
fn offsets(t: &DataType) -> Option<DataType> {
    match t {
        DataType::Utf8View => Some(DataType::Utf8),
        DataType::BinaryView => Some(DataType::Binary),
        DataType::List(f) => offsets(f.data_type())
            .map(|t| DataType::List(Arc::new(f.as_ref().clone().with_data_type(t)))),
        _ => None,
    }
}

fn holds_views(p: &Arc<dyn ExecutionPlan>) -> bool {
    p.schema()
        .fields()
        .iter()
        .any(|f| offsets(f.data_type()).is_some())
}

/// `p` over its child cast to offsets, cast back to views above it.
fn over_offsets(p: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let child = Arc::clone(p.children()[0]);
    let fields: Vec<_> = child
        .schema()
        .fields()
        .iter()
        .map(|f| match offsets(f.data_type()) {
            Some(t) => Arc::new(f.as_ref().clone().with_data_type(t)),
            None => Arc::clone(f),
        })
        .collect();
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        child.schema().metadata().clone(),
    ));
    let props = child
        .properties()
        .as_ref()
        .clone()
        .with_eq_properties(EquivalenceProperties::new(schema));
    let offsets = Arc::new(CastViewsExec {
        inner: child,
        props: Arc::new(props),
    });
    let views = Arc::clone(p.properties());
    Ok(Arc::new(CastViewsExec {
        inner: replace_children_if_necessary(p, vec![offsets])?,
        props: views,
    }))
}

/// Runs every operator that [`charges_inside`] over offsets, and puts [`CompactViewsExec`] over
/// every other operator that [`bloats`] and emits view columns.
#[derive(Debug)]
pub(super) struct CompactViews;

impl PhysicalOptimizerRule for CompactViews {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| {
            if charges_inside(&p) && holds_views(p.children()[0]) {
                return over_offsets(p).map(Transformed::yes);
            }
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
