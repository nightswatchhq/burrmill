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
//!
//! A cross join charges each batch of its left side as it collects it, and an aggregate's output
//! reaches it as slices that each keep the whole of it: 130,000 groups were charged 885 MB (#51).
//! That side is copied to what its rows hold, primitives included, before it is charged.

use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, MutableArrayData, make_array};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::source::DataSourceExec;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::expressions::{CastExpr, Column};
use datafusion_physical_expr::{
    EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
};
use datafusion_physical_plan::async_func::AsyncFuncExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PiecewiseMergeJoinExec, SortMergeJoinExec,
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
    /// Every sliced column is copied too, not only the views.
    held: bool,
}

impl DisplayAs for CompactViewsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl ExecutionPlan for CompactViewsExec {
    fn name(&self) -> &str {
        if self.held {
            "CompactHeldExec"
        } else {
            "CompactViewsExec"
        }
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
            held: self.held,
        }))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.inner.execute(partition, ctx)?;
        let schema = stream.schema();
        let held = self.held;
        let compacted = stream.map(move |item| item.and_then(|b| compact(b, held)));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, compacted)))
    }
}

fn compact(batch: RecordBatch, held: bool) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(|c| {
            Ok(match c.data_type() {
                DataType::Utf8View => Arc::new(c.as_string_view().gc()) as ArrayRef,
                DataType::BinaryView => Arc::new(c.as_binary_view().gc()),
                _ if held && c.to_data().get_slice_memory_size()? < c.get_array_memory_size() => {
                    let data = c.to_data();
                    let mut copy = MutableArrayData::new(vec![&data], false, c.len());
                    copy.try_extend(0, 0, c.len())?;
                    make_array(copy.freeze())
                }
                _ => Arc::clone(c),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    rebuilt(&batch, batch.schema(), columns)
}

/// `columns` as `batch`'s rows. A batch with no columns, a cross join's left side under `count(*)`,
/// has only its row count to say how many it holds.
fn rebuilt(batch: &RecordBatch, schema: SchemaRef, columns: Vec<ArrayRef>) -> Result<RecordBatch> {
    let rows = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema, columns, &rows).map_err(DataFusionError::from)
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
        || p.downcast_ref::<super::heldinput::CoalesceExec>().is_some()
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
                rebuilt(&batch, Arc::clone(&target), columns)
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

/// `t` with its views, a list's or a struct's included, as offsets; `None` if it holds none. An
/// aggregate's list state is the nesting #40 found charged for buffers, and an ordered aggregate's
/// keeps its sort keys as a list of structs (#74).
fn offsets(t: &DataType) -> Option<DataType> {
    match t {
        DataType::Utf8View => Some(DataType::Utf8),
        DataType::BinaryView => Some(DataType::Binary),
        DataType::List(f) => offsets(f.data_type())
            .map(|t| DataType::List(Arc::new(f.as_ref().clone().with_data_type(t)))),
        DataType::Struct(fs) if fs.iter().any(|f| offsets(f.data_type()).is_some()) => {
            Some(DataType::Struct(
                fs.iter()
                    .map(|f| match offsets(f.data_type()) {
                        Some(t) => Arc::new(f.as_ref().clone().with_data_type(t)),
                        None => Arc::clone(f),
                    })
                    .collect(),
            ))
        }
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
        inner: with_keys_as_planned(p, offsets)?,
        props: views,
    }))
}

/// Runs every operator that [`charges_inside`] over offsets, puts [`CompactViewsExec`] over every
/// other operator that [`bloats`] and emits view columns, and compacts a cross join's left side.
#[derive(Debug)]
pub(super) struct CompactViews;

impl PhysicalOptimizerRule for CompactViews {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| {
            if let Some(j) = p.downcast_ref::<CrossJoinExec>() {
                let left = Arc::new(CompactViewsExec {
                    inner: Arc::clone(j.left()),
                    held: true,
                });
                let right = Arc::clone(j.right());
                return replace_children_if_necessary(p, vec![left, right]).map(Transformed::yes);
            }
            if charges_inside(&p) && holds_views(p.children()[0]) {
                return over_offsets(p).map(Transformed::yes);
            }
            if !bloats(&p) || !has_views(&p) {
                return Ok(Transformed::no(p));
            }
            Ok(Transformed::yes(Arc::new(CompactViewsExec {
                inner: p,
                held: false,
            }) as Arc<dyn ExecutionPlan>))
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

/// `e` over its input cast to offsets, with each view column it reads inside a function cast back to
/// the type `e` was planned for. A bare column key reads offsets as they are.
fn as_planned(
    e: &Arc<dyn PhysicalExpr>,
    planned: &Schema,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    if e.downcast_ref::<Column>().is_some() {
        return Ok(Transformed::no(Arc::clone(e)));
    }
    views_as_planned(e, planned)
}

/// `e` with every view column it reads, itself included, cast back to the type it was planned for.
fn views_as_planned(
    e: &Arc<dyn PhysicalExpr>,
    planned: &Schema,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    Arc::clone(e).transform_up(|n| {
        let Some(c) = n.downcast_ref::<Column>() else {
            return Ok(Transformed::no(n));
        };
        let t = planned.field(c.index()).data_type();
        match offsets(t) {
            Some(_) => Ok(Transformed::yes(
                Arc::new(CastExpr::new(n, t.clone(), None)) as Arc<dyn PhysicalExpr>,
            )),
            None => Ok(Transformed::no(n)),
        }
    })
}

/// `p` over `child`, its keys rebuilt by [`as_planned`] against the schema they were planned for.
fn with_keys_as_planned(
    p: Arc<dyn ExecutionPlan>,
    child: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let planned = p.children()[0].schema();
    if let Some(s) = p.downcast_ref::<SortExec>() {
        let keys = s
            .expr()
            .iter()
            .map(|k| {
                Ok(as_planned(&k.expr, &planned)?
                    .update_data(|e| PhysicalSortExpr::new(e, k.options)))
            })
            .collect::<Result<Vec<_>>>()?;
        let bare_view = s.expr().iter().any(|k| {
            k.expr
                .downcast_ref::<Column>()
                .is_some_and(|c| offsets(planned.field(c.index()).data_type()).is_some())
        });
        if bare_view || keys.iter().any(|k| k.transformed) {
            // A fresh top-k filter: the scan's copy of the old one holds views where this sort's
            // thresholds are offsets, and Arrow will not compare the two. That copy stays `true`.
            let ordering =
                LexOrdering::new(keys.into_iter().map(|k| k.data)).expect("a sort has keys");
            let sort = SortExec::new(ordering, child)
                .with_preserve_partitioning(s.preserve_partitioning())
                .with_fetch(s.fetch());
            return Ok(Arc::new(sort));
        }
    }
    if let Some(r) = p.downcast_ref::<RepartitionExec>()
        && let Partitioning::Hash(keys, n) = r.partitioning()
    {
        // A bare key is hashed as its view too: Arrow hashes a view and its offsets differently, and
        // a partitioned join's other side may hash the same key as a view.
        let keys = keys
            .iter()
            .map(|k| views_as_planned(k, &planned))
            .collect::<Result<Vec<_>>>()?;
        if keys.iter().any(|k| k.transformed) {
            let keys = keys.into_iter().map(|k| k.data).collect();
            return Ok(Arc::new(RepartitionExec::try_new(
                child,
                Partitioning::Hash(keys, *n),
            )?));
        }
    }
    replace_children_if_necessary(p, vec![child])
}
