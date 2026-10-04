//! `RangeBounds`: a hash join's range condition bounds the probe's scan by the build side's extremes.
//!
//! DuckDB pushes the minimum and maximum of a join's build side into the probe's scan for its
//! comparison conditions, not only its equalities, and nuthatch's QoS views were written against
//! that: `JOIN qos_day_bounds k ON k.day = ... AND r.start_epoch BETWEEN k.first_start_epoch AND
//! k.last_start_epoch` read one day's row groups there and all 79M rows here (nuthatch #1800). Once
//! the build side is collected, every probe row the join can emit has `probe >= min(lo)` and
//! `probe <= max(hi)`, so the scan may skip row groups outside them. A build side with no bound
//! gives a NULL one, which skips everything, as the join emits nothing.
//!
//! Taken for a collect-left join that never emits an unmatched probe row, a conjunct of its filter
//! comparing plain columns of one non-float type, and a probe column that reaches a Parquet scan
//! through columns a projection, filter, union or repartition passes along. DataFusion's own join
//! filter bounds only the equality keys, here an expression no statistic covers.

use std::fmt;
use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{JoinSide, JoinType, Result, ScalarValue};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::{Accumulator, Operator};
use datafusion_functions_aggregate::min_max::{MaxAccumulator, MinAccumulator};
use datafusion_physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, Literal, lit,
};
use datafusion_physical_expr::{PhysicalExpr, split_conjunction};
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, replace_children_if_necessary,
};
use datafusion_session::PhysicalOptimizerRule;
use futures::StreamExt;

use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::source::DataSourceExec;

#[derive(Debug, Default)]
pub struct RangeBounds;

/// `probe op build`, normalised so the probe column is on the left.
#[derive(Debug, Clone, Copy)]
struct Condition {
    probe: usize,
    build: usize,
    op: Operator,
}

/// Every probe row a join of this type emits was matched.
fn drops_unmatched_probe_rows(t: JoinType) -> bool {
    matches!(
        t,
        JoinType::Inner
            | JoinType::Left
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::LeftMark
            | JoinType::RightSemi
    )
}

fn conditions(j: &HashJoinExec) -> Vec<Condition> {
    let Some(filter) = j.filter() else {
        return Vec::new();
    };
    let (left, right) = (j.left().schema(), j.right().schema());
    let side = |e: &Arc<dyn PhysicalExpr>| {
        let c = e.downcast_ref::<Column>()?;
        let i = filter.column_indices().get(c.index())?;
        Some((i.side, i.index))
    };
    split_conjunction(filter.expression())
        .into_iter()
        .filter_map(|e| {
            let b = e.downcast_ref::<BinaryExpr>()?;
            let (l, r) = (side(b.left())?, side(b.right())?);
            let ((probe, build), op) = match (l, r) {
                ((JoinSide::Right, p), (JoinSide::Left, q)) => ((p, q), *b.op()),
                ((JoinSide::Left, q), (JoinSide::Right, p)) => ((p, q), b.op().swap()?),
                _ => return None,
            };
            if !matches!(
                op,
                Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq
            ) {
                return None;
            }
            let t = right.field(probe).data_type();
            let comparable = t == left.field(build).data_type()
                && !t.is_floating()
                && !t.is_nested()
                && *t != DataType::Null;
            comparable.then_some(Condition { probe, build, op })
        })
        .collect()
}

/// `plan` with `filter`, written over its column `col`, in each Parquet scan that column comes from.
/// A union arm that is not such a scan, the unsealed tip, is read whole.
fn attach(
    plan: &Arc<dyn ExecutionPlan>,
    col: usize,
    filter: &Arc<dyn PhysicalExpr>,
    config: &ConfigOptions,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(d) = plan.downcast_ref::<DataSourceExec>() {
        if d.data_source().downcast_ref::<FileScanConfig>().is_none() {
            return Ok(None);
        }
        let pushed = d
            .data_source()
            .try_pushdown_filters(vec![Arc::clone(filter)], config)?;
        return Ok(pushed
            .updated_node
            .map(|source| Arc::new(d.clone().with_data_source(source)) as Arc<dyn ExecutionPlan>));
    }
    let children: Vec<Arc<dyn ExecutionPlan>> =
        plan.children().into_iter().map(Arc::clone).collect();
    let below = if let Some(p) = plan.downcast_ref::<ProjectionExec>() {
        match p.expr()[col].expr.downcast_ref::<Column>() {
            Some(c) => c.index(),
            None => return Ok(None),
        }
    } else if let Some(f) = plan.downcast_ref::<FilterExec>() {
        f.projection().as_ref().map_or(col, |p| p[col])
    } else if plan.is::<RepartitionExec>()
        || plan.is::<CoalescePartitionsExec>()
        || plan.is::<UnionExec>()
        || plan.is::<super::cancel::CancelExec>()
    {
        col
    } else {
        return Ok(None);
    };
    let mut attached = false;
    let mut out = Vec::with_capacity(children.len());
    for child in children {
        let column = Arc::new(Column::new(child.schema().field(below).name(), below));
        let remapped = Arc::clone(filter).with_new_children(vec![column])?;
        match attach(&child, below, &remapped, config)? {
            Some(c) => {
                attached = true;
                out.push(c);
            }
            None => out.push(child),
        }
    }
    if !attached {
        return Ok(None);
    }
    replace_children_if_necessary(Arc::clone(plan), out).map(Some)
}

impl PhysicalOptimizerRule for RangeBounds {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| {
            let Some(j) = p.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(p));
            };
            if *j.partition_mode() != PartitionMode::CollectLeft
                || !drops_unmatched_probe_rows(*j.join_type())
                || j.null_aware
                || j.left()
                    .properties()
                    .output_partitioning()
                    .partition_count()
                    != 1
            {
                return Ok(Transformed::no(p));
            }
            let conditions = conditions(j);
            let mut probe = Arc::clone(j.right());
            let mut bounds = Vec::new();
            let mut cols: Vec<usize> = conditions.iter().map(|c| c.probe).collect();
            cols.sort_unstable();
            cols.dedup();
            for col in cols {
                let column: Arc<dyn PhysicalExpr> =
                    Arc::new(Column::new(probe.schema().field(col).name(), col));
                let filter = Arc::new(DynamicFilterPhysicalExpr::new(
                    vec![Arc::clone(&column)],
                    lit(true),
                ));
                let Some(with) = attach(&probe, col, &(Arc::clone(&filter) as _), config)? else {
                    continue;
                };
                probe = with;
                for c in conditions.iter().filter(|c| c.probe == col) {
                    bounds.push(Bound {
                        build: c.build,
                        op: c.op,
                        filter: Arc::clone(&filter),
                        column: Arc::clone(&column),
                    });
                }
            }
            if bounds.is_empty() {
                return Ok(Transformed::no(p));
            }
            let build: Arc<dyn ExecutionPlan> = Arc::new(BoundsExec {
                input: Arc::clone(j.left()),
                bounds: Arc::new(bounds),
            });
            Ok(Transformed::yes(replace_children_if_necessary(
                Arc::clone(&p),
                vec![build, probe],
            )?))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "range_bounds"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[derive(Debug)]
struct Bound {
    build: usize,
    op: Operator,
    filter: Arc<DynamicFilterPhysicalExpr>,
    column: Arc<dyn PhysicalExpr>,
}

/// The build side, unchanged, setting each bound's filter from its extreme once it ends.
#[derive(Debug)]
struct BoundsExec {
    input: Arc<dyn ExecutionPlan>,
    bounds: Arc<Vec<Bound>>,
}

impl DisplayAs for BoundsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RangeBoundsExec: bounds={}", self.bounds.len())
    }
}

/// A lower bound (`probe > build`, `probe >= build`) takes the least build value, an upper the most.
fn accumulator(b: &Bound, t: &DataType) -> Result<Box<dyn Accumulator>> {
    Ok(match b.op {
        Operator::Gt | Operator::GtEq => Box::new(MinAccumulator::try_new(t)?),
        _ => Box::new(MaxAccumulator::try_new(t)?),
    })
}

fn publish(bounds: &[Bound], extremes: &mut [Box<dyn Accumulator>]) -> Result<()> {
    let mut by_filter: Vec<(&Arc<DynamicFilterPhysicalExpr>, Arc<dyn PhysicalExpr>)> = Vec::new();
    for (b, acc) in bounds.iter().zip(extremes.iter_mut()) {
        let value: ScalarValue = acc.evaluate()?;
        let cmp: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&b.column),
            b.op,
            Arc::new(Literal::new(value)),
        ));
        match by_filter
            .iter_mut()
            .find(|(f, _)| Arc::ptr_eq(f, &b.filter))
        {
            Some((_, e)) => {
                *e = Arc::new(BinaryExpr::new(Arc::clone(e), Operator::And, cmp));
            }
            None => by_filter.push((&b.filter, cmp)),
        }
    }
    for (f, e) in by_filter {
        f.update(e)?;
        f.mark_complete();
    }
    Ok(())
}

impl ExecutionPlan for BoundsExec {
    fn name(&self) -> &str {
        "RangeBoundsExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
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
            input: children.pop().expect("one child"),
            bounds: Arc::clone(&self.bounds),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.input.schema();
        let extremes = self
            .bounds
            .iter()
            .map(|b| accumulator(b, schema.field(b.build).data_type()))
            .collect::<Result<Vec<_>>>()?;
        let state = Arc::new(Mutex::new(extremes));
        let bounds = Arc::clone(&self.bounds);
        let seen = Arc::clone(&state);
        let rows = self.input.execute(partition, ctx)?.map(move |item| {
            let batch: RecordBatch = item?;
            let mut extremes = seen.lock().unwrap_or_else(|p| p.into_inner());
            for (b, acc) in bounds.iter().zip(extremes.iter_mut()) {
                acc.update_batch(&[Arc::clone(batch.column(b.build))])?;
            }
            Ok(batch)
        });
        let bounds = Arc::clone(&self.bounds);
        // Published only when the input ends: a build side that fails leaves the scan unbounded.
        let end = futures::stream::once(async move {
            let mut extremes = state.lock().unwrap_or_else(|p| p.into_inner());
            publish(&bounds, &mut extremes)
        })
        .filter_map(|r| async move { r.err().map(Err) });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            rows.chain(end),
        )))
    }
}
