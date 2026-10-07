//! `NarrowJoins`: an `ORDER BY ... LIMIT` or a `LIMIT` over left joins keeps its rows from the
//! preserved side first, and each joined side reads only the keys those rows carry.
//!
//! A nest's entity view is a chain of left joins from one row per entity, so a page of it derived
//! every entity before the limit applied (nuthatch#1951). The rows a left join keeps are its
//! preserved side's, each at least once, so the top `n` of the join come from the top `n` of that
//! side: those are computed once, and every other side of the chain is semi-joined to the keys they
//! carry, pushed as far down as it can go. Ties at the `n`th key are broken as arbitrarily as before.
//!
//! It runs once, after the optimizer, so the keys are the joins' own equi-keys. A filter between
//! the limit and the joins stops it. A shared subquery is not narrowed: other readers want all of
//! it, so a key filter stops above its reference.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion_catalog::Session;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, DFSchemaRef, NullEquality, Result, internal_err};
use datafusion_execution::TaskContext;
use datafusion_expr::logical_plan::{
    Aggregate, Distinct, Extension, Filter, Join, JoinConstraint, Limit, Projection, Sort,
    SubqueryAlias, Window,
};
use datafusion_expr::{
    Expr, JoinType, LogicalPlan, LogicalPlanBuilder, SortExpr, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore,
};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_session::{ExtensionPlanner, PhysicalPlanner};

use super::sharing::{Definitions, with_definitions};

/// The most rows a narrowed side keeps. Past this the semi joins cost more than they save.
pub(super) const MOST_ROWS: usize = 10_000;

pub(super) fn narrow(plan: LogicalPlan) -> Result<LogicalPlan> {
    with_definitions(plan, &mut |p, defs| {
        let mut cx = Cx {
            defs,
            sources: Vec::new(),
            fresh: 0,
        };
        // Outermost first: the optimizer copies a limit into each left side, and only the
        // statement's own decides which rows are kept. Not into subqueries: one still correlated
        // cannot have its rows computed once for the statement.
        p.transform_down(|n| cx.at(n)).map(|t| t.data)
    })
}

struct Cx<'a> {
    defs: &'a mut Definitions,
    /// References to the rows narrowed so far, each computed once: the kept rows of each chain and
    /// each joined side read through their keys. A later join of the chain takes its keys from them.
    sources: Vec<LogicalPlan>,
    fresh: usize,
}

impl Cx<'_> {
    fn at(&mut self, n: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let (input, order, rows) = match &n {
            LogicalPlan::Sort(Sort {
                expr,
                input,
                fetch: Some(f),
            }) if *f <= MOST_ROWS && !expr.iter().any(|s| s.expr.is_volatile()) => {
                (input, Some(expr.as_slice()), *f)
            }
            LogicalPlan::Limit(l) => {
                use datafusion_expr::logical_plan::{FetchType, SkipType};
                let (Ok(SkipType::Literal(skip)), Ok(FetchType::Literal(Some(fetch)))) =
                    (l.get_skip_type(), l.get_fetch_type())
                else {
                    return Ok(Transformed::no(n));
                };
                match skip.checked_add(fetch) {
                    Some(r) if r <= MOST_ROWS => (&l.input, None, r),
                    _ => return Ok(Transformed::no(n)),
                }
            }
            _ => return Ok(Transformed::no(n)),
        };
        let Some(narrowed) = self.descend(input, order, rows)? else {
            return Ok(Transformed::no(n));
        };
        let input = Arc::new(narrowed);
        let n = match n {
            LogicalPlan::Sort(s) => LogicalPlan::Sort(Sort { input, ..s }),
            LogicalPlan::Limit(l) => LogicalPlan::Limit(Limit { input, ..l }),
            _ => unreachable!("matched above"),
        };
        Ok(Transformed::new(n, true, TreeNodeRecursion::Jump))
    }

    /// `plan` with a left join beneath it narrowed to the rows the top `rows` by `order` can come
    /// from, or `None` if there is none to narrow.
    fn descend(
        &mut self,
        plan: &LogicalPlan,
        order: Option<&[SortExpr]>,
        rows: usize,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Projection(p) => {
                let order = match order {
                    Some(o) => match through_each(o, |e| through_projection(e, p)) {
                        Some(o) => Some(o),
                        None => return Ok(None),
                    },
                    None => None,
                };
                let Some(input) = self.descend(&p.input, order.as_deref(), rows)? else {
                    return Ok(None);
                };
                Ok(Some(LogicalPlan::Projection(
                    Projection::try_new_with_schema(
                        p.expr.clone(),
                        Arc::new(input),
                        Arc::clone(&p.schema),
                    )?,
                )))
            }
            LogicalPlan::SubqueryAlias(s) => {
                let order = match order {
                    Some(o) => match through_each(o, |e| through_alias(e, s)) {
                        Some(o) => Some(o),
                        None => return Ok(None),
                    },
                    None => None,
                };
                let Some(input) = self.descend(&s.input, order.as_deref(), rows)? else {
                    return Ok(None);
                };
                Ok(Some(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                    Arc::new(input),
                    s.alias.clone(),
                )?)))
            }
            // The optimizer's copy of a limit: any `rows` of what it keeps are any `rows` of its input.
            LogicalPlan::Limit(l) if order.is_none() => {
                use datafusion_expr::logical_plan::{FetchType, SkipType};
                let as_many = matches!(
                    (l.get_skip_type(), l.get_fetch_type()),
                    (Ok(SkipType::Literal(0)), Ok(FetchType::Literal(Some(f)))) if f >= rows
                );
                if !as_many {
                    return Ok(None);
                }
                let Some(input) = self.descend(&l.input, None, rows)? else {
                    return Ok(None);
                };
                Ok(Some(LogicalPlan::Limit(Limit {
                    input: Arc::new(input),
                    ..l.clone()
                })))
            }
            LogicalPlan::Join(j)
                if j.join_type == JoinType::Left
                    && order.is_none_or(|o| o.iter().all(|s| over(&s.expr, j.left.schema()))) =>
            {
                let left = match self.descend(&j.left, order, rows)? {
                    Some(l) => l,
                    None => self.keep(j.left.as_ref().clone(), order, rows)?,
                };
                let right = match self.reduce(&left, j)? {
                    Some(r) => {
                        let r = self.defs.define(r);
                        self.sources.push(r.clone());
                        Arc::new(r)
                    }
                    None => Arc::clone(&j.right),
                };
                Ok(Some(LogicalPlan::Join(Join::try_new(
                    Arc::new(narrowed(left)),
                    right,
                    j.on.clone(),
                    j.filter.clone(),
                    j.join_type,
                    j.join_constraint,
                    j.null_equality,
                    j.null_aware,
                )?)))
            }
            _ => Ok(None),
        }
    }

    /// The top `rows` of `plan`, computed once.
    fn keep(
        &mut self,
        plan: LogicalPlan,
        order: Option<&[SortExpr]>,
        rows: usize,
    ) -> Result<LogicalPlan> {
        let kept = match order {
            Some(o) => LogicalPlan::Sort(Sort {
                expr: o.to_vec(),
                input: Arc::new(plan),
                fetch: Some(rows),
            }),
            None => LogicalPlanBuilder::from(plan)
                .limit(0, Some(rows))?
                .build()?,
        };
        let reference = self.defs.define(kept);
        self.sources.push(reference.clone());
        Ok(reference)
    }

    /// `j`'s right side reading only the keys `left`, its narrowed left side, carries, or `None`
    /// when no key of the join can be traced to rows already narrowed. A join that matches NULL to
    /// NULL is left whole: the keys are matched as a semi join matches them, NULL to nothing.
    fn reduce(&mut self, left: &LogicalPlan, j: &Join) -> Result<Option<LogicalPlan>> {
        if j.null_equality != NullEquality::NullEqualsNothing {
            return Ok(None);
        }
        let mut source: Option<LogicalPlan> = None;
        let mut pairs: Vec<(Expr, Expr)> = Vec::new();
        for (l, r) in &j.on {
            let Some((from, e)) = self.trace(l, left) else {
                continue;
            };
            match &source {
                None => source = Some(from),
                Some(s) if *s == from => {}
                Some(_) => continue,
            }
            pairs.push((e, r.clone()));
        }
        let Some(source) = source else {
            return Ok(None);
        };
        let alias = format!("__burrmill_keys_{}", self.fresh);
        self.fresh += 1;
        let names: Vec<String> = (0..pairs.len()).map(|i| format!("k{i}")).collect();
        let keys = LogicalPlan::Projection(Projection::try_new(
            pairs
                .iter()
                .zip(&names)
                .map(|((e, _), n)| e.clone().alias(n))
                .collect(),
            Arc::new(source),
        )?);
        // Distinct: a key repeated in the build side is walked once per probe row that hits it.
        let keys = LogicalPlanBuilder::from(keys)
            .aggregate(
                names
                    .iter()
                    .map(|n| Expr::Column(Column::new_unqualified(n))),
                Vec::<Expr>::new(),
            )?
            .build()?;
        let keys = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(narrowed(keys)),
            alias.as_str(),
        )?);
        let on: Vec<(Expr, Expr)> = pairs
            .into_iter()
            .zip(&names)
            .map(|((_, r), n)| (Expr::Column(Column::new(Some(alias.as_str()), n)), r))
            .collect();
        semi(&j.right, on, &keys).map(Some)
    }

    /// Where `e`, over the columns of `plan` as [`Cx::descend`] built it, comes from: rows already
    /// narrowed, and `e` over their columns. A limit between only drops rows, so the keys found may
    /// be more than `plan` holds, never fewer, and a left join's padding NULLs match nothing.
    fn trace(&self, e: &Expr, plan: &LogicalPlan) -> Option<(LogicalPlan, Expr)> {
        if e.is_volatile() {
            return None;
        }
        if self.sources.contains(plan) {
            return Some((plan.clone(), e.clone()));
        }
        if let Some(input) = under_narrowed(plan) {
            return self.trace(e, input);
        }
        match plan {
            LogicalPlan::Projection(p) => self.trace(&through_projection(e, p)?, &p.input),
            LogicalPlan::SubqueryAlias(s) => self.trace(&through_alias(e, s)?, &s.input),
            LogicalPlan::Limit(l) => self.trace(e, &l.input),
            LogicalPlan::Join(j) if j.join_type == JoinType::Left => {
                if over(e, j.left.schema()) {
                    self.trace(e, &j.left)
                } else if over(e, j.right.schema()) {
                    self.trace(e, &j.right)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

/// `plan` keeping only rows whose `on` right-hand keys appear in `keys`, pushed below whatever the
/// keys pass through unchanged.
fn semi(plan: &LogicalPlan, on: Vec<(Expr, Expr)>, keys: &LogicalPlan) -> Result<LogicalPlan> {
    let mine =
        |on: &[(Expr, Expr)], f: &dyn Fn(&Expr) -> Option<Expr>| -> Option<Vec<(Expr, Expr)>> {
            on.iter()
                .map(|(k, e)| f(e).map(|e| (k.clone(), e)))
                .collect()
        };
    let all_over = |on: &[(Expr, Expr)], s: &DFSchema| on.iter().all(|(_, e)| over(e, s));
    match plan {
        LogicalPlan::Projection(p) => {
            if let Some(on) = mine(&on, &|e| through_projection(e, p))
                && !on.iter().any(|(_, e)| e.is_volatile())
            {
                let input = semi(&p.input, on, keys)?;
                return Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
                    p.expr.clone(),
                    Arc::new(input),
                    Arc::clone(&p.schema),
                )?));
            }
        }
        LogicalPlan::SubqueryAlias(s) => {
            if let Some(on) = mine(&on, &|e| through_alias(e, s)) {
                let input = semi(&s.input, on, keys)?;
                return Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                    Arc::new(input),
                    s.alias.clone(),
                )?));
            }
        }
        LogicalPlan::Filter(f) => {
            let input = semi(&f.input, on, keys)?;
            return Ok(LogicalPlan::Filter(Filter::try_new(
                f.predicate.clone(),
                Arc::new(input),
            )?));
        }
        LogicalPlan::Sort(s) if s.fetch.is_none() => {
            let input = semi(&s.input, on, keys)?;
            return Ok(LogicalPlan::Sort(Sort {
                input: Arc::new(input),
                ..s.clone()
            }));
        }
        LogicalPlan::Distinct(Distinct::All(input)) => {
            let input = semi(input, on, keys)?;
            return Ok(LogicalPlan::Distinct(Distinct::All(Arc::new(input))));
        }
        LogicalPlan::Aggregate(a) => {
            if let Some(on) = mine(&on, &|e| through_groups(e, a)) {
                let input = semi(&a.input, on, keys)?;
                return Ok(LogicalPlan::Aggregate(Aggregate::try_new_with_schema(
                    Arc::new(input),
                    a.group_expr.clone(),
                    a.aggr_expr.clone(),
                    Arc::clone(&a.schema),
                )?));
            }
        }
        LogicalPlan::Window(w) if on.iter().all(|(_, e)| partitions_every(e, w)) => {
            let input = semi(&w.input, on, keys)?;
            return Ok(LogicalPlan::Window(Window::try_new_with_schema(
                w.window_expr.clone(),
                Arc::new(input),
                Arc::clone(&w.schema),
            )?));
        }
        LogicalPlan::Join(j) => {
            let left = matches!(
                j.join_type,
                JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti
            ) && all_over(&on, j.left.schema());
            let right = matches!(
                j.join_type,
                JoinType::Inner | JoinType::Right | JoinType::RightSemi | JoinType::RightAnti
            ) && all_over(&on, j.right.schema());
            if left || right {
                let (l, r) = match left {
                    true => (Arc::new(semi(&j.left, on, keys)?), Arc::clone(&j.right)),
                    false => (Arc::clone(&j.left), Arc::new(semi(&j.right, on, keys)?)),
                };
                return Ok(LogicalPlan::Join(Join::try_new(
                    l,
                    r,
                    j.on.clone(),
                    j.filter.clone(),
                    j.join_type,
                    j.join_constraint,
                    j.null_equality,
                    j.null_aware,
                )?));
            }
        }
        _ => {}
    }
    Ok(LogicalPlan::Join(Join::try_new(
        Arc::new(keys.clone()),
        Arc::new(plan.clone()),
        on,
        None,
        JoinType::RightSemi,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?))
}

/// Rows derived from those a limit kept, or their keys: the side a hash join should build on.
/// Planned as [`NarrowedExec`], which passes its input through.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct Narrowed {
    input: LogicalPlan,
}

fn narrowed(input: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Extension(Extension {
        node: Arc::new(Narrowed { input }),
    })
}

fn under_narrowed(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::Extension(e) => e.node.as_any().downcast_ref::<Narrowed>().map(|n| &n.input),
        _ => None,
    }
}

impl UserDefinedLogicalNodeCore for Narrowed {
    fn name(&self) -> &str {
        "Narrowed"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Narrowed")
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            input: inputs.swap_remove(0),
        })
    }
}

#[derive(Debug)]
pub struct NarrowedPlanner;

#[async_trait]
impl ExtensionPlanner for NarrowedPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session: &dyn Session,
        _ctx: &datafusion_expr::physical_planning_context::PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if !node.as_any().is::<Narrowed>() {
            return Ok(None);
        }
        let [input] = physical_inputs else {
            return internal_err!("Narrowed takes one input");
        };
        Ok(Some(Arc::new(NarrowedExec {
            input: Arc::clone(input),
        })))
    }
}

/// [`Narrowed`]'s input, unchanged.
#[derive(Debug)]
pub struct NarrowedExec {
    input: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for NarrowedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NarrowedExec")
    }
}

impl ExecutionPlan for NarrowedExec {
    fn name(&self) -> &str {
        "NarrowedExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
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
        Ok(Arc::new(NarrowedExec {
            input: children.swap_remove(0),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, ctx)
    }
}

/// Whether every column `e` reads is one of `schema`'s.
fn over(e: &Expr, schema: &DFSchema) -> bool {
    e.column_refs().iter().all(|c| schema.has_column(c))
}

fn through_each(order: &[SortExpr], f: impl Fn(&Expr) -> Option<Expr>) -> Option<Vec<SortExpr>> {
    order
        .iter()
        .map(|s| {
            let e = f(&s.expr)?;
            (!e.is_volatile()).then(|| SortExpr {
                expr: e,
                ..s.clone()
            })
        })
        .collect()
}

/// `e` with each column of `p`'s output replaced by the expression that computes it.
fn through_projection(e: &Expr, p: &Projection) -> Option<Expr> {
    substitute(e, |c| {
        let i = p.schema.index_of_column(c).ok()?;
        Some(p.expr[i].clone().unalias_nested().data)
    })
}

/// `e` over the alias's input, column for column.
fn through_alias(e: &Expr, s: &SubqueryAlias) -> Option<Expr> {
    substitute(e, |c| {
        let i = s.schema.index_of_column(c).ok()?;
        Some(Expr::Column(Column::from(
            s.input.schema().qualified_field(i),
        )))
    })
}

/// `e` over the aggregate's input, when it reads only grouping columns.
fn through_groups(e: &Expr, a: &Aggregate) -> Option<Expr> {
    if a.group_expr
        .iter()
        .any(|g| matches!(g, Expr::GroupingSet(_)))
    {
        return None;
    }
    substitute(e, |c| {
        let i = a.schema.index_of_column(c).ok()?;
        (i < a.group_expr.len()).then(|| a.group_expr[i].clone().unalias_nested().data)
    })
}

/// Whether every column `e` reads partitions every window function of `w`, so a row's window
/// value depends only on rows sharing its key.
fn partitions_every(e: &Expr, w: &Window) -> bool {
    use datafusion_expr::expr::WindowFunction;
    let cols = e.column_refs();
    !cols.is_empty()
        && cols.iter().all(|c| {
            w.input.schema().has_column(c)
                && w.window_expr
                    .iter()
                    .all(|we| match we.clone().unalias_nested().data {
                        Expr::WindowFunction(f) => {
                            let WindowFunction { params, .. } = *f;
                            params
                                .partition_by
                                .iter()
                                .any(|p| matches!(p, Expr::Column(pc) if pc == *c))
                        }
                        _ => false,
                    })
        })
}

fn substitute(e: &Expr, mut f: impl FnMut(&Column) -> Option<Expr>) -> Option<Expr> {
    let mut ok = true;
    let out = e
        .clone()
        .transform(|x| match &x {
            Expr::Column(c) => match f(c) {
                Some(r) => Ok(Transformed::yes(r)),
                None => {
                    ok = false;
                    Ok(Transformed::no(x))
                }
            },
            _ => Ok(Transformed::no(x)),
        })
        .ok()?
        .data;
    ok.then_some(out)
}
