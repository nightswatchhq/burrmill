//! Correlated subqueries put into shapes DataFusion 55 decorrelates.
//!
//! `KeyedCorrelation`: a subquery correlated through an expression of its own rows.
//! DataFusion pulls a correlated `=` up over an aggregate only when the subquery's side is a
//! column, so `(SELECT count(*) FROM label l WHERE lower(l.addr) = lower(t.x))` stays correlated
//! and the physical planner refuses it. The subquery's side is computed beneath the filter as a
//! column of its own, which is the shape DataFusion already decorrelates; the filter's schema is
//! restored above it.
//!
//! `SubqueriesBelowAggregates`: DataFusion decorrelates a scalar subquery in a projection or a
//! filter, never in an aggregate's arguments, so `bool_or(EXISTS (...))` (counts, by then) over a
//! join was refused. Each is computed in a projection beneath the aggregate, whose outputs keep
//! their names.
//!
//! `NonEquiCorrelation`: an aggregate subquery correlated through anything but `inner = <outer>`
//! (`(SELECT count(*) FROM lbl x WHERE x.weight > e.amount)`), which DataFusion 55 does not
//! decorrelate at all. The outer rows are numbered, the subquery's relation LEFT JOINed on its
//! correlated conjuncts, and the aggregates taken per outer row, `count(*)` counting a marker only
//! matched rows carry so that no match is 0. Only aggregates a NULL-extended row leaves alone, and
//! only where DataFusion would refuse.

use datafusion_common::config::ConfigOptions;
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, Result};
use datafusion_expr::logical_plan::{Aggregate, Filter, JoinType, Projection};
use datafusion_expr::lit;
use datafusion_expr::utils::{conjunction, split_conjunction_owned};
use datafusion_expr::{BinaryExpr, Expr, LogicalPlan, LogicalPlanBuilder, Operator};
use datafusion_optimizer::analyzer::AnalyzerRule;

#[derive(Debug, Default)]
pub struct KeyedCorrelation;

impl AnalyzerRule for KeyedCorrelation {
    fn name(&self) -> &str {
        "keyed_correlation"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let mut n = 0;
        plan.transform_up_with_subqueries(|p| match &p {
            LogicalPlan::Filter(f) if f.predicate.contains_outer() => Ok(match keyed(f, &mut n)? {
                Some(k) => Transformed::yes(k),
                None => Transformed::no(p),
            }),
            _ => Ok(Transformed::no(p)),
        })
        .map(|t| t.data)
    }
}

fn outer_only(e: &Expr) -> bool {
    e.contains_outer() && !e.any_column_refs()
}

fn inner_only(e: &Expr) -> bool {
    !e.contains_outer() && e.any_column_refs()
}

/// What DataFusion's `can_pullup_over_aggregation` accepts as the subquery's side.
fn bare(e: &Expr) -> bool {
    match e {
        Expr::Column(_) => true,
        Expr::Cast(c) => matches!(*c.expr, Expr::Column(_)),
        _ => false,
    }
}

fn keyed(f: &Filter, n: &mut usize) -> Result<Option<LogicalPlan>> {
    let mut keys = Vec::new();
    let predicates = split_conjunction_owned(f.predicate.clone())
        .into_iter()
        .map(|e| match e {
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) => {
                let (inner, outer) = if inner_only(&left) && outer_only(&right) {
                    (left, right)
                } else if outer_only(&left) && inner_only(&right) {
                    (right, left)
                } else {
                    return Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right });
                };
                if bare(&inner) {
                    return Expr::BinaryExpr(BinaryExpr { left: inner, op: Operator::Eq, right: outer });
                }
                let name = format!("__burrmill_key{n}");
                *n += 1;
                keys.push(inner.alias(&name));
                Expr::Column(Column::new_unqualified(name)).eq(*outer)
            }
            e => e,
        })
        .collect::<Vec<_>>();
    if keys.is_empty() {
        return Ok(None);
    }
    let columns: Vec<Expr> = f.input.schema().columns().into_iter().map(Expr::Column).collect();
    let plan = LogicalPlanBuilder::from((*f.input).clone())
        .project(columns.iter().cloned().chain(keys))?
        .filter(conjunction(predicates).expect("at least one key"))?
        .project(columns)?
        .build()?;
    Ok(Some(plan))
}

#[derive(Debug, Default)]
pub struct SubqueriesBelowAggregates;

impl AnalyzerRule for SubqueriesBelowAggregates {
    fn name(&self) -> &str {
        "subqueries_below_aggregates"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let mut n = 0;
        plan.transform_up_with_subqueries(|p| match &p {
            LogicalPlan::Aggregate(a) if a.aggr_expr.iter().any(has_subquery) => {
                Ok(Transformed::yes(lifted(a, &mut n)?))
            }
            _ => Ok(Transformed::no(p)),
        })
        .map(|t| t.data)
    }
}

fn has_subquery(e: &Expr) -> bool {
    e.exists(|x| Ok(matches!(x, Expr::ScalarSubquery(_)))).unwrap_or(false)
}

fn lifted(a: &Aggregate, n: &mut usize) -> Result<LogicalPlan> {
    let mut below = Vec::new();
    let mut aggr = Vec::with_capacity(a.aggr_expr.len());
    for e in &a.aggr_expr {
        if !has_subquery(e) {
            aggr.push(e.clone());
            continue;
        }
        let name = e.schema_name().to_string();
        let e = e
            .clone()
            .transform_up(|x| match x {
                Expr::ScalarSubquery(_) => {
                    let col = format!("__burrmill_sq{n}");
                    *n += 1;
                    below.push(x.alias(&col));
                    Ok(Transformed::yes(Expr::Column(Column::new_unqualified(col))))
                }
                x => Ok(Transformed::no(x)),
            })?
            .data;
        aggr.push(match e {
            Expr::Alias(al) => Expr::Alias(al),
            e => e.alias(name),
        });
    }
    let columns = a.input.schema().columns().into_iter().map(Expr::Column);
    let input = LogicalPlanBuilder::from((*a.input).clone()).project(columns.chain(below))?.build()?;
    Ok(LogicalPlan::Aggregate(Aggregate::try_new(Arc::new(input), a.group_expr.clone(), aggr)?))
}

#[derive(Debug, Default)]
pub struct NonEquiCorrelation;

impl AnalyzerRule for NonEquiCorrelation {
    fn name(&self) -> &str {
        "non_equi_correlation"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let mut n = 0;
        plan.transform_up_with_subqueries(|p| {
            let mut p = p;
            let mut changed = false;
            while let Some(next) = decorrelated(&p, &mut n)? {
                p = next;
                changed = true;
            }
            Ok(if changed { Transformed::yes(p) } else { Transformed::no(p) })
        })
        .map(|t| t.data)
    }
}

/// Aggregates whose answer over no rows is their answer over one NULL-extended row.
const NULL_BLIND: &[&str] = &[
    "count", "sum", "min", "max", "avg", "bool_and", "bool_or", "string_agg", "checked_sum", "checked_sum_text", "checked_avg",
];

/// DataFusion's `can_pullup_over_aggregation`: `inner column = <outer only>`, either way round.
fn pullable(e: &Expr) -> bool {
    let Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) = e else {
        return false;
    };
    let column = |x: &Expr| match x {
        Expr::Column(_) => true,
        Expr::Cast(c) => matches!(*c.expr, Expr::Column(_)),
        _ => false,
    };
    (column(left) && !right.any_column_refs()) || (column(right) && !left.any_column_refs())
}

/// The parts of a scalar subquery this can rewrite: its output expression, aggregates, correlated
/// and plain conjuncts, and relation.
struct Parts {
    output: Expr,
    aggr: Vec<Expr>,
    correlated: Vec<Expr>,
    plain: Vec<Expr>,
    relation: LogicalPlan,
}

fn parts(sub: &LogicalPlan) -> Option<Parts> {
    let LogicalPlan::Projection(Projection { expr, input, .. }) = sub else { return None };
    let [output] = expr.as_slice() else { return None };
    let LogicalPlan::Aggregate(a) = input.as_ref() else { return None };
    if !a.group_expr.is_empty() || output.contains_outer() {
        return None;
    }
    let blind = a.aggr_expr.iter().all(|e| {
        let e = match e {
            Expr::Alias(al) => al.expr.as_ref(),
            e => e,
        };
        matches!(e, Expr::AggregateFunction(f) if NULL_BLIND.contains(&f.func.name()) && !e.contains_outer())
    });
    let LogicalPlan::Filter(f) = a.input.as_ref() else { return None };
    let outer_below = f.input.exists(|p| Ok(p.contains_outer_reference())).unwrap_or(true);
    if !blind || outer_below {
        return None;
    }
    let (correlated, plain): (Vec<Expr>, Vec<Expr>) =
        split_conjunction_owned(f.predicate.clone()).into_iter().partition(|e| e.contains_outer());
    if correlated.is_empty() || correlated.iter().all(pullable) {
        return None;
    }
    Some(Parts { output: output.clone(), aggr: a.aggr_expr.clone(), correlated, plain, relation: (*f.input).clone() })
}

fn first_subquery(p: &LogicalPlan) -> Option<(Expr, Parts)> {
    let mut found = None;
    let _ = p.apply_expressions(|e| {
        e.apply(|x| {
            if let Expr::ScalarSubquery(sq) = x
                && let Some(parts) = parts(&sq.subquery)
            {
                found = Some((x.clone(), parts));
                return Ok(datafusion_common::tree_node::TreeNodeRecursion::Stop);
            }
            Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
        })
    });
    found
}

fn qualifiers(p: &LogicalPlan) -> std::collections::HashSet<String> {
    p.schema().iter().filter_map(|(q, _)| q.map(|q| q.to_string())).collect()
}

fn decorrelated(p: &LogicalPlan, n: &mut usize) -> Result<Option<LogicalPlan>> {
    let input = match p {
        LogicalPlan::Projection(x) => &x.input,
        LogicalPlan::Filter(x) => &x.input,
        _ => return Ok(None),
    };
    let Some((subquery, parts)) = first_subquery(p) else {
        return Ok(None);
    };
    if !qualifiers(input).is_disjoint(&qualifiers(&parts.relation)) {
        return Ok(None);
    }
    let (rid, marker, value) = (format!("__burrmill_rid{n}"), format!("__burrmill_m{n}"), format!("__burrmill_sqv{n}"));
    *n += 1;
    let row_number = Expr::from(datafusion_expr::expr::WindowFunction::new(
        datafusion_expr::expr::WindowFunctionDefinition::WindowUDF(datafusion_functions_window::row_number::row_number_udwf()),
        vec![],
    ))
    .alias(&rid);
    let numbered = LogicalPlanBuilder::from((**input).clone()).window(vec![row_number])?.build()?;
    let mut relation = LogicalPlanBuilder::from(parts.relation);
    if let Some(w) = conjunction(parts.plain) {
        relation = relation.filter(w)?;
    }
    let columns: Vec<Expr> = relation.schema().columns().into_iter().map(Expr::Column).collect();
    let relation = relation.project(columns.into_iter().chain([lit(true).alias(&marker)]))?.build()?;
    let unouter = |e: Expr| -> Result<Expr> {
        e.transform(|x| match x {
            Expr::OuterReferenceColumn(_, c) => Ok(Transformed::yes(Expr::Column(c))),
            x => Ok(Transformed::no(x)),
        })
        .map(|t| t.data)
    };
    let on = parts.correlated.into_iter().map(unouter).collect::<Result<Vec<_>>>()?;
    let joined = LogicalPlanBuilder::from(numbered.clone()).join_on(relation, JoinType::Left, on)?.build()?;
    let keys: Vec<Expr> = numbered.schema().columns().into_iter().map(Expr::Column).collect();
    let aggr = parts
        .aggr
        .into_iter()
        .map(|e| {
            let name = e.schema_name().to_string();
            let counted = e
                .transform(|x| match x {
                    Expr::AggregateFunction(mut f)
                        if f.func.name() == "count"
                            && !f.params.distinct
                            && matches!(f.params.args.as_slice(), [Expr::Literal(v, _)] if !v.is_null()) =>
                    {
                        f.params.args = vec![Expr::Column(Column::new_unqualified(&marker))];
                        Ok(Transformed::yes(Expr::AggregateFunction(f)))
                    }
                    x => Ok(Transformed::no(x)),
                })?
                .data;
            Ok(if counted.schema_name().to_string() == name { counted } else { counted.alias(name) })
        })
        .collect::<Result<Vec<_>>>()?;
    let grouped = LogicalPlan::Aggregate(Aggregate::try_new(Arc::new(joined), keys, aggr)?);
    let outer: Vec<Expr> = input.schema().columns().into_iter().map(Expr::Column).collect();
    let with_value =
        LogicalPlanBuilder::from(grouped).project(outer.iter().cloned().chain([parts.output.alias(&value)]))?.build()?;
    let replace = |e: Expr| -> Result<Expr> {
        let name = e.schema_name().to_string();
        let t = e.transform(|x| {
            if x == subquery {
                Ok(Transformed::yes(Expr::Column(Column::new_unqualified(&value))))
            } else {
                Ok(Transformed::no(x))
            }
        })?;
        Ok(if t.transformed && t.data.schema_name().to_string() != name { t.data.alias(name) } else { t.data })
    };
    Ok(Some(match p {
        LogicalPlan::Projection(x) => {
            let exprs = x.expr.iter().cloned().map(replace).collect::<Result<Vec<_>>>()?;
            LogicalPlanBuilder::from(with_value).project(exprs)?.build()?
        }
        LogicalPlan::Filter(x) => LogicalPlanBuilder::from(with_value)
            .filter(replace(x.predicate.clone())?)?
            .project(outer)?
            .build()?,
        _ => unreachable!("matched above"),
    }))
}
