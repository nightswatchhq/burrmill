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

use datafusion_common::config::ConfigOptions;
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, Result};
use datafusion_expr::logical_plan::{Aggregate, Filter};
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
