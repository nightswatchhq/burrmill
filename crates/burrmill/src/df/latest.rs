//! `LatestCorrelation`: the latest matching row per outer row.
//!
//! `(SELECT d.v FROM d WHERE d.id = c.id AND d.b > c.b ORDER BY d.b DESC LIMIT 1)`, and the same as
//! a `LEFT` or `CROSS JOIN LATERAL` with any `LIMIT k`: a correlated top-N whose correlation carries
//! a range, which DataFusion 55 decorrelates in neither form. The outer rows are numbered, the
//! subquery's relation LEFT JOINed on its correlated conjuncts, ranked within each outer row by the
//! subquery's order, and cut at `k`. An outer row with no match keeps one NULL-extended row: the
//! scalar subquery's NULL and the left join's empty side, and what a cross join drops.
//!
//! A lateral with no relation, `CROSS JOIN LATERAL (SELECT f(c.x) AS y) r`, only names a value of
//! the outer row, one row per row: it becomes that column.

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, Result};
use datafusion_expr::expr::Sort as SortExpr;
use datafusion_expr::logical_plan::{FetchType, Join, JoinType, Limit, SkipType};
use datafusion_expr::utils::{conjunction, split_conjunction_owned};
use datafusion_expr::{Expr, ExprFunctionExt, LogicalPlan, LogicalPlanBuilder, lit};
use datafusion_optimizer::analyzer::AnalyzerRule;

#[derive(Debug, Default)]
pub struct LatestCorrelation;

impl AnalyzerRule for LatestCorrelation {
    fn name(&self) -> &str {
        "latest_correlation"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let mut n = 0;
        plan.transform_up_with_subqueries(|p| {
            let mut p = p;
            let mut changed = false;
            loop {
                let next = match &p {
                    LogicalPlan::Join(j) => lateral(j, &mut n)?,
                    LogicalPlan::Projection(_) | LogicalPlan::Filter(_) => scalar(&p, &mut n)?,
                    _ => None,
                };
                let Some(next) = next else { break };
                p = next;
                changed = true;
            }
            Ok(if changed {
                Transformed::yes(p)
            } else {
                Transformed::no(p)
            })
        })
        .map(|t| t.data)
    }
}

/// A correlated `ORDER BY … LIMIT k` over one relation, in the shape DataFusion plans it: `Limit`
/// over an output `Projection` (absent when the output is the sort's input), over `Sort`, over an
/// optional `Projection` adding the sort keys, over the `Filter` holding the correlation.
struct TopN {
    limit: usize,
    /// The output columns, unaliased, each with the name the subquery gives it.
    output: Vec<(Expr, String)>,
    order: Vec<SortExpr>,
    inner: Option<Vec<Expr>>,
    correlated: Vec<Expr>,
    plain: Vec<Expr>,
    relation: LogicalPlan,
}

fn top_n(sub: &LogicalPlan) -> Option<TopN> {
    let LogicalPlan::Limit(l) = sub else {
        return None;
    };
    let limit = literal_limit(l)?;
    let (out, sorted) = match l.input.as_ref() {
        LogicalPlan::Projection(p) => (Some(p), p.input.as_ref()),
        other => (None, other),
    };
    let LogicalPlan::Sort(s) = sorted else {
        return None;
    };
    let (inner, below) = match s.input.as_ref() {
        LogicalPlan::Projection(p) => (Some(p.expr.clone()), p.input.as_ref()),
        other => (None, other),
    };
    let LogicalPlan::Filter(f) = below else {
        return None;
    };
    let outer_below = f
        .input
        .exists(|p| Ok(p.contains_outer_reference()))
        .unwrap_or(true);
    let outer_above = out.is_some_and(|o| o.expr.iter().any(Expr::contains_outer))
        || s.expr.iter().any(|o| o.expr.contains_outer())
        || inner.iter().flatten().any(Expr::contains_outer);
    if outer_below || outer_above {
        return None;
    }
    let (correlated, plain): (Vec<Expr>, Vec<Expr>) = split_conjunction_owned(f.predicate.clone())
        .into_iter()
        .partition(Expr::contains_outer);
    if correlated.is_empty() {
        return None;
    }
    let output = match out {
        Some(out) => out
            .expr
            .iter()
            .zip(out.schema.fields())
            .map(|(e, f)| {
                let e = match e {
                    Expr::Alias(a) => a.expr.as_ref().clone(),
                    e => e.clone(),
                };
                (e, f.name().clone())
            })
            .collect(),
        None => s
            .input
            .schema()
            .columns()
            .into_iter()
            .map(|c| (c.name.clone(), Expr::Column(c)))
            .map(|(name, e)| (e, name))
            .collect(),
    };
    Some(TopN {
        limit,
        output,
        order: s.expr.clone(),
        inner,
        correlated,
        plain,
        relation: (*f.input).clone(),
    })
}

fn literal_limit(l: &Limit) -> Option<usize> {
    match (l.get_skip_type().ok()?, l.get_fetch_type().ok()?) {
        (SkipType::Literal(0), FetchType::Literal(Some(k))) if k > 0 => Some(k),
        _ => None,
    }
}

/// `SELECT <expressions of the outer row>` with no relation: the expressions, each with its name.
fn binding(sub: &LogicalPlan) -> Option<Vec<(Expr, String)>> {
    let LogicalPlan::Projection(p) = sub else {
        return None;
    };
    let LogicalPlan::EmptyRelation(e) = p.input.as_ref() else {
        return None;
    };
    if !e.produce_one_row {
        return None;
    }
    let plain = |x: &Expr| {
        !x.exists(|y| {
            Ok(matches!(
                y,
                Expr::ScalarSubquery(_)
                    | Expr::Exists(_)
                    | Expr::InSubquery(_)
                    | Expr::AggregateFunction(_)
                    | Expr::WindowFunction(_)
            ))
        })
        .unwrap_or(true)
    };
    p.expr.iter().all(plain).then(|| {
        p.expr
            .iter()
            .zip(p.schema.fields())
            .map(|(x, f)| {
                let x = match x {
                    Expr::Alias(a) => a.expr.as_ref().clone(),
                    x => x.clone(),
                };
                (x, f.name().clone())
            })
            .collect()
    })
}

/// An outer reference as the column it refers to, once the subquery is joined to its outer rows.
fn unouter(e: Expr) -> Result<Expr> {
    e.transform(|x| match x {
        Expr::OuterReferenceColumn(_, c) => Ok(Transformed::yes(Expr::Column(c))),
        x => Ok(Transformed::no(x)),
    })
    .map(|t| t.data)
}

fn qualifiers(p: &LogicalPlan) -> std::collections::HashSet<String> {
    p.schema()
        .iter()
        .filter_map(|(q, _)| q.map(|q| q.to_string()))
        .collect()
}

/// `outer` numbered, the top-N's relation joined to it and ranked within each outer row, cut at
/// `k`. Carries `outer`'s columns, the subquery's output under `names`, and the match marker.
fn ranked(
    outer: &LogicalPlan,
    t: TopN,
    n: &mut usize,
    names: &[String],
) -> Result<(LogicalPlan, String)> {
    let (rid, rank, marker) = (
        format!("__burrmill_lrid{n}"),
        format!("__burrmill_lrn{n}"),
        format!("__burrmill_lm{n}"),
    );
    *n += 1;
    let row_number = || {
        Expr::from(datafusion_expr::expr::WindowFunction::new(
            datafusion_expr::expr::WindowFunctionDefinition::WindowUDF(
                datafusion_functions_window::row_number::row_number_udwf(),
            ),
            vec![],
        ))
    };
    let numbered = LogicalPlanBuilder::from(outer.clone())
        .window(vec![row_number().alias(&rid)])?
        .build()?;
    let mut relation = LogicalPlanBuilder::from(t.relation);
    if let Some(w) = conjunction(t.plain) {
        relation = relation.filter(w)?;
    }
    let columns: Vec<Expr> = relation
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect();
    let relation = relation
        .project(columns.into_iter().chain([lit(true).alias(&marker)]))?
        .build()?;
    let on = t
        .correlated
        .into_iter()
        .map(unouter)
        .collect::<Result<Vec<_>>>()?;
    let joined = LogicalPlanBuilder::from(numbered.clone())
        .join_on(relation, JoinType::Left, on)?
        .build()?;
    let carried: Vec<Expr> = numbered
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect();
    let marked = Expr::Column(Column::new_unqualified(&marker));
    let shaped = match t.inner {
        Some(inner) => LogicalPlanBuilder::from(joined)
            .project(carried.iter().cloned().chain(inner).chain([marked.clone()]))?
            .build()?,
        None => joined,
    };
    let ranking = row_number()
        .partition_by(vec![Expr::Column(Column::new_unqualified(&rid))])
        .order_by(t.order)
        .build()?
        .alias(&rank);
    let cut = LogicalPlanBuilder::from(shaped)
        .window(vec![ranking])?
        .filter(Expr::Column(Column::new_unqualified(&rank)).lt_eq(lit(t.limit as u64)))?
        .build()?;
    let kept: Vec<Expr> = outer
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect();
    let values = t
        .output
        .into_iter()
        .zip(names)
        .map(|((e, _), name)| e.alias(name));
    let plan = LogicalPlanBuilder::from(cut)
        .project(kept.into_iter().chain(values).chain([marked]))?
        .build()?;
    Ok((plan, marker))
}

fn first_subquery(p: &LogicalPlan) -> Option<(Expr, TopN)> {
    let mut found = None;
    let _ = p.apply_expressions(|e| {
        e.apply(|x| {
            if let Expr::ScalarSubquery(sq) = x
                && let Some(t) = top_n(&sq.subquery)
                && t.limit == 1
                && t.output.len() == 1
            {
                found = Some((x.clone(), t));
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })
    });
    found
}

fn scalar(p: &LogicalPlan, n: &mut usize) -> Result<Option<LogicalPlan>> {
    let input = match p {
        LogicalPlan::Projection(x) => &x.input,
        LogicalPlan::Filter(x) => &x.input,
        _ => return Ok(None),
    };
    let Some((subquery, t)) = first_subquery(p) else {
        return Ok(None);
    };
    if !qualifiers(input).is_disjoint(&qualifiers(&t.relation)) {
        return Ok(None);
    }
    let value = format!("__burrmill_lv{n}");
    let (with_value, _) = ranked(input, t, n, std::slice::from_ref(&value))?;
    let replace = |e: Expr| -> Result<Expr> {
        let name = e.schema_name().to_string();
        let t = e.transform(|x| {
            if x == subquery {
                Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                    &value,
                ))))
            } else {
                Ok(Transformed::no(x))
            }
        })?;
        Ok(
            if t.transformed && t.data.schema_name().to_string() != name {
                t.data.alias(name)
            } else {
                t.data
            },
        )
    };
    let outer: Vec<Expr> = input
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect();
    Ok(Some(match p {
        LogicalPlan::Projection(x) => {
            let exprs = x
                .expr
                .iter()
                .cloned()
                .map(replace)
                .collect::<Result<Vec<_>>>()?;
            LogicalPlanBuilder::from(with_value)
                .project(exprs)?
                .build()?
        }
        LogicalPlan::Filter(x) => LogicalPlanBuilder::from(with_value)
            .filter(replace(x.predicate.clone())?)?
            .project(outer)?
            .build()?,
        _ => unreachable!("matched above"),
    }))
}

/// `outer LEFT|CROSS JOIN LATERAL (top-N) alias ON true`.
fn lateral(j: &Join, n: &mut usize) -> Result<Option<LogicalPlan>> {
    if !matches!(j.join_type, JoinType::Left | JoinType::Inner) || !j.on.is_empty() {
        return Ok(None);
    }
    if j.filter.as_ref().is_some_and(|f| *f != lit(true)) {
        return Ok(None);
    }
    let (alias, sub) = match j.right.as_ref() {
        LogicalPlan::SubqueryAlias(a) => (Some(a.alias.clone()), a.input.as_ref()),
        other => (None, other),
    };
    let LogicalPlan::Subquery(sq) = sub else {
        return Ok(None);
    };
    if let Some(bound) = binding(&sq.subquery) {
        let left: Vec<Expr> = j
            .left
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect();
        let right = bound
            .into_iter()
            .map(|(e, name)| match &alias {
                Some(a) => Ok(unouter(e)?.alias_qualified(Some(a.clone()), name)),
                None => Ok(unouter(e)?.alias(name)),
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(Some(
            LogicalPlanBuilder::from((*j.left).clone())
                .project(left.into_iter().chain(right))?
                .build()?,
        ));
    }
    let Some(t) = top_n(&sq.subquery) else {
        return Ok(None);
    };
    if !qualifiers(&j.left).is_disjoint(&qualifiers(&t.relation)) {
        return Ok(None);
    }
    // Private names until the end: a subquery column may share a name with an outer one.
    let names: Vec<String> = t.output.iter().map(|(_, name)| name.clone()).collect();
    let private: Vec<String> = (0..names.len())
        .map(|i| format!("__burrmill_lo{n}_{i}"))
        .collect();
    let (plan, marker) = ranked(&j.left, t, n, &private)?;
    let mut plan = LogicalPlanBuilder::from(plan);
    if j.join_type == JoinType::Inner {
        plan = plan.filter(Expr::Column(Column::new_unqualified(&marker)).is_not_null())?;
    }
    let left: Vec<Expr> = j
        .left
        .schema()
        .columns()
        .into_iter()
        .map(Expr::Column)
        .collect();
    let right = private.iter().zip(&names).map(|(p, name)| {
        let e = Expr::Column(Column::new_unqualified(p));
        match &alias {
            Some(a) => e.alias_qualified(Some(a.clone()), name),
            None => e.alias(name),
        }
    });
    Ok(Some(plan.project(left.into_iter().chain(right))?.build()?))
}
