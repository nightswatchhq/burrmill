//! `CheckedArithmetic`: an analyzer rule that makes integer and decimal arithmetic refuse on
//! overflow, and refuses the plan when it meets exact arithmetic it does not know (roadmap 6.3).
//!
//! It runs after DataFusion's `TypeCoercion`, so operand types are final, and is followed by a
//! second `TypeCoercion` because checked sums widen integers to `Decimal128(38, 0)`.
//!
//! `TRY_CAST` to an exact type is lossy: a value that does not fit becomes NULL, and an aggregate
//! then drops the row without a word. Every column derived from one is tainted. `SUM`/`AVG` of the
//! `TRY_CAST` itself, or of a column that is one, is rewritten to sum the source value exactly; any
//! other aggregate over a tainted value is refused.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::datatypes::DataType;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, Result, plan_err};
use datafusion_expr::expr::{AggregateFunction, ScalarFunction, WindowFunctionDefinition};
use datafusion_expr::logical_plan::{Aggregate, Distinct, Projection, SubqueryAlias, Union};
use datafusion_expr::{
    BinaryExpr, Expr, ExprSchemable, JoinType, LogicalPlan, Operator, ScalarUDF, TryCast,
};
use datafusion_optimizer::analyzer::AnalyzerRule;

use super::checked::{CheckedAgg, CheckedBinary, ExactWide, Mode, is_exact, is_text, sum_type};

/// Functions allowed to produce an integer or decimal. Anything else that does is refused, so a
/// new DataFusion function that wraps cannot slip in unexamined.
const SAFE_SCALARS: &[&str] = &[
    "abs",
    // On a DECIMAL it checks the rounded value against the precision and refuses.
    "round",
    "coalesce",
    "nullif",
    "nvl",
    "nvl2",
    "greatest",
    "least",
    "character_length",
    "octet_length",
    "bit_length",
    "strpos",
    "ascii",
    "levenshtein",
    "find_in_set",
    "date_part",
    "to_unixtime",
    "arrow_cast",
    "get_field",
    "regexp_count",
    "regexp_instr",
    "burrmill_intdiv",
    "burrmill_round_int",
    "burrmill_text_to_int",
    "burrmill_text_to_decimal",
    "checked_add",
    "checked_sub",
    "checked_mul",
    "checked_neg",
];
const SAFE_AGGREGATES: &[&str] = &[
    "count",
    "min",
    "max",
    "first_value",
    "last_value",
    "nth_value",
    "bit_and",
    "bit_or",
    "bit_xor",
    "approx_distinct",
    "grouping",
    "regr_count",
    "checked_sum",
    "checked_avg",
    "checked_sum_text",
];
const SAFE_WINDOWS: &[&str] = &[
    "row_number",
    "rank",
    "dense_rank",
    "ntile",
    "lag",
    "lead",
    "first_value",
    "last_value",
    "nth_value",
];

#[derive(Debug)]
pub struct CheckedArithmetic {
    add: Arc<ScalarUDF>,
    sub: Arc<ScalarUDF>,
    mul: Arc<ScalarUDF>,
    neg: Arc<ScalarUDF>,
    wide: Arc<ScalarUDF>,
    wide_neg: Arc<ScalarUDF>,
    fresh: AtomicUsize,
}

impl Default for CheckedArithmetic {
    fn default() -> Self {
        Self {
            add: CheckedBinary::udf(Some(Operator::Plus)),
            sub: CheckedBinary::udf(Some(Operator::Minus)),
            mul: CheckedBinary::udf(Some(Operator::Multiply)),
            neg: CheckedBinary::udf(None),
            wide: ExactWide::udf(false),
            wide_neg: ExactWide::udf(true),
            fresh: AtomicUsize::new(0),
        }
    }
}

impl AnalyzerRule for CheckedArithmetic {
    fn name(&self) -> &str {
        "checked_arithmetic"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|p| {
            let t = self.rewrite_expressions(p)?;
            if let LogicalPlan::Aggregate(a) = &t.data {
                let a = self.aggregate(a)?;
                return Ok(Transformed::yes(LogicalPlan::Aggregate(a)));
            }
            if let LogicalPlan::Window(_) = &t.data {
                self.window(&t.data)?;
            }
            Ok(t)
        })
        .map(|t| t.data)
    }
}

fn unalias(e: &Expr) -> &Expr {
    match e {
        Expr::Alias(a) => unalias(&a.expr),
        e => e,
    }
}

fn scale(t: &DataType) -> i8 {
    match t {
        DataType::Decimal32(_, s)
        | DataType::Decimal64(_, s)
        | DataType::Decimal128(_, s)
        | DataType::Decimal256(_, s) => *s,
        _ => 0,
    }
}

/// The schema a node's own expressions are evaluated against.
fn expr_schema(p: &LogicalPlan) -> DFSchema {
    if let LogicalPlan::TableScan(s) = p {
        return s.projected_schema.as_ref().clone();
    }
    let mut schema = DFSchema::empty();
    for i in p.inputs() {
        schema.merge(i.schema());
    }
    schema
}

impl CheckedArithmetic {
    fn rewrite_expressions(&self, p: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let schema = expr_schema(&p);
        let names_matter = matches!(
            p,
            LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_) | LogicalPlan::Window(_)
        );
        let t = p.map_expressions(|e| {
            let name = e.schema_name().to_string();
            let t = e.transform_up(|e| self.rewrite(e, &schema))?;
            check_whitelist(&t.data, &schema)?;
            if t.transformed && names_matter && t.data.schema_name().to_string() != name {
                Ok(Transformed::yes(t.data.alias(name)))
            } else {
                Ok(t)
            }
        })?;
        if t.transformed {
            Ok(Transformed::yes(t.data.recompute_schema()?))
        } else {
            Ok(t)
        }
    }

    fn rewrite(&self, e: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
        match e {
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if matches!(op, Operator::Plus | Operator::Minus | Operator::Multiply) =>
            {
                let lt = left.get_type(schema)?;
                let rt = right.get_type(schema)?;
                if is_exact(&lt) && is_exact(&rt) {
                    let f = match op {
                        Operator::Plus => &self.add,
                        Operator::Minus => &self.sub,
                        _ => &self.mul,
                    };
                    Ok(Transformed::yes(Expr::ScalarFunction(
                        ScalarFunction::new_udf(Arc::clone(f), vec![*left, *right]),
                    )))
                } else if is_exact(&lt) || is_exact(&rt) {
                    plan_err!("refusing plan: {lt} {op} {rt} has no checked form")
                } else {
                    Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr {
                        left,
                        op,
                        right,
                    })))
                }
            }
            Expr::Negative(inner) if is_exact(&inner.get_type(schema)?) => Ok(Transformed::yes(
                Expr::ScalarFunction(ScalarFunction::new_udf(Arc::clone(&self.neg), vec![*inner])),
            )),
            Expr::WindowFunction(wf) => checked_window_sum(*wf, schema),
            e => Ok(Transformed::no(e)),
        }
    }

    fn aggregate(&self, a: &Aggregate) -> Result<Aggregate> {
        let mut input = a.input.as_ref().clone();
        let mut exprs = Vec::with_capacity(a.aggr_expr.len());
        for e in &a.aggr_expr {
            let name = e.schema_name().to_string();
            let Expr::AggregateFunction(af) = unalias(e) else {
                exprs.push(e.clone());
                continue;
            };
            let func = af.func.name();
            let schema = input.schema().as_ref().clone();
            let taint = lossy(&input);
            if af
                .params
                .filter
                .as_ref()
                .is_some_and(|x| expr_lossy(x, &schema, &taint))
            {
                return plan_err!(
                    "refusing plan: a FILTER over a TRY_CAST value drops the rows that did not fit"
                );
            }
            let tainted = af
                .params
                .args
                .iter()
                .any(|x| expr_lossy(x, &schema, &taint));
            if !matches!(func, "sum" | "avg") {
                if tainted {
                    return plan_err!(
                        "refusing plan: {func} over a TRY_CAST value drops the rows that did not \
                         fit; only SUM and AVG of it are made exact"
                    );
                }
                exprs.push(e.clone());
                continue;
            }
            if af.params.distinct {
                return plan_err!("refusing plan: {func}(DISTINCT ...) has no checked form");
            }
            let arg = &af.params.args[0];
            let (arg, arg_ty) = if tainted {
                let Some((new_input, wide, target)) = self.exact_source(&input, arg)? else {
                    return plan_err!(
                        "refusing plan: {func} over a TRY_CAST value would drop the rows that did \
                         not fit, and its source cannot be summed exactly here; sum the source \
                         column with a CAST, or checked_sum_text"
                    );
                };
                input = new_input;
                (wide, target)
            } else {
                let t = arg.get_type(input.schema())?;
                if !is_exact(&t) {
                    exprs.push(e.clone());
                    continue;
                }
                (arg.clone(), t)
            };
            let udaf = match func {
                "sum" => CheckedAgg::udaf(Mode::Sum, sum_type(&arg_ty)),
                _ => CheckedAgg::udaf(Mode::Avg, Some(DataType::Float64)),
            };
            let mut params = af.params.clone();
            params.args = vec![arg];
            let checked = Expr::AggregateFunction(AggregateFunction { func: udaf, params });
            exprs.push(checked.alias(name));
        }
        Aggregate::try_new(Arc::new(input), a.group_expr.clone(), exprs)
    }

    /// For a tainted `SUM`/`AVG` argument: the plan with the exact source exposed, the source as
    /// an `exact_wide` value, and the `TRY_CAST`'s target type.
    fn exact_source(
        &self,
        input: &LogicalPlan,
        arg: &Expr,
    ) -> Result<Option<(LogicalPlan, Expr, DataType)>> {
        if let Expr::Column(c) = unalias(arg) {
            let idx = input.schema().index_of_column(c)?;
            let name = format!(
                "__burrmill_exact_{}",
                self.fresh.fetch_add(1, Ordering::Relaxed)
            );
            return Ok(self
                .expose(input, idx, &name)?
                .map(|(p, c, t)| (p, Expr::Column(c), t)));
        }
        Ok(self
            .wide_of(arg, input.schema(), &lossy(input))
            .map(|(w, t)| (input.clone(), w, t)))
    }

    /// `TRY_CAST(src AS D)`, its negation, or an exact expression that is not lossy at all, as a
    /// 320-bit `exact_wide` of the true value, with `D`. Scale 0 only: text carries no scale.
    fn wide_of(&self, e: &Expr, schema: &DFSchema, taint: &[bool]) -> Option<(Expr, DataType)> {
        let (inner, neg) = match unalias(e) {
            Expr::ScalarFunction(f) if f.func.name() == "checked_neg" => {
                (unalias(&f.args[0]), true)
            }
            Expr::Negative(x) => (unalias(x), true),
            x => (x, false),
        };
        let (source, target) = match inner {
            Expr::TryCast(TryCast { expr, field }) => (expr.as_ref(), field.data_type().clone()),
            x if !expr_lossy(x, schema, taint) => (x, x.get_type(schema).ok()?),
            _ => return None,
        };
        if !is_exact(&target) || scale(&target) != 0 || expr_lossy(source, schema, taint) {
            return None;
        }
        let source_ty = source.get_type(schema).ok()?;
        if !(is_text(&source_ty) || is_exact(&source_ty) && scale(&source_ty) == 0) {
            return None;
        }
        let f = if neg { &self.wide_neg } else { &self.wide };
        let call = ScalarFunction::new_udf(Arc::clone(f), vec![source.clone()]);
        Some((Expr::ScalarFunction(call), target))
    }

    fn window(&self, p: &LogicalPlan) -> Result<()> {
        let LogicalPlan::Window(w) = p else {
            return Ok(());
        };
        let schema = w.input.schema();
        let taint = lossy(&w.input);
        for e in &w.window_expr {
            let Expr::WindowFunction(wf) = unalias(e) else {
                continue;
            };
            let aggregate = matches!(wf.fun, WindowFunctionDefinition::AggregateUDF(_));
            if aggregate && wf.params.args.iter().any(|x| expr_lossy(x, schema, &taint)) {
                return plan_err!(
                    "refusing plan: window function {} over a TRY_CAST value drops the rows that \
                     did not fit",
                    wf.fun.name()
                );
            }
        }
        Ok(())
    }
}

/// Refuses any exact-typed expression the rule does not know to be safe.
fn check_whitelist(e: &Expr, schema: &DFSchema) -> Result<()> {
    e.apply(|x| {
        let exact = || x.get_type(schema).map(|t| is_exact(&t)).unwrap_or(true);
        match x {
            Expr::ScalarFunction(f) if !SAFE_SCALARS.contains(&f.func.name()) && exact() => {
                return plan_err!(
                    "refusing plan: {} returning an exact type is not known to refuse on overflow",
                    f.func.name()
                );
            }
            Expr::AggregateFunction(f)
                if !SAFE_AGGREGATES.contains(&f.func.name())
                    && !matches!(f.func.name(), "sum" | "avg")
                    && exact() =>
            {
                return plan_err!(
                    "refusing plan: aggregate {} returning an exact type is not known to refuse \
                     on overflow",
                    f.func.name()
                );
            }
            Expr::WindowFunction(w) if exact() => {
                let name = w.fun.name();
                let ok = match &w.fun {
                    WindowFunctionDefinition::AggregateUDF(f) => {
                        SAFE_AGGREGATES.contains(&f.name())
                    }
                    WindowFunctionDefinition::WindowUDF(_) => SAFE_WINDOWS.contains(&name),
                };
                if !ok {
                    return plan_err!(
                        "refusing plan: window function {name} returning an exact type is not \
                         known to refuse on overflow"
                    );
                }
            }
            Expr::BinaryExpr(b)
                if matches!(
                    b.op,
                    Operator::BitwiseShiftLeft | Operator::BitwiseShiftRight
                ) && exact() =>
            {
                return plan_err!("refusing plan: {} has no checked form", b.op);
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

/// Window `sum`/`avg` over exact input become the checked forms.
fn checked_window_sum(
    mut wf: datafusion_expr::expr::WindowFunction,
    schema: &DFSchema,
) -> Result<Transformed<Expr>> {
    let name = wf.fun.name().to_string();
    let is_sum = match &wf.fun {
        WindowFunctionDefinition::AggregateUDF(_) => matches!(name.as_str(), "sum" | "avg"),
        WindowFunctionDefinition::WindowUDF(_) => false,
    };
    let t = match wf.params.args.first() {
        Some(a) if is_sum => a.get_type(schema)?,
        _ => return Ok(Transformed::no(Expr::WindowFunction(Box::new(wf)))),
    };
    if !is_exact(&t) {
        return Ok(Transformed::no(Expr::WindowFunction(Box::new(wf))));
    }
    if wf.params.distinct {
        return plan_err!("refusing plan: {name}(DISTINCT ...) OVER has no checked form");
    }
    wf.fun = WindowFunctionDefinition::AggregateUDF(match name.as_str() {
        "sum" => CheckedAgg::udaf(Mode::Sum, sum_type(&t)),
        _ => CheckedAgg::udaf(Mode::Avg, Some(DataType::Float64)),
    });
    Ok(Transformed::yes(Expr::WindowFunction(Box::new(wf))))
}

/// True if `e` contains a `TRY_CAST` to an exact type or reads a tainted column.
fn expr_lossy(e: &Expr, schema: &DFSchema, taint: &[bool]) -> bool {
    match e {
        Expr::TryCast(TryCast { field, .. }) if is_exact(field.data_type()) => true,
        Expr::Column(c) => schema.index_of_column(c).is_ok_and(|i| taint[i]),
        // Whether a TRY_CAST came back NULL is an exact answer (nuthatch's `_overflow`), so taint
        // does not pass through IS [NOT] NULL.
        Expr::IsNull(_) | Expr::IsNotNull(_) => false,
        e => {
            let mut lossy = false;
            let _ = e.apply_children(|c| {
                lossy |= expr_lossy(c, schema, taint);
                Ok(if lossy { TreeNodeRecursion::Stop } else { TreeNodeRecursion::Continue })
            });
            lossy
        }
    }
}

/// Per output column of `plan`: does it carry a lossy `TRY_CAST` value?
fn lossy(plan: &LogicalPlan) -> Vec<bool> {
    let n = plan.schema().fields().len();
    let through = |input: &LogicalPlan, exprs: &[Expr]| -> Vec<bool> {
        let t = lossy(input);
        exprs
            .iter()
            .map(|e| expr_lossy(e, input.schema(), &t))
            .collect()
    };
    match plan {
        LogicalPlan::TableScan(_) | LogicalPlan::EmptyRelation(_) => vec![false; n],
        LogicalPlan::Extension(e) if e.node.name() == "OwnedSignedFold" => vec![false; n],
        LogicalPlan::Projection(p) => through(&p.input, &p.expr),
        LogicalPlan::Aggregate(a) => {
            let mut t = through(&a.input, &a.group_expr);
            t.resize(n, false);
            t
        }
        LogicalPlan::Window(w) => {
            let mut t = lossy(&w.input);
            t.resize(n, false);
            t
        }
        LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. })
        | LogicalPlan::Filter(datafusion_expr::Filter { input, .. })
        | LogicalPlan::Sort(datafusion_expr::Sort { input, .. })
        | LogicalPlan::Limit(datafusion_expr::Limit { input, .. })
        | LogicalPlan::Repartition(datafusion_expr::Repartition { input, .. })
        | LogicalPlan::Distinct(Distinct::All(input)) => lossy(input),
        LogicalPlan::Distinct(Distinct::On(d)) => through(&d.input, &d.select_expr),
        LogicalPlan::Join(j) => {
            let (l, r) = (lossy(&j.left), lossy(&j.right));
            let mut t = match j.join_type {
                JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => l,
                JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => r,
                _ => l.into_iter().chain(r).collect(),
            };
            t.resize(n, false);
            t
        }
        LogicalPlan::Union(u) => {
            let mut t = vec![false; n];
            for i in &u.inputs {
                for (acc, x) in t.iter_mut().zip(lossy(i)) {
                    *acc |= x;
                }
            }
            t
        }
        LogicalPlan::Values(v) => {
            let empty = DFSchema::empty();
            let mut t = vec![false; n];
            for row in &v.values {
                for (acc, e) in t.iter_mut().zip(row) {
                    *acc |= expr_lossy(e, &empty, &[]);
                }
            }
            t
        }
        _ => vec![true; n],
    }
}

impl CheckedArithmetic {
    /// Rebuilds `plan` so that it also outputs, under `name`, the exact value behind its lossy
    /// column `idx`. Follows the column down through pass-through nodes and every branch of a
    /// `UNION` to the projections that `TRY_CAST` it.
    fn expose(
        &self,
        plan: &LogicalPlan,
        idx: usize,
        name: &str,
    ) -> Result<Option<(LogicalPlan, Column, DataType)>> {
        let with_input =
            |input: &LogicalPlan, i: usize| -> Result<Option<(LogicalPlan, Column, DataType)>> {
                let Some((ni, col, target)) = self.expose(input, i, name)? else {
                    return Ok(None);
                };
                let rebuilt = plan.with_new_exprs(plan.expressions(), vec![ni])?;
                Ok(Some((rebuilt, col, target)))
            };
        match plan {
            LogicalPlan::Projection(p) => {
                let (inner, neg) = match unalias(&p.expr[idx]) {
                    Expr::ScalarFunction(f) if f.func.name() == "checked_neg" => {
                        (unalias(&f.args[0]), true)
                    }
                    Expr::Negative(x) => (unalias(x), true),
                    x => (x, false),
                };
                let (source, target, input) = match inner {
                    Expr::Column(c) => {
                        let i = p.input.schema().index_of_column(c)?;
                        let Some((ni, col, target)) = self.expose(&p.input, i, name)? else {
                            return Ok(None);
                        };
                        let source = if neg {
                            let f = Arc::clone(&self.wide_neg);
                            Expr::ScalarFunction(ScalarFunction::new_udf(
                                f,
                                vec![Expr::Column(col)],
                            ))
                        } else {
                            Expr::Column(col)
                        };
                        (source, target, Arc::new(ni))
                    }
                    _ => {
                        let taint = lossy(&p.input);
                        let e = &p.expr[idx];
                        let Some((w, target)) = self.wide_of(e, p.input.schema(), &taint) else {
                            return Ok(None);
                        };
                        (w, target, Arc::clone(&p.input))
                    }
                };
                let mut exprs = p.expr.clone();
                exprs.push(source.alias(name));
                let np = Projection::try_new(exprs, input)?;
                Ok(Some((
                    LogicalPlan::Projection(np),
                    Column::new_unqualified(name),
                    target,
                )))
            }
            LogicalPlan::SubqueryAlias(s) => {
                let Some((ni, _, target)) = self.expose(&s.input, idx, name)? else {
                    return Ok(None);
                };
                let na = SubqueryAlias::try_new(Arc::new(ni), s.alias.clone())?;
                Ok(Some((
                    LogicalPlan::SubqueryAlias(na),
                    Column::new(Some(s.alias.clone()), name),
                    target,
                )))
            }
            LogicalPlan::Union(u) => {
                let mut inputs = Vec::with_capacity(u.inputs.len());
                let mut first: Option<(Column, DataType)> = None;
                for i in &u.inputs {
                    let Some((ni, col, target)) = self.expose(i, idx, name)? else {
                        return Ok(None);
                    };
                    match &first {
                        Some((_, t)) if *t != target => return Ok(None),
                        Some(_) => {}
                        None => first = Some((col, target)),
                    }
                    inputs.push(Arc::new(ni));
                }
                let Some((col, target)) = first else {
                    return Ok(None);
                };
                let nu = Union::try_new_with_loose_types(inputs)?;
                Ok(Some((LogicalPlan::Union(nu), col, target)))
            }
            LogicalPlan::Filter(f) => with_input(&f.input, idx),
            LogicalPlan::Sort(s) => with_input(&s.input, idx),
            LogicalPlan::Limit(l) => with_input(&l.input, idx),
            LogicalPlan::Join(j)
                if matches!(
                    j.join_type,
                    JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
                ) =>
            {
                let nl = j.left.schema().fields().len();
                let (left, right) = (j.left.as_ref().clone(), j.right.as_ref().clone());
                let (inputs, col, target) = if idx < nl {
                    let Some((ni, col, t)) = self.expose(&j.left, idx, name)? else {
                        return Ok(None);
                    };
                    (vec![ni, right], col, t)
                } else {
                    let Some((ni, col, t)) = self.expose(&j.right, idx - nl, name)? else {
                        return Ok(None);
                    };
                    (vec![left, ni], col, t)
                };
                let rebuilt = plan.with_new_exprs(plan.expressions(), inputs)?;
                Ok(Some((rebuilt, col, target)))
            }
            _ => Ok(None),
        }
    }
}
