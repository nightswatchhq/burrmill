//! The parser boundary and the owned planner.
//!
//! RFC-0044 §3.9's tiny planner, in its first form. There is no cost model, no join reordering and
//! no rule engine: the admitted subset is a closed set of plan *shapes*, so planning is pattern
//! matching against that set and emitting the owned physical plan directly. DataFusion's per-query
//! planning was 4-5 ms; for a serving path with a 67.7 ms restart-to-ready budget, recognising a
//! known shape in microseconds is worth having.
//!
//! The allowlist is enforced here, against the parsed AST. Never against the SQL text: a denylist
//! over strings is how `sniff_csv` kept reading the filesystem after external access was turned off.

use sqlparser::ast::{
    BinaryOperator, DataType, ExactNumberInfo, Expr, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, Query, Select, SelectItem, SetExpr, SetOperator, SetQuantifier,
    Statement, TableFactor, UnaryOperator, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::{BurrmillError, Result};

/// The one plan shape Burrmill owns today.
///
/// It is deliberately parameterised over the columns rather than hardcoded to `to`/`from`/`value`.
/// The #987 operator was written for `net_balances`; the shape it implements is *signed union fold*
/// - one table read twice, one column crediting and one debiting the same signed value, grouped by
/// the party - and every balance-like fold in a nest is an instance of it. Owning the shape rather
/// than the query is what makes the coverage ratchet (§4.6) move by more than one query at a time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedFold {
    /// The arms of the `UNION ALL`, in the order written.
    ///
    /// **This used to be one table read twice**, a credit column and a debit column. Experiment A4
    /// (roadmap 4.1) parsed 65 statements of real authored views and found that shape **zero**
    /// times: every `UNION ALL` in the workload reads *different* tables, because a credit and a
    /// debit are different events and therefore different event tables. The single-table case is now
    /// the degenerate one - the same table listed twice with opposite signs - and nothing is lost by
    /// generalising, which is usually the sign that the general form was the right one all along.
    pub branches: Vec<FoldBranch>,
    /// Output name of the group key.
    pub key_alias: String,
    /// Output name of the sum.
    pub sum_alias: String,
    /// `HAVING SUM(d) <> 0` - drop the parties that net out. Part of the answer, not a filter we
    /// are free to skip.
    pub drop_zero: bool,
}

/// One arm of the fold: a table, the column it groups by, the column it sums, and its sign.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldBranch {
    /// Registered table name. Resolved against the catalog, never against a path.
    pub table: String,
    /// The column this arm contributes as the group key.
    pub key_col: String,
    /// Column holding the exact integer, stored as text because uint256 is 78 decimal digits and
    /// `Decimal256` tops out at 76.
    pub value_col: String,
    /// Whether this arm subtracts rather than adds.
    pub negated: bool,
    /// `CAST` rather than `TRY_CAST`: an unparseable value is an **error**, not a NULL.
    ///
    /// The real views write `CAST(tokens AS HUGEINT)`. Quietly reading that as `TRY_CAST` would
    /// change the answer - a bad row would be skipped instead of refused - and quietly changing an
    /// answer is the one thing this engine does not do. So the two are distinct modes and the
    /// stricter one is, if anything, more in keeping with the rest.
    pub strict_cast: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    SignedFold(SignedFold),
}

impl Plan {
    /// A one-line summary for `EXPLAIN`.
    pub fn describe(&self) -> String {
        match self {
            Plan::SignedFold(f) => format!(
                "SignedFoldExec  branches={}  [{}]  drop_zero={}",
                f.branches.len(),
                f.branches
                    .iter()
                    .map(|b| format!(
                        "{}{}.{}{}",
                        if b.negated { "-" } else { "+" },
                        b.table,
                        b.key_col,
                        if b.strict_cast { " strict" } else { "" }
                    ))
                    .collect::<Vec<_>>()
                    .join(", "),
                f.drop_zero
            ),
        }
    }
}

fn not_allowed(what: impl Into<String>) -> BurrmillError {
    BurrmillError::NotAllowed(what.into())
}

/// Parse and plan, or refuse.
pub fn plan(sql: &str) -> Result<Plan> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|e| BurrmillError::Parse(e.to_string()))?;
    if statements.len() != 1 {
        return Err(not_allowed(format!(
            "exactly one statement per query; got {}",
            statements.len()
        )));
    }
    let query = match statements.pop().expect("length checked") {
        Statement::Query(q) => q,
        other => {
            return Err(not_allowed(format!(
                "only SELECT is admitted; got `{}`",
                first_word(&other.to_string())
            )))
        }
    };
    match_signed_fold(&query).map(Plan::SignedFold)
}

fn first_word(s: &str) -> String {
    s.split_whitespace().next().unwrap_or("?").to_string()
}

// ---------------------------------------------------------------------------------------------
// Shape recognition. Each helper refuses with the reason, because a `NotAllowed` that does not say
// which clause offended is a support burden rather than a security boundary.
// ---------------------------------------------------------------------------------------------

fn match_signed_fold(query: &Query) -> Result<SignedFold> {
    reject_unsupported_query_clauses(query)?;
    let select = as_select(&query.body)?;
    reject_unsupported_clauses(select)?;

    // The outer projection: the group key, then the sum. A `CAST(SUM(d) AS VARCHAR)` wrapper is
    // accepted and dropped - it exists in the incumbent SQL only because i128 does not survive a
    // client's integer type, and Burrmill returns exact i128 either way.
    let (key_alias, key_expr) = projection_item(select, 0)?;
    let (sum_alias, sum_expr) = projection_item(select, 1)?;
    if select.projection.len() != 2 {
        return Err(not_allowed(format!(
            "the signed-fold shape projects exactly the key and the sum; got {} items",
            select.projection.len()
        )));
    }
    let key_ident = ident_name(key_expr)
        .ok_or_else(|| not_allowed("the first projected column must be the bare group key"))?;
    let summed = sum_argument(strip_varchar_cast(sum_expr))?;

    // The FROM must be a single derived table - the UNION ALL - with no joins.
    let derived = single_derived_table(select)?;
    let arms = union_all_branches(&derived.body)?;
    let mut matched: Vec<(FoldBranch, String, String)> = Vec::with_capacity(arms.len());
    for arm in &arms {
        matched.push(match_branch(arm)?);
    }
    let (_, first_key_alias, first_value_alias) = &matched[0];
    let (first_key_alias, first_value_alias) = (first_key_alias.clone(), first_value_alias.clone());
    for (_, k, v) in &matched {
        if *k != first_key_alias || *v != first_value_alias {
            return Err(not_allowed(
                "every arm of the union must use the same output aliases, or the group key and the \
                 summed column would not line up",
            ));
        }
    }
    let branches: Vec<FoldBranch> = matched.into_iter().map(|(b, _, _)| b).collect();

    if summed != first_value_alias {
        return Err(not_allowed(format!(
            "SUM must be over the union's value column `{first_value_alias}`; got `{summed}`"
        )));
    }
    if key_ident != first_key_alias {
        return Err(not_allowed(format!(
            "the projected key must be the union's key column `{first_key_alias}`; got `{key_ident}`"
        )));
    }

    // GROUP BY the key, and only the key.
    match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) if modifiers.is_empty() => {
            // `GROUP BY 1` as well as `GROUP BY <key>`. The ordinal form is what the real views are
            // written with (A4), it is unambiguous over a two-column projection, and refusing it
            // would have been refusing a spelling rather than a shape.
            let by_name = ident_name(&exprs[0]).as_deref() == Some(key_ident.as_str());
            let by_ordinal = matches!(&exprs[0], Expr::Value(v) if v.value.to_string() == "1");
            if exprs.len() != 1 || !(by_name || by_ordinal) {
                return Err(not_allowed(format!(
                    "the signed-fold shape groups by `{key_ident}` alone (or by ordinal `1`)"
                )));
            }
        }
        _ => return Err(not_allowed("GROUP BY ALL and grouping modifiers are not admitted")),
    }

    let drop_zero = match &select.having {
        None => false,
        Some(e) => {
            if !is_sum_ne_zero(e, &first_value_alias) {
                return Err(not_allowed("the only admitted HAVING is `SUM(<value>) <> 0`"));
            }
            true
        }
    };

    // ORDER BY is accepted but not load-bearing: canonical ordering by the group key ascending is
    // applied unconditionally (§3.3), because an oracle that returns rows in a different order
    // between runs is unusable. A query asking for a *different* order is refused rather than
    // silently overruled.
    check_order_by_is_canonical(query, &key_alias)?;

    Ok(SignedFold { branches, key_alias, sum_alias, drop_zero })
}

/// One arm of the union: `SELECT <col> AS addr, [-][TRY_]CAST(<value> AS <128-bit>) AS d FROM <t>`.
///
/// The sign is **read** rather than dictated. The old version demanded arm one be positive and arm
/// two negative, which is one way to spell a two-table fold and not the only one; with n arms it is
/// not even well defined. A fold whose arms are all positive is a perfectly ordinary sum over
/// several tables, and refusing it would be refusing arithmetic.
fn match_branch(select: &Select) -> Result<(FoldBranch, String, String)> {
    reject_unsupported_clauses(select)?;
    if select.projection.len() != 2 {
        return Err(not_allowed(
            "each arm of the union projects exactly the party column and the signed value",
        ));
    }
    let grouped = !matches!(&select.group_by, GroupByExpr::Expressions(e, m) if e.is_empty() && m.is_empty());
    if grouped || select.having.is_some() || select.selection.is_some() {
        return Err(not_allowed(
            "the arms of the union must be bare projections - no WHERE, GROUP BY or HAVING",
        ));
    }
    let (key_alias, key_expr) = projection_item(select, 0)?;
    let key_col = ident_name(key_expr)
        .ok_or_else(|| not_allowed("the party column must be a bare column reference"))?;
    let (value_alias, value_expr) = projection_item(select, 1)?;

    let (negated, inner) = match value_expr {
        Expr::UnaryOp { op: UnaryOperator::Minus, expr } => (true, expr.as_ref()),
        other => (false, other),
    };
    let (value_col, strict_cast) = cast_to_i128(inner)?;
    let table = single_named_table(select)?;

    Ok((FoldBranch { table, key_col, value_col, negated, strict_cast }, key_alias, value_alias))
}

/// `TRY_CAST(<col> AS HUGEINT | DECIMAL(38,0) | INT128)`.
///
/// **`TRY_CAST`, not `CAST`, and the difference is the answer.** A value that will not parse yields
/// NULL, and SUM ignores NULLs, so an unparseable value is *skipped* - never an error, and never a
/// zero. A zero would be a different answer that happens to look plausible, which is the class of
/// bug this whole project exists to refuse.
fn cast_to_i128(expr: &Expr) -> Result<(String, bool)> {
    let (kind, inner, data_type) = match expr {
        Expr::Cast { kind, expr, data_type, .. } => (kind, expr.as_ref(), data_type),
        _ => {
            return Err(not_allowed(
                "the value column must be wrapped in TRY_CAST to a 128-bit integer",
            ))
        }
    };
    // **Both spellings, kept distinct.** `TRY_CAST` yields NULL on an unparseable value and `SUM`
    // ignores NULLs, so the row is skipped; plain `CAST` errors. They are different answers and the
    // difference is the point, so the mode is carried into the plan rather than one being quietly
    // read as the other.
    //
    // Plain `CAST` used to be refused outright, on the grounds that skipping is what the answer is
    // defined as. A4 then found that every real fold in the workload is written with `CAST`, so the
    // rule was refusing the SQL people actually write in order to protect a semantic they had not
    // asked for. Refusing on a bad value is, if anything, more in keeping with the rest of this
    // engine than skipping it.
    let strict_cast = match kind {
        sqlparser::ast::CastKind::TryCast => false,
        sqlparser::ast::CastKind::Cast => true,
        _ => {
            return Err(not_allowed(
                "only CAST and TRY_CAST are admitted around the value column",
            ))
        }
    };
    // **Three spellings, one width, and Burrmill owns which one is meant.** `HUGEINT` is DuckDB's,
    // `DECIMAL(38,0)` is DataFusion's and the standard's, `INT128` is nobody's in particular. They
    // all denote exactly i128, so all three plan to the same operator: a migration should not have
    // to rewrite its queries to change engine, and neither incumbent's vocabulary is authoritative.
    let width_ok = match data_type {
        DataType::HugeInt | DataType::Int128 => true,
        DataType::Decimal(ExactNumberInfo::PrecisionAndScale(38, 0)) => true,
        _ => false,
    };
    if !width_ok {
        return Err(not_allowed(format!(
            "the value cast must be a 128-bit exact integer (HUGEINT or DECIMAL(38,0)); got `{data_type}`"
        )));
    }
    let col = ident_name(inner)
        .ok_or_else(|| not_allowed("the cast must be applied to a bare column"))?;
    Ok((col, strict_cast))
}

fn as_select(body: &SetExpr) -> Result<&Select> {
    match body {
        SetExpr::Select(s) => Ok(s),
        SetExpr::Query(q) => as_select(&q.body),
        _ => Err(not_allowed("the outer query must be a SELECT")),
    }
}

/// Every arm of a left-nested `UNION ALL` chain, in written order.
///
/// Two was the old limit and A4 found folds with four and five arms in the wild, so the limit was a
/// statement about the implementation rather than about the shape.
fn union_all_branches(body: &SetExpr) -> Result<Vec<&Select>> {
    let mut out = Vec::new();
    fn walk<'a>(s: &'a SetExpr, out: &mut Vec<&'a Select>) -> Result<()> {
        match s {
            SetExpr::Select(sel) => {
                out.push(sel.as_ref());
                Ok(())
            }
            SetExpr::SetOperation { op: SetOperator::Union, set_quantifier, left, right } => {
                if !matches!(set_quantifier, SetQuantifier::All) {
                    return Err(not_allowed(
                        "the union must be UNION ALL: plain UNION deduplicates, which silently \
                         drops rows that a fold must count",
                    ));
                }
                walk(left, out)?;
                walk(right, out)
            }
            _ => Err(not_allowed("the derived table must be a UNION ALL of bare projections")),
        }
    }
    walk(body, &mut out)?;
    if out.len() < 2 {
        return Err(not_allowed("the derived table must be a UNION ALL of at least two projections"));
    }
    Ok(out)
}

#[allow(dead_code)]
fn union_all_halves(body: &SetExpr) -> Result<(&Select, &Select)> {
    match body {
        SetExpr::SetOperation { op: SetOperator::Union, set_quantifier, left, right } => {
            if !matches!(set_quantifier, SetQuantifier::All) {
                return Err(not_allowed(
                    "UNION must be UNION ALL: deduplicating would silently drop a party's second \
                     transfer, which is a different answer",
                ));
            }
            Ok((as_select(left)?, as_select(right)?))
        }
        _ => Err(not_allowed("the derived table must be a UNION ALL of two projections")),
    }
}

fn single_derived_table(select: &Select) -> Result<&Query> {
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(not_allowed("joins are not in the admitted subset"));
    }
    match &select.from[0].relation {
        TableFactor::Derived { subquery, lateral, .. } if !lateral => Ok(subquery),
        _ => Err(not_allowed("the outer FROM must be the union subquery")),
    }
}

fn single_named_table(select: &Select) -> Result<String> {
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(not_allowed("each half of the union reads exactly one table"));
    }
    match &select.from[0].relation {
        TableFactor::Table { name, args: None, .. } => Ok(name
            .0
            .last()
            .map(|p| unquote(&p.to_string()))
            .ok_or_else(|| not_allowed("empty table name"))?),
        // **This is the security boundary showing its teeth.** `read_parquet('/etc/passwd')` parses
        // as a table function, and this is where it stops - not because the name is on a denylist,
        // but because table functions do not exist in the admitted subset at all.
        TableFactor::Table { args: Some(_), .. } => Err(not_allowed(
            "table functions are not registered; Burrmill resolves names against a positive \
             allowlist and has no way to name a path",
        )),
        _ => Err(not_allowed("each half of the union must read a registered table by name")),
    }
}

fn projection_item(select: &Select, i: usize) -> Result<(String, &Expr)> {
    match select.projection.get(i) {
        Some(SelectItem::ExprWithAlias { expr, alias }) => Ok((unquote(&alias.to_string()), expr)),
        Some(SelectItem::UnnamedExpr(expr)) => {
            let name = ident_name(expr).ok_or_else(|| {
                not_allowed("a computed projection needs an explicit alias")
            })?;
            Ok((name, expr))
        }
        Some(SelectItem::Wildcard(_)) | Some(SelectItem::QualifiedWildcard(..)) => Err(not_allowed(
            "SELECT * is not admitted: the result schema is part of the contract",
        )),
        // `SELECT expr AS (a, b)` - a tuple alias. It projects more than one column from one item,
        // which the shape's positional matching cannot represent, so it is refused rather than
        // half-understood.
        Some(other) => Err(not_allowed(format!(
            "unsupported projection item `{other}` in the signed-fold shape"
        ))),
        None => Err(not_allowed("projection is shorter than the shape requires")),
    }
}

fn strip_varchar_cast(expr: &Expr) -> &Expr {
    match expr {
        Expr::Cast { expr: inner, data_type, .. }
            if matches!(
                data_type,
                DataType::Varchar(_) | DataType::Text | DataType::String(_)
            ) =>
        {
            inner.as_ref()
        }
        other => other,
    }
}

/// `SUM(<col>)` and nothing else - no DISTINCT, no FILTER, no window.
fn sum_argument(expr: &Expr) -> Result<String> {
    let f = match expr {
        Expr::Function(f) => f,
        _ => return Err(not_allowed("the second projected column must be SUM(<value>)")),
    };
    if f.over.is_some() {
        return Err(not_allowed("window functions are not in the admitted subset"));
    }
    if f.name.0.last().map(|p| p.to_string().to_ascii_uppercase()) != Some("SUM".into()) {
        return Err(not_allowed(format!(
            "the only admitted aggregate in this shape is SUM; got `{}`",
            f.name
        )));
    }
    let args = match &f.args {
        FunctionArguments::List(list) => list,
        _ => return Err(not_allowed("SUM takes exactly one column")),
    };
    if args.duplicate_treatment.is_some() || !args.clauses.is_empty() || args.args.len() != 1 {
        return Err(not_allowed("SUM(DISTINCT ...) and aggregate clauses are not admitted"));
    }
    match &args.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
            ident_name(e).ok_or_else(|| not_allowed("SUM must be applied to a bare column"))
        }
        _ => Err(not_allowed("SUM must be applied to a bare column")),
    }
}

fn is_sum_ne_zero(expr: &Expr, value_col: &str) -> bool {
    let Expr::BinaryOp { left, op, right } = expr else { return false };
    if !matches!(op, BinaryOperator::NotEq) {
        return false;
    }
    let zero = matches!(right.as_ref(), Expr::Value(v) if is_zero(&v.value));
    zero && sum_argument(left).is_ok_and(|c| c == value_col)
}

fn is_zero(v: &Value) -> bool {
    matches!(v, Value::Number(n, _) if n.trim_start_matches(['+', '-']).trim_end_matches(['.', '0']).is_empty() || n.parse::<i128>() == Ok(0))
}

/// Canonical ordering is applied whether or not it is asked for. The only thing refused here is a
/// query asking for a *different* order, because silently overruling it would be worse than saying no.
fn check_order_by_is_canonical(query: &Query, key_alias: &str) -> Result<()> {
    let Some(order_by) = &query.order_by else { return Ok(()) };
    let exprs = match &order_by.kind {
        sqlparser::ast::OrderByKind::Expressions(e) => e,
        _ => return Err(not_allowed("ORDER BY ALL is not admitted")),
    };
    if exprs.len() != 1 {
        return Err(not_allowed(
            "the signed-fold result is canonically ordered by the group key ascending",
        ));
    }
    let ob = &exprs[0];
    if ident_name(&ob.expr).as_deref() != Some(key_alias) {
        return Err(not_allowed(format!(
            "the signed-fold result is canonically ordered by `{key_alias}`; a different ORDER BY \
             would be silently overruled, so it is refused instead"
        )));
    }
    if ob.options.asc == Some(false) {
        return Err(not_allowed("canonical ordering is ascending"));
    }
    Ok(())
}

/// A bare column reference, unquoted. `"from"` and `from` are the same column; a quoted identifier
/// is how you spell a reserved word, not a different name.
fn ident_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(i) => Some(unquote(&i.to_string())),
        Expr::CompoundIdentifier(parts) => parts.last().map(|p| unquote(&p.to_string())),
        Expr::Nested(inner) => ident_name(inner),
        _ => None,
    }
}

fn unquote(s: &str) -> String {
    let t = s.trim();
    for q in ['"', '`', '\''] {
        if t.len() >= 2 && t.starts_with(q) && t.ends_with(q) {
            return t[1..t.len() - 1].to_string();
        }
    }
    t.to_string()
}

/// Everything the shape does not use must be absent rather than ignored.
///
/// Quietly dropping a `LIMIT` or a `DISTINCT` would return a wrong answer to a caller who asked a
/// reasonable question, which is worse than refusing them.
fn reject_unsupported_clauses(select: &Select) -> Result<()> {
    if select.distinct.is_some() {
        return Err(not_allowed("DISTINCT is not in the admitted subset"));
    }
    if !select.named_window.is_empty() {
        return Err(not_allowed("named windows are not in the admitted subset"));
    }
    if select.qualify.is_some() {
        return Err(not_allowed("QUALIFY is not in the admitted subset"));
    }
    if select.into.is_some() {
        return Err(not_allowed(
            "SELECT INTO is not in the admitted subset: Burrmill has no write path from SQL",
        ));
    }
    Ok(())
}

/// Clauses that attach to the whole query rather than to a SELECT.
///
/// **Found by a test that expected a refusal and got a plan.** `LIMIT 10` parsed, planned, and was
/// silently dropped - so the caller would have received the full result of a query they had
/// explicitly bounded, with no error anywhere. That is a wrong answer to a reasonable question,
/// which is worse than a refusal, and it is exactly the class of defect the admitted-subset
/// discipline exists to prevent. Anything the shape does not implement must be *absent*, never
/// ignored.
fn reject_unsupported_query_clauses(query: &Query) -> Result<()> {
    if query.with.is_some() {
        return Err(not_allowed("common table expressions are not in the admitted subset"));
    }
    if query.limit_clause.is_some() {
        return Err(not_allowed(
            "LIMIT and OFFSET are not in the admitted subset: the fold materialises one row per \
             party, and silently dropping the clause would return more rows than asked for",
        ));
    }
    if query.fetch.is_some() {
        return Err(not_allowed("FETCH is not in the admitted subset"));
    }
    if !query.locks.is_empty() {
        return Err(not_allowed("locking clauses are not in the admitted subset"));
    }
    if query.for_clause.is_some() || query.format_clause.is_some() || query.settings.is_some() {
        return Err(not_allowed("output-format and settings clauses are not in the admitted subset"));
    }
    if !query.pipe_operators.is_empty() {
        return Err(not_allowed("pipe operators are not in the admitted subset"));
    }
    Ok(())
}
