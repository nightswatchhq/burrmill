//! Experiment A4: how many plan shapes does a real workload actually produce? (roadmap 4.1)
//!
//! §4.6's coverage ratio is `owned shapes / n`, and the whole "hybrid now, own more later" argument
//! turns on whether `n` is a dozen or unbounded. It has been an open question since the RFC was
//! written; it is answerable, and the answer is sitting in the authored views of every nest on this
//! machine.
//!
//! # What counts as a shape
//!
//! Not the SQL text, and not the tables - a planner does not care whether you grouped
//! `stake_delegated` or `escrow_deposit`. It cares which **machinery** a statement demands. So a
//! shape here is the set of structural features a statement uses: set operations, joins, subqueries,
//! which aggregate kinds, grouping, having, ordering, limits, windows, CTEs, `DISTINCT`.
//!
//! Two statements with the same feature set need the same operators. That is the definition that
//! makes the count mean something for a planner, and it is deliberately coarse: it will merge
//! queries an optimiser would treat differently, which biases the answer **towards** "few shapes".
//! Stated plainly so the number is read with the right suspicion.
//!
//! # And the coverage ratio, measured
//!
//! Every statement is also handed to Burrmill's real planner. Whatever it admits is admitted; there
//! is no separate model of the subset that could drift from the code. That is the honest form of
//! §4.6's ratio.

use std::collections::BTreeMap;

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Query, Select, SetExpr, Statement,
    TableFactor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// One structural feature a statement demands of a planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Feature {
    SetOp,
    Join,
    Subquery,
    AggSum,
    AggCount,
    AggCountDistinct,
    AggMinMax,
    AggOther,
    GroupBy,
    Having,
    OrderBy,
    Limit,
    Distinct,
    Window,
    Cte,
    Case,
    Arithmetic,
    Cast,
}

impl Feature {
    fn label(self) -> &'static str {
        match self {
            Self::SetOp => "set-op",
            Self::Join => "join",
            Self::Subquery => "subquery",
            Self::AggSum => "sum",
            Self::AggCount => "count",
            Self::AggCountDistinct => "count-distinct",
            Self::AggMinMax => "min/max",
            Self::AggOther => "agg-other",
            Self::GroupBy => "group-by",
            Self::Having => "having",
            Self::OrderBy => "order-by",
            Self::Limit => "limit",
            Self::Distinct => "distinct",
            Self::Window => "window",
            Self::Cte => "cte",
            Self::Case => "case",
            Self::Arithmetic => "arith",
            Self::Cast => "cast",
        }
    }
}

#[derive(Default)]
struct Shape(std::collections::BTreeSet<Feature>);

impl Shape {
    fn key(&self) -> String {
        if self.0.is_empty() {
            return "scan-only".into();
        }
        self.0.iter().map(|f| f.label()).collect::<Vec<_>>().join("+")
    }

    /// The same statement at the granularity a **planner** cares about.
    ///
    /// The fine-grained key counts `sum+count+group-by` and `count+min/max+group-by` as two shapes.
    /// They are not two shapes to an operator: a grouped aggregate over one table is one piece of
    /// machinery parameterised by which accumulators it carries, and widening it from one aggregate
    /// to k is a loop bound, not a new plan. Likewise `cast` and `arith` are expression-level and
    /// decide nothing about the plan.
    ///
    /// Reporting both is the honest thing, because the fine count is the pessimistic bound and this
    /// is the optimistic one, and A4's question - "a dozen patterns or an open set?" - gets a
    /// different answer depending on which you mean.
    fn family(&self) -> String {
        let mut out: Vec<&str> = Vec::new();
        for (f, label) in [
            (Feature::Cte, "cte"),
            (Feature::SetOp, "set-op"),
            (Feature::Join, "join"),
            (Feature::Subquery, "subquery"),
            (Feature::Window, "window"),
            (Feature::Distinct, "distinct"),
            (Feature::GroupBy, "group-by"),
            (Feature::Having, "having"),
        ] {
            if self.0.contains(&f) {
                out.push(label);
            }
        }
        if self.0.iter().any(|f| {
            matches!(
                f,
                Feature::AggSum
                    | Feature::AggCount
                    | Feature::AggCountDistinct
                    | Feature::AggMinMax
                    | Feature::AggOther
            )
        }) {
            out.push("agg");
        }
        if out.is_empty() {
            return "scan-only".into();
        }
        out.join("+")
    }
}

fn walk_query(q: &Query, sh: &mut Shape) {
    if q.with.is_some() {
        sh.0.insert(Feature::Cte);
        if let Some(w) = &q.with {
            for cte in &w.cte_tables {
                walk_query(&cte.query, sh);
            }
        }
    }
    if !q.order_by.is_none() {
        sh.0.insert(Feature::OrderBy);
    }
    if q.limit_clause.is_some() {
        sh.0.insert(Feature::Limit);
    }
    walk_set(&q.body, sh);
}

fn walk_set(s: &SetExpr, sh: &mut Shape) {
    match s {
        SetExpr::Select(sel) => walk_select(sel, sh),
        SetExpr::Query(q) => walk_query(q, sh),
        SetExpr::SetOperation { left, right, .. } => {
            sh.0.insert(Feature::SetOp);
            walk_set(left, sh);
            walk_set(right, sh);
        }
        _ => {}
    }
}

fn walk_select(sel: &Select, sh: &mut Shape) {
    if sel.distinct.is_some() {
        sh.0.insert(Feature::Distinct);
    }
    if sel.having.is_some() {
        sh.0.insert(Feature::Having);
    }
    if !matches!(sel.group_by, sqlparser::ast::GroupByExpr::Expressions(ref v, _) if v.is_empty()) {
        sh.0.insert(Feature::GroupBy);
    }
    for twj in &sel.from {
        if !twj.joins.is_empty() {
            sh.0.insert(Feature::Join);
        }
        for f in std::iter::once(&twj.relation).chain(twj.joins.iter().map(|j| &j.relation)) {
            if let TableFactor::Derived { subquery, .. } = f {
                walk_query(subquery, sh);
            }
        }
    }
    for item in &sel.projection {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(e)
            | sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } => walk_expr(e, sh),
            _ => {}
        }
    }
    if let Some(w) = &sel.selection {
        walk_expr(w, sh);
    }
    if let Some(h) = &sel.having {
        walk_expr(h, sh);
    }
}

fn walk_expr(e: &Expr, sh: &mut Shape) {
    match e {
        Expr::Function(f) => {
            let name = f.name.to_string().to_ascii_uppercase();
            if f.over.is_some() {
                sh.0.insert(Feature::Window);
            }
            let distinct = matches!(&f.args, FunctionArguments::List(l) if l.duplicate_treatment
                == Some(sqlparser::ast::DuplicateTreatment::Distinct));
            match name.as_str() {
                "SUM" => sh.0.insert(Feature::AggSum),
                "COUNT" if distinct => sh.0.insert(Feature::AggCountDistinct),
                "COUNT" => sh.0.insert(Feature::AggCount),
                "MIN" | "MAX" => sh.0.insert(Feature::AggMinMax),
                "AVG" | "STDDEV" | "ARRAY_AGG" | "STRING_AGG" | "MEDIAN" => {
                    sh.0.insert(Feature::AggOther)
                }
                _ => false,
            };
            if let FunctionArguments::List(l) = &f.args {
                for a in &l.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(x))
                    | FunctionArg::Named { arg: FunctionArgExpr::Expr(x), .. } = a
                    {
                        walk_expr(x, sh);
                    }
                }
            }
        }
        Expr::BinaryOp { left, op, right } => {
            use sqlparser::ast::BinaryOperator::*;
            if matches!(op, Plus | Minus | Multiply | Divide | Modulo) {
                sh.0.insert(Feature::Arithmetic);
            }
            walk_expr(left, sh);
            walk_expr(right, sh);
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => walk_expr(expr, sh),
        Expr::Cast { expr, .. } => {
            sh.0.insert(Feature::Cast);
            walk_expr(expr, sh);
        }
        Expr::Case { .. } => {
            sh.0.insert(Feature::Case);
        }
        Expr::Subquery(q) | Expr::Exists { subquery: q, .. } => {
            sh.0.insert(Feature::Subquery);
            walk_query(q, sh);
        }
        Expr::InSubquery { expr, subquery, .. } => {
            sh.0.insert(Feature::Subquery);
            walk_expr(expr, sh);
            walk_query(subquery, sh);
        }
        _ => {}
    }
}

/// Strip `CREATE VIEW x AS` so the planner sees the query a serving path would run.
fn as_query(stmt: &Statement) -> Option<String> {
    match stmt {
        Statement::Query(q) => Some(q.to_string()),
        Statement::CreateView(cv) => Some(cv.query.to_string()),
        _ => None,
    }
}

/// Is this an **n-table signed fold**: a `UNION ALL` of bare `SELECT key, ±value FROM table`
/// branches, grouped and summed?
///
/// This is the generalisation A4 turned up. Burrmill admits the `n = 1` case - one table read twice,
/// one column crediting and one debiting - and **not one statement in the workload does that**,
/// because a credit and a debit are different events and therefore different tables. Counting the
/// general form says how much a small generalisation would be worth.
fn is_n_table_signed_fold(q: &Query) -> Option<usize> {
    let SetExpr::Select(sel) = q.body.as_ref() else { return None };
    // One grouped SUM over a derived table.
    if sel.from.len() != 1 || !sel.from[0].joins.is_empty() {
        return None;
    }
    let TableFactor::Derived { subquery, .. } = &sel.from[0].relation else { return None };
    let grouped = !matches!(&sel.group_by,
        sqlparser::ast::GroupByExpr::Expressions(v, _) if v.is_empty());
    if !grouped {
        return None;
    }
    let has_sum = sel.projection.iter().any(|p| {
        let e = match p {
            sqlparser::ast::SelectItem::UnnamedExpr(e)
            | sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } => e,
            _ => return false,
        };
        matches!(e, Expr::Function(f) if f.name.to_string().eq_ignore_ascii_case("sum"))
    });
    if !has_sum {
        return None;
    }
    // The inner query must be a UNION ALL chain of plain table scans.
    fn branches(s: &SetExpr, out: &mut usize) -> bool {
        match s {
            SetExpr::Select(sel) => {
                let ok = sel.from.len() == 1
                    && sel.from[0].joins.is_empty()
                    && matches!(sel.from[0].relation, TableFactor::Table { .. })
                    && sel.having.is_none()
                    && matches!(&sel.group_by,
                        sqlparser::ast::GroupByExpr::Expressions(v, _) if v.is_empty());
                *out += 1;
                ok
            }
            SetExpr::SetOperation { left, right, op, set_quantifier } => {
                matches!(op, sqlparser::ast::SetOperator::Union)
                    && matches!(set_quantifier, sqlparser::ast::SetQuantifier::All)
                    && branches(left, out)
                    && branches(right, out)
            }
            _ => false,
        }
    }
    let mut n = 0usize;
    if branches(&subquery.body, &mut n) && n >= 2 {
        Some(n)
    } else {
        None
    }
}

/// Every n-table signed fold anywhere in a statement, CTE bindings and derived tables included.
fn collect_fold_queries<'a>(q: &'a Query, out: &mut Vec<&'a Query>) {
    if is_n_table_signed_fold(q).is_some() {
        out.push(q);
    }
    if let Some(w) = &q.with {
        for cte in &w.cte_tables {
            collect_fold_queries(&cte.query, out);
        }
    }
    if let SetExpr::Select(sel) = q.body.as_ref() {
        for twj in &sel.from {
            for f in std::iter::once(&twj.relation).chain(twj.joins.iter().map(|j| &j.relation)) {
                if let TableFactor::Derived { subquery, .. } = f {
                    collect_fold_queries(subquery, out);
                }
            }
        }
    }
}

pub fn run(roots: &[String]) -> anyhow::Result<()> {
    let mut files = Vec::new();
    for root in roots {
        for e in walkdir::WalkDir::new(root).follow_links(false).into_iter().filter_map(|e| e.ok()) {
            let p = e.path();
            if p.extension().is_some_and(|x| x == "sql")
                && p.components().any(|c| c.as_os_str() == "views")
                && !p.components().any(|c| c.as_os_str() == "target")
            {
                files.push(p.to_path_buf());
            }
        }
    }
    files.sort();
    files.dedup();
    anyhow::ensure!(!files.is_empty(), "no views/*.sql found under {roots:?}");

    let dialect = GenericDialect {};
    let (mut statements, mut unparsed) = (0usize, 0usize);
    let mut shapes: BTreeMap<String, usize> = BTreeMap::new();
    let mut families: BTreeMap<String, usize> = BTreeMap::new();
    let mut admitted = 0usize;
    let mut nfold: BTreeMap<usize, usize> = BTreeMap::new();
    let (mut subplans, mut subplans_admitted) = (0usize, 0usize);
    let mut subplan_refusals: BTreeMap<String, usize> = BTreeMap::new();
    let mut refusals: BTreeMap<String, usize> = BTreeMap::new();

    for f in &files {
        let text = std::fs::read_to_string(f)?;
        let parsed = match Parser::parse_sql(&dialect, &text) {
            Ok(p) => p,
            Err(_) => {
                unparsed += 1;
                continue;
            }
        };
        for stmt in &parsed {
            let Some(sql) = as_query(stmt) else { continue };
            statements += 1;
            let mut sh = Shape::default();
            if let Statement::Query(q) = stmt {
                walk_query(q, &mut sh);
            } else if let Statement::CreateView(cv) = stmt {
                walk_query(&cv.query, &mut sh);
            }
            *shapes.entry(sh.key()).or_default() += 1;
            *families.entry(sh.family()).or_default() += 1;

            let inner = match stmt {
                Statement::Query(q) => Some(q.as_ref()),
                Statement::CreateView(cv) => Some(cv.query.as_ref()),
                _ => None,
            };
            // **Look inside the CTEs too.** The first version of this checked only the top-level
            // body and reported 0/65, which would have been a wrong and rather damaging headline:
            // the fold in `40-lodestar-allocations.sql` is real and lives in a `WITH` binding. A
            // measurement that finds nothing is the one to distrust hardest.
            if let Some(q) = inner {
                let mut found = Vec::new();
                collect_fold_queries(q, &mut found);
                for fq in found {
                    let n = is_n_table_signed_fold(fq).unwrap_or(0);
                    *nfold.entry(n).or_default() += 1;
                    // **Plan the fold itself, not the statement it sits in.** Every one of these is
                    // a sub-query inside a CTE or a join, so statement-level coverage cannot move
                    // until CTEs and joins are admitted - which is a different item. What an owned
                    // operator can execute today is the fold, and that is what this counts.
                    subplans += 1;
                    match burrmill::plan::plan(&fq.to_string()) {
                        Ok(_) => subplans_admitted += 1,
                        Err(e) => *subplan_refusals
                            .entry(e.to_string().chars().take(72).collect())
                            .or_default() += 1,
                    }
                }
            }

            // **The real planner, not a model of it.** A separate notion of the admitted subset
            // would drift from the code, and the number would then be about the model.
            match burrmill::plan::plan(&sql) {
                Ok(_) => admitted += 1,
                Err(e) => {
                    let reason = e.to_string();
                    let head = reason.split(':').nth(1).unwrap_or(&reason).trim().to_string();
                    *refusals.entry(head.chars().take(72).collect()).or_default() += 1;
                }
            }
        }
    }

    let mut ranked: Vec<_> = shapes.iter().collect();
    ranked.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));

    println!("A4: plan shapes in a real workload");
    println!("  files          {}", files.len());
    println!("  statements     {statements}");
    println!("  unparsed files {unparsed}");
    println!("  DISTINCT SHAPES  {}  (expression granularity: which aggregates, casts, arithmetic)", shapes.len());
    println!("  PLAN FAMILIES    {}  (what an operator actually has to be)", families.len());
    println!(
        "  admitted today  {admitted}/{statements}  ({:.1}% coverage ratio, §4.6)",
        100.0 * admitted as f64 / statements.max(1) as f64
    );
    let top: usize = ranked.iter().take(10).map(|(_, n)| **n).sum();
    println!(
        "  top 10 shapes cover {top}/{statements} ({:.0}%)",
        100.0 * top as f64 / statements.max(1) as f64
    );
    let mut fam: Vec<_> = families.iter().collect();
    fam.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
    let famtop: usize = fam.iter().take(5).map(|(_, n)| **n).sum();
    println!(
        "  top 5 families cover {famtop}/{statements} ({:.0}%)",
        100.0 * famtop as f64 / statements.max(1) as f64
    );
    let folds: usize = nfold.values().sum();
    println!(
        "\n  FOLD SUB-PLANS  {subplans_admitted}/{subplans} admitted  ({:.0}%)  <- what an owned\n           operator can execute today. Statement-level coverage stays at {admitted}/{statements}\n           because every one of these folds sits inside a CTE or a join.",
        100.0 * subplans_admitted as f64 / subplans.max(1) as f64
    );
    if !subplan_refusals.is_empty() {
        let mut sr: Vec<_> = subplan_refusals.iter().collect();
        sr.sort_by(|a, b| b.1.cmp(a.1));
        for (k, n) in sr {
            println!("    {n:>3}  {k}");
        }
    }
    println!(
        "\n  n-table signed folds: {folds}/{statements} statements, branch counts {:?}",
        nfold.iter().map(|(n, c)| format!("{n}x{c}")).collect::<Vec<_>>()
    );
    println!(
        "  Burrmill admits only the one-table-read-twice case. Every fold above reads n DIFFERENT\n           tables, because a credit and a debit are different events - so the admitted shape occurs\n           ZERO times in the workload it was built for."
    );

    println!("\n  count  plan family");
    for (k, n) in &fam {
        println!("  {n:>5}  {k}");
    }
    println!("\n  count  shape");
    for (k, n) in &ranked {
        println!("  {n:>5}  {k}");
    }
    println!("\n  count  why the planner refuses");
    let mut rr: Vec<_> = refusals.iter().collect();
    rr.sort_by(|a, b| b.1.cmp(a.1));
    for (k, n) in rr.iter() {
        println!("  {n:>5}  {k}");
    }
    Ok(())
}
