//! DuckDB's column names for derived tables and CTEs.
//!
//! Inside a subquery DuckDB renames a repeated column, `a, a` to `a, a_1`, compared without regard
//! to case, and names an unaliased expression by its text, `(x + 1)`; the outer query sees and may
//! use those names. DataFusion refuses the repeat, or renames it `a:1`, or, for an unaliased
//! subquery, passes both through under one name, where the result encoder keeps one of the two:
//! `SELECT * FROM (SELECT * FROM t a JOIN t b ON ...)` lost half its columns without a word.
//!
//! Each derived table and CTE is walked bottom-up for its output names, wildcards expanded from the
//! tables' columns or from an inner subquery already named, and its select list rewritten where a
//! name changes. Where the names cannot be known for certain (`USING`, semi joins, table functions,
//! wildcard options, an expression with no printed name) the query is left as it was.

use sqlparser::ast::{
    self as sq, Expr, Ident, JoinConstraint, JoinOperator, Query, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, TableFactor, TableWithJoins,
};

use super::dialect::Known;

/// A relation in a FROM clause: the name it is referred to by, and its columns if known.
struct Relation {
    qualifier: Option<String>,
    columns: Option<Vec<String>>,
}

type Ctes = Vec<(String, Option<Vec<String>>)>;

/// Names every derived table and CTE under `q` as DuckDB does; `q`'s own output is left alone.
pub fn name(q: &mut Query, known: &Known) {
    let mut ctes = Vec::new();
    query(q, known, &mut ctes, false);
}

fn query(q: &mut Query, known: &Known, ctes: &mut Ctes, rename: bool) -> Option<Vec<String>> {
    let depth = ctes.len();
    if let Some(with) = q.with.as_mut() {
        for cte in with.cte_tables.iter_mut() {
            let names = if with.recursive {
                // The body refers to itself before its names exist.
                None
            } else {
                query(&mut cte.query, known, ctes, true)
            };
            let names = with_column_aliases(names, &cte.alias.columns);
            ctes.push((cte.alias.name.value.to_lowercase(), names));
        }
    }
    let out = set_expr(&mut q.body, known, ctes, rename);
    ctes.truncate(depth);
    out
}

fn set_expr(b: &mut SetExpr, known: &Known, ctes: &mut Ctes, rename: bool) -> Option<Vec<String>> {
    match b {
        SetExpr::Select(s) => select(s, known, ctes, rename),
        SetExpr::Query(q) => query(q, known, ctes, rename),
        // The first branch names a set operation's columns; the others are walked for their own
        // subqueries.
        SetExpr::SetOperation { left, right, .. } => {
            let names = set_expr(left, known, ctes, rename);
            set_expr(right, known, ctes, false);
            names
        }
        _ => None,
    }
}

fn with_column_aliases(names: Option<Vec<String>>, aliases: &[sq::TableAliasColumnDef]) -> Option<Vec<String>> {
    if aliases.is_empty() {
        return names;
    }
    let mut names = names?;
    if aliases.len() > names.len() {
        return None;
    }
    for (n, a) in names.iter_mut().zip(aliases) {
        *n = a.name.value.clone();
    }
    Some(dedupe(&names))
}

/// DuckDB's rule: a name already taken, compared without case, gets `_1`, `_2`, ... until free.
fn dedupe(names: &[String]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut count = std::collections::HashMap::<String, usize>::new();
    names
        .iter()
        .map(|n| {
            let mut name = n.clone();
            let c = count.entry(n.to_lowercase()).or_insert(0);
            while seen.contains(&name.to_lowercase()) {
                *c += 1;
                name = format!("{n}_{c}");
            }
            seen.insert(name.to_lowercase());
            name
        })
        .collect()
}

fn factor(t: &mut TableFactor, known: &Known, ctes: &mut Ctes, out: &mut Vec<Relation>) {
    match t {
        TableFactor::Table { name, alias, args: None, .. } => {
            let table = name.0.last().map(|p| p.to_string()).unwrap_or_default();
            let lower = table.trim_matches('"').to_lowercase();
            let columns = if name.0.len() == 1 {
                match ctes.iter().rev().find(|(n, _)| *n == lower) {
                    Some((_, c)) => c.clone(),
                    None => known.columns(&lower),
                }
            } else {
                known.columns(&lower)
            };
            let (qualifier, columns) = match alias {
                Some(a) => (a.name.value.clone(), with_column_aliases(columns, &a.columns)),
                None => (table.trim_matches('"').to_string(), columns),
            };
            out.push(Relation { qualifier: Some(qualifier), columns });
        }
        TableFactor::Derived { subquery, alias, .. } => {
            let names = query(subquery, known, ctes, true);
            match alias {
                Some(a) => out.push(Relation {
                    qualifier: Some(a.name.value.clone()),
                    columns: with_column_aliases(names, &a.columns),
                }),
                None => out.push(Relation { qualifier: None, columns: names }),
            }
        }
        TableFactor::NestedJoin { table_with_joins, alias: None } => {
            from(std::slice::from_mut(table_with_joins.as_mut()), known, ctes, out);
        }
        _ => out.push(Relation { qualifier: None, columns: None }),
    }
}

/// The relations a FROM clause brings into scope, in the order `*` lists them. A join whose columns
/// `*` does not simply concatenate is a relation of unknown columns.
fn from(tables: &mut [TableWithJoins], known: &Known, ctes: &mut Ctes, out: &mut Vec<Relation>) {
    for t in tables {
        factor(&mut t.relation, known, ctes, out);
        for j in t.joins.iter_mut() {
            let plain = match &j.join_operator {
                JoinOperator::Join(c)
                | JoinOperator::Inner(c)
                | JoinOperator::Left(c)
                | JoinOperator::LeftOuter(c)
                | JoinOperator::Right(c)
                | JoinOperator::RightOuter(c)
                | JoinOperator::FullOuter(c)
                | JoinOperator::CrossJoin(c) => matches!(c, JoinConstraint::On(_) | JoinConstraint::None),
                _ => false,
            };
            factor(&mut j.relation, known, ctes, out);
            if !plain {
                out.push(Relation { qualifier: None, columns: None });
            }
        }
    }
}

fn plain_wildcard(o: &sq::WildcardAdditionalOptions) -> bool {
    o.opt_ilike.is_none()
        && o.opt_exclude.is_none()
        && o.opt_except.is_none()
        && o.opt_replace.is_none()
        && o.opt_rename.is_none()
        && o.opt_alias.is_none()
}

/// What an item contributes: `(name, qualifier to spell it with)` per column, `None` if unknown.
fn item_names(item: &SelectItem, relations: &[Relation]) -> Option<Vec<(String, Option<String>)>> {
    match item {
        SelectItem::UnnamedExpr(Expr::Identifier(i)) => Some(vec![(i.value.clone(), None)]),
        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(v)) => {
            Some(vec![(v.last()?.value.clone(), None)])
        }
        SelectItem::UnnamedExpr(e) => Some(vec![(super::names::printed(e)?, None)]),
        SelectItem::ExprWithAlias { alias, .. } => Some(vec![(alias.value.clone(), None)]),
        SelectItem::Wildcard(o) if plain_wildcard(o) => {
            let mut out = Vec::new();
            for r in relations {
                for c in r.columns.as_ref()? {
                    out.push((c.clone(), Some(r.qualifier.clone()?)));
                }
            }
            Some(out)
        }
        SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(n), o)
            if plain_wildcard(o) =>
        {
            let q = n.0.last()?.to_string();
            let q = q.trim_matches('"');
            let r = relations
                .iter()
                .find(|r| r.qualifier.as_deref().is_some_and(|x| x.eq_ignore_ascii_case(q)))?;
            let qualifier = r.qualifier.clone()?;
            Some(r.columns.as_ref()?.iter().map(|c| (c.clone(), Some(qualifier.clone()))).collect())
        }
        _ => None,
    }
}

fn select(s: &mut Select, known: &Known, ctes: &mut Ctes, rename: bool) -> Option<Vec<String>> {
    let mut relations = Vec::new();
    from(&mut s.from, known, ctes, &mut relations);
    let items: Vec<Option<Vec<(String, Option<String>)>>> =
        s.projection.iter().map(|i| item_names(i, &relations)).collect();
    let items: Vec<Vec<(String, Option<String>)>> = items.into_iter().collect::<Option<_>>()?;
    let written: Vec<String> = items.iter().flatten().map(|(n, _)| n.clone()).collect();
    let finals = dedupe(&written);
    if !rename {
        return Some(finals);
    }
    let unnamed = |i: &SelectItem| {
        matches!(i, SelectItem::UnnamedExpr(e) if !matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_)))
    };
    if finals == written && !s.projection.iter().any(unnamed) {
        return Some(finals);
    }
    let mut next = finals.iter();
    let mut projection = Vec::with_capacity(finals.len());
    for (item, cols) in s.projection.drain(..).zip(items) {
        let renamed: Vec<&String> = cols.iter().map(|_| next.next().expect("one name per column")).collect();
        let changed = cols.iter().zip(&renamed).any(|((w, _), f)| w != *f);
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) if changed => {
                for ((col, qualifier), f) in cols.into_iter().zip(renamed) {
                    let qualifier = qualifier.expect("a wildcard's columns are qualified");
                    projection.push(SelectItem::ExprWithAlias {
                        expr: Expr::CompoundIdentifier(vec![Ident::with_quote('"', qualifier), Ident::with_quote('"', col)]),
                        alias: Ident::with_quote('"', f.clone()),
                    });
                }
            }
            SelectItem::UnnamedExpr(e) if changed || unnamed(&SelectItem::UnnamedExpr(e.clone())) => {
                projection.push(SelectItem::ExprWithAlias { expr: e, alias: Ident::with_quote('"', renamed[0].clone()) });
            }
            SelectItem::ExprWithAlias { expr, .. } if changed => {
                projection.push(SelectItem::ExprWithAlias { expr, alias: Ident::with_quote('"', renamed[0].clone()) });
            }
            item => projection.push(item),
        }
    }
    s.projection = projection;
    Some(finals)
}
