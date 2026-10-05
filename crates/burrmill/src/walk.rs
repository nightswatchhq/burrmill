//! sqlparser's AST walk, compiled once per direction (#7).
//!
//! `Visit::visit` is generic over its visitor, so each visitor type, and each closure handed to
//! `visit_expressions`, compiled its own copy of the walk: some thousands of functions under a
//! `Statement`. Every walk here goes through one `dyn` visitor instead.

use std::ops::ControlFlow;

use sqlparser::ast::{
    Expr, ObjectName, Query, Select, Statement, TableFactor, ValueWithSpan, Visit, VisitMut,
    Visitor, VisitorMut,
};

struct Dyn<'a>(&'a mut dyn Visitor<Break = ()>);
struct DynMut<'a>(&'a mut dyn VisitorMut<Break = ()>);

macro_rules! forward {
    ($($m:ident: $t:ty),* $(,)?) => {
        $(fn $m(&mut self, x: $t) -> ControlFlow<()> {
            self.0.$m(x)
        })*
    };
}

impl Visitor for Dyn<'_> {
    type Break = ();
    forward!(
        pre_visit_query: &Query, post_visit_query: &Query,
        pre_visit_select: &Select, post_visit_select: &Select,
        pre_visit_relation: &ObjectName, post_visit_relation: &ObjectName,
        pre_visit_table_factor: &TableFactor, post_visit_table_factor: &TableFactor,
        pre_visit_expr: &Expr, post_visit_expr: &Expr,
        pre_visit_statement: &Statement, post_visit_statement: &Statement,
        pre_visit_value: &ValueWithSpan, post_visit_value: &ValueWithSpan,
    );
}

impl VisitorMut for DynMut<'_> {
    type Break = ();
    forward!(
        pre_visit_query: &mut Query, post_visit_query: &mut Query,
        pre_visit_select: &mut Select, post_visit_select: &mut Select,
        pre_visit_relation: &mut ObjectName, post_visit_relation: &mut ObjectName,
        pre_visit_table_factor: &mut TableFactor, post_visit_table_factor: &mut TableFactor,
        pre_visit_expr: &mut Expr, post_visit_expr: &mut Expr,
        pre_visit_statement: &mut Statement, post_visit_statement: &mut Statement,
        pre_visit_value: &mut ValueWithSpan, post_visit_value: &mut ValueWithSpan,
    );
}

pub(crate) fn walk<T: Visit + ?Sized>(
    root: &T,
    v: &mut dyn Visitor<Break = ()>,
) -> ControlFlow<()> {
    root.visit(&mut Dyn(v))
}

pub(crate) fn walk_mut<T: VisitMut + ?Sized>(
    root: &mut T,
    v: &mut dyn VisitorMut<Break = ()>,
) -> ControlFlow<()> {
    root.visit(&mut DynMut(v))
}

/// `visit_expressions`: `f` on each expression, before its children.
#[cfg(feature = "datafusion")]
pub(crate) fn exprs<T: Visit + ?Sized>(
    root: &T,
    mut f: impl FnMut(&Expr) -> ControlFlow<()>,
) -> ControlFlow<()> {
    struct F<'a>(&'a mut dyn FnMut(&Expr) -> ControlFlow<()>);
    impl Visitor for F<'_> {
        type Break = ();
        fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
            (self.0)(e)
        }
    }
    walk(root, &mut F(&mut f))
}

/// `visit_expressions_mut`: `f` on each expression, after its children.
#[cfg(feature = "datafusion")]
pub(crate) fn exprs_mut<T: VisitMut + ?Sized>(
    root: &mut T,
    mut f: impl FnMut(&mut Expr) -> ControlFlow<()>,
) -> ControlFlow<()> {
    struct F<'a>(&'a mut dyn FnMut(&mut Expr) -> ControlFlow<()>);
    impl VisitorMut for F<'_> {
        type Break = ();
        fn post_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
            (self.0)(e)
        }
    }
    walk_mut(root, &mut F(&mut f))
}

/// `visit_relations`: `f` on each table name.
#[cfg(feature = "datafusion")]
pub(crate) fn relations<T: Visit + ?Sized>(
    root: &T,
    mut f: impl FnMut(&ObjectName) -> ControlFlow<()>,
) -> ControlFlow<()> {
    struct F<'a>(&'a mut dyn FnMut(&ObjectName) -> ControlFlow<()>);
    impl Visitor for F<'_> {
        type Break = ();
        fn pre_visit_relation(&mut self, r: &ObjectName) -> ControlFlow<()> {
            (self.0)(r)
        }
    }
    walk(root, &mut F(&mut f))
}
