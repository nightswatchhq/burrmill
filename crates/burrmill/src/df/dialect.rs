//! DuckDB's dialect on DataFusion (roadmap 6.5).
//!
//! Statements parse with sqlparser's DuckDB dialect, so `HUGEINT`, `UBIGINT` and `//` parse at all,
//! and the AST is then rewritten into what DataFusion plans:
//!
//! - `HUGEINT` → `DECIMAL(38,0)`, the same 128 bits; `UHUGEINT` is refused, having no home.
//! - `UBIGINT`, `UINTEGER`, `USMALLINT`, `UTINYINT` → DataFusion's `... UNSIGNED` spellings.
//! - `a // b` → `burrmill_intdiv(a, b)`, exact and truncating. Built on `/` it would become a
//!   float, because `/` is DOUBLE in DuckDB and is made so here too.
//! - `(a, b) > (c, d)` → the lexicographic comparison it means.
//!
//! Semantic differences that type checking must see (`/` as DOUBLE) live in [`DuckSemantics`], an
//! analyzer rule, not here.

use std::ops::ControlFlow;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Decimal128Builder, Int64Builder};
use arrow::datatypes::{DataType, Decimal128Type, Int64Type, TimeUnit, UInt64Type};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, Result as DFResult, exec_err, plan_err};
use datafusion_expr::{
    BinaryExpr, Cast, ColumnarValue, Expr, ExprSchemable, LogicalPlan, Operator,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TryCast, Volatility,
};
use datafusion_optimizer::analyzer::AnalyzerRule;
use datafusion_sql::parser::{DFParser, Statement as DfStatement};
use sqlparser::ast::{
    self as sq, BinaryOperator, DataType as SqlType, ExactNumberInfo, Expr as SqlExpr, VisitorMut,
};
use sqlparser::dialect::DuckDbDialect;

use crate::error::{BurrmillError, Result};

/// Parse with DuckDB's dialect and rewrite into what DataFusion plans. The result columns' DuckDB
/// names come back too, taken from the statement as written, before any rewrite.
pub fn parse(sql: &str, known: &Known) -> Result<(DfStatement, Vec<Option<String>>)> {
    let stmts = DFParser::parse_sql_with_dialect(sql, &Duck)
        .map_err(|e| BurrmillError::Parse(super::errors::restate(format!("SQL error: {e:?}"))))?;
    let Some(mut stmt) = stmts.into_iter().next() else {
        return Err(BurrmillError::Parse("empty statement".into()));
    };
    let mut names = match &stmt {
        DfStatement::Statement(s) => match s.as_ref() {
            sq::Statement::Query(q) => super::names::default_names(q),
            _ => vec![],
        },
        _ => vec![],
    };
    rewrite(&mut stmt, known, &mut names)?;
    Ok((stmt, names))
}

/// DuckDB's dialect with `IS [NOT] DISTINCT FROM` bound as DuckDB binds it. sqlparser reads its
/// right side as a whole expression, so `a IS DISTINCT FROM b OR c = d` became
/// `a IS DISTINCT FROM (b OR c = d)`; here it stops where `IS` does, as in Postgres.
#[derive(Debug)]
struct Duck;

macro_rules! as_duckdb {
    ($($f:ident),* $(,)?) => {
        $(fn $f(&self) -> bool { DuckDbDialect {}.$f() })*
    };
}

impl sqlparser::dialect::Dialect for Duck {
    fn dialect(&self) -> std::any::TypeId {
        std::any::TypeId::of::<DuckDbDialect>()
    }
    fn is_identifier_start(&self, ch: char) -> bool {
        DuckDbDialect {}.is_identifier_start(ch)
    }
    fn is_identifier_part(&self, ch: char) -> bool {
        DuckDbDialect {}.is_identifier_part(ch)
    }
    as_duckdb!(
        supports_trailing_commas,
        supports_filter_during_aggregation,
        supports_group_by_expr,
        supports_bitwise_shift_operators,
        supports_named_fn_args_with_eq_operator,
        supports_named_fn_args_with_assignment_operator,
        supports_dictionary_syntax,
        support_map_literal_syntax,
        supports_lambda_functions,
        allow_extract_single_quotes,
        supports_explain_with_utility_options,
        supports_load_extension,
        supports_array_typedef_with_brackets,
        supports_from_first_select,
        supports_order_by_all,
        supports_select_wildcard_exclude,
        supports_notnull_operator,
        supports_install,
        supports_detach,
        supports_select_wildcard_replace,
        supports_comma_separated_trim,
    );

    fn parse_infix(
        &self,
        parser: &mut sqlparser::parser::Parser,
        expr: &SqlExpr,
        _precedence: u8,
    ) -> Option<std::result::Result<SqlExpr, sqlparser::parser::ParserError>> {
        use sqlparser::dialect::Precedence;
        use sqlparser::keywords::Keyword::{DISTINCT, FROM, IS, NOT};
        let not = if parser.parse_keywords(&[IS, DISTINCT, FROM]) {
            false
        } else if parser.parse_keywords(&[IS, NOT, DISTINCT, FROM]) {
            true
        } else {
            return None;
        };
        let (a, b) = match parser.parse_subexpr(self.prec_value(Precedence::Is)) {
            Ok(b) => (Box::new(expr.clone()), Box::new(b)),
            Err(e) => return Some(Err(e)),
        };
        Some(Ok(if not { SqlExpr::IsNotDistinctFrom(a, b) } else { SqlExpr::IsDistinctFrom(a, b) }))
    }
}

/// Names a statement may refer to: the nest's tables and columns, and the statement's own aliases.
/// DuckDB resolves identifiers without regard to case, quoted or not; DataFusion, as configured
/// here, resolves them exactly.
#[derive(Clone, Default)]
pub struct Known {
    exact: std::collections::HashSet<String>,
    folded: std::collections::HashMap<String, std::collections::BTreeSet<String>>,
    /// Each table's and view's columns in order, by lowercased name, for expanding `*`.
    tables: std::collections::HashMap<String, Vec<String>>,
}

impl Known {
    pub fn add_table(&mut self, name: &str, columns: Vec<String>) {
        self.add(name);
        for c in &columns {
            self.add(c);
        }
        self.tables.insert(name.to_lowercase(), columns);
    }

    pub fn columns(&self, lowercased: &str) -> Option<Vec<String>> {
        self.tables.get(lowercased).cloned()
    }

    pub fn add(&mut self, name: &str) {
        self.exact.insert(name.to_string());
        self.folded
            .entry(name.to_lowercase())
            .or_default()
            .insert(name.to_string());
    }

    /// An identifier written in another case than the one name it can mean, as that name.
    fn resolve(&self, written: &str) -> Option<&str> {
        if self.exact.contains(written) {
            return None;
        }
        match self.folded.get(&written.to_lowercase()) {
            Some(set) if set.len() == 1 => set.iter().next().map(String::as_str),
            _ => None,
        }
    }
}

struct Aliases<'a>(&'a mut Known);

impl sq::Visitor for Aliases<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, q: &sq::Query) -> ControlFlow<()> {
        for cte in q.with.iter().flat_map(|w| &w.cte_tables) {
            self.alias(&cte.alias);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, s: &sq::Select) -> ControlFlow<()> {
        for item in &s.projection {
            if let sq::SelectItem::ExprWithAlias { alias, .. } = item {
                self.0.add(&alias.value);
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, t: &sq::TableFactor) -> ControlFlow<()> {
        match t {
            sq::TableFactor::Table { alias: Some(a), .. }
            | sq::TableFactor::Derived { alias: Some(a), .. } => self.alias(a),
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

impl Aliases<'_> {
    fn alias(&mut self, a: &sq::TableAlias) {
        self.0.add(&a.name.value);
        for c in &a.columns {
            self.0.add(&c.name.value);
        }
    }
}

struct CaseFix<'a>(&'a Known);

impl CaseFix<'_> {
    fn fix(&self, i: &mut sq::Ident) {
        if let Some(name) = self.0.resolve(&i.value) {
            i.value = name.to_string();
        }
    }
}

impl VisitorMut for CaseFix<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, e: &mut SqlExpr) -> ControlFlow<()> {
        match e {
            SqlExpr::Identifier(i) => self.fix(i),
            SqlExpr::CompoundIdentifier(v) => v.iter_mut().for_each(|i| self.fix(i)),
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, r: &mut sq::ObjectName) -> ControlFlow<()> {
        for part in &mut r.0 {
            if let sq::ObjectNamePart::Identifier(i) = part {
                self.fix(i);
            }
        }
        ControlFlow::Continue(())
    }
}

/// Marks a private suffix on a repeated output name; [`strip_dup_suffix`] takes it off the result.
pub const DUP: char = '\u{1}';

/// DuckDB allows two result columns of one name (`SELECT *, value`); DataFusion's projection does
/// not. Repeated top-level items get a private suffix here and lose it again on the result.
fn dedupe_output_names(q: &mut sq::Query, names: &mut [Option<String>]) {
    let sq::SetExpr::Select(sel) = q.body.as_mut() else {
        return;
    };
    let wildcard = sel.projection.iter().any(|i| {
        matches!(
            i,
            sq::SelectItem::Wildcard(_) | sq::SelectItem::QualifiedWildcard(..)
        )
    });
    // `ORDER BY s.x` beside `... AS x`: DuckDB sorts by the source column, DataFusion adds it to
    // the projection and then refuses `s.x` beside `x` as ambiguous. The alias is suffixed instead.
    let ordered: std::collections::HashSet<String> = match &q.order_by {
        Some(sq::OrderBy {
            kind: sq::OrderByKind::Expressions(items),
            ..
        }) => items
            .iter()
            .filter_map(|o| match &o.expr {
                SqlExpr::CompoundIdentifier(v) => v.last().map(|i| i.value.to_lowercase()),
                _ => None,
            })
            .collect(),
        _ => Default::default(),
    };
    let mut seen = std::collections::HashSet::new();
    let mut pos = 0usize;
    for (i, item) in sel.projection.iter_mut().enumerate() {
        let collides = match item {
            sq::SelectItem::ExprWithAlias { expr, alias } => {
                ordered.contains(&alias.value.to_lowercase())
                    && !matches!(expr, SqlExpr::CompoundIdentifier(v)
                        if v.last().is_some_and(|l| l.value.eq_ignore_ascii_case(&alias.value)))
            }
            _ => false,
        };
        let (key, display, bare) = match item {
            sq::SelectItem::UnnamedExpr(e) => match e {
                SqlExpr::Identifier(id) => (id.value.to_lowercase(), id.value.clone(), true),
                SqlExpr::CompoundIdentifier(v) => {
                    let last = v.last().map(|x| x.value.clone()).unwrap_or_default();
                    (last.to_lowercase(), last, true)
                }
                e => {
                    let shown = names
                        .get(i)
                        .cloned()
                        .flatten()
                        .unwrap_or_else(|| e.to_string());
                    (e.to_string(), shown, false)
                }
            },
            sq::SelectItem::ExprWithAlias { alias, .. } => {
                (alias.value.to_lowercase(), alias.value.clone(), false)
            }
            _ => continue,
        };
        let repeated = !seen.insert(key) || (wildcard && bare) || collides;
        if repeated {
            let alias = sq::Ident::with_quote('"', format!("{display}{DUP}{pos}"));
            pos += 1;
            *item = match std::mem::replace(item, sq::SelectItem::Wildcard(Default::default())) {
                sq::SelectItem::UnnamedExpr(expr) | sq::SelectItem::ExprWithAlias { expr, .. } => {
                    sq::SelectItem::ExprWithAlias { expr, alias }
                }
                other => other,
            };
            if let Some(n) = names.get_mut(i) {
                *n = None;
            }
        }
    }
}

/// The result's field names without the suffixes [`dedupe_output_names`] added.
pub fn strip_dup_suffix(b: arrow::record_batch::RecordBatch) -> arrow::record_batch::RecordBatch {
    let schema = b.schema();
    if !schema.fields().iter().any(|f| f.name().contains(DUP)) {
        return b;
    }
    let fields: Vec<arrow::datatypes::FieldRef> = schema
        .fields()
        .iter()
        .map(|f| match f.name().split_once(DUP) {
            Some((n, _)) => Arc::new(f.as_ref().clone().with_name(n)),
            None => Arc::clone(f),
        })
        .collect();
    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
    arrow::record_batch::RecordBatch::try_new(schema, b.columns().to_vec()).expect("same columns")
}

/// The rewrites, on a statement or on the one an `EXPLAIN` wraps.
fn rewrite(stmt: &mut DfStatement, known: &Known, names: &mut [Option<String>]) -> Result<()> {
    let s = match stmt {
        DfStatement::Statement(s) => s,
        DfStatement::Explain(e) => return rewrite(e.statement.as_mut(), known, &mut []),
        _ => return Ok(()),
    };
    let mut known = known.clone();
    let _ = sq::Visit::visit(s.as_ref(), &mut Aliases(&mut known));
    let _ = sq::VisitMut::visit(s.as_mut(), &mut CaseFix(&known));
    if let sq::Statement::Query(q) = s.as_mut() {
        dedupe_output_names(q, names);
        super::subqueries::name(q, &known);
    }
    let mut rw = Rewriter { refused: None, lambda: vec![] };
    let _ = sq::VisitMut::visit(s.as_mut(), &mut rw);
    match rw.refused {
        Some(why) => Err(BurrmillError::NotAllowed(why)),
        None => Ok(()),
    }
}

struct Rewriter {
    refused: Option<String>,
    /// Parameters of the lambdas the walk is inside.
    lambda: Vec<String>,
}

fn call(name: &str, args: Vec<SqlExpr>) -> SqlExpr {
    SqlExpr::Function(sq::Function {
        name: sq::ObjectName::from(vec![sq::Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: sq::FunctionArguments::None,
        args: sq::FunctionArguments::List(sq::FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|a| sq::FunctionArg::Unnamed(sq::FunctionArgExpr::Expr(a)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

fn binop(l: SqlExpr, op: BinaryOperator, r: SqlExpr) -> SqlExpr {
    SqlExpr::BinaryOp {
        left: Box::new(l),
        op,
        right: Box::new(r),
    }
}

/// `(a1, a2) op (b1, b2)`, lexicographically.
fn tuple_cmp(a: &[SqlExpr], b: &[SqlExpr], op: &BinaryOperator) -> SqlExpr {
    use BinaryOperator::*;
    let strict = if matches!(op, Gt | GtEq) { Gt } else { Lt };
    if a.len() == 1 {
        return binop(a[0].clone(), op.clone(), b[0].clone());
    }
    let head = binop(a[0].clone(), strict, b[0].clone());
    let eq = binop(a[0].clone(), Eq, b[0].clone());
    let tail = tuple_cmp(&a[1..], &b[1..], op);
    binop(head, Or, SqlExpr::Nested(Box::new(binop(eq, And, tail))))
}

/// DuckDB's type names DataFusion does not plan, renamed in place; `Some` is a refusal.
fn retype(t: &mut SqlType) -> Option<String> {
    *t = match t {
        SqlType::HugeInt => SqlType::Decimal(ExactNumberInfo::PrecisionAndScale(38, 0)),
        SqlType::UBigInt => SqlType::BigIntUnsigned(None),
        SqlType::UHugeInt => {
            return Some(
                "UHUGEINT has no exact home here: DECIMAL(38,0) stops short of 2^128".into(),
            );
        }
        _ => return None,
    };
    None
}

impl VisitorMut for Rewriter {
    type Break = ();

    fn post_visit_expr(&mut self, e: &mut SqlExpr) -> ControlFlow<()> {
        if let SqlExpr::Lambda(l) = e {
            self.lambda.truncate(self.lambda.len() - l.params.len());
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &mut SqlExpr) -> ControlFlow<()> {
        match e {
            SqlExpr::Lambda(l) => self.lambda.extend(l.params.iter().map(|p| p.name.value.clone())),
            // DuckDB types a literal by its spelling: `1e3` is DOUBLE, `0.5` DECIMAL(2,1) and `.5`
            // DECIMAL(1,1), digits as written, and past 38 digits DOUBLE. DataFusion drops the
            // leading zero, so the type is written out here, as a cast of the literal's own text
            // (text, so the walk that descends into the cast finds no number to wrap again).
            SqlExpr::Value(v)
                if matches!(&v.value, sq::Value::Number(n, _) if n.contains(['e', 'E', '.'])) =>
            {
                let sq::Value::Number(n, _) = &v.value else { unreachable!() };
                let data_type = match n.split_once('.') {
                    Some((int, frac)) if !n.contains(['e', 'E']) && int.len() + frac.len() <= 38 => {
                        SqlType::Decimal(ExactNumberInfo::PrecisionAndScale(
                            (int.len() + frac.len()) as u64,
                            frac.len() as i64,
                        ))
                    }
                    _ => SqlType::Double(ExactNumberInfo::None),
                };
                let text = SqlExpr::Value(sq::Value::SingleQuotedString(n.clone()).into());
                *e = SqlExpr::Cast {
                    kind: sq::CastKind::Cast,
                    expr: Box::new(text),
                    data_type,
                    array: false,
                    format: None,
                };
            }
            // DataFusion looks lambda parameters up by bare name only; `acc.rate` would be read as
            // column `rate` of a table `acc`. As a field access on `acc`, it reaches the parameter.
            SqlExpr::CompoundIdentifier(parts)
                if parts.len() > 1 && self.lambda.contains(&parts[0].value) =>
            {
                let root = SqlExpr::Identifier(parts[0].clone());
                let access_chain = parts[1..]
                    .iter()
                    .map(|f| {
                        sq::AccessExpr::Dot(SqlExpr::Value(
                            sq::Value::SingleQuotedString(f.value.clone()).into(),
                        ))
                    })
                    .collect();
                *e = SqlExpr::CompoundFieldAccess { root: Box::new(root), access_chain };
            }
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::DuckIntegerDivide,
                right,
            } => {
                *e = call("burrmill_intdiv", vec![*left.clone(), *right.clone()]);
            }
            // DataFusion's SQL planner type-checks NOT before any analyzer rule could cast text;
            // `x = false` is the same three-valued answer and reaches `DuckComparisons`.
            SqlExpr::UnaryOp {
                op: sq::UnaryOperator::Not,
                expr,
            } => {
                let x = *expr.clone();
                *e = binop(
                    x,
                    BinaryOperator::Eq,
                    SqlExpr::Value(sq::Value::Boolean(false).into()),
                );
            }
            SqlExpr::Cast { data_type, .. } => {
                let hugeint = matches!(data_type, SqlType::HugeInt);
                if let Some(why) = retype(data_type) {
                    self.refused = Some(why);
                    return ControlFlow::Break(());
                }
                // DuckDB rounds a float to HUGEINT half to even, and to DECIMAL(38,0) half away
                // from zero; both are DECIMAL(38,0) here, so `DuckSemantics` is told which it was.
                if hugeint {
                    *e = call("burrmill_hugeint", vec![e.clone()]);
                }
            }
            SqlExpr::BinaryOp { left, op, right } => {
                use BinaryOperator::*;
                if let (SqlExpr::Tuple(a), SqlExpr::Tuple(b)) = (left.as_ref(), right.as_ref())
                    && matches!(op, Gt | Lt | GtEq | LtEq)
                    && a.len() == b.len()
                    && !a.is_empty()
                {
                    *e = tuple_cmp(a, b, op);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// `burrmill_intdiv(a, b)`: DuckDB's `//`, exact and truncating, checked. Integers stay BIGINT;
/// anything decimal is `DECIMAL(38,0)`, as `HUGEINT` is.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct IntDiv {
    sig: Signature,
}

impl IntDiv {
    pub fn udf() -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
        }))
    }
}

impl ScalarUDFImpl for IntDiv {
    fn name(&self) -> &str {
        "burrmill_intdiv"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, args: &[DataType]) -> DFResult<Vec<DataType>> {
        let [a, b] = args else {
            return plan_err!("// takes two operands");
        };
        let exact = |t: &DataType| {
            t.is_integer() || matches!(t, DataType::Decimal128(_, 0) | DataType::Null)
        };
        if !exact(a) || !exact(b) {
            return plan_err!("// needs integer or scale-0 decimal operands, not {a} and {b}");
        }
        // As DuckDB types it: unsigned stays unsigned, unsigned beside signed is HUGEINT (a literal
        // having been fitted to its partner already), the rest BIGINT.
        let unsigned = |t: &DataType| t.is_unsigned_integer() || t.is_null();
        let wide = matches!(a, DataType::Decimal128(..))
            || matches!(b, DataType::Decimal128(..))
            || (a.is_unsigned_integer() && b.is_signed_integer())
            || (a.is_signed_integer() && b.is_unsigned_integer());
        let t = if wide {
            DataType::Decimal128(38, 0)
        } else if unsigned(a) && unsigned(b) && !(a.is_null() && b.is_null()) {
            DataType::UInt64
        } else {
            DataType::Int64
        };
        Ok(vec![t.clone(), t])
    }
    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        Ok(args[0].clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let (a, b) = (&arrays[0], &arrays[1]);
        let n = a.len();
        let out: ArrayRef = match a.data_type() {
            DataType::UInt64 => {
                let (a, b) = (a.as_primitive::<UInt64Type>(), b.as_primitive::<UInt64Type>());
                let mut o = arrow::array::UInt64Builder::with_capacity(n);
                for i in 0..n {
                    if a.is_null(i) || b.is_null(i) || b.value(i) == 0 {
                        o.append_null();
                    } else {
                        o.append_value(a.value(i) / b.value(i));
                    }
                }
                std::sync::Arc::new(o.finish())
            }
            DataType::Int64 => {
                let (a, b) = (a.as_primitive::<Int64Type>(), b.as_primitive::<Int64Type>());
                let mut o = Int64Builder::with_capacity(n);
                for i in 0..n {
                    if a.is_null(i) || b.is_null(i) || b.value(i) == 0 {
                        o.append_null();
                    } else {
                        match a.value(i).checked_div(b.value(i)) {
                            Some(v) => o.append_value(v),
                            None => {
                                return exec_err!(
                                    "Overflow in division of {} // {}",
                                    a.value(i),
                                    b.value(i)
                                );
                            }
                        }
                    }
                }
                std::sync::Arc::new(o.finish())
            }
            _ => {
                let (a, b) = (
                    a.as_primitive::<Decimal128Type>(),
                    b.as_primitive::<Decimal128Type>(),
                );
                let mut o = Decimal128Builder::with_capacity(n);
                for i in 0..n {
                    if a.is_null(i) || b.is_null(i) || b.value(i) == 0 {
                        o.append_null();
                    } else {
                        match a.value(i).checked_div(b.value(i)) {
                            Some(v) => o.append_value(v),
                            None => {
                                return exec_err!(
                                    "Overflow in division of {} // {}",
                                    a.value(i),
                                    b.value(i)
                                );
                            }
                        }
                    }
                }
                std::sync::Arc::new(o.finish().with_precision_and_scale(38, 0)?)
            }
        };
        if scalar {
            Ok(ColumnarValue::Scalar(
                datafusion_common::ScalarValue::try_from_array(&out, 0)?,
            ))
        } else {
            Ok(ColumnarValue::Array(out))
        }
    }
}

/// DuckDB semantics that change a result's type, applied once types are known. `/` between
/// numbers is DOUBLE in DuckDB, where DataFusion divides integers as integers.
#[derive(Debug)]
pub struct DuckSemantics {
    round: Arc<ScalarUDF>,
}

impl Default for DuckSemantics {
    fn default() -> Self {
        Self {
            round: RoundInt::udf(),
        }
    }
}

impl AnalyzerRule for DuckSemantics {
    fn name(&self) -> &str {
        "duck_semantics"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        plan.transform_up_with_subqueries(|p| {
            let mut schema = DFSchema::empty();
            for i in p.inputs() {
                schema.merge(i.schema());
            }
            let names_matter = matches!(
                p,
                LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_) | LogicalPlan::Window(_)
            );
            let t = p.map_expressions(|e| {
                let name = e.schema_name().to_string();
                let t = e.transform_up(|e| {
                    let t = divide_as_double(e, &schema)?;
                    let t = t.transform_data(|e| timestamp_as_duckdb(e, &schema))?;
                    let t = t.transform_data(|e| round_before_int_cast(e, &schema, &self.round))?;
                    t.transform_data(|e| hugeint_rounding(e, &schema, &self.round))
                })?;
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
        })
        .map(|t| t.data)
    }
}

/// `to_timestamp` is TIMESTAMPTZ in DuckDB: microseconds, UTC. DataFusion's is nanoseconds with
/// no zone, which nuthatch's JSON prints differently and `date_trunc` reads differently.
fn timestamp_as_duckdb(e: Expr, schema: &DFSchema) -> DFResult<Transformed<Expr>> {
    let Expr::ScalarFunction(f) = &e else {
        return Ok(Transformed::no(e));
    };
    if f.func.name() != "to_timestamp"
        || !matches!(
            e.get_type(schema)?,
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        )
    {
        return Ok(Transformed::no(e));
    }
    let utc = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    Ok(Transformed::yes(Expr::Cast(Cast::new(Box::new(e), utc))))
}

fn numeric(t: &DataType) -> bool {
    t.is_integer()
        || t.is_floating()
        || matches!(t, DataType::Decimal128(..) | DataType::Decimal256(..))
}

fn divide_as_double(e: Expr, schema: &DFSchema) -> DFResult<Transformed<Expr>> {
    let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Divide,
        right,
    }) = e
    else {
        return Ok(Transformed::no(e));
    };
    let (lt, rt) = (left.get_type(schema)?, right.get_type(schema)?);
    if !numeric(&lt) || !numeric(&rt) || (lt == DataType::Float64 && rt == DataType::Float64) {
        return Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Divide,
            right,
        })));
    }
    let f64 = |x: Box<Expr>, t: &DataType| {
        if *t == DataType::Float64 {
            *x
        } else {
            Expr::Cast(Cast::new(x, DataType::Float64))
        }
    };
    Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
        Box::new(f64(left, &lt)),
        Operator::Divide,
        Box::new(f64(right, &rt)),
    ))))
}

/// DuckDB's comparisons between text and other types, decided before DataFusion's coercion
/// inserts casts that would make a written `CAST` and an implicit one indistinguishable:
///
/// - ordering (`<`, `>`, `<=`, `>=`, `BETWEEN`) between text and a number is refused, as DuckDB
///   refuses it; equality casts, as DuckDB does, and DataFusion already agrees;
/// - text beside a boolean, or under `AND`/`OR`/`NOT`, is cast to BOOLEAN, as DuckDB casts it.
#[derive(Debug, Default)]
pub struct DuckComparisons;

impl AnalyzerRule for DuckComparisons {
    fn name(&self) -> &str {
        "duck_comparisons"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        let changed = plan.transform_up_with_subqueries(|p| {
            let mut schema = DFSchema::empty();
            for i in p.inputs() {
                schema.merge(i.schema());
            }
            let names_matter = matches!(
                p,
                LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_) | LogicalPlan::Window(_)
            );
            let t = p.map_expressions(|e| {
                let name = e.schema_name().to_string();
                let t = e.transform_up(|e| compare_as_duckdb(e, &schema))?;
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
        })?;
        if !changed.transformed {
            return Ok(changed.data);
        }
        // A union's schema was fixed when the SQL was planned, from the types before this rule, and
        // `recompute_schema` keeps it while the width is unchanged; everything above inherits it.
        // Re-derive it from the inputs and recompute upwards, so coercion sees the types as they are.
        changed
            .data
            .transform_up_with_subqueries(|p| match p {
                LogicalPlan::Union(u) => Ok(Transformed::yes(LogicalPlan::Union(
                    datafusion_expr::logical_plan::Union::try_new_with_loose_types(u.inputs)?,
                ))),
                p => Ok(Transformed::yes(p.recompute_schema()?)),
            })
            .map(|t| t.data)
    }
}

fn is_text(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8)
}

fn duck_name(t: &DataType, e: &Expr) -> String {
    match (t, e) {
        (t, Expr::Literal(..)) if t.is_integer() => "INTEGER_LITERAL".into(),
        (t, _) => super::errors::duck_type(&t.to_string()),
    }
}

fn as_bool(e: Expr) -> Expr {
    Expr::Cast(Cast::new(Box::new(e), DataType::Boolean))
}

/// DuckDB types an integer literal to fit the integer beside it (`UBIGINT - 1` stays UBIGINT);
/// DataFusion widens such a pair to DECIMAL(20,0), which nuthatch prints as a string.
fn fit_literal(e: Expr, to: &DataType) -> Expr {
    match &e {
        Expr::Literal(v, meta)
            if v.data_type().is_integer() && to.is_integer() && v.data_type() != *to =>
        {
            match v.cast_to(to) {
                Ok(c) if !c.is_null() => Expr::Literal(c, meta.clone()),
                _ => e,
            }
        }
        _ => e,
    }
}

/// The one integer type among `types` that is not a bare literal, if there is exactly one.
fn sole_integer(exprs: &[&Expr], schema: &DFSchema) -> DFResult<Option<DataType>> {
    let mut found: Option<DataType> = None;
    for e in exprs {
        if matches!(e, Expr::Literal(..)) {
            continue;
        }
        let t = e.get_type(schema)?;
        if !t.is_integer() {
            return Ok(None);
        }
        match &found {
            Some(f) if *f != t => return Ok(None),
            _ => found = Some(t),
        }
    }
    Ok(found)
}

fn compare_as_duckdb(e: Expr, schema: &DFSchema) -> DFResult<Transformed<Expr>> {
    let before = e.clone();
    let t = compare_inner(e, schema)?;
    if !t.transformed && t.data != before {
        return Ok(Transformed::yes(t.data));
    }
    Ok(t)
}

fn compare_inner(e: Expr, schema: &DFSchema) -> DFResult<Transformed<Expr>> {
    let e = match e {
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if matches!(
                op,
                Operator::Plus
                    | Operator::Minus
                    | Operator::Multiply
                    | Operator::Modulo
                    | Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::Gt
                    | Operator::LtEq
                    | Operator::GtEq
            ) =>
        {
            // Types only where a literal is fitted: asking re-types the whole subtree, and at every
            // node of a long chain that made planning quadratic.
            let (l, r) = match (left.as_ref(), right.as_ref()) {
                (Expr::Literal(..), r) if !matches!(r, Expr::Literal(..)) => {
                    let rt = right.get_type(schema)?;
                    (fit_literal(*left, &rt), *right)
                }
                (l, Expr::Literal(..)) if !matches!(l, Expr::Literal(..)) => {
                    let lt = left.get_type(schema)?;
                    (*left, fit_literal(*right, &lt))
                }
                _ => (*left, *right),
            };
            Expr::BinaryExpr(BinaryExpr::new(Box::new(l), op, Box::new(r)))
        }
        Expr::Case(mut c) => {
            let mut branches: Vec<&Expr> =
                c.when_then_expr.iter().map(|(_, t)| t.as_ref()).collect();
            if let Some(e) = &c.else_expr {
                branches.push(e);
            }
            if let Some(t) = sole_integer(&branches, schema)? {
                for (_, then) in c.when_then_expr.iter_mut() {
                    **then = fit_literal(then.as_ref().clone(), &t);
                }
                if let Some(e) = c.else_expr.as_mut() {
                    **e = fit_literal(e.as_ref().clone(), &t);
                }
            }
            Expr::Case(c)
        }
        Expr::ScalarFunction(mut f)
            if matches!(f.func.name(), "coalesce" | "greatest" | "least" | "burrmill_intdiv") =>
        {
            let args: Vec<&Expr> = f.args.iter().collect();
            if let Some(t) = sole_integer(&args, schema)? {
                f.args = f.args.into_iter().map(|a| fit_literal(a, &t)).collect();
            }
            Expr::ScalarFunction(f)
        }
        e => e,
    };
    match e {
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if matches!(
                op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::Gt
                    | Operator::LtEq
                    | Operator::GtEq
                    | Operator::And
                    | Operator::Or
            ) =>
        {
            let (lt, rt) = (left.get_type(schema)?, right.get_type(schema)?);
            let ordering = matches!(
                op,
                Operator::Lt | Operator::Gt | Operator::LtEq | Operator::GtEq
            );
            if ordering && (is_text(&lt) && numeric(&rt) || numeric(&lt) && is_text(&rt)) {
                return plan_err!(
                    "Binder Error: Cannot compare values of type {} and type {} - an explicit cast \
                     is required",
                    duck_name(&lt, &left),
                    duck_name(&rt, &right)
                );
            }
            let boolean = matches!(op, Operator::And | Operator::Or)
                || matches!(op, Operator::Eq | Operator::NotEq)
                    && (lt == DataType::Boolean || rt == DataType::Boolean);
            if boolean && (is_text(&lt) || is_text(&rt)) {
                let l = if is_text(&lt) { as_bool(*left) } else { *left };
                let r = if is_text(&rt) {
                    as_bool(*right)
                } else {
                    *right
                };
                return Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(l),
                    op,
                    Box::new(r),
                ))));
            }
            Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr {
                left,
                op,
                right,
            })))
        }
        Expr::Between(b) => {
            let (t, lo, hi) = (
                b.expr.get_type(schema)?,
                b.low.get_type(schema)?,
                b.high.get_type(schema)?,
            );
            if is_text(&t) && (numeric(&lo) || numeric(&hi))
                || numeric(&t) && (is_text(&lo) || is_text(&hi))
            {
                let other = if numeric(&lo) || is_text(&lo) && numeric(&t) {
                    (&lo, &b.low)
                } else {
                    (&hi, &b.high)
                };
                let (a, bn) = if is_text(&t) {
                    ("VARCHAR".to_string(), duck_name(other.0, other.1))
                } else {
                    (duck_name(&t, &b.expr), "VARCHAR".to_string())
                };
                return plan_err!(
                    "Binder Error: Cannot mix values of type {a} and {bn} in BETWEEN clause - an \
                     explicit cast is required"
                );
            }
            Ok(Transformed::no(Expr::Between(b)))
        }
        Expr::InList(mut l) => {
            let t = l.expr.get_type(schema)?;
            let types = l.list.iter().map(|x| x.get_type(schema)).collect::<DFResult<Vec<_>>>()?;
            if t == DataType::Boolean {
                if let Some(n) = types.iter().find(|x| numeric(x)) {
                    l.expr = Box::new(Expr::Cast(Cast::new(l.expr, n.clone())));
                    return Ok(Transformed::yes(Expr::InList(l)));
                }
            } else if numeric(&t) && types.contains(&DataType::Boolean) {
                l.list = l
                    .list
                    .into_iter()
                    .zip(types)
                    .map(|(x, xt)| {
                        if xt == DataType::Boolean { Expr::Cast(Cast::new(Box::new(x), t.clone())) } else { x }
                    })
                    .collect();
                return Ok(Transformed::yes(Expr::InList(l)));
            }
            Ok(Transformed::no(Expr::InList(l)))
        }
        Expr::Not(x) if is_text(&x.get_type(schema)?) => {
            Ok(Transformed::yes(Expr::Not(Box::new(as_bool(*x)))))
        }
        e => Ok(Transformed::no(e)),
    }
}

/// A boolean compared with a number is read as the number, `true` as 1, so `2 = true` is false
/// and `2 > false` true. Done while the SQL is planned, because DataFusion types a `SELECT` list
/// there and refuses `Int64 = Boolean` before any analyzer rule could cast it.
#[derive(Debug)]
pub struct DuckPlanner;

impl datafusion_expr::planner::ExprPlanner for DuckPlanner {
    fn plan_binary_op(
        &self,
        mut e: datafusion_expr::planner::RawBinaryExpr,
        schema: &DFSchema,
    ) -> DFResult<datafusion_expr::planner::PlannerResult<datafusion_expr::planner::RawBinaryExpr>> {
        use datafusion_expr::planner::PlannerResult;
        use sqlparser::ast::BinaryOperator as B;
        if !matches!(e.op, B::Eq | B::NotEq | B::Lt | B::Gt | B::LtEq | B::GtEq) {
            return Ok(PlannerResult::Original(e));
        }
        let (Ok(lt), Ok(rt)) = (e.left.get_type(schema), e.right.get_type(schema)) else {
            return Ok(PlannerResult::Original(e));
        };
        // Through BIGINT: arrow casts a boolean to integers and floats but not to DECIMAL.
        let as_number = |b: Expr, t: DataType| {
            let i = Expr::Cast(Cast::new(Box::new(b), DataType::Int64));
            if t == DataType::Int64 { i } else { Expr::Cast(Cast::new(Box::new(i), t)) }
        };
        if lt == DataType::Boolean && numeric(&rt) {
            e.left = as_number(e.left, rt);
        } else if rt == DataType::Boolean && numeric(&lt) {
            e.right = as_number(e.right, lt);
        }
        Ok(PlannerResult::Original(e))
    }
}

/// `burrmill_round_int(x)`: `x` rounded to a whole number as DuckDB rounds it when casting to an
/// integer: half to even for floats, half away from zero for decimals. Arrow's cast truncates.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RoundInt {
    sig: Signature,
}

impl RoundInt {
    pub fn udf() -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self {
            sig: Signature::user_defined(Volatility::Immutable),
        }))
    }
}

impl ScalarUDFImpl for RoundInt {
    fn name(&self) -> &str {
        "burrmill_round_int"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, args: &[DataType]) -> DFResult<Vec<DataType>> {
        match args {
            [DataType::Float32] | [DataType::Float16] => Ok(vec![DataType::Float64]),
            [t @ (DataType::Float64 | DataType::Decimal128(..))] => Ok(vec![t.clone()]),
            _ => plan_err!("burrmill_round_int takes a float or a decimal"),
        }
    }
    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        Ok(args[0].clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use arrow::array::{Decimal128Array, Float64Array};
        use arrow::datatypes::Float64Type;
        let scalar = args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let out: ArrayRef = match a.data_type() {
            DataType::Float64 => Arc::new(
                a.as_primitive::<Float64Type>()
                    .iter()
                    .map(|v| v.map(f64::round_ties_even))
                    .collect::<Float64Array>(),
            ),
            DataType::Decimal128(p, s) => {
                let unit = 10i128.pow(*s as u32);
                let mut out = Vec::with_capacity(a.len());
                for v in a.as_primitive::<Decimal128Type>().iter() {
                    out.push(match v {
                        None => None,
                        Some(v) => {
                            let (q, r) = (v / unit, v % unit);
                            let q = if r.abs() * 2 >= unit {
                                q + r.signum()
                            } else {
                                q
                            };
                            match q.checked_mul(unit) {
                                Some(x) => Some(x),
                                None => {
                                    return exec_err!("Overflow rounding {v} to a whole number");
                                }
                            }
                        }
                    });
                }
                Arc::new(Decimal128Array::from(out).with_precision_and_scale(*p, *s)?)
            }
            t => return exec_err!("burrmill_round_int: unsupported {t}"),
        };
        if scalar {
            Ok(ColumnarValue::Scalar(
                datafusion_common::ScalarValue::try_from_array(&out, 0)?,
            ))
        } else {
            Ok(ColumnarValue::Array(out))
        }
    }
}

/// A cast from a float or a scaled decimal to an integer rounds first, as DuckDB's does.
fn round_before_int_cast(
    e: Expr,
    schema: &DFSchema,
    round: &Arc<ScalarUDF>,
) -> DFResult<Transformed<Expr>> {
    let (inner, to, try_) = match &e {
        Expr::Cast(Cast { expr, field }) => (expr, field.data_type().clone(), false),
        Expr::TryCast(TryCast { expr, field }) => (expr, field.data_type().clone(), true),
        _ => return Ok(Transformed::no(e)),
    };
    if !to.is_integer() {
        return Ok(Transformed::no(e));
    }
    let from = inner.get_type(schema)?;
    let needs = from.is_floating() || matches!(from, DataType::Decimal128(_, s) if s > 0);
    if !needs
        || matches!(inner.as_ref(), Expr::ScalarFunction(f) if f.func.name() == "burrmill_round_int")
    {
        return Ok(Transformed::no(e));
    }
    let rounded = Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
        Arc::clone(round),
        vec![inner.as_ref().clone()],
    ));
    Ok(Transformed::yes(if try_ {
        Expr::TryCast(TryCast::new(Box::new(rounded), to))
    } else {
        Expr::Cast(Cast::new(Box::new(rounded), to))
    }))
}

/// `burrmill_hugeint(CAST(x AS DECIMAL(38,0)))`, the marker the parse leaves on a HUGEINT cast:
/// from a float the value is rounded half to even first, as DuckDB's `nearbyint` does, and either
/// way the marker goes.
fn hugeint_rounding(e: Expr, schema: &DFSchema, round: &Arc<ScalarUDF>) -> DFResult<Transformed<Expr>> {
    let Expr::ScalarFunction(f) = &e else {
        return Ok(Transformed::no(e));
    };
    if f.func.name() != "burrmill_hugeint" {
        return Ok(Transformed::no(e));
    }
    let [arg] = f.args.as_slice() else {
        return plan_err!("burrmill_hugeint takes one argument");
    };
    let rounded = |inner: &Expr| {
        Box::new(Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
            Arc::clone(round),
            vec![inner.clone()],
        )))
    };
    Ok(Transformed::yes(match arg {
        Expr::Cast(Cast { expr, field }) if expr.get_type(schema)?.is_floating() => {
            Expr::Cast(Cast::new(rounded(expr), field.data_type().clone()))
        }
        Expr::TryCast(TryCast { expr, field }) if expr.get_type(schema)?.is_floating() => {
            Expr::TryCast(TryCast::new(rounded(expr), field.data_type().clone()))
        }
        other => other.clone(),
    }))
}

/// Marks a cast written as HUGEINT until [`hugeint_rounding`] takes it off; returns its argument.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct HugeintMark {
    sig: Signature,
}

impl HugeintMark {
    pub fn udf() -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self { sig: Signature::user_defined(Volatility::Immutable) }))
    }
}

impl ScalarUDFImpl for HugeintMark {
    fn name(&self) -> &str {
        "burrmill_hugeint"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, args: &[DataType]) -> DFResult<Vec<DataType>> {
        Ok(args.to_vec())
    }
    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        Ok(args[0].clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(args.args[0].clone())
    }
}

/// DuckDB's `from_hex(text)`: the bytes a hex string spells, an odd length padded with a leading
/// zero (`abc` is `0a bc`), a bad digit refused, as measured against DuckDB.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct FromHex {
    sig: Signature,
}

impl FromHex {
    pub fn udf() -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self { sig: Signature::user_defined(Volatility::Immutable) }))
    }
}

impl ScalarUDFImpl for FromHex {
    fn name(&self) -> &str {
        "from_hex"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, args: &[DataType]) -> DFResult<Vec<DataType>> {
        match args {
            [DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 | DataType::Null] => {
                Ok(vec![DataType::Utf8])
            }
            _ => plan_err!("from_hex takes one text argument"),
        }
    }
    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Binary)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        use arrow::array::BinaryBuilder;
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let a = a.as_string::<i32>();
        let mut out = BinaryBuilder::with_capacity(a.len(), 0);
        let digit = |c: u8| -> DFResult<u8> {
            (c as char).to_digit(16).map(|d| d as u8).ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(format!(
                    "Invalid Input Error: Invalid input for hex digit: {}",
                    c as char
                ))
            })
        };
        let mut buf = Vec::new();
        for i in 0..a.len() {
            if a.is_null(i) {
                out.append_null();
                continue;
            }
            let s = a.value(i).as_bytes();
            buf.clear();
            let rest = if s.len() % 2 == 1 {
                buf.push(digit(s[0])?);
                &s[1..]
            } else {
                s
            };
            for pair in rest.chunks(2) {
                buf.push((digit(pair[0])? << 4) | digit(pair[1])?);
            }
            out.append_value(&buf);
        }
        let out: ArrayRef = Arc::new(out.finish());
        Ok(if scalar {
            ColumnarValue::Scalar(datafusion_common::ScalarValue::try_from_array(&out, 0)?)
        } else {
            ColumnarValue::Array(out)
        })
    }
}

/// `decode`: DuckDB's one-argument form, bytes to text, refused unless valid UTF-8. The
/// two-argument form stays DataFusion's own `decode(x, encoding)`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Decode {
    sig: Signature,
    inner: Arc<ScalarUDF>,
}

impl Decode {
    pub fn udf(inner: Arc<ScalarUDF>) -> Arc<ScalarUDF> {
        Arc::new(ScalarUDF::from(Self { sig: Signature::user_defined(Volatility::Immutable), inner }))
    }
}

impl ScalarUDFImpl for Decode {
    fn name(&self) -> &str {
        "decode"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn coerce_types(&self, args: &[DataType]) -> DFResult<Vec<DataType>> {
        match args {
            [DataType::Binary | DataType::BinaryView | DataType::LargeBinary | DataType::Null] => {
                Ok(vec![DataType::Binary])
            }
            [_] => plan_err!("decode takes a BLOB"),
            _ => self.inner.coerce_types(args),
        }
    }
    fn return_type(&self, args: &[DataType]) -> DFResult<DataType> {
        match args {
            [_] => Ok(DataType::Utf8),
            _ => self.inner.return_type(args),
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 1 {
            return self.inner.invoke_with_args(args);
        }
        use arrow::array::StringBuilder;
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let a = a.as_binary::<i32>();
        let mut out = StringBuilder::new();
        for i in 0..a.len() {
            if a.is_null(i) {
                out.append_null();
                continue;
            }
            match std::str::from_utf8(a.value(i)) {
                Ok(s) => out.append_value(s),
                Err(_) => {
                    return exec_err!(
                        "Conversion Error: Failure in decode: could not convert blob to UTF8 string, the blob contained invalid UTF8 characters."
                    );
                }
            }
        }
        let out: ArrayRef = Arc::new(out.finish());
        Ok(if scalar {
            ColumnarValue::Scalar(datafusion_common::ScalarValue::try_from_array(&out, 0)?)
        } else {
            ColumnarValue::Array(out)
        })
    }
}
