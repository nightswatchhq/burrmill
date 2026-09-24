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
use arrow::datatypes::{DataType, Decimal128Type, Int64Type, TimeUnit};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, Result as DFResult, exec_err, plan_err};
use datafusion_expr::{
    BinaryExpr, Cast, ColumnarValue, Expr, ExprSchemable, LogicalPlan, Operator,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
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
pub fn parse(sql: &str) -> Result<(DfStatement, Vec<Option<String>>)> {
    let stmts = DFParser::parse_sql_with_dialect(sql, &DuckDbDialect {})
        .map_err(|e| BurrmillError::Parse(super::errors::restate(format!("SQL error: {e:?}"))))?;
    let Some(mut stmt) = stmts.into_iter().next() else {
        return Err(BurrmillError::Parse("empty statement".into()));
    };
    let names = match &stmt {
        DfStatement::Statement(s) => match s.as_ref() {
            sq::Statement::Query(q) => super::names::default_names(q),
            _ => vec![],
        },
        _ => vec![],
    };
    if let DfStatement::Statement(s) = &mut stmt {
        let mut rw = Rewriter { refused: None };
        let _ = sq::VisitMut::visit(s.as_mut(), &mut rw);
        if let Some(why) = rw.refused {
            return Err(BurrmillError::NotAllowed(why));
        }
    }
    Ok((stmt, names))
}

struct Rewriter {
    refused: Option<String>,
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

    fn pre_visit_expr(&mut self, e: &mut SqlExpr) -> ControlFlow<()> {
        match e {
            SqlExpr::BinaryOp {
                left,
                op: BinaryOperator::DuckIntegerDivide,
                right,
            } => {
                let args = [*left.clone(), *right.clone()]
                    .into_iter()
                    .map(|a| sq::FunctionArg::Unnamed(sq::FunctionArgExpr::Expr(a)))
                    .collect();
                *e = SqlExpr::Function(sq::Function {
                    name: sq::ObjectName::from(vec![sq::Ident::new("burrmill_intdiv")]),
                    uses_odbc_syntax: false,
                    parameters: sq::FunctionArguments::None,
                    args: sq::FunctionArguments::List(sq::FunctionArgumentList {
                        duplicate_treatment: None,
                        args,
                        clauses: vec![],
                    }),
                    filter: None,
                    null_treatment: None,
                    over: None,
                    within_group: vec![],
                });
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
                if let Some(why) = retype(data_type) {
                    self.refused = Some(why);
                    return ControlFlow::Break(());
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
        let wide = matches!(a, DataType::Decimal128(..)) || matches!(b, DataType::Decimal128(..));
        let t = if wide {
            DataType::Decimal128(38, 0)
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
#[derive(Debug, Default)]
pub struct DuckSemantics;

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
                    t.transform_data(|e| timestamp_as_duckdb(e, &schema))
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

fn compare_as_duckdb(e: Expr, schema: &DFSchema) -> DFResult<Transformed<Expr>> {
    match e {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
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
        Expr::Not(x) if is_text(&x.get_type(schema)?) => {
            Ok(Transformed::yes(Expr::Not(Box::new(as_bool(*x)))))
        }
        e => Ok(Transformed::no(e)),
    }
}
