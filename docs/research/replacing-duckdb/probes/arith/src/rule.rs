//! An AnalyzerRule that swaps every built-in sum/avg and every integer or
//! decimal `+ - *` and unary `-` for the checked versions, and refuses the plan
//! when it meets one it cannot swap.
//!
//! Runs after DataFusion's own TypeCoercion, so operand types are final. The
//! replacement is aliased to the original expression's schema name so parents
//! that reference the column by name keep resolving.

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{plan_err, DFSchema, Result};
use datafusion::logical_expr::expr::{AggregateFunction, ScalarFunction, WindowFunctionDefinition};
use datafusion::logical_expr::{AggregateUDF, BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator, ScalarUDF};
use datafusion::optimizer::AnalyzerRule;

use crate::checked::{supported_input, CheckedAgg, CheckedBinary, Mode};

#[derive(Debug)]
pub struct CheckedArithmetic {
    sum: Arc<AggregateUDF>,
    avg: Arc<AggregateUDF>,
    add: Arc<ScalarUDF>,
    sub: Arc<ScalarUDF>,
    mul: Arc<ScalarUDF>,
    neg: Arc<ScalarUDF>,
}

impl Default for CheckedArithmetic {
    fn default() -> Self {
        Self {
            sum: CheckedAgg::udaf(Mode::Sum),
            avg: CheckedAgg::udaf(Mode::Avg),
            add: CheckedBinary::udf(Some(Operator::Plus)),
            sub: CheckedBinary::udf(Some(Operator::Minus)),
            mul: CheckedBinary::udf(Some(Operator::Multiply)),
            neg: CheckedBinary::udf(None),
        }
    }
}

fn is_int_or_dec(t: &datafusion::arrow::datatypes::DataType) -> bool {
    t.is_integer() || matches!(t, datafusion::arrow::datatypes::DataType::Decimal128(_, _) | datafusion::arrow::datatypes::DataType::Decimal256(_, _))
}

impl CheckedArithmetic {
    fn agg_replacement(&self, name: &str) -> Option<&Arc<AggregateUDF>> {
        match name {
            "sum" => Some(&self.sum),
            "avg" => Some(&self.avg),
            _ => None,
        }
    }

    fn rewrite(&self, e: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
        match e {
            Expr::AggregateFunction(af) => {
                let Some(f) = self.agg_replacement(af.func.name()) else {
                    return Ok(Transformed::no(Expr::AggregateFunction(af)));
                };
                if af.params.distinct {
                    return plan_err!("refusing plan: {}(DISTINCT ...) has no checked form", af.func.name());
                }
                let ty = af.params.args[0].get_type(schema)?;
                if !supported_input(&ty) {
                    return plan_err!("refusing plan: {}({ty}) has no checked form", af.func.name());
                }
                Ok(Transformed::yes(Expr::AggregateFunction(AggregateFunction { func: f.clone(), params: af.params })))
            }
            Expr::WindowFunction(mut wf) => {
                if let WindowFunctionDefinition::AggregateUDF(f) = &wf.fun {
                    if let Some(r) = self.agg_replacement(f.name()) {
                        let ty = wf.params.args[0].get_type(schema)?;
                        if !supported_input(&ty) {
                            return plan_err!("refusing plan: {}({ty}) OVER (...) has no checked form", f.name());
                        }
                        wf.fun = WindowFunctionDefinition::AggregateUDF(r.clone());
                        return Ok(Transformed::yes(Expr::WindowFunction(wf)));
                    }
                }
                Ok(Transformed::no(Expr::WindowFunction(wf)))
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if matches!(op, Operator::Plus | Operator::Minus | Operator::Multiply) =>
            {
                let lt = left.get_type(schema)?;
                let rt = right.get_type(schema)?;
                if is_int_or_dec(&lt) && is_int_or_dec(&rt) {
                    let f = match op {
                        Operator::Plus => &self.add,
                        Operator::Minus => &self.sub,
                        _ => &self.mul,
                    };
                    Ok(Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(f.clone(), vec![*left, *right]))))
                } else if is_int_or_dec(&lt) || is_int_or_dec(&rt) {
                    plan_err!("refusing plan: {lt} {op} {rt} has no checked form")
                } else {
                    Ok(Transformed::no(Expr::BinaryExpr(BinaryExpr { left, op, right })))
                }
            }
            Expr::Negative(inner) => {
                if is_int_or_dec(&inner.get_type(schema)?) {
                    Ok(Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(self.neg.clone(), vec![*inner]))))
                } else {
                    Ok(Transformed::no(Expr::Negative(inner)))
                }
            }
            e => Ok(Transformed::no(e)),
        }
    }
}

impl AnalyzerRule for CheckedArithmetic {
    fn name(&self) -> &str {
        "checked_arithmetic"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|p| {
            let mut schema = DFSchema::empty();
            for i in p.inputs() {
                schema.merge(i.schema());
            }
            let names_matter = matches!(p, LogicalPlan::Projection(_) | LogicalPlan::Aggregate(_) | LogicalPlan::Window(_));
            let t = p.map_expressions(|e| {
                let name = e.schema_name().to_string();
                let t = e.transform_up(|e| self.rewrite(e, &schema))?;
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
