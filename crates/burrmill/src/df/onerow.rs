//! `SingleRowSubqueries`: a correlated scalar subquery that is not aggregated.
//!
//! `(SELECT p.name FROM pool p WHERE p.id = c.pool)`, a to-one lookup: DuckDB runs it and fails the
//! statement if an outer row finds two rows; DataFusion 55 refuses to plan it. It becomes the
//! aggregate DataFusion decorrelates, `{n: count(*), v: first_value(v)}`, and `burrmill_single`
//! over it in the outer row fails on `n > 1`. The check stays outside: decorrelated, the aggregate
//! runs for every key of the inner relation, including keys no outer row asks for.

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::logical_plan::Subquery;
use datafusion_expr::{
    ColumnarValue, Expr, LogicalPlan, LogicalPlanBuilder, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility, lit,
};
use datafusion_optimizer::analyzer::AnalyzerRule;

#[derive(Debug)]
pub struct SingleRowSubqueries(Arc<ScalarUDF>);

impl Default for SingleRowSubqueries {
    fn default() -> Self {
        Self(Arc::new(ScalarUDF::from(Single(Signature::any(1, Volatility::Immutable)))))
    }
}

impl AnalyzerRule for SingleRowSubqueries {
    fn name(&self) -> &str {
        "single_row_subqueries"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|p| {
            let before = Arc::clone(p.schema());
            let projection = matches!(p, LogicalPlan::Projection(_));
            let t = p.map_expressions(|e| {
                if !e.exists(|x| Ok(matches!(x, Expr::ScalarSubquery(_))))? {
                    return Ok(Transformed::no(e));
                }
                let name = e.schema_name().to_string();
                let t = e.transform_up(|x| match x {
                    Expr::ScalarSubquery(q)
                        if !q.outer_ref_columns.is_empty()
                            && q.subquery.max_rows().is_none_or(|n| n > 1)
                            && q.subquery.schema().fields().len() == 1 =>
                    {
                        let counted = Expr::ScalarSubquery(counted(&q)?);
                        Ok(Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
                            Arc::clone(&self.0),
                            vec![counted],
                        ))))
                    }
                    x => Ok(Transformed::no(x)),
                })?;
                Ok(if projection && t.transformed { t.map_data(|e| e.alias_if_changed(name))? } else { t })
            })?;
            if !t.transformed {
                return Ok(t);
            }
            t.map_data(|p| super::nullsub::renamed(p.recompute_schema()?, &before))
        })
        .map(|t| t.data)
    }
}

/// The subquery as one row per correlation: `{n, v}`, its row count and a value of its column.
fn counted(q: &Subquery) -> Result<Subquery> {
    let schema = q.subquery.schema();
    let value = Expr::Column(Column::from(schema.qualified_field(0)));
    let count = datafusion_functions_aggregate::count::count_udaf().call(vec![lit(1)]).alias("__burrmill_one_n");
    let first = datafusion_functions_aggregate::first_last::first_value_udaf().call(vec![value]).alias("__burrmill_one_v");
    let pair = datafusion_functions::core::named_struct().call(vec![
        lit("n"),
        Expr::Column(Column::new_unqualified("__burrmill_one_n")),
        lit("v"),
        Expr::Column(Column::new_unqualified("__burrmill_one_v")),
    ]);
    let plan = LogicalPlanBuilder::from(q.subquery.as_ref().clone())
        .aggregate(Vec::<Expr>::new(), vec![count, first])?
        .project(vec![pair.alias("__burrmill_one")])?
        .build()?;
    Ok(q.with_plan(Arc::new(plan)))
}

/// `burrmill_single({n, v})`: `v`, or DuckDB's error where `n > 1`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Single(Signature);

impl ScalarUDFImpl for Single {
    fn name(&self) -> &str {
        "burrmill_single"
    }
    fn signature(&self) -> &Signature {
        &self.0
    }
    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        match args {
            [DataType::Struct(f)] if f.len() == 2 => Ok(f[1].data_type().clone()),
            _ => plan_err!("burrmill_single takes a {{n, v}} struct"),
        }
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), self.return_type(&[args.arg_fields[0].data_type().clone()])?, true)))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let a = ColumnarValue::values_to_arrays(&args.args)?.remove(0);
        let pair = a.as_struct();
        let n = pair.column(0).as_primitive::<Int64Type>();
        if (0..pair.len()).any(|i| pair.is_valid(i) && n.is_valid(i) && n.value(i) > 1) {
            return exec_err!(
                "More than one row returned by a subquery used as an expression - scalar subqueries can only \
                 return a single row."
            );
        }
        let v = match pair.nulls() {
            Some(nulls) if nulls.null_count() > 0 => {
                arrow::compute::nullif(pair.column(1), &arrow::array::BooleanArray::new(!nulls.inner(), None))?
            }
            _ => Arc::clone(pair.column(1)),
        };
        Ok(if scalar { ColumnarValue::Scalar(ScalarValue::try_from_array(&v, 0)?) } else { ColumnarValue::Array(v) })
    }
}
