//! `OutermostOrder`: a view's or derived table's `ORDER BY` orders the answer when nothing above it
//! reorders, as DuckDB's does in practice (nuthatch#1984).
//!
//! DataFusion's SQL planner drops every sort without a fetch from a relation in `FROM`, and it
//! inlines a view there, so `SELECT * FROM v` came back unordered whatever `v` said. That pass is
//! switched off in the session, and this rule drops only the sorts whose order cannot reach the
//! answer or decide which rows a limit keeps: those under an aggregate, join, union, window,
//! distinct or another sort. Each subquery expression is its own answer.

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::LogicalPlan;
use datafusion_optimizer::analyzer::AnalyzerRule;

#[derive(Debug, Default)]
pub struct OutermostOrder;

impl AnalyzerRule for OutermostOrder {
    fn name(&self) -> &str {
        "outermost_order"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        keep(plan, true)
    }
}

/// `decides`: a sort here would order the answer, or choose the rows of a limit above.
#[recursive::recursive]
fn keep(plan: LogicalPlan, decides: bool) -> Result<LogicalPlan> {
    let plan = plan
        .map_subqueries(|q| keep(q, true).map(Transformed::yes))?
        .data;
    let plan = match plan {
        LogicalPlan::Sort(s) if s.fetch.is_none() && !decides => {
            return keep(Arc::unwrap_or_clone(s.input), false);
        }
        p => p,
    };
    let below = match &plan {
        LogicalPlan::Limit(_) => true,
        LogicalPlan::Projection(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::Explain(_)
        | LogicalPlan::Analyze(_) => decides,
        _ => false,
    };
    plan.map_children(|c| keep(c, below).map(Transformed::yes))
        .map(|t| t.data)
}
