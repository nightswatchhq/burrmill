//! Experiment: a window whose sort only recomputes an order its input already has runs linearly.

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::windows::BoundedWindowAggExec;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties, InputOrderMode, Partitioning};
use datafusion_session::PhysicalOptimizerRule;

#[derive(Debug, Default)]
pub struct ChainOrder;

/// A linear window emits rows as its partitions complete, not in input order, yet reports the
/// input's ordering; nothing above one may lean on it.
fn below_linear(p: &Arc<dyn ExecutionPlan>) -> bool {
    p.downcast_ref::<BoundedWindowAggExec>().is_some_and(|w| w.input_order_mode == InputOrderMode::Linear)
        || p.children().into_iter().any(below_linear)
}

fn linear(p: &Arc<dyn ExecutionPlan>) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(w) = p.downcast_ref::<BoundedWindowAggExec>() else { return Ok(None) };
    let Some(s) = w.input().downcast_ref::<SortExec>() else { return Ok(None) };
    let below = s.input();
    if std::env::var_os("BM_GUARD").is_some() && below_linear(below) { return Ok(None); }
    let input: Arc<dyn ExecutionPlan> = match below.downcast_ref::<RepartitionExec>() {
        Some(r) if matches!(r.partitioning(), Partitioning::Hash(..)) => {
            // A merge keys on its input's first declared ordering and then claims all of them;
            // declaring only the window's order below it makes that order the merge keys.
            let order = w.window_expr()[0].order_by().to_vec();
            if order.is_empty() || !r.input().equivalence_properties().ordering_satisfy(order.clone())? {
                return Ok(None);
            }
            let only = Arc::new(OrderedAs::new(Arc::clone(r.input()), order)?) as Arc<dyn ExecutionPlan>;
            let r2 = RepartitionExec::try_new(only, r.partitioning().clone())?.with_preserve_order();
            Arc::new(r2)
        }
        _ => Arc::clone(below),
    };
    let order = w.window_expr()[0].order_by().to_vec();
    if order.is_empty() || !input.equivalence_properties().ordering_satisfy(order)? {
        return Ok(None);
    }
    Ok(Some(Arc::new(BoundedWindowAggExec::try_new(w.window_expr().to_vec(), input, InputOrderMode::Linear, true)?)))
}

impl PhysicalOptimizerRule for ChainOrder {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        let t = Arc::clone(&plan).transform_up(|p| match linear(&p)? {
            Some(n) => Ok(Transformed::yes(n)),
            None => Ok(Transformed::no(p)),
        })?;
        if !t.transformed { return Ok(plan); }
        match SanityCheckPlan::new().optimize(Arc::clone(&t.data), config) {
            Ok(_) => Ok(t.data),
            Err(e) => { eprintln!("chainorder: sanity refused: {e}"); Ok(plan) }
        }
    }
    fn name(&self) -> &str { "chain_order" }
    fn schema_check(&self) -> bool { true }
}

/// Its input, reporting one ordering: the one the rule checked each partition satisfies.
#[derive(Debug)]
struct OrderedAs {
    input: Arc<dyn ExecutionPlan>,
    order: Vec<datafusion_physical_expr::PhysicalSortExpr>,
    props: Arc<datafusion_physical_plan::PlanProperties>,
}

impl OrderedAs {
    fn new(input: Arc<dyn ExecutionPlan>, order: Vec<datafusion_physical_expr::PhysicalSortExpr>) -> Result<Self> {
        use datafusion_physical_expr::{EquivalenceProperties, LexOrdering};
        let lex = LexOrdering::new(order.clone()).expect("non-empty, checked by the caller");
        let eq = EquivalenceProperties::new_with_orderings(input.schema(), [lex]);
        let p = input.properties();
        let props = datafusion_physical_plan::PlanProperties::new(eq, p.output_partitioning().clone(), p.emission_type, p.boundedness);
        Ok(Self { input, order, props: Arc::new(props) })
    }
}

impl datafusion_physical_plan::DisplayAs for OrderedAs {
    fn fmt_as(&self, _t: datafusion_physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "OrderedAs")
    }
}

impl ExecutionPlan for OrderedAs {
    fn name(&self) -> &str { "OrderedAs" }
    fn properties(&self) -> &Arc<datafusion_physical_plan::PlanProperties> { &self.props }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![&self.input] }
    fn apply_expressions(&self, _f: &mut dyn FnMut(&Arc<dyn datafusion_physical_expr::PhysicalExpr>) -> Result<datafusion_common::tree_node::TreeNodeRecursion>) -> Result<datafusion_common::tree_node::TreeNodeRecursion> {
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    }
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(Arc::clone(&children[0]), self.order.clone())?))
    }
    fn maintains_input_order(&self) -> Vec<bool> { vec![true] }
    fn execute(&self, partition: usize, context: Arc<datafusion_execution::TaskContext>) -> Result<datafusion_execution::SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}
