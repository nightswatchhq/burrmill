//! `ShareRepeats`: a subquery that appears more than once is computed once.
//!
//! DataFusion inlines CTEs and views, so `per_epoch` used twice in `epoch_boundaries` aggregated
//! its half-million rows twice, and a view joined twice is planned twice. Identical aliased
//! subtrees that occur more than once, and contain an aggregate (so what is kept is bounded), are
//! wrapped in a node sharing one cache: the first consumer computes the batches and the others
//! read them. The node lets nothing through it from above, so every copy is optimised alike.

use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion_catalog::Session;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchemaRef, DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_expr::logical_plan::{Extension, SubqueryAlias};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore};
use datafusion_optimizer::analyzer::AnalyzerRule;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, collect,
};
use datafusion_session::{ExtensionPlanner, PhysicalPlanner};
use futures::StreamExt;

type Cache = Arc<tokio::sync::OnceCell<Arc<Vec<RecordBatch>>>>;

#[derive(Debug, Default)]
pub struct ShareRepeats;

fn worth_sharing(p: &LogicalPlan) -> bool {
    p.exists(|n| Ok(matches!(n, LogicalPlan::Aggregate(_))))
        .unwrap_or(false)
}

/// An alias over a subquery worth sharing that is not shared yet, through any aliases of aliases.
fn candidate(n: &LogicalPlan) -> Option<&LogicalPlan> {
    let LogicalPlan::SubqueryAlias(s) = n else {
        return None;
    };
    let mut under = s.input.as_ref();
    while let LogicalPlan::SubqueryAlias(inner) = under {
        under = inner.input.as_ref();
    }
    let shared = matches!(under, LogicalPlan::Extension(e) if e.node.as_any().is::<SharedNode>());
    (!shared && worth_sharing(&s.input)).then_some(s.input.as_ref())
}

/// Whether a repeated candidate sits anywhere beneath `p`, subqueries included.
fn repeats_below(p: &LogicalPlan, counts: &HashMap<u64, usize>) -> Result<bool> {
    let mut found = false;
    p.apply_with_subqueries(|n| {
        if let Some(input) = candidate(n)
            && counts.contains_key(&key(input))
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(found)
}

fn key(p: &LogicalPlan) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    p.hash(&mut h);
    h.finish()
}

impl AnalyzerRule for ShareRepeats {
    fn name(&self) -> &str {
        "share_repeats"
    }

    /// Innermost repeats first, one layer per pass. Sharing an outer subquery whole hid every
    /// repeat inside it from the other occurrences, which then computed it again: a view read by
    /// four views that are themselves repeated was scanned once per copy, not once.
    fn analyze(&self, mut plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let mut caches: HashMap<u64, (LogicalPlan, Cache)> = HashMap::new();
        loop {
            let mut counts: HashMap<u64, usize> = HashMap::new();
            plan.apply_with_subqueries(|n| {
                if let Some(input) = candidate(n) {
                    *counts.entry(key(input)).or_default() += 1;
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            counts.retain(|_, c| *c >= 2);
            if counts.is_empty() {
                return Ok(plan);
            }
            let pass = plan.transform_up_with_subqueries(|n| {
                let Some(input) = candidate(&n) else {
                    return Ok(Transformed::no(n));
                };
                let k = key(input);
                if !counts.contains_key(&k) || repeats_below(input, &counts)? {
                    return Ok(Transformed::no(n));
                }
                let (first, cache) = caches
                    .entry(k)
                    .or_insert_with(|| (input.clone(), Arc::new(tokio::sync::OnceCell::new())))
                    .clone();
                if first != *input {
                    return Ok(Transformed::no(n));
                }
                let LogicalPlan::SubqueryAlias(s) = &n else {
                    unreachable!("candidate is an alias")
                };
                let shared = LogicalPlan::Extension(Extension {
                    node: Arc::new(SharedNode {
                        input: first,
                        cache,
                        id: k,
                    }),
                });
                let alias = SubqueryAlias::try_new(Arc::new(shared), s.alias.clone())?;
                Ok(Transformed::yes(LogicalPlan::SubqueryAlias(alias)))
            })?;
            if !pass.transformed {
                return Ok(pass.data);
            }
            plan = pass.data;
        }
    }
}

pub struct SharedNode {
    input: LogicalPlan,
    cache: Cache,
    id: u64,
}

impl fmt::Debug for SharedNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Shared {:016x}", self.id)
    }
}
impl PartialEq for SharedNode {
    fn eq(&self, o: &Self) -> bool {
        self.id == o.id && Arc::ptr_eq(&self.cache, &o.cache)
    }
}
impl Eq for SharedNode {}
impl Hash for SharedNode {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.id.hash(h);
    }
}
impl PartialOrd for SharedNode {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&o.id)
    }
}

impl UserDefinedLogicalNodeCore for SharedNode {
    fn name(&self) -> &str {
        "Shared"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Shared: computed once, id={:016x}", self.id)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            input: inputs.swap_remove(0),
            cache: Arc::clone(&self.cache),
            id: self.id,
        })
    }
}

#[derive(Debug)]
pub struct SharedPlanner;

#[async_trait]
impl ExtensionPlanner for SharedPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session: &dyn Session,
        _ctx: &datafusion_expr::physical_planning_context::PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(n) = node.as_any().downcast_ref::<SharedNode>() else {
            return Ok(None);
        };
        let child = Arc::clone(&physical_inputs[0]);
        let schema = child.schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Some(Arc::new(SharedExec {
            child,
            schema,
            cache: Arc::clone(&n.cache),
            properties,
        })))
    }
}

#[derive(Debug)]
struct SharedExec {
    child: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    cache: Cache,
    properties: Arc<PlanProperties>,
}

impl DisplayAs for SharedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SharedExec: computed once")
    }
}

impl ExecutionPlan for SharedExec {
    fn name(&self) -> &str {
        "SharedExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SharedExec {
            child: children.swap_remove(0),
            schema: Arc::clone(&self.schema),
            cache: Arc::clone(&self.cache),
            properties: Arc::clone(&self.properties),
        }))
    }
    fn execute(
        &self,
        _partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let (child, cache) = (Arc::clone(&self.child), Arc::clone(&self.cache));
        let stream = futures::stream::once(async move {
            let batches = cache
                .get_or_try_init(|| async move { collect(child, ctx).await.map(Arc::new) })
                .await
                .map_err(|e| DataFusionError::Context("shared subquery".into(), Box::new(e)))?;
            Ok::<_, DataFusionError>(futures::stream::iter(
                batches
                    .iter()
                    .cloned()
                    .map(Ok::<_, DataFusionError>)
                    .collect::<Vec<_>>(),
            ))
        })
        .flat_map(|r| match r {
            Ok(s) => s.left_stream(),
            Err(e) => futures::stream::once(async move { Err(e) }).right_stream(),
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}
