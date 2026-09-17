//! Component-crate session: no umbrella `datafusion` crate, no DDL, no table functions.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion_catalog::default_table_source::provider_as_source;
use datafusion_catalog::{
    CatalogProvider, CatalogProviderList, MemoryCatalogProvider, MemoryCatalogProviderList,
    MemorySchemaProvider, Session, TableProvider,
};
use datafusion_common::alias::AliasGenerator;
use datafusion_common::config::{ConfigOptions, TableOptions};
use datafusion_common::{DFSchema, Result as DFResult, TableReference, plan_datafusion_err};
use datafusion_execution::config::SessionConfig;
use datafusion_execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion_execution::TaskContext;
use datafusion_execution::cache::cache_manager::CacheManagerConfig;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::planner::{ContextProvider, ExprPlanner};
use datafusion_expr::registry::{
    ExtensionTypeRegistryRef, FunctionRegistry, MemoryExtensionTypeRegistry,
};
use datafusion_expr::{
    AggregateUDF, Expr, HigherOrderUDF, LogicalPlan, ScalarUDF, TableSource, WindowUDF,
};
use datafusion_functions::core::planner::CoreFunctionPlanner;
use datafusion_optimizer::analyzer::Analyzer;
use datafusion_optimizer::optimizer::{Optimizer, OptimizerConfig};
use datafusion_physical_expr::create_physical_expr;
use datafusion_physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion_session::{PhysicalOptimizerRule, PhysicalPlanner, QueryPlanner};

use super::physical_planner::DefaultPhysicalPlanner;

pub struct MiniSession {
    session_id: String,
    config: SessionConfig,
    runtime: Arc<RuntimeEnv>,
    catalog_list: Arc<MemoryCatalogProviderList>,
    scalar: HashMap<String, Arc<ScalarUDF>>,
    higher: HashMap<String, Arc<HigherOrderUDF>>,
    aggregate: HashMap<String, Arc<AggregateUDF>>,
    window: HashMap<String, Arc<WindowUDF>>,
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
    tables: HashMap<String, Arc<dyn TableSource>>,
    analyzer: Analyzer,
    optimizer: Optimizer,
    physical_optimizers: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    execution_props: ExecutionProps,
    table_options: TableOptions,
    extension_types: ExtensionTypeRegistryRef,
    alias_generator: Arc<AliasGenerator>,
    query_start: DateTime<Utc>,
    query_planner: Arc<dyn QueryPlanner + Send + Sync>,
}

impl std::fmt::Debug for MiniSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MiniSession")
    }
}

impl MiniSession {
    pub fn new(threads: usize) -> DFResult<Self> {
        let mut config = SessionConfig::new()
            .with_target_partitions(threads.max(1))
            .with_collect_statistics(false);
        config.options_mut().sql_parser.enable_ident_normalization = false;
        let runtime = RuntimeEnvBuilder::new()
            .with_cache_manager(
                CacheManagerConfig::default().with_metadata_cache_limit(1024 * 1024 * 1024),
            )
            .build_arc()?;
        let mut scalar = HashMap::new();
        for f in datafusion_functions::all_default_functions() {
            scalar.insert(f.name().to_string(), f);
        }
        let mut aggregate = HashMap::new();
        for f in datafusion_functions_aggregate::all_default_aggregate_functions() {
            aggregate.insert(f.name().to_string(), f);
        }
        let mut window = HashMap::new();
        for f in datafusion_functions_window::all_default_window_functions() {
            window.insert(f.name().to_string(), f);
        }
        let expr_planners: Vec<Arc<dyn ExprPlanner>> = vec![
            Arc::new(CoreFunctionPlanner::default()),
            Arc::new(datafusion_functions_aggregate::planner::AggregateFunctionPlanner),
            Arc::new(datafusion_functions_window::planner::WindowFunctionPlanner),
        ];
        let catalog_list = Arc::new(MemoryCatalogProviderList::new());
        let catalog = MemoryCatalogProvider::new();
        catalog
            .register_schema("public", Arc::new(MemorySchemaProvider::new()))
            .expect("schema");
        catalog_list.register_catalog("datafusion".into(), Arc::new(catalog));

        Ok(Self {
            session_id: "s".into(),
            config,
            runtime,
            catalog_list,
            scalar,
            higher: HashMap::new(),
            aggregate,
            window,
            expr_planners,
            tables: HashMap::new(),
            analyzer: Analyzer::new(),
            optimizer: Optimizer::new(),
            physical_optimizers: PhysicalOptimizer::new().rules,
            execution_props: ExecutionProps::new(),
            table_options: TableOptions::new(),
            extension_types: Arc::new(MemoryExtensionTypeRegistry::default()),
            alias_generator: Arc::new(AliasGenerator::new()),
            query_start: DateTime::<Utc>::from(SystemTime::now()),
            query_planner: Arc::new(MiniQueryPlanner),
        })
    }

    pub fn register_table(&mut self, name: &str, table: Arc<dyn TableProvider>) {
        self.tables.insert(name.to_string(), provider_as_source(table));
    }

    pub fn table_names(&self) -> Vec<String> {
        let mut n: Vec<String> = self.tables.keys().cloned().collect();
        n.sort();
        n
    }
}

#[derive(Debug)]
struct MiniQueryPlanner;

#[async_trait]
impl QueryPlanner for MiniQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::default()
            .create_physical_plan(logical_plan, session)
            .await
    }
}

impl OptimizerConfig for MiniSession {
    fn query_execution_start_time(&self) -> Option<DateTime<Utc>> {
        Some(self.query_start)
    }
    fn alias_generator(&self) -> &Arc<AliasGenerator> {
        &self.alias_generator
    }
    fn options(&self) -> Arc<ConfigOptions> {
        Arc::clone(self.config.options())
    }
    fn function_registry(&self) -> Option<&dyn FunctionRegistry> {
        Some(self)
    }
}

impl FunctionRegistry for MiniSession {
    fn udfs(&self) -> HashSet<String> {
        self.scalar.keys().cloned().collect()
    }
    fn higher_order_function_names(&self) -> HashSet<String> {
        self.higher.keys().cloned().collect()
    }
    fn udafs(&self) -> HashSet<String> {
        self.aggregate.keys().cloned().collect()
    }
    fn udwfs(&self) -> HashSet<String> {
        self.window.keys().cloned().collect()
    }
    fn udf(&self, name: &str) -> DFResult<Arc<ScalarUDF>> {
        self.scalar
            .get(name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("no udf {name}"))
    }
    fn higher_order_function(&self, name: &str) -> DFResult<Arc<HigherOrderUDF>> {
        self.higher
            .get(name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("no hof {name}"))
    }
    fn udaf(&self, name: &str) -> DFResult<Arc<AggregateUDF>> {
        self.aggregate
            .get(name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("no udaf {name}"))
    }
    fn udwf(&self, name: &str) -> DFResult<Arc<WindowUDF>> {
        self.window
            .get(name)
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("no udwf {name}"))
    }
    fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> {
        self.expr_planners.clone()
    }
}

impl ContextProvider for MiniSession {
    fn get_table_source(&self, name: TableReference) -> DFResult<Arc<dyn TableSource>> {
        self.tables
            .get(name.table())
            .cloned()
            .ok_or_else(|| plan_datafusion_err!("no table {name}"))
    }
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.scalar.get(name).cloned()
    }
    fn get_higher_order_meta(&self, name: &str) -> Option<Arc<HigherOrderUDF>> {
        self.higher.get(name).cloned()
    }
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.aggregate.get(name).cloned()
    }
    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.window.get(name).cloned()
    }
    fn get_variable_type(&self, _variable_names: &[String]) -> Option<arrow::datatypes::DataType> {
        None
    }
    fn options(&self) -> &ConfigOptions {
        self.config.options().as_ref()
    }
    fn udf_names(&self) -> Vec<String> {
        self.scalar.keys().cloned().collect()
    }
    fn higher_order_function_names(&self) -> Vec<String> {
        self.higher.keys().cloned().collect()
    }
    fn udaf_names(&self) -> Vec<String> {
        self.aggregate.keys().cloned().collect()
    }
    fn udwf_names(&self) -> Vec<String> {
        self.window.keys().cloned().collect()
    }
    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.expr_planners
    }
}

#[async_trait]
impl Session for MiniSession {
    fn session_id(&self) -> &str {
        &self.session_id
    }
    fn config(&self) -> &SessionConfig {
        &self.config
    }
    fn catalog_list(&self) -> Arc<dyn CatalogProviderList> {
        Arc::clone(&self.catalog_list) as Arc<dyn CatalogProviderList>
    }
    fn query_planner(&self) -> Arc<dyn QueryPlanner + Send + Sync> {
        Arc::clone(&self.query_planner)
    }
    fn optimize(&self, plan: &LogicalPlan) -> DFResult<LogicalPlan> {
        let analyzed =
            self.analyzer
                .execute_and_check(plan.clone(), self.config.options().as_ref(), |_, _| {})?;
        self.optimizer.optimize(analyzed, self, |_, _| {})
    }
    fn physical_optimizers(&self) -> &[Arc<dyn PhysicalOptimizerRule + Send + Sync>] {
        &self.physical_optimizers
    }
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let logical_plan = self.optimize(logical_plan)?;
        self.query_planner
            .create_physical_plan(&logical_plan, self)
            .await
    }
    fn create_physical_expr(
        &self,
        expr: Expr,
        df_schema: &DFSchema,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        create_physical_expr(
            &expr,
            df_schema,
            &self.execution_props,
            &PhysicalPlanningContext::default(),
        )
    }
    fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
        &self.scalar
    }
    fn higher_order_functions(&self) -> &HashMap<String, Arc<HigherOrderUDF>> {
        &self.higher
    }
    fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
        &self.aggregate
    }
    fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
        &self.window
    }
    fn extension_type_registry(&self) -> &ExtensionTypeRegistryRef {
        &self.extension_types
    }
    fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        &self.runtime
    }
    fn execution_props(&self) -> &ExecutionProps {
        &self.execution_props
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn table_options(&self) -> &TableOptions {
        &self.table_options
    }
    fn table_options_mut(&mut self) -> &mut TableOptions {
        &mut self.table_options
    }
    fn task_ctx(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::new(
            None,
            self.session_id.clone(),
            self.config.clone(),
            self.scalar.clone(),
            self.higher.clone(),
            self.aggregate.clone(),
            self.window.clone(),
            Arc::clone(&self.runtime),
        ))
    }
}
