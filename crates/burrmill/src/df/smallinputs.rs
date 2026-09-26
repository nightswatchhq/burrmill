//! `SmallInputs`: no round-robin fan-out over a few megabytes.
//!
//! Statistics being off, DataFusion cannot tell a small table from a large one, and spreads every
//! pipeline over all its partitions with round-robin repartitions: `lodestar_disputes` ran 18
//! operators, four of them repartitions to 8 partitions, for 8 rows. Turning that off everywhere
//! halved it and slowed the mid-sized views. Here a round-robin repartition goes where the scans
//! beneath it read less than [`SMALL_BYTES`] from disk, which the catalogue knows exactly. It only
//! spreads work over threads, so removing it cannot change an answer. A source it cannot measure
//! counts as large. Over the nest, 1 to 64 MiB were within noise of each other and of off, except
//! for small statements (`lodestar_disputes` 19 to 7-11 ms); 4 MiB leaves mid-sized work parallel.

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::{ExecutionPlan, Partitioning};
use datafusion_physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion_session::PhysicalOptimizerRule;

pub const SMALL_BYTES: u64 = 4 << 20;

#[derive(Debug, Default)]
pub struct SmallInputs;

/// Bytes the scans under `p` read, or `None` if one cannot be measured.
fn bytes_read(p: &Arc<dyn ExecutionPlan>) -> Option<u64> {
    if let Some(d) = p.downcast_ref::<DataSourceExec>() {
        if let Some(f) = d.data_source().downcast_ref::<FileScanConfig>() {
            return Some(f.file_groups.iter().flat_map(|g| g.iter()).map(|f| f.object_meta.size).sum());
        }
        return d.data_source().is::<MemorySourceConfig>().then_some(0);
    }
    if p.is::<EmptyExec>() || p.is::<PlaceholderRowExec>() {
        return Some(0);
    }
    let children = p.children();
    if children.is_empty() {
        return None;
    }
    children.into_iter().map(bytes_read).sum()
}

impl PhysicalOptimizerRule for SmallInputs {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        let small = Arc::clone(&plan).transform_up(|p| {
            let Some(r) = p.downcast_ref::<RepartitionExec>() else {
                return Ok(Transformed::no(p));
            };
            if !matches!(r.partitioning(), Partitioning::RoundRobinBatch(_)) {
                return Ok(Transformed::no(p));
            }
            match bytes_read(r.input()) {
                Some(b) if b < SMALL_BYTES => Ok(Transformed::yes(Arc::clone(r.input()))),
                _ => Ok(Transformed::no(p)),
            }
        })?;
        if !small.transformed {
            return Ok(plan);
        }
        // An operator above may have wanted the partitions removed; DataFusion's own check says.
        match SanityCheckPlan::new().optimize(Arc::clone(&small.data), config) {
            Ok(_) => Ok(small.data),
            Err(_) => Ok(plan),
        }
    }

    fn name(&self) -> &str {
        "small_inputs"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
