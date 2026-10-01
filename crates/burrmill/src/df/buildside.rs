//! `BuildOnSmaller`: a hash join builds its table on the input that reads less from disk.
//!
//! DataFusion builds on the left input, the first table written, and swaps only when statistics say
//! the left is larger; statistics are off here. The QoS nest writes its 2.5 GB of rows first and
//! joins three small tables to them, so each of three nested joins held every row of the large one:
//! 60 GB and killed where DuckDB answers in 512 MB. The catalogue knows what each scan reads, as
//! [`super::smallinputs`] already uses, and that is enough to choose. Swapping is DataFusion's own
//! `swap_inputs`, which `JoinSelection` uses, so no answer changes; it runs straight after that
//! rule, before repartitions and dynamic filters are placed on the join's children.

use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{JoinType, Result};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::joins::HashJoinExec;
use datafusion_session::PhysicalOptimizerRule;

use super::smallinputs::bytes_read;

#[derive(Debug, Default)]
pub struct BuildOnSmaller;

impl PhysicalOptimizerRule for BuildOnSmaller {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|p| {
            let Some(j) = p.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(p));
            };
            if !matches!(j.join_type(), JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full) {
                return Ok(Transformed::no(p));
            }
            match (bytes_read(j.left()), bytes_read(j.right())) {
                (Some(build), Some(probe)) if build > probe => {
                    Ok(Transformed::yes(j.swap_inputs(*j.partition_mode())?))
                }
                _ => Ok(Transformed::no(p)),
            }
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "build_on_smaller"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
