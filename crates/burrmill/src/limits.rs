//! Batteries included: good behaviour with no configuration.
//!
//! DuckDB reads Parquet fast out of the box; DataFusion needs
//! `datafusion.execution.parquet.pushdown_filters=true` set by hand and still has not made it the
//! default in v55 (EPIC #20324). A library whose good performance depends on the caller knowing
//! which knobs to turn has not finished the job, so Burrmill's defaults are the tuned ones.

use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Rows returned to the caller before the query is refused.
    pub max_rows: u64,
    /// Bytes of result before the query is refused.
    pub max_bytes: u64,
    /// Wall-clock budget, checked at morsel boundaries.
    pub timeout: Duration,
    /// Ceiling on the operator's own working set. The RFC-0004 CI gate is 256 MB peak RSS for the
    /// whole process, so an operator budget below that leaves room for everything else.
    pub mem_pool_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_rows: 10_000_000,
            max_bytes: 512 << 20,
            timeout: Duration::from_secs(60),
            mem_pool_bytes: 192 << 20,
        }
    }
}

impl Limits {
    /// A serving profile: something a public endpoint can hold open without a slow query becoming
    /// an availability problem. #986 measured DuckDB's p99 going from 29.5 ms to 7066 ms between 1
    /// and 32 clients, flat on throughput, because it sits behind one connection mutex. Burrmill
    /// takes no global lock, so the interesting limit here is per-query and not per-process.
    pub fn serving() -> Self {
        Self {
            max_rows: 100_000,
            max_bytes: 32 << 20,
            timeout: Duration::from_secs(5),
            mem_pool_bytes: 64 << 20,
        }
    }
}
