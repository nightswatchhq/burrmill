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
    /// **Threads one query may use, and the reason a memory budget means anything.**
    ///
    /// Before this existed, the fold took whatever rayon's global pool offered, so the same binary
    /// measured 147 MB on one thread and 349 MB on thirty-two at a million groups. A budget that
    /// depends on the host's core count is not a budget, and the RFC's 256 MB never said at what
    /// parallelism (roadmap 1.2c).
    ///
    /// Eight, because the cores past it are not buying anything. Measured at 1M groups on a 32-core
    /// box: 575 ms at 1 thread, 220 at 4, **163 at 8**, 183 at 16, 153 at 32. Eight threads is
    /// within 6% of the whole machine and leaves the other twenty-four for other queries - which is
    /// the entire concurrency argument. #986 measured DuckDB going from 40.3 to 39.6 qps between one
    /// client and thirty-two while p99 went 29.5 ms to 7066 ms, because it sits behind one
    /// connection mutex; a fold that hands every core to a single query has reinvented that.
    pub max_threads: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_rows: 10_000_000,
            max_bytes: 512 << 20,
            timeout: Duration::from_secs(60),
            mem_pool_bytes: 192 << 20,
            max_threads: 8,
        }
    }
}

impl Limits {
    /// A serving profile: something a public endpoint can hold open without a slow query becoming
    /// an availability problem. #986 measured DuckDB's p99 going from 29.5 ms to 7066 ms between 1
    /// and 32 clients, flat on throughput, because it sits behind one connection mutex.
    ///
    /// **Four threads rather than eight, and roadmap 5.2 says that is still not the answer.** The
    /// pool is per handle and shared by every concurrent query, so a larger `max_threads` does not
    /// make a serving path faster - it makes each query hog more of the pool and the queue behind it
    /// longer. Measured at 32 clients on a 32-core box: one thread per query gives 15 qps and serves
    /// **everybody**; four gives 55 qps and serves some clients **nothing**; eight gives 99 qps and
    /// starves them just as thoroughly.
    ///
    /// So four is a guess between two bad ends rather than a setting anybody derived, and no
    /// constant here is the fix. What is needed is a fair queue in front of the pool and a
    /// per-query parallelism that shrinks as load rises - roadmap 5.3.
    pub fn serving() -> Self {
        Self {
            max_rows: 100_000,
            max_bytes: 32 << 20,
            timeout: Duration::from_secs(5),
            mem_pool_bytes: 64 << 20,
            // A serving profile wants many queries in flight, not one query going slightly faster.
            max_threads: 4,
        }
    }
}
