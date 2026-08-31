//! # Burrmill
//!
//! SQL over sealed Parquet segments plus a live tip, with exact integer arithmetic and
//! refuse-on-overflow, in one binary with nothing to configure.
//!
//! ## What this is
//!
//! A **query engine-layer**. It owns the semantics *and* the execution of a deliberately small
//! admitted subset of SQL - the shapes a blockchain indexer actually runs - and it owns them all the
//! way down to the vectorised operator. It is not a general database and does not want to be. It
//! needs to be faster than DuckDB on *these* shapes over *this* layout, and merely honest about
//! everything else.
//!
//! ## What it rents, permanently
//!
//! Arrow for the in-memory format and its kernels, and parquet-rs for decode. Nobody solo-maintains
//! a better SIMD kernel library, and the evidence that trying would pay does not exist. Everything
//! above them is ours.
//!
//! ## The three claims, and how to check them
//!
//! - **Faster.** The ancestor of [`exec::SignedFoldExec`] measured 0.55-0.85x DuckDB across 24 of 24
//!   configurations where general DataFusion measured 2.53-2.80x *slower* on the same query. The
//!   `burrmill-bench` crate reproduces that head to head, parity first, timings second.
//! - **Exact.** Integer overflow returns [`BurrmillError::Overflow`], never a wrapped number.
//!   DataFusion wraps silently and has no core flag to stop it; DuckDB errors on `HUGEINT` but is
//!   not watertight everywhere.
//! - **Closed.** Table names resolve against a positive allowlist and there are no file-reading SQL
//!   functions to register. `read_parquet('/etc/passwd')` does not fail a check - it has nowhere to
//!   parse to.
//!
//! ```no_run
//! use burrmill::{Burrmill, Limits};
//!
//! let db = Burrmill::open_segments("t", std::path::Path::new("./segments"))?;
//! let answer = db.query(
//!     r#"SELECT addr, SUM(d) AS net FROM (
//!            SELECT "to" AS addr, TRY_CAST("value" AS HUGEINT) AS d FROM t
//!            UNION ALL
//!            SELECT "from" AS addr, -TRY_CAST("value" AS HUGEINT) AS d FROM t
//!        ) GROUP BY addr HAVING SUM(d) <> 0 ORDER BY addr"#,
//!     Limits::default(),
//! )?;
//! println!("{} parties", answer.rows().len());
//! # Ok::<(), burrmill::BurrmillError>(())
//! ```

pub mod error;
pub mod exec;
pub mod limits;
pub mod plan;
pub mod seam;
pub mod segment;

pub use error::{BurrmillError, Result};
pub use exec::agg::Rows;
pub use exec::{CancelToken, FoldMetrics};
pub use limits::Limits;
pub use plan::{Plan, SignedFold};
pub use seam::{HotRow, HotSnapshot, HotTip, MemoryTip};
pub use segment::{Catalog, SealedSegments};

use std::path::Path;

use arrow::record_batch::RecordBatch;

/// A finished answer.
///
/// Not a stream yet, and the docs say so rather than implying otherwise. Streaming and the async
/// cancellation contract belong to the concurrency slice; today a fold's result is a materialised,
/// canonically ordered table, which is what the folds produce anyway - one row per party.
#[derive(Debug)]
pub struct Answer {
    rows: Rows,
    plan: Plan,
    metrics: FoldMetrics,
}

impl Answer {
    /// Key and exact i128 sum, canonically ordered.
    ///
    /// The keys live in the arenas they were aggregated in rather than in an allocation each, so
    /// this borrows rather than handing out a `Vec<(Box<str>, i128)>`. See [`Rows`].
    pub fn rows(&self) -> &Rows {
        &self.rows
    }

    /// The same answer as Arrow, with the sum as `Decimal128(38, 0)`.
    pub fn to_record_batch(&self) -> Result<RecordBatch> {
        let Plan::SignedFold(f) = &self.plan;
        exec::to_record_batch(&self.rows, f)
    }

    pub fn metrics(&self) -> FoldMetrics {
        self.metrics
    }

    pub fn plan(&self) -> &Plan {
        &self.plan
    }
}

/// The handle. One per nest, cheap to clone into as many concurrent queries as you like.
///
/// **There is no global lock in here.** DuckDB in nuthatch sits behind a single connection mutex,
/// and the measured consequence is a p99 that goes from 29.5 ms to 7066 ms between one client and
/// thirty-two while throughput stays flat at about 40 qps. That is not a tuning problem, it is the
/// architecture, and not having it is most of why a serving path wants this.
/// **Parallelism is bounded here, not inherited from the host.** See [`Limits::max_threads`]. The
/// pool is built once per handle and shared by every clone, so a query pays nothing to be bounded.
#[derive(Clone)]
pub struct Burrmill {
    catalog: Catalog,
    pool: std::sync::Arc<rayon::ThreadPool>,
    /// The unsealed tip, when there is one. `None` means every row is cold, which is the whole of
    /// slice 1 and remains the default.
    hot: Option<std::sync::Arc<dyn seam::HotTip>>,
    /// The column carrying the block number, used only to apply the seam boundary to cold rows.
    block_col: std::sync::Arc<str>,
}

impl std::fmt::Debug for Burrmill {
    /// A `ThreadPool` is not `Debug`, and printing one would be noise anyway.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Burrmill")
            .field("catalog", &self.catalog)
            .field("threads", &self.pool.current_num_threads())
            .field("hot_tip", &self.hot.is_some())
            .finish()
    }
}

/// Build the bounded pool, falling back to the ambient parallelism if the machine has fewer cores
/// than the budget allows. Failing to build a pool is a substrate problem, not a query problem.
fn build_pool(threads: usize) -> Result<std::sync::Arc<rayon::ThreadPool>> {
    let want = threads
        .max(1)
        .min(std::thread::available_parallelism().map(|n| n.get()).unwrap_or(threads.max(1)));
    rayon::ThreadPoolBuilder::new()
        .num_threads(want)
        .thread_name(|i| format!("burrmill-{i}"))
        .build()
        .map(std::sync::Arc::new)
        .map_err(|e| BurrmillError::Substrate(format!("could not build the fold's thread pool: {e}")))
}

impl Burrmill {
    /// Open with an explicit catalog and the default thread budget.
    pub fn new(catalog: Catalog) -> Self {
        Self::with_threads(catalog, Limits::default().max_threads)
            .expect("the default thread budget always builds a pool")
    }

    /// Open with an explicit catalog and an explicit thread budget.
    ///
    /// The budget lives on the handle rather than on [`Limits`] because a thread pool is expensive
    /// to build and cheap to share, and a per-query pool would put four hundred microseconds of
    /// thread spawning inside a fifteen-millisecond query.
    pub fn with_threads(catalog: Catalog, threads: usize) -> Result<Self> {
        Ok(Self { catalog, pool: build_pool(threads)?, hot: None, block_col: "block_number".into() })
    }

    /// Register every `*.parquet` under `dir` as one table.
    pub fn open_segments(name: &str, dir: &Path) -> Result<Self> {
        let mut catalog = Catalog::new();
        catalog.register(SealedSegments::discover(name, dir)?);
        Ok(Self {
            catalog,
            pool: build_pool(Limits::default().max_threads)?,
            hot: None,
            block_col: "block_number".into(),
        })
    }

    /// Register the segments of a nest table whose files share a `<contract>__<event>-` prefix.
    pub fn open_nest_table(name: &str, segments_dir: &Path, prefix: &str) -> Result<Self> {
        let all = SealedSegments::discover("_all", segments_dir)?;
        let table = all.with_prefix(name, prefix);
        // **A table name that matches nothing is refused, not answered emptily.** Found by pointing
        // the harness at a table that does not exist: DuckDB said "No files found that match the
        // pattern" and Burrmill planned `files=0 morsels=0` and would have handed back an empty
        // answer. A nest keeps every table's segments in one directory, so a mistyped table name is
        // always exactly one prefix away, and an empty balance sheet is a plausible-looking lie.
        //
        // The error names what *is* there, because the failure is nearly always a near-miss and a
        // list of neighbours turns a puzzled afternoon into a glance.
        if table.files().is_empty() {
            let mut found: Vec<String> = all
                .files()
                .iter()
                .filter_map(|p| p.file_name().and_then(|f| f.to_str()))
                .filter_map(|f| f.rsplit_once('-').map(|(head, _)| head.to_string()))
                .collect();
            found.sort();
            found.dedup();
            let shown: Vec<&str> = found.iter().map(|s| s.as_str()).take(12).collect();
            return Err(BurrmillError::NoSegments(format!(
                "no segment in {} matches `{prefix}`; {} table prefixes are present{}: {}",
                segments_dir.display(),
                found.len(),
                if found.len() > shown.len() { ", first 12" } else { "" },
                shown.join(", ")
            )));
        }
        let mut catalog = Catalog::new();
        catalog.register(table);
        Ok(Self {
            catalog,
            pool: build_pool(Limits::default().max_threads)?,
            hot: None,
            block_col: "block_number".into(),
        })
    }

    /// Attach an unsealed tip, making every query a hot∪cold seam (RFC-0044 §3.4).
    ///
    /// The block column is named rather than assumed: it is `block_number` in every nest seen so
    /// far, and `tests/seal_layout.rs` exists because assuming a layout is how the reading breaks
    /// silently.
    pub fn with_hot_tip(mut self, hot: std::sync::Arc<dyn seam::HotTip>, block_col: &str) -> Self {
        self.hot = Some(hot);
        self.block_col = block_col.into();
        self
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Plan and run, or refuse and say which clause offended.
    pub fn query(&self, sql: &str, limits: Limits) -> Result<Answer> {
        self.query_with_cancel(sql, limits, CancelToken::new())
    }

    pub fn query_with_cancel(
        &self,
        sql: &str,
        limits: Limits,
        cancel: CancelToken,
    ) -> Result<Answer> {
        let plan = plan::plan(sql)?;
        let Plan::SignedFold(fold) = &plan;

        // **The snapshot is taken before the segments are listed, and that order is the invariant.**
        // Sealing is append-only, so a listing taken afterwards is a superset of what the watermark
        // promises. Taking it the other way round lets a seal land in between, and the range it
        // moved is then in neither half - dropped silently, which in a balance query means a number
        // that is simply too small. See [`crate::seam`].
        let snapshot = match &self.hot {
            Some(hot) => Some(hot.snapshot(&fold.table)?),
            None => None,
        };
        // **Listed after the snapshot, and this is the load-bearing line.** The first version of
        // this took the catalog as the caller had built it, which meant the listing was from
        // whenever the handle was opened - so a range sealed in between was in neither half and the
        // COR-1 test caught it on run 0. A caller cannot be expected to open its handle at the
        // right instant; the ordering has to live here.
        let resolved;
        let segments = match &snapshot {
            Some(_) => {
                resolved = self.catalog.resolve(&fold.table)?.refresh()?;
                &resolved
            }
            None => self.catalog.resolve(&fold.table)?,
        };
        let seam = snapshot
            .as_ref()
            .map(|s| exec::signed_fold::Seam { snapshot: s, block_col: &self.block_col });

        // **Inside the bounded pool.** Peak RSS at a million groups is 147 MB on one thread and 349
        // on thirty-two, so an unbounded fold makes `mem_pool_bytes` a statement about the machine
        // rather than about the query. Nested `install` on the same pool is free.
        let (rows, metrics) = self.pool.install(|| {
            let mut exec = exec::SignedFoldExec::new(fold, segments, limits).with_cancel(cancel);
            if let Some(seam) = seam.as_ref() {
                exec = exec.with_seam(seam);
            }
            exec.run()
        })?;
        Ok(Answer { rows, plan: plan.clone(), metrics })
    }

    /// What the query would do, without doing it.
    ///
    /// Batteries included rather than optional polish: this is how you debug a parity failure at two
    /// in the morning without attaching a profiler.
    pub fn explain(&self, sql: &str) -> Result<String> {
        let plan = plan::plan(sql)?;
        let Plan::SignedFold(fold) = &plan;
        let segments = self.catalog.resolve(&fold.table)?;
        let morsels = segments.morsels()?;
        Ok(format!(
            "{}\n  CanonicalSort key={} asc\n  SegmentScan table={} files={} morsels={}\n  \
             arithmetic=checked-i128 (refuse-on-overflow)\n  parallelism=morsel per row group",
            plan.describe(),
            fold.key_alias,
            fold.table,
            segments.files().len(),
            morsels.len(),
        ))
    }
}
