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
pub mod segment;

pub use error::{BurrmillError, Result};
pub use exec::agg::Rows;
pub use exec::{CancelToken, FoldMetrics};
pub use limits::Limits;
pub use plan::{Plan, SignedFold};
pub use segment::{Catalog, SealedSegments};

use std::path::Path;

use arrow::record_batch::RecordBatch;

/// A finished answer.
///
/// Not a stream yet, and the docs say so rather than implying otherwise. Streaming and the async
/// cancellation contract belong to the concurrency slice; today a fold's result is a materialised,
/// canonically ordered table, which is what the folds produce anyway - one row per party.
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
#[derive(Debug, Clone)]
pub struct Burrmill {
    catalog: Catalog,
}

impl Burrmill {
    /// Open with an explicit catalog.
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }

    /// Register every `*.parquet` under `dir` as one table.
    pub fn open_segments(name: &str, dir: &Path) -> Result<Self> {
        let mut catalog = Catalog::new();
        catalog.register(SealedSegments::discover(name, dir)?);
        Ok(Self { catalog })
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
        Ok(Self { catalog })
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
        let segments = self.catalog.resolve(&fold.table)?;
        let (rows, metrics) = exec::SignedFoldExec::new(fold, segments, limits)
            .with_cancel(cancel)
            .run()?;
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
