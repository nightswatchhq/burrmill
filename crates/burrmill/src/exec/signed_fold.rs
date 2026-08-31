//! `SignedFoldExec` - the owned operator, and the reason the project exists.
//!
//! Its ancestor is nuthatch's #987 spike, which measured **0.55-0.85x DuckDB across 24 of 24
//! configurations** on `net_balances` while general DataFusion on the same shape measured
//! 2.53-2.80x *slower* (#964). That is a three-to-fivefold swing between renting general execution
//! and owning a specialised one, and it is the entire architectural argument.
//!
//! What makes it fast is not clever: it is the same three things DuckDB does, applied to one shape.
//! Morsel-driven parallelism over row groups rather than files; thread-local hash tables merged
//! once at the end rather than one shared table under contention; and a fast non-cryptographic
//! hash, because std's SipHash is DoS resistance we are not paying for here.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, ArrayRef, Decimal128Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use rayon::prelude::*;

use crate::error::{BurrmillError, Result};
use crate::exec::checked::checked_neg;
use crate::limits::Limits;
use crate::plan::SignedFold;
use crate::segment::{Morsel, SealedSegments};

/// Cooperative cancellation.
///
/// §3.5's contract, and it is a design property here rather than a hope. DataFusion's own joins do
/// not yield to cancellation (#19358, with `make_cooperative` proposed in #19360); an owned
/// operator picks its own yield points, so the delay between asking to stop and stopping is bounded
/// by one morsel.
#[derive(Debug, Clone, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

/// What the fold cost, so a caller can see it without attaching a profiler. Feeds `EXPLAIN ANALYZE`.
#[derive(Debug, Clone, Copy, Default)]
pub struct FoldMetrics {
    pub morsels: usize,
    pub rows_read: u64,
    pub rows_skipped: u64,
    pub groups: usize,
    /// Reading the segment footers and cutting them into morsels. Serial in the first version, and
    /// at ten thousand segments it was the whole query.
    pub plan_ms: u128,
    /// The parallel fold itself.
    pub scan_ms: u128,
    /// Merging the per-morsel tables, plus the canonical sort.
    pub merge_ms: u128,
    pub elapsed_ms: u128,
    /// The aggregate's own live bytes at the moment the scan finished, before any row is built.
    /// This is what `mem_pool_bytes` is checked against, and printing it beside peak RSS is how you
    /// tell an aggregation that is too big from an output phase that is.
    pub agg_bytes: usize,
}

/// The aggregation lives in [`crate::exec::agg`]. It is not a `HashMap<String, i128>` and the
/// reason is measured rather than aesthetic: see that module's header for the three versions this
/// replaced and what each of them cost.
use crate::exec::agg::{Rows, Scatter, SharedAgg};

pub struct SignedFoldExec<'a> {
    plan: &'a SignedFold,
    segments: &'a SealedSegments,
    limits: Limits,
    cancel: CancelToken,
}

impl<'a> SignedFoldExec<'a> {
    pub fn new(plan: &'a SignedFold, segments: &'a SealedSegments, limits: Limits) -> Self {
        Self { plan, segments, limits, cancel: CancelToken::new() }
    }

    pub fn with_cancel(mut self, cancel: CancelToken) -> Self {
        self.cancel = cancel;
        self
    }

    /// Run the fold and return the canonically ordered rows plus what it cost.
    pub fn run(&self) -> Result<(Rows, FoldMetrics)> {
        let started = Instant::now();
        let morsels = self.segments.morsels()?;
        let plan_ms = started.elapsed().as_millis();
        let deadline = started + self.limits.timeout;

        // **One aggregation per worker, not per work unit.** An earlier version gave every batch its
        // own table and combined them on a single thread: at two hundred thousand groups that was 30
        // ms of scanning against 182 ms of merging, and at a million, 36 against 419. The merge
        // *was* the query. A table count that follows the core count is also exactly the behaviour
        // this RFC criticises DataFusion for (#6937), so reproducing it would have been embarrassing
        // as well as slow.
        let scan_started = Instant::now();
        let batches = coalesce(&morsels);

        // **One aggregate for the whole query, not one per worker.** The previous version gave each
        // worker a table spanning the whole key space and merged twelve of them at the end; that
        // passed on latency and failed the memory gate by 3.9x, because a million distinct parties
        // became roughly twelve million live entries to produce a 57 MB answer. Here a key lives in
        // exactly one partition table however many threads touched it, so the aggregate's size is a
        // property of the data rather than of the core count. See [`crate::exec::agg::SharedAgg`].
        let shared = SharedAgg::default();
        let counted: Vec<(u64, u64)> = batches
            .par_iter()
            .try_fold(
                || (Scatter::default(), 0u64, 0u64),
                |mut st, batch| -> Result<(Scatter, u64, u64)> {
                    for m in *batch {
                        let (read, skipped) = self.fold_morsel(m, deadline, &mut st.0, &shared)?;
                        st.1 += read;
                        st.2 += skipped;
                    }
                    // Drained at every batch boundary, so a worker's buffer never outlives the batch
                    // that filled it and the fold state can be dropped without a final pass.
                    st.0.flush(&shared)?;
                    // Per batch, not per row. The first version read the clock once per merged
                    // entry - five million clock reads at ten thousand morsels, to enforce a budget
                    // that cannot change that fast. Checking a limit must not cost more than the
                    // work it is limiting.
                    self.check_budget(shared.bytes(), started)?;
                    Ok(st)
                },
            )
            .map(|r| r.map(|(_, read, skipped)| (read, skipped)))
            .collect::<Result<Vec<_>>>()?;
        let scan_ms = scan_started.elapsed().as_millis();

        let agg_bytes = shared.bytes();

        let merge_started = Instant::now();
        let mut rows_read = 0u64;
        let mut rows_skipped = 0u64;
        for (read, skipped) in &counted {
            rows_read += read;
            rows_skipped += skipped;
        }

        // There is no merge left to do. The workers wrote into the answer as they went, so this is
        // the output phase and nothing else: each partition's table becomes rows, in parallel,
        // because a partition's keys never meet another's.
        let mut out = shared.into_rows(self.plan.drop_zero);

        let groups = out.len();
        if groups as u64 > self.limits.max_rows {
            return Err(BurrmillError::LimitExceeded(format!(
                "{groups} rows exceeds max_rows {}",
                self.limits.max_rows
            )));
        }

        let bytes = out.bytes();
        if bytes as u64 > self.limits.max_bytes {
            return Err(BurrmillError::LimitExceeded(format!(
                "the result is {} MB, over max_bytes {} MB",
                bytes >> 20,
                self.limits.max_bytes >> 20
            )));
        }

        // **Canonical ordering, applied unconditionally** (§3.3). Byte-wise ascending on the key.
        // Required here twice over: an oracle whose row order varies between runs is not one anybody
        // can build on, and the partitions come back in hash order, which is no order at all.
        out.sort_canonical();
        let merge_ms = merge_started.elapsed().as_millis();

        Ok((
            out,
            FoldMetrics {
                morsels: morsels.len(),
                rows_read,
                rows_skipped,
                groups,
                plan_ms,
                scan_ms,
                merge_ms,
                elapsed_ms: started.elapsed().as_millis(),
                agg_bytes,
            },
        ))
    }

    /// One morsel, folded into a table it shares with the rest of its batch.
    ///
    /// The unit of cancellation and of the deadline check; **not** the unit of the hash table, which
    /// is the batch. See [`coalesce`].
    fn fold_morsel(
        &self,
        m: &Morsel,
        deadline: Instant,
        scatter: &mut Scatter,
        shared: &SharedAgg,
    ) -> Result<(u64, u64)> {
        if self.cancel.is_cancelled() {
            return Err(BurrmillError::Cancelled);
        }
        if Instant::now() > deadline {
            return Err(BurrmillError::Timeout);
        }

        let file = std::fs::File::open(&m.path)?;
        // `new_with_metadata`, not `try_new`: the footer was parsed once when the morsels were cut,
        // and re-parsing it per morsel would put the cost back that parallelising the scan removed.
        // Sound because a sealed segment is content-addressed and immutable - the footer cannot go
        // stale under us the way a live file's could.
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(file, m.meta.clone())
            // 8192, not Arrow's 1024 default. DuckDB's vector is 2048; the fold is a tight loop over
            // three string columns and larger batches amortise the per-batch downcast without
            // pushing the working set out of L2.
            .with_batch_size(8192)
            .with_row_groups(vec![m.row_group])
            .with_offset(m.offset)
            .with_limit(m.len);
        // **Read three columns, not fourteen.** Without this the fold decodes every column in the
        // segment and uses the three it asked for. The synthetic fixture is four columns wide so the
        // waste was invisible; a real nest event is twelve to fourteen, two of them 64-character hex
        // hashes, and on `staking_legacy__stake_delegated` this was the difference between 2.2x
        // DuckDB and parity. Measured on the real nest, which is the only place it shows.
        let builder = match projection_mask(&m.meta, self.plan) {
            Some(mask) => builder.with_projection(mask),
            None => builder,
        };
        let reader = builder.build()?;

        let mut rows_read = 0u64;
        let mut rows_skipped = 0u64;

        for batch in reader {
            let batch = batch?;
            let credit = utf8_column(&batch, &self.plan.credit_col)?;
            let debit = utf8_column(&batch, &self.plan.debit_col)?;
            let value = utf8_column(&batch, &self.plan.value_col)?;

            for i in 0..batch.num_rows() {
                rows_read += 1;
                // TRY_CAST semantics: unparseable becomes NULL, and SUM ignores NULLs. Skip, never
                // substitute zero - a zero is a different answer that happens to look plausible.
                if value.is_null(i) {
                    rows_skipped += 1;
                    continue;
                }
                let Ok(d) = value.value(i).parse::<i128>() else {
                    rows_skipped += 1;
                    continue;
                };
                let minus_d = checked_neg(d, debit.value(i))?;
                scatter.push(credit.value(i).as_bytes(), d, shared)?;
                scatter.push(debit.value(i).as_bytes(), minus_d, shared)?;
            }
        }
        Ok((rows_read, rows_skipped))
    }

    /// The budget is now the query's whole aggregation rather than one worker's share of it.
    ///
    /// This used to read `mem_pool_bytes * threads` in practice and said so, because twelve workers
    /// each held a table of their own. With a single shared aggregate the number passed in is the
    /// real total, which is most of roadmap 1.3. It is still not process RSS: Parquet decode buffers
    /// and the Arrow batches in flight sit outside it, and the doc comment says so rather than
    /// implying a guarantee the operator cannot make.
    fn check_budget(&self, bytes: usize, started: Instant) -> Result<()> {
        if bytes as u64 > self.limits.mem_pool_bytes {
            return Err(BurrmillError::LimitExceeded(format!(
                "the query's aggregation is {} MB, over the mem_pool_bytes budget of {} MB",
                bytes >> 20,
                self.limits.mem_pool_bytes >> 20
            )));
        }
        if started.elapsed() > self.limits.timeout {
            return Err(BurrmillError::Timeout);
        }
        Ok(())
    }
}

/// Read a column as Utf8 whichever string layout the writer chose.
///
/// Parquet readers disagree here and it is a real migration hazard rather than a curiosity: nuthatch
/// seals `Utf8`, DataFusion reads the same bytes back as `Utf8View`. Casting rather than assuming
/// keeps the answer about the answer.
fn utf8_column(batch: &RecordBatch, name: &str) -> Result<StringArray> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| BurrmillError::Substrate(format!("no column `{name}` in the segment")))?;
    let col = batch.column(idx);
    if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
        return Ok(s.clone());
    }
    let cast = arrow::compute::cast(col, &DataType::Utf8)?;
    cast.as_any()
        .downcast_ref::<StringArray>()
        .cloned()
        .ok_or_else(|| BurrmillError::Substrate(format!("column `{name}` will not read as Utf8")))
}

/// The result as Arrow.
///
/// The sum is `Decimal128(38, 0)`, which is exactly i128 - not a float, and not text. uint256 needs
/// 78 decimal digits and `Decimal256` stops at 76, which is why the *stored* value stays text and
/// only the fold's output is a decimal.
pub fn to_record_batch(rows: &Rows, plan: &SignedFold) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(&plan.key_alias, DataType::Utf8, false),
        Field::new(&plan.sum_alias, DataType::Decimal128(38, 0), false),
    ]));
    let keys: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|(k, _)| k)));
    let sums: ArrayRef = Arc::new(
        Decimal128Array::from_iter_values(rows.iter().map(|(_, v)| v))
            .with_precision_and_scale(38, 0)?,
    );
    Ok(RecordBatch::try_new(schema, vec![keys, sums])?)
}

/// Group morsels into work units big enough to be worth a hash table of their own.
///
/// **This is the bimodal segment layout showing up as a performance bug.** With ten thousand
/// segments of two hundred rows each, one table per morsel meant ten thousand freshly allocated maps
/// each growing to roughly five hundred entries - so nearly every key in the fold allocated its own
/// boxed string instead of hitting an existing entry, four million allocations to answer a query
/// over two million rows. Measured cost: 1.05x DuckDB where the same operator was 0.55x on one
/// segment. The rows had not changed; only how they were filed.
///
/// A batch therefore targets a row budget rather than a file. The budget is derived from the
/// available parallelism rather than fixed, because a batch large enough to amortise allocation is
/// also large enough to starve the thread pool if the table is small.
/// The leaf columns the fold actually reads, as a Parquet projection.
///
/// `None` when the schema is not flat or a name is missing, in which case the reader falls back to
/// decoding everything - slower, never wrong. A sealed segment's schema is flat today, so this is a
/// guard against a future layout rather than a live branch, and [`SealedSegments`] has no path
/// dependency that would warn us if that changed.
fn projection_mask(
    meta: &parquet::arrow::arrow_reader::ArrowReaderMetadata,
    plan: &SignedFold,
) -> Option<ProjectionMask> {
    let descr = meta.metadata().file_metadata().schema_descr();
    let root = descr.root_schema();
    // Flat only. A nested group would make root index and leaf index disagree, and a projection that
    // silently selects the wrong column is exactly the failure this project exists to refuse.
    if root.get_fields().iter().any(|f| !f.is_primitive()) {
        return None;
    }
    let index_of = |name: &str| root.get_fields().iter().position(|f| f.name() == name);
    let mut idx = Vec::with_capacity(3);
    for name in [&plan.credit_col, &plan.debit_col, &plan.value_col] {
        idx.push(index_of(name)?);
    }
    idx.sort_unstable();
    idx.dedup();
    Some(ProjectionMask::roots(descr, idx))
}

fn coalesce(morsels: &[Morsel]) -> Vec<&[Morsel]> {
    let total: usize = morsels.iter().map(|m| m.len).sum();
    let threads = rayon::current_num_threads().max(1);
    // Four units per thread: enough slack for work-stealing to even out the bimodal size
    // distribution, without cutting the units so fine that the fixed cost returns.
    let target = (total / (threads * 4)).clamp(4_096, crate::segment::MORSEL_ROWS);

    let mut out = Vec::new();
    let mut start = 0usize;
    let mut rows = 0usize;
    for (i, m) in morsels.iter().enumerate() {
        rows += m.len;
        if rows >= target {
            out.push(&morsels[start..=i]);
            start = i + 1;
            rows = 0;
        }
    }
    if start < morsels.len() {
        out.push(&morsels[start..]);
    }
    out
}
