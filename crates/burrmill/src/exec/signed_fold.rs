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
}

/// The aggregation lives in [`crate::exec::agg`]. It is not a `HashMap<String, i128>` and the
/// reason is measured rather than aesthetic: see that module's header for the three versions this
/// replaced and what each of them cost.
use crate::exec::agg::{merge_partition, PartTable, PartitionedAgg, PARTITIONS};

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
    pub fn run(&self) -> Result<(Vec<(Box<str>, i128)>, FoldMetrics)> {
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
        let per_worker: Vec<(PartitionedAgg, u64, u64)> = batches
            .par_iter()
            .try_fold(
                || (PartitionedAgg::default(), 0u64, 0u64),
                |mut st, batch| -> Result<(PartitionedAgg, u64, u64)> {
                    for m in *batch {
                        let (read, skipped) = self.fold_morsel(m, deadline, &mut st.0)?;
                        st.1 += read;
                        st.2 += skipped;
                    }
                    // Per batch, not per row. The first version read the clock once per merged
                    // entry - five million clock reads at ten thousand morsels, to enforce a budget
                    // that cannot change that fast. Checking a limit must not cost more than the
                    // work it is limiting.
                    self.check_budget(st.0.bytes(), started)?;
                    Ok(st)
                },
            )
            .collect::<Result<Vec<_>>>()?;
        let scan_ms = scan_started.elapsed().as_millis();

        let merge_started = Instant::now();
        let mut rows_read = 0u64;
        let mut rows_skipped = 0u64;
        let drop_zero = self.plan.drop_zero;
        let mut per_worker = per_worker;
        for (_, read, skipped) in &per_worker {
            rows_read += read;
            rows_skipped += skipped;
        }

        // **Two merge strategies, and the cheap one is the default.** If no worker's table grew past
        // the promotion threshold, the whole aggregate is small and a serial combine of a dozen
        // tables costs less than the machinery to parallelise it. Forcing the partitioned path on a
        // small aggregate measured 1.55x DuckDB where the serial one was 0.90x.
        let any_promoted = per_worker.iter().any(|(a, _, _)| a.is_promoted());
        let mut out: Vec<(Box<str>, i128)> = if !any_promoted {
            let tables: Vec<PartTable> = per_worker
                .into_iter()
                .filter_map(|(agg, _, _)| agg.parts.into_iter().next())
                .collect();
            merge_partition(tables)?.into_rows(drop_zero)
        } else {
            // Promotion is itself parallel: it is a full pass over a worker's entries, and doing a
            // dozen of those on the collecting thread is how the previous version lost.
            per_worker
                .par_iter_mut()
                .try_for_each(|(agg, _, _)| agg.ensure_partitioned())?;

            // Transpose workers-by-partitions into partitions-by-workers. Moves only, no rehashing,
            // and it is what makes the merge parallel: partition p's tables never meet partition
            // q's, so each partition is combined and emitted independently.
            let mut columns: Vec<Vec<PartTable>> =
                (0..PARTITIONS).map(|_| Vec::with_capacity(per_worker.len())).collect();
            for (agg, _, _) in per_worker {
                for (p, table) in agg.parts.into_iter().enumerate() {
                    if !table.is_empty() {
                        columns[p].push(table);
                    }
                }
            }
            let per_partition: Vec<Vec<(Box<str>, i128)>> = columns
                .into_par_iter()
                .map(|tables| -> Result<Vec<(Box<str>, i128)>> {
                    Ok(merge_partition(tables)?.into_rows(drop_zero))
                })
                .collect::<Result<Vec<_>>>()?;
            let total: usize = per_partition.iter().map(|v| v.len()).sum();
            let mut out = Vec::with_capacity(total);
            for part in per_partition {
                out.extend(part);
            }
            out
        };

        let groups = out.len();
        if groups as u64 > self.limits.max_rows {
            return Err(BurrmillError::LimitExceeded(format!(
                "{groups} rows exceeds max_rows {}",
                self.limits.max_rows
            )));
        }

        // **Canonical ordering, applied unconditionally** (§3.3). Byte-wise ascending on the key.
        // Required here twice over: an oracle whose row order varies between runs is not one anybody
        // can build on, and the partitions come back in hash order, which is no order at all.
        out.par_sort_unstable_by(|a, b| a.0.cmp(&b.0));
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
        acc: &mut PartitionedAgg,
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
        let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(file, m.meta.clone())
            // 8192, not Arrow's 1024 default. DuckDB's vector is 2048; the fold is a tight loop over
            // three string columns and larger batches amortise the per-batch downcast without
            // pushing the working set out of L2.
            .with_batch_size(8192)
            .with_row_groups(vec![m.row_group])
            .with_offset(m.offset)
            .with_limit(m.len)
            .build()?;

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
                acc.add(credit.value(i).as_bytes(), d)?;
                acc.add(debit.value(i).as_bytes(), minus_d)?;
            }
        }
        Ok((rows_read, rows_skipped))
    }

    /// The budget is per worker, and the docs say so rather than implying a process-wide guarantee.
    ///
    /// A fold running on twelve threads holds twelve of these, so the real ceiling is
    /// `mem_pool_bytes * threads` - which is honest about the shape of the thing rather than
    /// flattering. Making it a genuine global budget needs the workers to coordinate, and that is
    /// the concurrency slice's job, not this one's.
    fn check_budget(&self, bytes: usize, started: Instant) -> Result<()> {
        if bytes as u64 > self.limits.mem_pool_bytes {
            return Err(BurrmillError::LimitExceeded(format!(
                "a worker's aggregation table is {} MB, over the per-worker mem_pool_bytes budget \
                 of {} MB",
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
pub fn to_record_batch(rows: &[(Box<str>, i128)], plan: &SignedFold) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(&plan.key_alias, DataType::Utf8, false),
        Field::new(&plan.sum_alias, DataType::Decimal128(38, 0), false),
    ]));
    let keys: ArrayRef = Arc::new(StringArray::from_iter_values(rows.iter().map(|(k, _)| k.as_ref())));
    let sums: ArrayRef = Arc::new(
        Decimal128Array::from_iter_values(rows.iter().map(|(_, v)| *v))
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
