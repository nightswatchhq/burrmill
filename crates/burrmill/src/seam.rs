//! The hot∪cold seam, and COR-1 (RFC-0044 §3.4).
//!
//! The highest-risk invariant in the design, and the one place where owning execution and getting
//! the semantics right are the same job.
//!
//! # What the seam actually is
//!
//! Not what §3.4's one-line summary suggests, and the difference matters enough to write down.
//! Reading nuthatch's `store.rs` and `seal.rs` rather than the RFC's precis:
//!
//! - Rows live in the **hot store** until their block range is *final*.
//! - Once final, every table's rows in that range are written to content-addressed Parquet segments.
//!   All tables in a nest seal together per finalized range, so **`sealed_through` is a single
//!   global watermark**, not a per-table one.
//! - The range is then **pruned from hot**, and `prune_and_set_meta` does the prune and the
//!   watermark advance **in one transaction**.
//! - The cold layer is append-only and never sees a reorg; reorgs only ever touch hot.
//!
//! That last point is the whole invariant, and it is why the choice of hot store is not load-bearing
//! here: COR-1 rests on the store offering *snapshot isolation*, which redb does and any plausible
//! alternative would too.
//!
//! # The hazard, precisely
//!
//! Pin `S = sealed_through`. Cold holds rows with `block <= S`; hot holds rows with `block > S`.
//! Both halves must come from a view where those two facts are true *at the same instant*.
//!
//! The dangerous order is: read the watermark, then read hot. Between the two, the indexer seals
//! `(S, S']` and prunes it. The rows in that range are now in a segment the query never listed, and
//! gone from the hot rows it did read. **They are silently dropped** - and a fold that drops rows
//! returns a short balance, which looks exactly like a balance.
//!
//! The safe order is the other one, and it is not obvious until you have written down why:
//!
//! 1. Take **one** hot snapshot, and read `sealed_through` **from that snapshot**. Because the prune
//!    and the watermark advance are one transaction, a snapshot can never show a pruned range with
//!    an unadvanced watermark, nor an advanced watermark with the range still present.
//! 2. List cold segments **at or after** that instant. Sealing is append-only, so a later listing is
//!    a superset: everything at or below `S` is certainly there.
//! 3. Filter cold to `block <= S` and take hot whole. Anything newly sealed above `S` is excluded by
//!    the filter, and anything at or below `S` is in cold exactly once.
//!
//! Hence [`HotTip::snapshot`] returns the watermark and the rows **together**. Splitting it into two
//! calls would put the bug back, so the trait is shaped to make the bug unspellable rather than
//! merely documented.
//!
//! # What is not here
//!
//! **No redb.** Burrmill has no path dependency on nuthatch by design - see `tests/seal_layout.rs`
//! for what that costs and buys - and the hot rows live there as JSON entities in a schema that is
//! nuthatch's business. A redb-backed [`HotTip`] is a thin adapter and belongs where that encoding
//! is known rather than guessed at. What is owned here is the invariant, and [`MemoryTip`] exercises
//! it under concurrent sealing.

use crate::error::Result;

/// One unsealed row of the signed-fold shape.
///
/// Shaped to the one plan shape Burrmill admits, deliberately. A general column-set trait would be
/// speculative today and the admitted subset is where this project's honesty lives; it widens when
/// the shapes do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HotRow {
    /// The block this row was indexed at. Above the watermark, by construction.
    pub block: u64,
    pub credit: Box<str>,
    pub debit: Box<str>,
    /// `None` is SQL NULL: `TRY_CAST` failed or the column was absent. Skipped by the fold, never
    /// substituted with zero.
    pub value: Option<Box<str>>,
}

/// The hot tip and its watermark, read together from one consistent view.
#[derive(Debug, Clone, Default)]
pub struct HotSnapshot {
    /// Everything at or below this block is sealed into cold segments. `None` means **nothing has
    /// been sealed yet**, and the distinction is not pedantry.
    ///
    /// The first version of this was a bare `u64` defaulting to zero, which has to mean both
    /// "nothing is sealed" and "block zero is sealed" and cannot mean both. The COR-1 test found it
    /// on a genesis-block row within five runs. A sentinel in a boundary is exactly where seam bugs
    /// live, so the ambiguity is removed rather than worked around.
    pub sealed_through: Option<u64>,
    /// The unsealed rows, all of which must be above `sealed_through`.
    pub rows: Vec<HotRow>,
}

impl HotSnapshot {
    /// The disjointness half of COR-1, checked rather than assumed.
    ///
    /// A hot row at or below the watermark is also in a cold segment, so counting it would
    /// double-count a balance. Cheap enough to run on every query and precise about what went wrong.
    pub fn check_disjoint(&self) -> Result<()> {
        let Some(watermark) = self.sealed_through else { return Ok(()) };
        if let Some(bad) = self.rows.iter().find(|r| r.block <= watermark) {
            return Err(crate::BurrmillError::Seam(format!(
                "a hot row at block {} is at or below sealed_through {watermark}, so it is also in \
                 a cold segment. Counting it would double a balance; refusing instead.",
                bad.block
            )));
        }
        Ok(())
    }
}

/// A provider of unsealed rows.
///
/// **One call, deliberately.** The watermark and the rows must come from the same consistent view,
/// and a two-call interface - `sealed_through()` then `rows()` - is precisely the shape that drops
/// rows when a seal lands between them. The trait exists in this shape so that the bug cannot be
/// written, rather than so it can be documented.
pub trait HotTip: Send + Sync {
    fn snapshot(&self, table: &str) -> Result<HotSnapshot>;
}

/// A hot tip in memory, for tests and for anyone embedding Burrmill without nuthatch.
///
/// It models the two operations that make COR-1 hard, with the same atomicity the real store has:
/// [`MemoryTip::append`] adds an unsealed row, and [`MemoryTip::seal_through`] advances the
/// watermark and drops the rows at or below it **under one lock**, exactly as nuthatch's
/// `prune_and_set_meta` does in one transaction.
#[derive(Debug, Default)]
pub struct MemoryTip {
    inner: std::sync::Mutex<HotSnapshot>,
}

impl MemoryTip {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn append(&self, row: HotRow) {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).rows.push(row);
    }

    /// Advance the watermark and prune, atomically. Returns the rows that left hot, which is what a
    /// test needs in order to assert they turned up in cold exactly once.
    pub fn seal_through(&self, block: u64) -> Vec<HotRow> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if g.sealed_through.is_some_and(|s| block <= s) {
            return Vec::new();
        }
        g.sealed_through = Some(block);
        let watermark = block;
        let (sealed, kept): (Vec<_>, Vec<_>) =
            std::mem::take(&mut g.rows).into_iter().partition(|r| r.block <= watermark);
        g.rows = kept;
        sealed
    }

    /// How many rows are still unsealed. For tests that want to assert the tip drained.
    pub fn snapshot_rows_len(&self) -> usize {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).rows.len()
    }

    pub fn sealed_through(&self) -> Option<u64> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).sealed_through
    }
}

impl HotTip for MemoryTip {
    fn snapshot(&self, _table: &str) -> Result<HotSnapshot> {
        // The lock is the snapshot. redb would hand back an MVCC read transaction instead; what
        // matters either way is that the watermark and the rows are read together.
        Ok(self.inner.lock().unwrap_or_else(|e| e.into_inner()).clone())
    }
}
