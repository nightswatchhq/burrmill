//! Owned execution: the operators Burrmill does not rent.

pub mod agg;
pub mod checked;
pub mod signed_fold;

pub use checked::{CheckedSumI128, checked_add, checked_neg};
pub use signed_fold::{CancelToken, FoldMetrics, Seam, SignedFoldExec, to_record_batch};
