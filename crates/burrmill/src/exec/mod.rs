//! Owned execution: the operators Burrmill does not rent.

pub mod agg;
pub mod checked;
pub mod signed_fold;

pub use checked::{checked_add, checked_neg, CheckedSumI128};
pub use signed_fold::{to_record_batch, CancelToken, FoldMetrics, Seam, SignedFoldExec};
