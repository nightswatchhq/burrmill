//! The seal-layout canary (roadmap 1.4).
//!
//! **Burrmill has no path dependency on nuthatch, by design.** It reads Parquet files out of a
//! directory and never links the thing that wrote them, which is what makes the "no C++, no
//! DataFusion, pure Rust" dependency claim true and what lets the two evolve separately.
//!
//! The cost of that independence is that a layout change cannot break the build. It breaks the
//! *reading*, at runtime, quietly. If nuthatch renames a column the query fails loudly and that is
//! fine; if nuthatch changes the separator between a table name and its content hash, every table
//! resolves to zero segments and the honest-looking answer is an empty one.
//!
//! So this file is the layout contract, written down as assertions rather than as a paragraph in a
//! README. It is deliberately explicit about things that look too obvious to test, because the whole
//! failure mode is that they stopped being true somewhere else.
//!
//! Set `BURRMILL_NEST=/path/to/segments` to check the contract against a real sealed nest as well.
//! Without it the grammar tests still run against synthetic names, which is where the sharpest
//! hazard lives anyway.

use std::path::Path;

use burrmill::{Burrmill, BurrmillError, SealedSegments};

fn touch(dir: &Path, name: &str) {
    std::fs::write(dir.join(name), b"not a real parquet, only its name matters here").unwrap();
}

/// The naming convention Burrmill assumes: `<contract>__<event>-<content hash>.parquet`, with the
/// table selected by everything before the **last** hyphen.
const HASH: &str = "bb04b072d5ecb39489f65ddbb5dac50d78c2ad8a407ebb721fdc6ae5c9f916bc0";

/// **One table's name is a prefix of another's, and this is not hypothetical.** A real nest carries
/// both `staking__stake_delegated` and `staking__stake_delegated_withdrawn`. Selecting a table by
/// bare `starts_with` would fold the second into the first and produce a wrong balance that looks
/// entirely reasonable - the delegations would simply appear larger than they are.
///
/// The separator is what saves it, which means the separator is load-bearing and belongs in a test
/// rather than in a comment.
#[test]
fn a_table_whose_name_prefixes_another_is_not_absorbed_by_it() {
    let dir = tempfile::tempdir().unwrap();
    touch(dir.path(), &format!("staking__stake_delegated-{HASH}.parquet"));
    touch(dir.path(), &format!("staking__stake_delegated_withdrawn-{HASH}.parquet"));
    touch(dir.path(), &format!("staking__stake_delegated_withdrawn-{HASH}1.parquet"));

    let all = SealedSegments::discover("_all", dir.path()).unwrap();
    assert_eq!(all.files().len(), 3);

    let delegated = all.with_prefix("t", "staking__stake_delegated-");
    assert_eq!(
        delegated.files().len(),
        1,
        "`staking__stake_delegated` must not swallow `staking__stake_delegated_withdrawn`; got {:?}",
        delegated.files()
    );

    let withdrawn = all.with_prefix("t", "staking__stake_delegated_withdrawn-");
    assert_eq!(withdrawn.files().len(), 2);
}

/// A table name that matches nothing is a **refusal**, never an empty answer.
///
/// Found by pointing the harness at a table that does not exist: DuckDB said "No files found that
/// match the pattern" and Burrmill planned `files=0 morsels=0` and would have returned an empty
/// result. A nest keeps every table in one directory, so a mistyped name is always one prefix away.
#[test]
fn a_table_that_matches_no_segment_is_refused_not_answered_emptily() {
    let dir = tempfile::tempdir().unwrap();
    touch(dir.path(), &format!("escrow__deposit-{HASH}.parquet"));

    let err = Burrmill::open_nest_table("t", dir.path(), "escrow__depsoit-").unwrap_err();
    match &err {
        BurrmillError::NoSegments(m) => assert!(
            m.contains("escrow__deposit"),
            "the refusal must name the tables that *are* present, since the mistake is nearly \
             always a near-miss; got {m}"
        ),
        other => panic!("expected NoSegments, got {other:?}"),
    }
}

/// Only `.parquet` is a segment. A nest directory picks up companions - checkpoint files, partials,
/// whatever a future nuthatch writes beside the data - and reading one as a segment would be a
/// decode error at best.
#[test]
fn only_parquet_files_are_segments() {
    let dir = tempfile::tempdir().unwrap();
    touch(dir.path(), &format!("escrow__deposit-{HASH}.parquet"));
    touch(dir.path(), &format!("escrow__deposit-{HASH}.parquet.tmp"));
    touch(dir.path(), "escrow__deposit-manifest.json");
    touch(dir.path(), "SEAL_LOG");

    let all = SealedSegments::discover("_all", dir.path()).unwrap();
    assert_eq!(all.files().len(), 1, "got {:?}", all.files());
}

/// The contract against a real sealed nest, when one is available.
///
/// `BURRMILL_NEST=/path/to/nest/segments cargo test --test seal_layout -- --nocapture`
#[test]
fn a_real_nest_still_matches_the_layout_this_crate_assumes() {
    let Ok(dir) = std::env::var("BURRMILL_NEST") else {
        eprintln!("skipped: set BURRMILL_NEST to a real nest's segments directory to run this");
        return;
    };
    let dir = Path::new(&dir);
    let all = SealedSegments::discover("_all", dir).unwrap();
    assert!(!all.files().is_empty(), "{} holds no .parquet segments", dir.display());

    let mut tables = std::collections::BTreeSet::new();
    for f in all.files() {
        let name = f.file_name().and_then(|n| n.to_str()).expect("segment names are utf8");
        let stem = name.strip_suffix(".parquet").expect("discover filtered on the extension");
        let (table, hash) = stem.rsplit_once('-').unwrap_or_else(|| {
            panic!("segment `{name}` has no `-` separating table from content hash; the layout \
                    Burrmill assumes has changed and every table would now resolve to zero segments")
        });
        assert!(
            !hash.is_empty() && hash.chars().all(|c| c.is_ascii_hexdigit()),
            "segment `{name}`: the part after the last `-` is not a hex content hash"
        );
        assert!(
            !table.is_empty() && !table.contains(std::path::MAIN_SEPARATOR),
            "segment `{name}`: the part before the last `-` is not a usable table name"
        );
        tables.insert(table.to_string());
    }
    // **Not every sealed table is `<contract>__<event>`.** This assertion used to require the `__`
    // and the canary failed the first time it met a real nest: `grt_total_supply` is a *call* table,
    // sealed from an `eth_call` rather than from a log, with `calldata` / `result` / `reverted`
    // columns and no event name to speak of. The grammar was this crate's assumption and not
    // nuthatch's, which is precisely what a canary is for - it is only worth having if it is
    // occasionally allowed to win.
    //
    // The mix is reported rather than asserted, because a nest gaining a third shape is news and not
    // necessarily a fault. Burrmill has only ever been run against the event tables; the signed fold
    // has no meaning on a call table, but the catalog will open one quite happily.
    let (events, calls): (Vec<_>, Vec<_>) = tables.iter().partition(|t| t.contains("__"));
    eprintln!(
        "{} segments, {} tables in {}: {} event-shaped, {} not ({:?})",
        all.files().len(),
        tables.len(),
        dir.display(),
        events.len(),
        calls.len(),
        calls
    );

    // Every table must open, which exercises the prefix selection against the real distribution of
    // names - including whichever pairs happen to prefix one another today.
    for table in &tables {
        Burrmill::open_nest_table("t", dir, &format!("{table}-"))
            .unwrap_or_else(|e| panic!("table `{table}` is present but will not open: {e}"));
    }
}
