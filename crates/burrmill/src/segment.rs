//! The positive allowlist of data sources, and the morsel unit.
//!
//! Burrmill registers named providers and **registers no file-I/O SQL functions at all**. There is
//! no `read_parquet(path)`, no `read_csv`, no `COPY TO`, no `getenv`. The attack surface is not
//! denied, it is absent - which is a materially different claim from DuckDB's denylist, where
//! CVE-2024-41672 let `sniff_csv` read the filesystem *with `enable_external_access=false` set*.
//! A path cannot arrive from user SQL because there is nowhere in the grammar to put one.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use rayon::prelude::*;

use crate::error::{BurrmillError, Result};

/// Target rows per morsel.
///
/// **Measured, not chosen.** The first version of this operator made a morsel "one row group" and
/// was 1.16-1.23x DuckDB on a single-segment table - slower - because `ArrowWriter`'s default row
/// group is 1,048,576 rows, so two million rows is *two* units of work and the fold ran on two
/// threads against DuckDB's twelve. The unit of parallelism has to be a bounded row range, which is
/// what "morsel-driven" meant in the first place; DuckDB's own is around 120k rows.
pub const MORSEL_ROWS: usize = 65_536;

/// One unit of parallel work: a bounded row range inside one row group of one sealed segment.
///
/// **Not a file.** A nest's segment sizes are bimodal - #889 measured 80% of `horizon-nest`'s
/// segments under 20 KB against a 6.3 KB median, because the backfill path batches at 20,000 rows
/// while the tip path seals whatever finalised. Splitting work by file would give a one-segment
/// table no parallelism at all and would flatter whichever layout happened to suit the operator,
/// which is the confound the segment sweep exists to remove.
///
/// **And not a whole row group either**, for the same reason one size up: a table sealed as a few
/// large groups would be just as starved. The morsel carries its file's parsed footer so the scan
/// does not re-read it - sealed segments are content-addressed and immutable, so a parsed footer
/// stays valid for as long as the process does.
#[derive(Debug, Clone)]
pub struct Morsel {
    pub path: PathBuf,
    pub meta: ArrowReaderMetadata,
    pub row_group: usize,
    /// Rows to skip within the selected row group.
    pub offset: usize,
    /// Rows to read.
    pub len: usize,
}

/// A named, content-addressed set of sealed cold segments.
///
/// Constructed by `Burrmill::open` from a nest directory; never from a string inside a query.
#[derive(Debug, Clone)]
pub struct SealedSegments {
    /// Where this set came from, when it came from a directory, so it can be re-listed.
    ///
    /// **The seam needs this.** COR-1 requires the cold listing to be taken at or after the hot
    /// snapshot, and a set built once at open time is a listing from whenever that was. Carrying
    /// the source lets a query refresh it in the right order instead of trusting the caller to
    /// have opened the handle at the right moment - which is not a thing a caller can even know.
    source: Option<(PathBuf, Option<String>)>,
    name: String,
    files: Vec<PathBuf>,
}

impl SealedSegments {
    /// Every `*.parquet` directly under `dir`, in sorted order.
    ///
    /// Sorted for reproducibility of *timings*, not of answers: the folds are exact integer
    /// arithmetic and therefore associative, so partials merge to the same result in any order.
    /// A float sum could not claim that.
    pub fn discover(name: impl Into<String>, dir: &Path) -> Result<Self> {
        let mut files: Vec<PathBuf> = std::fs::read_dir(dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
            .collect();
        files.sort();
        Ok(Self { name: name.into(), files, source: Some((dir.to_path_buf(), None)) })
    }

    /// A set over an explicit list of files, in the order given after sorting.
    ///
    /// `discover` is the ordinary path; this is for when the caller already knows which segments it
    /// wants. The bench uses it to build a half-sized catalog for the scale check, and the seam will
    /// want it to pin a query to the segments sealed before a boundary.
    pub fn from_files(name: impl Into<String>, files: impl IntoIterator<Item = PathBuf>) -> Self {
        let mut files: Vec<PathBuf> = files.into_iter().collect();
        files.sort();
        // No source: an explicit list is exactly what the caller asked for and refreshing it would
        // be inventing an intent. Such a set is not re-listed for the seam, and `refresh` says so.
        Self { name: name.into(), files, source: None }
    }

    /// A set restricted to the segments whose file name starts with `prefix`.
    ///
    /// A real nest keeps every table in one `segments/` directory, named
    /// `<contract>__<event>-<hash>.parquet`, so selecting a table is a prefix over that naming
    /// convention rather than a directory walk.
    pub fn with_prefix(&self, name: impl Into<String>, prefix: &str) -> Self {
        Self {
            name: name.into(),
            source: self.source.as_ref().map(|(d, _)| (d.clone(), Some(prefix.to_string()))),
            files: self
                .files
                .iter()
                .filter(|p| {
                    p.file_name()
                        .and_then(|f| f.to_str())
                        .is_some_and(|f| f.starts_with(prefix))
                })
                .cloned()
                .collect(),
        }
    }

    /// Re-list this set from the directory it was discovered in.
    ///
    /// Cheap - one `read_dir`, no footers - and the seam calls it **after** pinning the hot
    /// snapshot, because sealing is append-only and a later listing is therefore a superset of what
    /// the watermark promises. A set built from an explicit file list has nothing to refresh from
    /// and comes back unchanged.
    pub fn refresh(&self) -> Result<Self> {
        let Some((dir, prefix)) = &self.source else { return Ok(self.clone()) };
        let all = Self::discover(self.name.clone(), dir)?;
        Ok(match prefix {
            Some(p) => all.with_prefix(self.name.clone(), p),
            None => Self { name: self.name.clone(), files: all.files, source: all.source },
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn files(&self) -> &[PathBuf] {
        &self.files
    }

    /// Expand the segment set into morsels by reading each file's footer.
    ///
    /// This is the one place a query pays for metadata, and it is why sealed segments want to be
    /// sized sensibly at seal time: DuckDB's own guidance is 100k-1M rows per row group, and
    /// sub-5000-row groups degrade reads by 5-10x. Burrmill cannot fix a badly sealed nest at query
    /// time, so it reports the shape rather than hiding it.
    pub fn morsels(&self) -> Result<Vec<Morsel>> {
        // **Parallel, because at ten thousand segments this phase was the query.** Reading ten
        // thousand footers on one thread before any fold work starts is a serial prologue DuckDB
        // does not pay, and it showed up as a flat penalty that grew with segment count.
        let per_file: Vec<Vec<Morsel>> = self
            .files
            .par_iter()
            .map(|path| -> Result<Vec<Morsel>> {
                let file = std::fs::File::open(path)?;
                let meta = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new())?;
                let md = meta.metadata().clone();
                let mut out = Vec::new();
                for row_group in 0..md.num_row_groups() {
                    let rows = md.row_group(row_group).num_rows() as usize;
                    let mut offset = 0usize;
                    while offset < rows {
                        let len = MORSEL_ROWS.min(rows - offset);
                        out.push(Morsel {
                            path: path.clone(),
                            meta: meta.clone(),
                            row_group,
                            offset,
                            len,
                        });
                        offset += len;
                    }
                }
                Ok(out)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(per_file.into_iter().flatten().collect())
    }
}

/// Everything a query is allowed to name.
///
/// Resolution is a lookup in this map and nothing else. A name that is not here is
/// `NotAllowed`, and there is no fallback that consults the filesystem.
#[derive(Debug, Clone, Default)]
pub struct Catalog {
    tables: BTreeMap<String, SealedSegments>,
}

impl Catalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, segments: SealedSegments) {
        self.tables.insert(segments.name().to_string(), segments);
    }

    pub fn resolve(&self, name: &str) -> Result<&SealedSegments> {
        self.tables.get(name).ok_or_else(|| {
            BurrmillError::NotAllowed(format!(
                "no registered table `{name}`; registered: [{}]. Burrmill resolves table names \
                 against a positive allowlist and has no file-reading SQL functions, so a path \
                 cannot be named here",
                self.tables.keys().cloned().collect::<Vec<_>>().join(", ")
            ))
        })
    }

    pub fn table_names(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }
}
