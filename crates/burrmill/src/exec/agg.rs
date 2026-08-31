//! The owned aggregation table.
//!
//! This file is the difference between "a fold written in Rust" and "an aggregation we own", and it
//! exists because the measurements said so. Three versions preceded it, each faster than the last
//! and each still losing to DuckDB above about ten thousand groups:
//!
//! | version | 200k groups | 1M groups |
//! |---|---|---|
//! | one `FxHashMap<Box<str>, _>` per work unit, serial merge | 3.11x DuckDB | 2.88x |
//! | one per thread, parallel tree merge | 1.96x | 2.29x |
//! | this: arena keys, radix partitions, partition-wise merge | see `docs/bench` | |
//!
//! Two things were wrong with the map-per-thread version, and they are the two things DuckDB does
//! differently:
//!
//! 1. **A heap allocation per distinct key.** A `Box<str>` for a forty-two byte address is a malloc,
//!    and at a million distinct parties across twelve worker tables that is on the order of twelve
//!    million of them to answer a query over two million rows. Here the key bytes are appended to a
//!    per-partition arena and the table holds an offset and a length, so a new group costs a memcpy
//!    into a `Vec<u8>` that is already warm.
//! 2. **Every table spanning the whole key space.** Twelve tables of a million entries each is both
//!    cache-hostile and, under a 256 MB budget, simply too much. Radix partitioning on the high bits
//!    of the hash means a partition's table is a sixty-fourth of the key space, the merge is
//!    parallel across partitions rather than serial across tables, and each partition is freed as
//!    soon as its rows are produced.

use hashbrown::HashTable;
use rayon::prelude::*;
use rustc_hash::FxHasher;
use std::hash::Hasher;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use crate::error::Result;
use crate::exec::checked::checked_add;

/// Radix partitions. Sixty-four is a compromise: enough that a partition's table stays small and
/// the merge has more units than the machine has cores, few enough that an arena per partition per
/// thread is not itself the memory problem.
pub const PARTITIONS: usize = 64;

#[derive(Clone, Copy)]
struct Entry {
    /// Kept rather than recomputed. Eight bytes against re-hashing forty-two on every resize, and
    /// the entry is padded to thirty-two either way.
    hash: u64,
    off: u32,
    len: u32,
    sum: i128,
}

/// Hash, with the high bits deliberately mixed.
///
/// Both consumers here read the *top* bits - hashbrown for its control byte, and the radix split for
/// the partition index. A hash whose entropy sits in the low bits would put every key in one
/// partition and turn the parallel merge back into a serial one, silently and only at scale.
#[inline]
pub fn hash_key(k: &[u8]) -> u64 {
    let mut h = FxHasher::default();
    h.write(k);
    let x = h.finish();
    // splitmix64's finaliser. Cheap, and it makes the top bits as good as the bottom.
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[inline]
pub fn partition_of(hash: u64) -> usize {
    (hash >> 58) as usize & (PARTITIONS - 1)
}

/// One partition's table: a contiguous key arena plus a SwissTable of offsets into it.
#[derive(Default)]
pub struct PartTable {
    arena: Vec<u8>,
    table: HashTable<Entry>,
}

impl std::fmt::Debug for PartTable {
    /// Deliberately shows shape and not contents: a debug print of a million-entry aggregate helps
    /// nobody, and the arena is bytes rather than anything a person reads.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartTable")
            .field("groups", &self.table.len())
            .field("arena_bytes", &self.arena.len())
            .finish()
    }
}

impl PartTable {
    pub fn len(&self) -> usize {
        self.table.len()
    }

    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Approximate live bytes. Used for the memory budget, so it counts what actually grows: the
    /// arena, and the table's entries at SwissTable's load factor.
    pub fn bytes(&self) -> usize {
        self.arena.capacity() + self.table.capacity() * (std::mem::size_of::<Entry>() + 1)
    }

    #[inline]
    pub fn add(&mut self, hash: u64, key: &[u8], v: i128) -> Result<()> {
        // Split the borrow explicitly: the equality closure reads the arena while the table is held
        // mutably, which only type-checks because these are two distinct fields.
        let Self { arena, table } = self;
        let eq = |e: &Entry| {
            e.hash == hash
                && e.len as usize == key.len()
                && &arena[e.off as usize..e.off as usize + key.len()] == key
        };
        if let Some(e) = table.find_mut(hash, eq) {
            // Checked, because a balance that wraps is a wrong answer that looks like a balance.
            e.sum = checked_add(e.sum, v, "aggregate")?;
            return Ok(());
        }
        let off = arena.len() as u32;
        arena.extend_from_slice(key);
        table.insert_unique(hash, Entry { hash, off, len: key.len() as u32, sum: v }, |e| e.hash);
        Ok(())
    }



    /// Consume the table into an index over its own arena, applying `HAVING SUM(d) <> 0`.
    ///
    /// **The arena is moved out, not copied.** The previous version built a `Box<str>` per group,
    /// which put a million individual forty-two byte mallocs between the aggregate and the answer -
    /// the exact per-key allocation this module's header says it removed, reintroduced at the output
    /// and costing more there than it ever did in the table. The hash table is dropped here, so a
    /// partition's buckets are freed the moment its rows exist.
    fn into_index(self, drop_zero: bool) -> (Vec<u8>, Vec<(u32, u32, i128)>) {
        let Self { arena, table } = self;
        let idx = table
            .into_iter()
            .filter(|e| !drop_zero || e.sum != 0)
            .map(|e| (e.off, e.len, e.sum))
            .collect();
        (arena, idx)
    }
}


/// One row of the answer: where its key sits in the arena, and the exact sum.
///
/// Thirty-two bytes, which is what an `i128`'s alignment costs whatever else is in the struct, and
/// the same width as the `(Box<str>, i128)` this replaced. The saving is not the width, it is the
/// million individual mallocs that are no longer behind it.
#[derive(Clone, Copy)]
struct Row {
    off: u32,
    len: u32,
    sum: i128,
}

/// The answer: every key in one contiguous arena, plus an index.
///
/// A result set that copies each key into its own allocation carries two copies of the answer at the
/// moment it can least afford to - peak memory is exactly when the aggregate is complete and the
/// rows are being built - and then makes the canonical sort chase a pointer per comparison.
///
/// **One arena rather than sixty-four.** The first version of this kept the partition arenas where
/// they were and gave each row a partition index, which avoids a copy and looked obviously better.
/// It measured 79 ms of output phase against 26 ms, because every comparison in the sort then paid
/// two bounds-checked indirections instead of one. Concatenating costs a single sequential 42 MB
/// memcpy and buys the sort contiguous keys. The copy is cheaper than the indirection, which is not
/// what the first guess said.
pub struct Rows {
    arena: Vec<u8>,
    idx: Vec<Row>,
}

impl Rows {
    pub fn len(&self) -> usize {
        self.idx.len()
    }

    pub fn is_empty(&self) -> bool {
        self.idx.is_empty()
    }

    #[inline]
    fn bytes_of<'a>(arena: &'a [u8], r: &Row) -> &'a [u8] {
        &arena[r.off as usize..r.off as usize + r.len as usize]
    }

    #[inline]
    pub fn key(&self, i: usize) -> &str {
        // Lossy would silently corrupt an address. The bytes came out of a Utf8 Arrow array, so this
        // cannot fail; if it ever did, a mangled key is worse than a panic.
        std::str::from_utf8(Self::bytes_of(&self.arena, &self.idx[i]))
            .expect("keys are copied verbatim out of a Utf8 array")
    }

    #[inline]
    pub fn sum(&self, i: usize) -> i128 {
        self.idx[i].sum
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, i128)> + '_ {
        (0..self.len()).map(move |i| (self.key(i), self.idx[i].sum))
    }

    /// Live bytes of the answer itself. Feeds `max_bytes`, which until now was a field in
    /// `Limits` that nothing read.
    pub fn bytes(&self) -> usize {
        self.arena.capacity() + self.idx.capacity() * std::mem::size_of::<Row>()
    }

    /// **Canonical ordering, byte-wise ascending on the key** (RFC-0044 §3.3).
    ///
    /// `par_sort_unstable_by` rather than the stable sort on purpose: it is an in-place parallel
    /// quicksort and allocates no scratch, and the keys are distinct so stability decides nothing.
    pub fn sort_canonical(&mut self) {
        let arena = &self.arena;
        self.idx
            .par_sort_unstable_by(|a, b| Self::bytes_of(arena, a).cmp(Self::bytes_of(arena, b)));
    }
}

/// A row waiting to be applied, keyed into its [`Scatter`]'s own arena.
#[derive(Clone, Copy)]
struct Pending {
    hash: u64,
    off: u32,
    len: u32,
    val: i128,
}

/// Rows a worker buffers before it takes any lock.
///
/// The whole cost of a shared aggregate is the lock, so it has to be amortised over enough rows that
/// it disappears, and the two regimes this has to survive are far apart. A synthetic segment is
/// twenty thousand rows; a real nest segment averages **a hundred and sixteen**, so flushing per
/// batch would take sixty-four locks for every hundred-odd rows. Buffering across morsels instead
/// makes the flush rate a function of rows rather than of how the writer happened to cut the files.
///
/// **Four thousand and ninety-six, measured rather than picked.** Every worker holds one of these,
/// so the constant is multiplied by the core count and shows up directly in peak RSS. Swept at a
/// million groups on 32 threads:
///
/// | `FLUSH_ROWS` | peak RSS, 32 threads | peak RSS, 8 threads | latency |
/// |---|---:|---:|---:|
/// | 1 024 | 335 MB | 204 MB | 179 ms |
/// | **4 096** | **346 MB** | **204 MB** | **158 ms** |
/// | 16 384 | 380 MB | 228 MB | 161 ms |
/// | 65 536 | 467 MB | 270 MB | 177 ms |
///
/// 16,384 was the first guess and it was worse on both axes. Below 4,096 the lock stops being
/// amortised and latency goes back up for a saving that is within run-to-run spread.
const FLUSH_ROWS: usize = 4_096;

/// One worker's outbound buffer: rows sorted into partitions, not yet aggregated.
///
/// It owns its key bytes rather than borrowing them from the Arrow batch, which costs one memcpy per
/// row - about eight milliseconds on two million rows - and buys the freedom to buffer across morsel
/// boundaries. Borrowing would tie the flush to the lifetime of a batch, and on a real nest a batch
/// is a hundred rows.
pub struct Scatter {
    arena: Vec<u8>,
    parts: Vec<Vec<Pending>>,
    pending: usize,
}

impl Default for Scatter {
    fn default() -> Self {
        Self { arena: Vec::new(), parts: (0..PARTITIONS).map(|_| Vec::new()).collect(), pending: 0 }
    }
}

impl Scatter {
    #[inline]
    pub fn push(&mut self, key: &[u8], val: i128, into: &SharedAgg) -> Result<()> {
        let hash = hash_key(key);
        let off = self.arena.len() as u32;
        self.arena.extend_from_slice(key);
        self.parts[partition_of(hash)].push(Pending { hash, off, len: key.len() as u32, val });
        self.pending += 1;
        if self.pending >= FLUSH_ROWS {
            self.flush(into)?;
        }
        Ok(())
    }

    /// Drain every partition into the shared aggregate, one lock at a time.
    ///
    /// Partition `p`'s lock is taken once and held for its whole run of pending rows, so contention
    /// is a function of flushes rather than of rows. On an error the buffer is left dirty on
    /// purpose: the query is being abandoned, and tidying state nobody will read is work for its own
    /// sake.
    pub fn flush(&mut self, into: &SharedAgg) -> Result<()> {
        let Self { arena, parts, pending } = self;
        for (p, rows) in parts.iter_mut().enumerate() {
            if rows.is_empty() {
                continue;
            }
            // A poisoned lock means another worker panicked mid-flush and this query is already
            // dying. Take the data anyway rather than panicking a second time and burying the first.
            let mut table = into.parts[p].lock().unwrap_or_else(|e| e.into_inner());
            for r in rows.iter() {
                let key = &arena[r.off as usize..r.off as usize + r.len as usize];
                table.add(r.hash, key, r.val)?;
            }
            into.bytes[p].store(table.bytes(), Ordering::Relaxed);
            rows.clear();
        }
        arena.clear();
        *pending = 0;
        Ok(())
    }
}

/// The query's whole aggregation: one table per radix partition, shared by every worker.
///
/// **This is the fix for slice 1's memory gate and it is a change of shape, not a tuning knob.** The
/// previous version gave each worker a table spanning the whole key space, so a million distinct
/// parties meant roughly twelve million live entries to produce a 57 MB answer, and peak RSS came in
/// at 1008 MB against a 256 MB budget. That is also precisely the DataFusion behaviour RFC-0044 §4.1
/// criticises by name (#6937, memory growing with core count), so reproducing it was embarrassing as
/// well as over budget.
///
/// Here a key exists in exactly one table no matter how many threads touched it, so the aggregate's
/// size is a property of the data and not of the machine. What replaces the duplication is a lock
/// per partition, amortised by [`Scatter`] over [`FLUSH_ROWS`] rows, and the merge phase disappears
/// entirely: there is nothing left to merge, because the workers were writing into the answer all
/// along.
pub struct SharedAgg {
    parts: Vec<Mutex<PartTable>>,
    /// Published on flush and read without locking. The budget check runs at every batch boundary
    /// and must not be the thing that serialises the workers it is measuring.
    bytes: Vec<AtomicUsize>,
}

impl Default for SharedAgg {
    fn default() -> Self {
        Self {
            parts: (0..PARTITIONS).map(|_| Mutex::new(PartTable::default())).collect(),
            bytes: (0..PARTITIONS).map(|_| AtomicUsize::new(0)).collect(),
        }
    }
}

impl SharedAgg {
    /// Approximate live bytes across every partition. Lock-free, and slightly stale by design - a
    /// budget checked at batch boundaries cannot be tighter than one batch anyway.
    pub fn bytes(&self) -> usize {
        self.bytes.iter().map(|b| b.load(Ordering::Relaxed)).sum()
    }

    pub fn len(&self) -> usize {
        self.parts.iter().map(|m| m.lock().map(|t| t.len()).unwrap_or(0)).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Consume the aggregate into rows, one partition per task.
    ///
    /// Parallel because a partition's rows never meet another's: the radix split that bounds the
    /// memory also hands the output phase its parallelism for nothing. Each partition's hash table
    /// is freed as its index is built, so the buckets and the rows are never both fully live.
    pub fn into_rows(self, drop_zero: bool) -> Rows {
        let tables: Vec<PartTable> = self
            .parts
            .into_iter()
            .map(|m| m.into_inner().unwrap_or_else(|e| e.into_inner()))
            .collect();
        let per_partition: Vec<(Vec<u8>, Vec<(u32, u32, i128)>)> =
            tables.into_par_iter().map(|t| t.into_index(drop_zero)).collect();

        let total_rows: usize = per_partition.iter().map(|(_, idx)| idx.len()).sum();
        let total_keys: usize = per_partition.iter().map(|(a, _)| a.len()).sum();

        // Both allocated exactly once, at the exact size. Each partition's arena is dropped as soon
        // as it has been copied across, so the two copies of the key bytes only ever overlap by the
        // partition being moved rather than by the whole answer.
        let mut arena = Vec::with_capacity(total_keys);
        let mut idx = Vec::with_capacity(total_rows);
        for (part_arena, rows) in per_partition {
            let base = arena.len() as u32;
            arena.extend_from_slice(&part_arena);
            drop(part_arena);
            for (off, len, sum) in rows {
                idx.push(Row { off: base + off, len, sum });
            }
        }
        Rows { arena, idx }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Materialise an answer for comparison. Tests want owned, sorted, comparable rows; the
    /// production path deliberately does not, which is the whole point of [`Rows`].
    fn collect(rows: &Rows) -> Vec<(String, i128)> {
        let mut v: Vec<(String, i128)> = rows.iter().map(|(k, s)| (k.to_string(), s)).collect();
        v.sort();
        v
    }

    /// The partition split must actually spread. A hash whose entropy is in the low bits would put
    /// every key in partition zero, and nothing downstream would notice - the answer would still be
    /// right, just serial, and only at scale.
    #[test]
    fn addresses_spread_across_partitions() {
        let mut counts = [0usize; PARTITIONS];
        for i in 0..100_000u64 {
            let k = format!("0x{i:040x}");
            counts[partition_of(hash_key(k.as_bytes()))] += 1;
        }
        let expected = 100_000 / PARTITIONS;
        let worst = counts.iter().map(|c| c.abs_diff(expected)).max().unwrap();
        assert!(
            worst < expected / 2,
            "partitions are lopsided: expected ~{expected} each, worst deviation {worst}, {counts:?}"
        );
    }

    #[test]
    fn two_scatters_into_one_aggregate_sum_exactly() {
        let shared = SharedAgg::default();
        let mut a = Scatter::default();
        let mut b = Scatter::default();
        for i in 0..1000u64 {
            a.push(format!("0x{i:040x}").as_bytes(), i as i128, &shared).unwrap();
            b.push(format!("0x{i:040x}").as_bytes(), -(i as i128) * 2, &shared).unwrap();
        }
        a.flush(&shared).unwrap();
        b.flush(&shared).unwrap();
        let rows = collect(&shared.into_rows(true));
        assert_eq!(rows.len(), 999, "party zero nets to zero and HAVING drops it");
        for (k, v) in &rows {
            let i = i128::from_str_radix(k.trim_start_matches("0x"), 16).unwrap();
            assert_eq!(*v, -i, "{k} should be i - 2i");
        }
    }

    #[test]
    fn overflow_across_two_workers_is_refused() {
        let shared = SharedAgg::default();
        let mut a = Scatter::default();
        let mut b = Scatter::default();
        a.push(b"0xdead", i128::MAX, &shared).unwrap();
        a.flush(&shared).unwrap();
        b.push(b"0xdead", 1, &shared).unwrap();
        let err = b.flush(&shared).unwrap_err();
        assert!(matches!(err, crate::BurrmillError::Overflow(_)), "got {err:?}");
    }

    /// **The property the shared aggregate exists to preserve.** Many threads writing into one
    /// partitioned table must produce exactly what one thread writing serially produces - every key,
    /// every sum, no lost update. A per-partition lock that was merely *mostly* right would show up
    /// here as a handful of short balances and nowhere else, and a balance that is quietly short is
    /// the worst failure this project has.
    #[test]
    fn concurrent_scatter_equals_serial_scatter() {
        const KEYS: u64 = 20_000;
        const WORKERS: usize = 8;
        const ROUNDS: u64 = 5;

        let serial = SharedAgg::default();
        let mut one = Scatter::default();
        for _ in 0..(WORKERS as u64 * ROUNDS) {
            for i in 0..KEYS {
                one.push(format!("0x{i:040x}").as_bytes(), i as i128, &serial).unwrap();
            }
        }
        one.flush(&serial).unwrap();
        let expected = collect(&serial.into_rows(false));

        let shared = SharedAgg::default();
        std::thread::scope(|s| {
            for _ in 0..WORKERS {
                s.spawn(|| {
                    let mut sc = Scatter::default();
                    for _ in 0..ROUNDS {
                        for i in 0..KEYS {
                            sc.push(format!("0x{i:040x}").as_bytes(), i as i128, &shared).unwrap();
                        }
                    }
                    sc.flush(&shared).unwrap();
                });
            }
        });
        let got = collect(&shared.into_rows(false));

        assert_eq!(got.len(), KEYS as usize, "every key must survive the exchange");
        assert_eq!(got, expected, "concurrent and serial aggregation must agree exactly");
    }

    /// A key must land in exactly one partition table, whoever wrote it. This is the memory claim
    /// stated as an assertion rather than as a benchmark: if a key could be duplicated across
    /// tables, the whole point of the change is gone and only a profiler would tell you.
    #[test]
    fn a_key_lives_in_exactly_one_partition() {
        let shared = SharedAgg::default();
        let sref = &shared;
        std::thread::scope(|s| {
            for w in 0..6 {
                s.spawn(move || {
                    let mut sc = Scatter::default();
                    for i in 0..5_000u64 {
                        sc.push(format!("0x{i:040x}").as_bytes(), w as i128, sref).unwrap();
                    }
                    sc.flush(sref).unwrap();
                });
            }
        });
        assert_eq!(shared.len(), 5_000, "six workers, one table per key, no duplication");
    }
}
