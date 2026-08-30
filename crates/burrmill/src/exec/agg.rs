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
use rustc_hash::FxHasher;
use std::hash::Hasher;

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

    /// Fold another partition table of the same partition into this one.
    fn absorb(&mut self, other: &PartTable) -> Result<()> {
        for e in other.table.iter() {
            let key = &other.arena[e.off as usize..e.off as usize + e.len as usize];
            self.add(e.hash, key, e.sum)?;
        }
        Ok(())
    }

    /// Move every entry into `dest`, chosen by radix. Used once, when a table outgrows being one
    /// table.
    fn redistribute(self, dest: &mut [PartTable]) -> Result<()> {
        let Self { arena, table } = self;
        for e in table.iter() {
            let key = &arena[e.off as usize..e.off as usize + e.len as usize];
            dest[partition_of(e.hash)].add(e.hash, key, e.sum)?;
        }
        Ok(())
    }

    /// Consume the table into rows, applying `HAVING SUM(d) <> 0`.
    pub fn into_rows(self, drop_zero: bool) -> Vec<(Box<str>, i128)> {
        let Self { arena, table } = self;
        table
            .into_iter()
            .filter(|e| !drop_zero || e.sum != 0)
            .map(|e| {
                let bytes = &arena[e.off as usize..e.off as usize + e.len as usize];
                // Lossy would silently corrupt an address. The bytes came out of a Utf8 Arrow array,
                // so this cannot fail; if it ever did, a mangled key is worse than a panic.
                let s: Box<str> = std::str::from_utf8(bytes)
                    .expect("keys are copied verbatim out of a Utf8 array")
                    .into();
                (s, e.sum)
            })
            .collect()
    }
}

/// Entries in one table before it is split into [`PARTITIONS`].
///
/// **Partitioning is not free and it is not always worth it.** Measured on a hundred-segment table:
/// partitioning unconditionally took the ten-thousand-group case from 0.90x DuckDB to 1.00x, because
/// sixty-four arenas and sixty-four control-byte arrays for ten thousand entries is sixty-four times
/// the cache footprint for a table that fits comfortably in L2 as one. It is a large win only once a
/// single table stops fitting - which is also when the serial merge it avoids starts hurting. So the
/// table starts as one and promotes itself, the same shape of decision DuckDB makes when it decides
/// to repartition a growing aggregate.
const PROMOTE_AT: usize = 32_768;

/// One worker's whole aggregation: one table, or a table per radix partition once it has grown.
pub struct PartitionedAgg {
    pub parts: Vec<PartTable>,
    promoted: bool,
}

impl Default for PartitionedAgg {
    fn default() -> Self {
        Self { parts: vec![PartTable::default()], promoted: false }
    }
}

impl PartitionedAgg {
    #[inline]
    pub fn add(&mut self, key: &[u8], v: i128) -> Result<()> {
        let h = hash_key(key);
        if self.promoted {
            return self.parts[partition_of(h)].add(h, key, v);
        }
        self.parts[0].add(h, key, v)?;
        if self.parts[0].len() > PROMOTE_AT {
            self.promote()?;
        }
        Ok(())
    }

    fn promote(&mut self) -> Result<()> {
        let single = std::mem::take(&mut self.parts).into_iter().next().unwrap_or_default();
        let mut parts: Vec<PartTable> = (0..PARTITIONS).map(|_| PartTable::default()).collect();
        single.redistribute(&mut parts)?;
        self.parts = parts;
        self.promoted = true;
        Ok(())
    }

    /// Bring an unpromoted aggregation up to [`PARTITIONS`] tables so it can be transposed against
    /// its peers. A worker that never saw enough groups to promote still has to line up with one
    /// that did.
    ///
    /// Only worth calling when *some* worker did promote. Doing it unconditionally cost the
    /// ten-thousand-group case 1.55x DuckDB against 0.90x, because promoting twelve small tables on
    /// one thread is a hundred and twenty thousand re-inserts to buy a parallel merge that a
    /// hundred and twenty thousand entries did not need.
    pub fn ensure_partitioned(&mut self) -> Result<()> {
        if !self.promoted {
            self.promote()?;
        }
        Ok(())
    }

    pub fn is_promoted(&self) -> bool {
        self.promoted
    }

    pub fn len(&self) -> usize {
        self.parts.iter().map(|p| p.len()).sum()
    }

    pub fn bytes(&self) -> usize {
        self.parts.iter().map(|p| p.bytes()).sum()
    }
}

/// Merge one partition's tables from every worker.
///
/// Largest first, and the rest folded into it: merging a large table into a small one would rehash
/// and recopy the bulk of the entries for nothing.
pub fn merge_partition(mut tables: Vec<PartTable>) -> Result<PartTable> {
    if tables.is_empty() {
        return Ok(PartTable::default());
    }
    let biggest = tables
        .iter()
        .enumerate()
        .max_by_key(|(_, t)| t.len())
        .map(|(i, _)| i)
        .unwrap_or(0);
    let mut base = tables.swap_remove(biggest);
    for t in &tables {
        base.absorb(t)?;
    }
    Ok(base)
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn absorbing_preserves_exact_sums() {
        let mut a = PartitionedAgg::default();
        let mut b = PartitionedAgg::default();
        for i in 0..1000u64 {
            a.add(format!("0x{i:040x}").as_bytes(), i as i128).unwrap();
            b.add(format!("0x{i:040x}").as_bytes(), -(i as i128) * 2).unwrap();
        }
        a.ensure_partitioned().unwrap();
        b.ensure_partitioned().unwrap();
        let mut rows = Vec::new();
        for p in 0..PARTITIONS {
            let merged = merge_partition(vec![
                std::mem::take(&mut a.parts[p]),
                std::mem::take(&mut b.parts[p]),
            ])
            .unwrap();
            rows.extend(merged.into_rows(true));
        }
        rows.sort();
        assert_eq!(rows.len(), 999, "party zero nets to zero and HAVING drops it");
        for (k, v) in &rows {
            let i = i128::from_str_radix(k.trim_start_matches("0x"), 16).unwrap();
            assert_eq!(*v, -i, "{k} should be i - 2i");
        }
    }

    #[test]
    fn overflow_in_the_merge_is_refused() {
        let mut a = PartitionedAgg::default();
        let mut b = PartitionedAgg::default();
        a.add(b"0xdead", i128::MAX).unwrap();
        b.add(b"0xdead", 1).unwrap();
        a.ensure_partitioned().unwrap();
        b.ensure_partitioned().unwrap();
        let p = partition_of(hash_key(b"0xdead"));
        let err = merge_partition(vec![
            std::mem::take(&mut a.parts[p]),
            std::mem::take(&mut b.parts[p]),
        ])
        .unwrap_err();
        assert!(matches!(err, crate::BurrmillError::Overflow(_)), "got {err:?}");
    }
}
