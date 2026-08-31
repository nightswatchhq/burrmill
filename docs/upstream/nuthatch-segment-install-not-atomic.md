# nuthatch: sealed segments are installed non-atomically

**Status: drafted, not sent.** Found while building Burrmill's hot∪cold seam (RFC-0044 §3.4).

## Summary

`seal.rs:176` installs a finished segment with

```rust
std::fs::write(seg_dir.join(&file), &bytes).context("failed to write segment")?;
```

`fs::write` creates the file and then writes it. A concurrent reader listing `segments/` can observe
the entry after creation and before the bytes land, and gets a zero-length or truncated Parquet file.

The manifest, ninety lines further down the same file, is installed correctly — temp file, fsync,
rename, fsync the directory — with a comment (COR-9) explaining precisely why. The segments deserve
the same treatment.

## Reproduction

Burrmill's `tests/cor1_seam.rs` runs a reader folding a nest while a sealer advances the boundary.
Written the way `seal.rs` writes, it fails within a handful of runs:

    the seam refused a well-formed nest: substrate error: EOF: Parquet file too small. Size is 0 but need 8

Switching the test's writer to temp-then-rename makes it pass, and it is the only change needed.

## Why a reader cannot work around it

Burrmill deliberately does not read the manifest: it resolves a table by globbing the segments
directory, which is what keeps it free of any path dependency on nuthatch's internals. Given a
segment it cannot parse, it has exactly two options and both are wrong:

- **Skip it.** If the file was mid-write, correct. If it was corrupt, a range of rows silently
  vanishes and the query returns a short balance that looks entirely ordinary.
- **Refuse.** Correct for corruption, but then every query issued during a seal fails.

It cannot tell the two apart, so it refuses and says why. The fix belongs at the writer, where the
distinction is known for free.

## Suggested fix

Mirror the manifest path: write `<file>.tmp` in the same directory, fsync it, `rename` over the
target, fsync the directory. `rename` within one filesystem is atomic, so a reader sees either no
segment or a complete one. Content-addressed names mean a leftover `.tmp` from a crash is inert and
can be swept.

Worth also confirming that `.tmp` files cannot match a table prefix glob — Burrmill filters on the
`.parquet` extension, so a `.parquet.tmp` suffix would be ignored, but a bare `.tmp` sibling is the
safer convention.
