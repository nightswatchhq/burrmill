// The engine's identity for a host's reuse keys: any change to the source or the manifest (which
// pins DataFusion) changes it. FNV-1a, because a persisted key cannot ride on std's hasher.
use std::path::{Path, PathBuf};

fn files(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .collect();
    entries.sort();
    for p in entries {
        if p.is_dir() {
            files(&p, out);
        } else {
            out.push(p);
        }
    }
}

fn main() {
    let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let mut all = vec![root.join("Cargo.toml")];
    files(&root.join("src"), &mut all);
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for p in &all {
        let rel = p
            .strip_prefix(&root)
            .unwrap()
            .to_string_lossy()
            .into_owned();
        for b in rel.bytes().chain(std::fs::read(p).unwrap()) {
            h = (h ^ b as u64).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    println!("cargo:rustc-env=BURRMILL_SOURCE_HASH={h:016x}");
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=Cargo.toml");
}
