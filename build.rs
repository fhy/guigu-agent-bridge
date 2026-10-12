use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use sha2::{Digest, Sha256};

fn main() {
    println!("cargo:rerun-if-changed=Cargo.lock");
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=.git");
    println!("cargo:rerun-if-env-changed=RUSTC");
    let (source, source_dirty) = source_identity(Path::new("."));
    let lock_hash = Sha256::digest(fs::read("Cargo.lock").expect("Cargo.lock"));
    let rustc = env::var("RUSTC").unwrap_or_else(|_| "rustc".to_owned());
    let rustc_output = Command::new(rustc).arg("-vV").output().expect("rustc -vV");
    assert!(rustc_output.status.success(), "rustc -vV failed");
    let rustc_hash = Sha256::digest(rustc_output.stdout);
    let target = env::var("TARGET").expect("TARGET");
    let profile = env::var("PROFILE").expect("PROFILE");
    let mut features = env::vars()
        .filter_map(|(key, _)| key.strip_prefix("CARGO_FEATURE_").map(str::to_owned))
        .collect::<Vec<_>>();
    features.sort();
    let mut build = Sha256::new();
    build.update(b"guigu-t050-build-v1\0");
    build.update(source);
    build.update(lock_hash);
    build.update(rustc_hash);
    build.update(target.as_bytes());
    build.update([0]);
    build.update(profile.as_bytes());
    build.update([0]);
    build.update(features.join(",").as_bytes());
    let build: [u8; 32] = build.finalize().into();
    let out = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR")).join("t050_build.rs");
    fs::write(
        out,
        format!(
            "pub const SOURCE_COMMIT: [u8; 20] = {source:?};\npub const BUILD_IDENTITY: [u8; 32] = {build:?};\npub const SOURCE_DIRTY: bool = {source_dirty};\n"
        ),
    )
    .expect("write T050 build identity");
}

pub(crate) fn source_identity(root: &Path) -> ([u8; 20], bool) {
    // Packages have no checkout identity. Never borrow an enclosing repository or
    // promote .cargo_vcs_info.json (a provenance hint) into artifact authority.
    if !root.join(".git").exists() {
        println!(
            "cargo:warning=source checkout identity unavailable; legacy artifact admission disabled"
        );
        return ([0; 20], true);
    }
    let git_head = Command::new("git")
        .current_dir(root)
        .args(["rev-parse", "--git-path", "HEAD"])
        .output()
        .expect("git path is required for T050 artifact identity");
    assert!(git_head.status.success(), "cannot resolve git HEAD path");
    println!(
        "cargo:rerun-if-changed={}",
        String::from_utf8(git_head.stdout).unwrap().trim()
    );
    let head = Command::new("git")
        .current_dir(root)
        .args(["rev-parse", "--verify", "HEAD"])
        .output()
        .expect("git is required for T050 artifact identity");
    assert!(head.status.success(), "cannot resolve source commit");
    let hex = String::from_utf8(head.stdout).expect("commit is ASCII");
    let hex = hex.trim();
    assert_eq!(hex.len(), 40, "T050 requires a SHA-1 git object id");
    let mut source = [0u8; 20];
    for (index, byte) in source.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).unwrap();
    }
    let dirty = Command::new("git")
        .current_dir(root)
        .args(["status", "--porcelain", "--untracked-files=no"])
        .output()
        .expect("git status is required for T050 artifact identity");
    assert!(dirty.status.success(), "cannot inspect source state");
    (source, !dirty.stdout.is_empty())
}
