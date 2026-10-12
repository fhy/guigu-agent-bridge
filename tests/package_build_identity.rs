//! Exercise the actual build-script identity decision without running legacy tools.
#[allow(dead_code)]
#[path = "../build.rs"]
mod build_script;

use std::{fs, path::Path, process::Command};

fn git(root: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(root)
        .args(args)
        .output()
        .unwrap();
    assert!(output.status.success(), "git fixture failed");
    String::from_utf8(output.stdout).unwrap()
}

fn checkout() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    git(root.path(), &["init", "-q"]);
    fs::write(root.path().join("tracked"), "original").unwrap();
    git(root.path(), &["add", "tracked"]);
    git(
        root.path(),
        &[
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
    );
    root
}

#[test]
fn clean_checkout_keeps_exact_source_identity() {
    let root = checkout();
    let (source, dirty) = build_script::source_identity(root.path());
    let hex = source
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(hex, git(root.path(), &["rev-parse", "HEAD"]).trim());
    assert!(!dirty);
}

#[test]
fn tracked_changes_remain_dirty() {
    let root = checkout();
    fs::write(root.path().join("tracked"), "changed").unwrap();
    assert!(build_script::source_identity(root.path()).1);
}

#[test]
fn package_without_git_has_no_artifact_authority_even_with_vcs_hint() {
    let root = tempfile::tempdir().unwrap();
    fs::write(
        root.path().join(".cargo_vcs_info.json"),
        r#"{"git":{"sha1":"1111111111111111111111111111111111111111","dirty":false}}"#,
    )
    .unwrap();
    assert_eq!(build_script::source_identity(root.path()), ([0; 20], true));
}

#[test]
fn extracted_package_does_not_borrow_enclosing_git_identity() {
    let root = checkout();
    let package = root.path().join("extracted");
    fs::create_dir(&package).unwrap();
    assert_eq!(build_script::source_identity(&package), ([0; 20], true));
}
