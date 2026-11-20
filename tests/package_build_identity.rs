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
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
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

fn commit_hex(root: &Path) -> String {
    git(root, &["rev-parse", "HEAD"]).trim().to_owned()
}

#[test]
fn clean_checkout_keeps_exact_source_identity() {
    let root = checkout();
    let (source, dirty) = build_script::source_identity(root.path());
    let hex = source
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(hex, commit_hex(root.path()));
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

#[test]
fn branch_checkout_watches_head_ref_and_packed_refs() {
    let root = checkout();
    let paths = build_script::git_watch_paths(root.path());
    assert_eq!(
        paths.len(),
        3,
        "branch checkout must watch HEAD, ref file, and packed-refs: {paths:?}"
    );
    assert!(
        paths[0].ends_with("HEAD"),
        "first watch path must be HEAD: {}",
        paths[0]
    );
}

#[test]
fn worktree_branch_commit_updates_identity() {
    let root = checkout();
    git(root.path(), &["branch", "wt-branch"]);
    let wt = root.path().join("wt");
    git(
        root.path(),
        &["worktree", "add", "-q", wt.to_str().unwrap(), "wt-branch"],
    );

    let paths = build_script::git_watch_paths(&wt);
    assert!(
        paths.len() >= 3,
        "worktree branch must watch HEAD, ref file, and packed-refs: {paths:?}"
    );

    let before = commit_hex(&wt);
    fs::write(wt.join("new-file"), "data").unwrap();
    git(&wt, &["add", "new-file"]);
    git(
        &wt,
        &[
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-qm",
            "worktree commit",
        ],
    );
    let after = commit_hex(&wt);
    assert_ne!(before, after, "worktree commit must advance HEAD");

    let (source, dirty) = build_script::source_identity(&wt);
    let hex = source
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(hex, after, "identity must reflect the new worktree commit");
    assert!(!dirty);
}

#[test]
fn packed_refs_are_watched_on_branch() {
    let root = checkout();
    git(root.path(), &["pack-refs", "--all"]);
    let paths = build_script::git_watch_paths(root.path());
    assert!(
        paths.iter().any(|p| p.ends_with("packed-refs")),
        "packed-refs must be in watch paths: {paths:?}"
    );
}

#[test]
fn detached_head_watches_only_head() {
    let root = checkout();
    let sha = commit_hex(root.path());
    git(root.path(), &["checkout", "-q", "--detach", &sha]);
    let paths = build_script::git_watch_paths(root.path());
    assert_eq!(
        paths.len(),
        1,
        "detached HEAD must watch only the HEAD file: {paths:?}"
    );
    assert!(paths[0].ends_with("HEAD"));

    let (source, dirty) = build_script::source_identity(root.path());
    let hex = source
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(hex, sha);
    assert!(!dirty);
}

#[test]
fn no_change_rebuild_is_stable() {
    let root = checkout();
    let (source1, dirty1) = build_script::source_identity(root.path());
    let (source2, dirty2) = build_script::source_identity(root.path());
    assert_eq!(source1, source2, "identity must be deterministic");
    assert_eq!(dirty1, dirty2);
}
