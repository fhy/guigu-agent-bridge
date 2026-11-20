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

fn assert_paths_exist(root: &Path, paths: &[String]) {
    for p in paths {
        let resolved = if Path::new(p).is_absolute() {
            std::path::PathBuf::from(p)
        } else {
            root.join(p)
        };
        assert!(
            resolved.exists(),
            "watch path must exist: {p} (resolved: {})",
            resolved.display()
        );
    }
}

// ---------------------------------------------------------------------------
// Identity resolution tests
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Watch-path resolution tests
// ---------------------------------------------------------------------------

#[test]
fn branch_checkout_watches_head_and_existing_ref_files() {
    let root = checkout();
    let paths = build_script::git_watch_paths(root.path());
    assert!(
        paths.len() >= 2,
        "branch checkout must watch HEAD and at least the loose ref: {paths:?}"
    );
    assert!(
        paths[0].ends_with("HEAD"),
        "first watch path must be HEAD: {}",
        paths[0]
    );
    assert_paths_exist(root.path(), &paths);
}

#[test]
fn worktree_branch_watches_head_and_existing_ref_files() {
    let root = checkout();
    git(root.path(), &["branch", "wt-branch"]);
    let wt = root.path().join("wt");
    git(
        root.path(),
        &["worktree", "add", "-q", wt.to_str().unwrap(), "wt-branch"],
    );
    let paths = build_script::git_watch_paths(&wt);
    assert!(
        paths.len() >= 2,
        "worktree branch must watch HEAD and at least the loose ref: {paths:?}"
    );
    assert_paths_exist(&wt, &paths);
}

#[test]
fn packed_refs_watched_when_present() {
    let root = checkout();
    git(root.path(), &["pack-refs", "--all"]);
    let paths = build_script::git_watch_paths(root.path());
    assert!(
        paths.iter().any(|p| p.ends_with("packed-refs")),
        "packed-refs must be watched when present: {paths:?}"
    );
    assert_paths_exist(root.path(), &paths);
}

#[test]
fn packed_branch_watches_parent_dir_when_loose_ref_missing() {
    let root = checkout();
    git(root.path(), &["pack-refs", "--all"]);
    let head_content = fs::read_to_string(root.path().join(".git/HEAD")).unwrap();
    let branch = head_content
        .trim()
        .strip_prefix("ref: refs/heads/")
        .expect("branch ref");
    let loose_ref = root.path().join(".git/refs/heads").join(branch);
    assert!(
        !loose_ref.exists(),
        "loose ref must not exist after pack-refs: {}",
        loose_ref.display()
    );
    let paths = build_script::git_watch_paths(root.path());
    assert!(
        paths.iter().any(|p| p.ends_with("packed-refs")),
        "packed-refs must be watched: {paths:?}"
    );
    assert!(
        paths.iter().any(|p| p.ends_with("refs/heads")),
        "parent dir of missing loose ref must be watched: {paths:?}"
    );
    assert_paths_exist(root.path(), &paths);
}

#[test]
fn packed_to_loose_transition_watches_new_loose_ref() {
    let root = checkout();
    let sha = commit_hex(root.path());
    git(root.path(), &["pack-refs", "--all"]);
    let head_content = fs::read_to_string(root.path().join(".git/HEAD")).unwrap();
    let branch = head_content
        .trim()
        .strip_prefix("ref: refs/heads/")
        .expect("branch ref");
    let ref_dir = root.path().join(".git/refs/heads");
    fs::create_dir_all(&ref_dir).unwrap();
    fs::write(ref_dir.join(branch), format!("{sha}\n")).unwrap();
    let paths = build_script::git_watch_paths(root.path());
    assert!(
        paths.iter().any(|p| p.ends_with("packed-refs")),
        "packed-refs still watched: {paths:?}"
    );
    assert!(
        paths.iter().any(|p| p.ends_with(branch)),
        "loose ref watched after unpack: {paths:?}"
    );
    assert_paths_exist(root.path(), &paths);
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
    assert_paths_exist(root.path(), &paths);

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

// ---------------------------------------------------------------------------
// Real Cargo regression: proves build-script auto-invalidation using the
// actual project build.rs
// ---------------------------------------------------------------------------

fn create_micro_project(root: &Path) -> std::path::PathBuf {
    let project = root.join("proj");
    fs::create_dir_all(project.join("src")).unwrap();

    let build_rs = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("build.rs"))
        .expect("read actual build.rs");
    fs::write(project.join("build.rs"), build_rs).unwrap();

    fs::write(
        project.join("Cargo.toml"),
        "[package]\nname = \"identity-test\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[build-dependencies]\nsha2 = \"0.10.9\"\n",
    )
    .unwrap();
    fs::write(project.join("src/main.rs"), "fn main() {}\n").unwrap();
    project
}

fn extract_commit(content: &str) -> String {
    let marker = "SOURCE_COMMIT: [u8; 20] = [";
    let start = content.find(marker).expect("SOURCE_COMMIT not found") + marker.len();
    let end = content[start..].find(']').expect("closing bracket") + start;
    content[start..end]
        .split(',')
        .map(|s| format!("{:02x}", s.trim().parse::<u8>().unwrap()))
        .collect()
}

fn find_out_file(wt: &Path, name: &str) -> std::path::PathBuf {
    let build_dir = wt.join("target/debug/build");
    fs::read_dir(&build_dir)
        .expect("build dir missing")
        .flat_map(|e| {
            let p = e.unwrap().path();
            let candidate = p.join("out").join(name);
            if candidate.exists() {
                Some(candidate)
            } else {
                None
            }
        })
        .next()
        .unwrap_or_else(|| panic!("{name} not found under {}", build_dir.display()))
}

fn cargo_build(wt: &Path) {
    let output = Command::new("cargo")
        .current_dir(wt)
        .args(["build", "--offline", "-q"])
        .output()
        .expect("cargo build spawn failed");
    assert!(
        output.status.success(),
        "cargo build failed (exit {}): {}",
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn git_commit(root: &Path, message: &str) {
    git(
        root,
        &[
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-qm",
            message,
        ],
    );
}

#[test]
fn cargo_auto_invalidates_on_worktree_commit_without_touch() {
    let root = tempfile::tempdir().unwrap();
    let project = create_micro_project(root.path());

    git(&project, &["init", "-q"]);
    git(&project, &["add", "."]);
    git_commit(&project, "initial");

    git(&project, &["branch", "wt-branch"]);
    let wt = root.path().join("wt");
    git(
        &project,
        &["worktree", "add", "-q", wt.to_str().unwrap(), "wt-branch"],
    );

    cargo_build(&wt);
    let out_file = find_out_file(&wt, "t050_build.rs");
    let content1 = fs::read_to_string(&out_file).unwrap();
    let commit1 = extract_commit(&content1);
    let head1 = commit_hex(&wt);
    assert_eq!(commit1, head1, "initial identity must match HEAD");
    let mtime1 = fs::metadata(&out_file).unwrap().modified().unwrap();

    cargo_build(&wt);
    let content2 = fs::read_to_string(&out_file).unwrap();
    let mtime2 = fs::metadata(&out_file).unwrap().modified().unwrap();
    assert_eq!(
        content1, content2,
        "no-change rebuild must not alter output"
    );
    assert_eq!(
        mtime1, mtime2,
        "no-change rebuild must not re-run the build script"
    );

    fs::write(wt.join("new-file"), "data").unwrap();
    git(&wt, &["add", "new-file"]);
    git_commit(&wt, "worktree commit");

    cargo_build(&wt);
    let content3 = fs::read_to_string(&out_file).unwrap();
    let commit3 = extract_commit(&content3);
    let head3 = commit_hex(&wt);
    assert_eq!(commit3, head3, "identity must advance to new HEAD");
    assert_ne!(
        commit1, commit3,
        "embedded commit must advance after worktree commit: {commit1} -> {commit3}"
    );
}

#[test]
fn cargo_auto_invalidates_on_packed_to_loose_transition() {
    let root = tempfile::tempdir().unwrap();
    let project = create_micro_project(root.path());

    git(&project, &["init", "-q"]);
    git(&project, &["add", "."]);
    git_commit(&project, "initial");

    // Create branch and worktree FIRST, then pack.
    git(&project, &["branch", "wt-branch"]);
    let wt = root.path().join("wt");
    git(
        &project,
        &["worktree", "add", "-q", wt.to_str().unwrap(), "wt-branch"],
    );

    git(&project, &["pack-refs", "--all"]);

    // Assert the target loose ref does NOT exist after packing.
    let ref_rel = git(&wt, &["rev-parse", "--git-path", "refs/heads/wt-branch"]);
    let ref_rel = ref_rel.trim().to_owned();
    let loose_ref = if Path::new(&ref_rel).is_absolute() {
        std::path::PathBuf::from(&ref_rel)
    } else {
        wt.join(&ref_rel)
    };
    assert!(
        !loose_ref.exists(),
        "loose ref must not exist after pack-refs: {}",
        loose_ref.display()
    );

    cargo_build(&wt);
    let out_file = find_out_file(&wt, "t050_build.rs");
    let content1 = fs::read_to_string(&out_file).unwrap();
    let commit1 = extract_commit(&content1);
    let head1 = commit_hex(&wt);
    assert_eq!(commit1, head1, "initial identity must match HEAD");
    let mtime1 = fs::metadata(&out_file).unwrap().modified().unwrap();

    cargo_build(&wt);
    let mtime2 = fs::metadata(&out_file).unwrap().modified().unwrap();
    assert_eq!(
        mtime1, mtime2,
        "no-change rebuild must not re-run the build script"
    );

    // Commit in the worktree: git creates a new loose ref file.
    fs::write(wt.join("new-file"), "data").unwrap();
    git(&wt, &["add", "new-file"]);
    git_commit(&wt, "packed commit");

    assert!(
        loose_ref.exists(),
        "loose ref must exist after commit: {}",
        loose_ref.display()
    );

    cargo_build(&wt);
    let content3 = fs::read_to_string(&out_file).unwrap();
    let commit3 = extract_commit(&content3);
    let head3 = commit_hex(&wt);
    assert_eq!(
        commit3, head3,
        "identity must match new HEAD after packed-to-loose transition"
    );
    assert_ne!(
        commit1, commit3,
        "commit must advance: {commit1} -> {commit3}"
    );
}
