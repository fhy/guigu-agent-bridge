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
// Real Cargo regression: proves build-script auto-invalidation
// ---------------------------------------------------------------------------

const MICRO_BUILD_RS: &str = r#"
use std::{env, fs, path::{Path, PathBuf}, process::Command};

fn git_watch_paths(root: &Path) -> Vec<String> {
    let git_head = Command::new("git")
        .current_dir(root)
        .args(["rev-parse", "--git-path", "HEAD"])
        .output()
        .expect("git path required");
    assert!(git_head.status.success());
    let head_path = String::from_utf8(git_head.stdout).unwrap().trim().to_owned();
    let mut paths = vec![head_path.clone()];

    let head_file = if Path::new(&head_path).is_absolute() {
        PathBuf::from(&head_path)
    } else {
        root.join(&head_path)
    };
    if let Ok(content) = fs::read_to_string(&head_file) {
        if let Some(ref_name) = content.trim().strip_prefix("ref: ") {
            let ref_out = Command::new("git")
                .current_dir(root)
                .args(["rev-parse", "--git-path", ref_name])
                .output()
                .expect("git path required");
            assert!(ref_out.status.success());
            let ref_path = String::from_utf8(ref_out.stdout).unwrap().trim().to_owned();
            let ref_resolved = if Path::new(&ref_path).is_absolute() {
                PathBuf::from(&ref_path)
            } else {
                root.join(&ref_path)
            };
            if ref_resolved.exists() {
                paths.push(ref_path);
            }

            let packed_out = Command::new("git")
                .current_dir(root)
                .args(["rev-parse", "--git-path", "packed-refs"])
                .output()
                .expect("git path required");
            assert!(packed_out.status.success());
            let packed_path = String::from_utf8(packed_out.stdout).unwrap().trim().to_owned();
            let packed_resolved = if Path::new(&packed_path).is_absolute() {
                PathBuf::from(&packed_path)
            } else {
                root.join(&packed_path)
            };
            if packed_resolved.exists() {
                paths.push(packed_path);
            }
        }
    }

    paths
}

fn main() {
    for path in git_watch_paths(Path::new(".")) {
        println!("cargo:rerun-if-changed={path}");
    }

    let head = Command::new("git")
        .current_dir(".")
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("git required");
    assert!(head.status.success());
    let commit = String::from_utf8(head.stdout).unwrap().trim().to_owned();

    let out = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
    fs::write(out.join("commit.txt"), &commit).expect("write commit");

    let counter_file = out.join("run_count.txt");
    let count: u32 = fs::read_to_string(&counter_file)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);
    fs::write(&counter_file, (count + 1).to_string()).expect("write counter");
}
"#;

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

#[test]
fn cargo_auto_invalidates_on_worktree_commit_without_touch() {
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("proj");
    fs::create_dir_all(project.join("src")).unwrap();

    fs::write(
        project.join("Cargo.toml"),
        "[package]\nname = \"identity-test\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    )
    .unwrap();
    fs::write(project.join("src/main.rs"), "fn main() {}\n").unwrap();
    fs::write(project.join("build.rs"), MICRO_BUILD_RS).unwrap();

    git(&project, &["init", "-q"]);
    git(&project, &["add", "."]);
    git(
        &project,
        &[
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-qm",
            "initial",
        ],
    );

    git(&project, &["branch", "wt-branch"]);
    let wt = root.path().join("wt");
    git(
        &project,
        &["worktree", "add", "-q", wt.to_str().unwrap(), "wt-branch"],
    );

    cargo_build(&wt);
    let commit_file = find_out_file(&wt, "commit.txt");
    let count_file = find_out_file(&wt, "run_count.txt");
    let commit1 = fs::read_to_string(&commit_file).unwrap().trim().to_owned();
    let count1: u32 = fs::read_to_string(&count_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_eq!(count1, 1, "first build must run script once");

    cargo_build(&wt);
    let count2: u32 = fs::read_to_string(&count_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_eq!(
        count2, 1,
        "no-change build must not re-run the build script (count={count2})"
    );

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

    cargo_build(&wt);
    let commit2 = fs::read_to_string(&commit_file).unwrap().trim().to_owned();
    let count3: u32 = fs::read_to_string(&count_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_eq!(
        count3, 2,
        "build script must re-run after worktree commit (count={count3})"
    );
    assert_ne!(
        commit1, commit2,
        "embedded commit must advance after worktree commit: {commit1} -> {commit2}"
    );
}

#[test]
fn cargo_auto_invalidates_on_packed_ref_commit() {
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("proj");
    fs::create_dir_all(project.join("src")).unwrap();

    fs::write(
        project.join("Cargo.toml"),
        "[package]\nname = \"identity-test\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    )
    .unwrap();
    fs::write(project.join("src/main.rs"), "fn main() {}\n").unwrap();
    fs::write(project.join("build.rs"), MICRO_BUILD_RS).unwrap();

    git(&project, &["init", "-q"]);
    git(&project, &["add", "."]);
    git(
        &project,
        &[
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-qm",
            "initial",
        ],
    );

    git(&project, &["pack-refs", "--all"]);

    git(&project, &["branch", "wt-branch"]);
    let wt = root.path().join("wt");
    git(
        &project,
        &["worktree", "add", "-q", wt.to_str().unwrap(), "wt-branch"],
    );

    cargo_build(&wt);
    let commit_file = find_out_file(&wt, "commit.txt");
    let count_file = find_out_file(&wt, "run_count.txt");
    let commit1 = fs::read_to_string(&commit_file).unwrap().trim().to_owned();
    let count1: u32 = fs::read_to_string(&count_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_eq!(count1, 1);

    cargo_build(&wt);
    let count2: u32 = fs::read_to_string(&count_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_eq!(count2, 1, "no-change build must not re-run");

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
            "packed commit",
        ],
    );

    cargo_build(&wt);
    let commit2 = fs::read_to_string(&commit_file).unwrap().trim().to_owned();
    let count3: u32 = fs::read_to_string(&count_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_eq!(
        count3, 2,
        "build script must re-run after packed-ref commit"
    );
    assert_ne!(commit1, commit2, "commit must advance");
}
