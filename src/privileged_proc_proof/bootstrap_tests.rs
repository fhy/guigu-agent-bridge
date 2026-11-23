use super::bootstrap::{ProvisionLayout, set_construction_fault, take_fsync_events};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

struct Roots {
    _temp: tempfile::TempDir,
    etc: PathBuf,
    libexec: PathBuf,
    uid: u32,
}

impl Roots {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let etc = temp.path().join("etc");
        let libexec = temp.path().join("usr-libexec");
        std::fs::create_dir(&etc).unwrap();
        std::fs::create_dir(&libexec).unwrap();
        std::fs::set_permissions(&etc, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::set_permissions(&libexec, std::fs::Permissions::from_mode(0o755)).unwrap();
        let uid = std::fs::metadata(&etc).unwrap().uid();
        assert_eq!(std::fs::metadata(&etc).unwrap().gid(), uid);
        Self {
            _temp: temp,
            etc,
            libexec,
            uid,
        }
    }

    fn artifact_dir(&self) -> PathBuf {
        self.etc.join("guigu-agent-bridge/t050-proc-proof")
    }

    fn helper_dir(&self) -> PathBuf {
        self.libexec.join("guigu-agent-bridge")
    }

    fn acquire(&self) -> Result<ProvisionLayout, ()> {
        ProvisionLayout::at_roots(&self.etc, &self.libexec, self.uid)
    }
}

fn mode(path: &Path) -> u32 {
    std::fs::symlink_metadata(path)
        .unwrap()
        .permissions()
        .mode()
        & 0o7777
}

fn acquire_with_fault(roots: &Roots, stage: &str) -> Result<ProvisionLayout, ()> {
    set_construction_fault(Some(stage));
    let result = roots.acquire();
    set_construction_fault(None);
    result
}

#[test]
fn first_bootstrap_tracks_and_cleans_owned_tree_and_lock() {
    let roots = Roots::new();
    take_fsync_events();
    let layout = roots.acquire().unwrap();
    assert_eq!(mode(&roots.artifact_dir()), 0o755);
    assert_eq!(mode(&roots.helper_dir()), 0o755);
    assert_eq!(mode(&roots.artifact_dir().join(".provision.lock")), 0o600);
    layout.finish(false).unwrap();
    assert!(!roots.etc.join("guigu-agent-bridge").exists());
    assert!(!roots.helper_dir().exists());
    assert_eq!(
        take_fsync_events(),
        [
            "mkdir",
            "mkdir",
            "mkdir",
            "lock-create",
            "lock-unlink",
            "directory-unlink",
            "directory-unlink",
            "directory-unlink",
        ]
    );
}

#[test]
fn success_retains_fixed_directories_but_never_the_lock() {
    let roots = Roots::new();
    roots.acquire().unwrap().finish(true).unwrap();
    assert_eq!(mode(&roots.etc.join("guigu-agent-bridge")), 0o755);
    assert_eq!(mode(&roots.artifact_dir()), 0o755);
    assert_eq!(mode(&roots.helper_dir()), 0o755);
    assert!(!roots.artifact_dir().join(".provision.lock").exists());

    roots.acquire().unwrap().finish(true).unwrap();
    assert!(!roots.artifact_dir().join(".provision.lock").exists());
}

#[test]
fn hostile_existing_components_refuse_without_tree_mutation() {
    for location in ["etc", "artifact", "libexec"] {
        for case in ["symlink", "file", "mode"] {
            let roots = Roots::new();
            let bridge = roots.etc.join("guigu-agent-bridge");
            let hostile = match location {
                "etc" => bridge.clone(),
                "artifact" => {
                    std::fs::create_dir(&bridge).unwrap();
                    std::fs::set_permissions(&bridge, std::fs::Permissions::from_mode(0o755))
                        .unwrap();
                    bridge.join("t050-proc-proof")
                }
                "libexec" => roots.helper_dir(),
                _ => unreachable!(),
            };
            let outside = roots
                ._temp
                .path()
                .join(format!("outside-{location}-{case}"));
            match case {
                "symlink" => {
                    std::fs::create_dir(&outside).unwrap();
                    std::os::unix::fs::symlink(&outside, &hostile).unwrap();
                }
                "file" => std::fs::write(&hostile, b"hostile").unwrap(),
                "mode" => {
                    std::fs::create_dir(&hostile).unwrap();
                    std::fs::set_permissions(&hostile, std::fs::Permissions::from_mode(0o775))
                        .unwrap();
                }
                _ => unreachable!(),
            }
            assert!(roots.acquire().is_err(), "location={location} case={case}");
            assert!(!roots.artifact_dir().join(".provision.lock").exists());
            match case {
                "symlink" => {
                    assert!(
                        std::fs::symlink_metadata(&hostile)
                            .unwrap()
                            .file_type()
                            .is_symlink()
                    );
                    assert!(outside.is_dir());
                }
                "file" => assert_eq!(std::fs::read(&hostile).unwrap(), b"hostile"),
                "mode" => assert_eq!(mode(&hostile), 0o775),
                _ => unreachable!(),
            }
        }
    }
}

#[test]
fn contention_and_lock_replacement_never_remove_foreign_leaf() {
    let roots = Roots::new();
    let first = roots.acquire().unwrap();
    assert!(roots.acquire().is_err());
    assert!(roots.artifact_dir().join(".provision.lock").is_file());
    first.finish(true).unwrap();

    let layout = roots.acquire().unwrap();
    let lock = roots.artifact_dir().join(".provision.lock");
    let displaced = roots.artifact_dir().join("owned-lock");
    std::fs::rename(&lock, &displaced).unwrap();
    std::fs::write(&lock, b"replacement").unwrap();
    std::fs::set_permissions(&lock, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert!(layout.finish(false).is_err());
    assert_eq!(std::fs::read(&lock).unwrap(), b"replacement");
    assert!(displaced.is_file());
}

#[test]
fn nonempty_owned_directory_is_retained_after_lock_cleanup() {
    let roots = Roots::new();
    let layout = roots.acquire().unwrap();
    let marker = roots.artifact_dir().join("concurrent");
    std::fs::write(&marker, b"keep").unwrap();
    assert!(layout.finish(false).is_err());
    assert!(!roots.artifact_dir().join(".provision.lock").exists());
    assert_eq!(std::fs::read(marker).unwrap(), b"keep");
}

#[test]
fn directory_replacement_is_preserved_after_owned_lock_cleanup() {
    let roots = Roots::new();
    let layout = roots.acquire().unwrap();
    let artifact = roots.artifact_dir();
    let displaced = roots.etc.join("guigu-agent-bridge/displaced");
    std::fs::rename(&artifact, &displaced).unwrap();
    std::fs::create_dir(&artifact).unwrap();
    std::fs::set_permissions(&artifact, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::write(artifact.join("replacement"), b"keep").unwrap();

    assert!(layout.finish(false).is_err());
    assert_eq!(
        std::fs::read(artifact.join("replacement")).unwrap(),
        b"keep"
    );
    assert!(displaced.is_dir());
    assert!(!displaced.join(".provision.lock").exists());
}

#[test]
fn every_directory_construction_boundary_cleans_the_owned_tree() {
    for component in [
        "etc/guigu-agent-bridge",
        "etc/guigu-agent-bridge/t050-proc-proof",
        "libexec/guigu-agent-bridge",
    ] {
        for boundary in [
            "after-mkdir",
            "after-reopen",
            "after-metadata",
            "after-clone",
            "after-fsync",
        ] {
            let roots = Roots::new();
            let stage = format!("{component}:{boundary}");
            assert!(acquire_with_fault(&roots, &stage).is_err(), "{stage}");
            assert!(!roots.etc.join("guigu-agent-bridge").exists(), "{stage}");
            assert!(!roots.helper_dir().exists(), "{stage}");
        }
    }
}

#[test]
fn every_lock_construction_boundary_cleans_lock_and_owned_tree() {
    for stage in [
        "lock:after-open",
        "lock:after-metadata",
        "lock:after-flock",
        "lock:after-fsync",
    ] {
        let roots = Roots::new();
        assert!(acquire_with_fault(&roots, stage).is_err(), "{stage}");
        assert!(!roots.artifact_dir().join(".provision.lock").exists());
        assert!(!roots.etc.join("guigu-agent-bridge").exists());
        assert!(!roots.helper_dir().exists());
    }
}

#[test]
fn pending_cleanup_ambiguity_stops_without_touching_contents() {
    for stage in ["directory-cleanup-inspect", "directory-cleanup-unlink"] {
        let roots = Roots::new();
        let faults = format!("etc/guigu-agent-bridge:after-mkdir,{stage}");
        set_construction_fault(Some(&faults));
        assert!(roots.acquire().is_err(), "{stage}");
        set_construction_fault(None);
        assert!(roots.etc.join("guigu-agent-bridge").is_dir(), "{stage}");
        assert!(!roots.helper_dir().exists(), "{stage}");
    }

    let roots = Roots::new();
    set_construction_fault(Some("lock:after-open,lock-cleanup-unlink"));
    assert!(roots.acquire().is_err());
    set_construction_fault(None);
    assert!(roots.artifact_dir().join(".provision.lock").is_file());
    assert!(roots.helper_dir().is_dir());

    let roots = Roots::new();
    set_construction_fault(Some(
        "etc/guigu-agent-bridge/t050-proc-proof:after-mkdir,directory-cleanup-fsync",
    ));
    assert!(roots.acquire().is_err());
    set_construction_fault(None);
    assert!(roots.etc.join("guigu-agent-bridge").is_dir());
    assert!(!roots.artifact_dir().exists());
    assert!(!roots.helper_dir().exists());

    let roots = Roots::new();
    set_construction_fault(Some("lock:after-open,lock-cleanup-fsync"));
    assert!(roots.acquire().is_err());
    set_construction_fault(None);
    assert!(!roots.artifact_dir().join(".provision.lock").exists());
    assert!(roots.artifact_dir().is_dir());
    assert!(roots.helper_dir().is_dir());
}
