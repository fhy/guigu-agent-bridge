mod backup;
mod cleanup;
mod cleanup_codec;
#[cfg(test)]
mod cleanup_fault_matrix_tests;
mod cleanup_journal;
mod cleanup_witness;
mod codec;
mod fault;
mod fs;
mod journal;
mod lock;
mod restore;
mod restore_io;
mod rollback;
mod sqlite;

use std::{ffi::OsString, fs::File, path::PathBuf};

pub(crate) const STATE_ROOT: &str = "/home/fhy/.local/share/guigu-codex-test";
pub(crate) const DATABASE: &str = "/home/fhy/.local/share/guigu-codex-test/state.db";
pub(crate) const CONFIG: &str = "/home/fhy/.config/guigu-codex-test/config.toml";
pub(crate) const NAMESPACE: &str = ".t050-backup-v1";
pub(crate) const MAX_FILE: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct Paths {
    pub root: PathBuf,
    pub database: PathBuf,
    pub config: PathBuf,
}

impl Paths {
    fn production() -> Self {
        Self {
            root: PathBuf::from(STATE_ROOT),
            database: PathBuf::from(DATABASE),
            config: PathBuf::from(CONFIG),
        }
    }

    pub(crate) fn namespace(&self) -> PathBuf {
        self.root.join(NAMESPACE)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Failure {
    pub command: &'static str,
    pub cause: &'static str,
    pub status: u8,
}

impl Failure {
    pub(crate) fn rejected(command: &'static str, cause: &'static str) -> Self {
        Self {
            command,
            cause,
            status: 3,
        }
    }
}

struct BoundLeaf {
    file: File,
    identity: codec::Identity,
    hash: Option<[u8; 32]>,
}

pub(crate) struct OwnerBindingAnchor {
    live_root: File,
    live_main: BoundLeaf,
    live_wal: Option<BoundLeaf>,
    live_shm: Option<BoundLeaf>,
    namespace: File,
    manifest: BoundLeaf,
    snapshot: BoundLeaf,
    decoded_manifest: codec::Manifest,
    source_set: backup::SourceSet,
}

fn bound_leaf(parent: &File, name: &str, content: bool) -> Result<BoundLeaf, ()> {
    let mut file = fs::open_leaf(parent, name, false)?;
    let (hash, identity) = fs::hash(&mut file)?;
    Ok(BoundLeaf {
        file,
        identity,
        hash: content.then_some(hash),
    })
}

fn optional_bound(parent: &File, name: &str, content: bool) -> Result<Option<BoundLeaf>, ()> {
    if fs::leaf_absent(parent, name)? {
        Ok(None)
    } else {
        bound_leaf(parent, name, content).map(Some)
    }
}

fn same_bound(parent: &File, name: &str, expected: &BoundLeaf) -> Result<(), ()> {
    let current = bound_leaf(parent, name, expected.hash.is_some())?;
    let held = fs::identity(&expected.file.metadata().map_err(|_| ())?)?;
    if !fs::same(current.identity, expected.identity)
        || current.hash != expected.hash
        || !fs::same(held, expected.identity)
    {
        return Err(());
    }
    Ok(())
}

impl OwnerBindingAnchor {
    pub(crate) fn source_set_hex(&self) -> String {
        backup::hex(self.source_set.hash)
    }

    pub(crate) fn snapshot_fd(&self) -> &File {
        &self.snapshot.file
    }

    pub(crate) fn live_fd(&self) -> &File {
        &self.live_main.file
    }

    fn same_live(parent: &File, name: &str, expected: &BoundLeaf) -> Result<(), ()> {
        let current = bound_leaf(parent, name, false)?;
        let held = fs::identity(&expected.file.metadata().map_err(|_| ())?)?;
        if !fs::same_object(current.identity, expected.identity)
            || !fs::same_object(held, expected.identity)
        {
            return Err(());
        }
        Ok(())
    }

    fn same_live_optional(
        parent: &File,
        name: &str,
        expected: &Option<BoundLeaf>,
    ) -> Result<(), ()> {
        match expected {
            Some(expected) => Self::same_live(parent, name, expected),
            None if fs::leaf_absent(parent, name)? => Ok(()),
            None => Err(()),
        }
    }

    pub(crate) fn refresh_live_sidecars(
        &mut self,
    ) -> Result<(), crate::owner_binding_remediation::Failure> {
        Self::same_live(&self.live_root, "state.db", &self.live_main).map_err(|_| {
            crate::owner_binding_remediation::Failure {
                cause: "postflight",
                substage: "database-identity",
            }
        })?;
        self.live_main = bound_leaf(&self.live_root, "state.db", false).map_err(|_| {
            crate::owner_binding_remediation::Failure {
                cause: "postflight",
                substage: "database-identity",
            }
        })?;
        self.live_wal = optional_bound(&self.live_root, "state.db-wal", false).map_err(|_| {
            crate::owner_binding_remediation::Failure {
                cause: "postflight",
                substage: "sidecar",
            }
        })?;
        self.live_shm = optional_bound(&self.live_root, "state.db-shm", false).map_err(|_| {
            crate::owner_binding_remediation::Failure {
                cause: "postflight",
                substage: "sidecar",
            }
        })?;
        Ok(())
    }

    pub(crate) fn revalidate(&self) -> Result<(), crate::owner_binding_remediation::Failure> {
        Self::same_live(&self.live_root, "state.db", &self.live_main)
            .and_then(|()| {
                Self::same_live_optional(&self.live_root, "state.db-wal", &self.live_wal)
            })
            .and_then(|()| {
                Self::same_live_optional(&self.live_root, "state.db-shm", &self.live_shm)
            })
            .map_err(|_| crate::owner_binding_remediation::Failure {
                cause: "postflight",
                substage: "database-identity",
            })?;
        same_bound(&self.namespace, codec::MANIFEST, &self.manifest)
            .and_then(|()| same_bound(&self.namespace, codec::SNAPSHOT, &self.snapshot))
            .map_err(|_| crate::owner_binding_remediation::Failure {
                cause: "postflight",
                substage: "backup-identity",
            })?;
        let decoded = backup::load_manifest(&self.namespace).map_err(|_| {
            crate::owner_binding_remediation::Failure {
                cause: "postflight",
                substage: "backup-identity",
            }
        })?;
        if decoded != self.decoded_manifest {
            return Err(crate::owner_binding_remediation::Failure {
                cause: "postflight",
                substage: "backup-identity",
            });
        }
        Ok(())
    }
}

pub(crate) async fn owner_binding_anchor(
    snapshot: &str,
    source_set: &str,
) -> Result<OwnerBindingAnchor, crate::owner_binding_remediation::Failure> {
    let paths = Paths::production();
    let output = backup::verify(paths.clone()).await.map_err(|_| {
        crate::owner_binding_remediation::Failure {
            cause: "backup",
            substage: "manifest",
        }
    })?;
    let expected_snapshot = format!("snapshot_sha256={snapshot}");
    if !output
        .split_ascii_whitespace()
        .any(|value| value == expected_snapshot)
    {
        return Err(crate::owner_binding_remediation::Failure {
            cause: "backup",
            substage: "snapshot",
        });
    }
    let live_root =
        fs::open_dir(&paths.root).map_err(|_| crate::owner_binding_remediation::Failure {
            cause: "identity",
            substage: "database",
        })?;
    let live_main = bound_leaf(&live_root, "state.db", true).map_err(|_| {
        crate::owner_binding_remediation::Failure {
            cause: "identity",
            substage: "database",
        }
    })?;
    let live_wal = optional_bound(&live_root, "state.db-wal", true).map_err(|_| {
        crate::owner_binding_remediation::Failure {
            cause: "identity",
            substage: "sidecar",
        }
    })?;
    let live_shm = optional_bound(&live_root, "state.db-shm", false).map_err(|_| {
        crate::owner_binding_remediation::Failure {
            cause: "identity",
            substage: "sidecar",
        }
    })?;
    let live_source_set =
        backup::source_set(&paths).map_err(|_| crate::owner_binding_remediation::Failure {
            cause: "identity",
            substage: "database",
        })?;
    if live_main.identity != live_source_set.main
        || live_main.hash != Some(live_source_set.main_hash)
        || live_wal.as_ref().map(|leaf| leaf.identity) != live_source_set.wal
        || live_wal.as_ref().and_then(|leaf| leaf.hash) != live_source_set.wal_hash
        || live_shm.as_ref().map(|leaf| leaf.identity) != live_source_set.shm
    {
        return Err(crate::owner_binding_remediation::Failure {
            cause: "identity",
            substage: "database",
        });
    }
    let namespace = fs::open_dir(&paths.namespace()).map_err(|_| {
        crate::owner_binding_remediation::Failure {
            cause: "backup",
            substage: "namespace",
        }
    })?;
    let decoded_manifest = backup::load_manifest(&namespace).map_err(|_| {
        crate::owner_binding_remediation::Failure {
            cause: "backup",
            substage: "manifest",
        }
    })?;
    if backup::hex(decoded_manifest.source_set_hash) != source_set {
        return Err(crate::owner_binding_remediation::Failure {
            cause: "backup",
            substage: "source-binding",
        });
    }
    let manifest = bound_leaf(&namespace, codec::MANIFEST, true).map_err(|_| {
        crate::owner_binding_remediation::Failure {
            cause: "backup",
            substage: "manifest",
        }
    })?;
    let snapshot = bound_leaf(&namespace, codec::SNAPSHOT, true).map_err(|_| {
        crate::owner_binding_remediation::Failure {
            cause: "backup",
            substage: "snapshot",
        }
    })?;
    if snapshot.hash != Some(decoded_manifest.snapshot_hash)
        || !fs::same(snapshot.identity, decoded_manifest.snapshot)
    {
        return Err(crate::owner_binding_remediation::Failure {
            cause: "backup",
            substage: "replacement",
        });
    }
    let anchor = OwnerBindingAnchor {
        live_root,
        live_main,
        live_wal,
        live_shm,
        namespace,
        manifest,
        snapshot,
        decoded_manifest,
        source_set: live_source_set,
    };
    anchor.revalidate()?;
    Ok(anchor)
}

fn exact_hash(value: &OsString) -> Option<[u8; 32]> {
    let value = value.to_str()?;
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; 32];
    for (index, item) in out.iter_mut().enumerate() {
        *item = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(out)
}

pub async fn dispatch(args: Vec<OsString>) -> Option<Result<String, Failure>> {
    let command = args.get(1)?.to_str()?;
    let paths = Paths::production();
    Some(match command {
        "backup-isolated-t050-state" if args.len() == 2 => backup::run(paths).await,
        "verify-isolated-t050-backup" if args.len() == 2 => backup::verify(paths).await,
        "restore-isolated-t050-state"
            if args.len() == 6
                && args[2] == "--expected-backup-sha256"
                && args[4] == "--expected-current-set-sha256" =>
        {
            match (exact_hash(&args[3]), exact_hash(&args[5])) {
                (Some(snapshot), Some(current)) => restore::run(paths, snapshot, current).await,
                _ => Err(Failure::rejected(command_name(command), "identity")),
            }
        }
        "cleanup-isolated-t050-backup"
            if args.len() == 4 && args[2] == "--expected-backup-sha256" =>
        {
            match exact_hash(&args[3]) {
                Some(snapshot) => cleanup::run(paths, snapshot),
                None => Err(Failure::rejected(command_name(command), "identity")),
            }
        }
        "backup-isolated-t050-state"
        | "verify-isolated-t050-backup"
        | "restore-isolated-t050-state"
        | "cleanup-isolated-t050-backup" => {
            Err(Failure::rejected(command_name(command), "identity"))
        }
        _ => return None,
    })
}

fn command_name(value: &str) -> &'static str {
    match value {
        "backup-isolated-t050-state" => "backup-isolated-t050-state",
        "verify-isolated-t050-backup" => "verify-isolated-t050-backup",
        "restore-isolated-t050-state" => "restore-isolated-t050-state",
        _ => "cleanup-isolated-t050-backup",
    }
}

#[cfg(test)]
mod backup_runtime_tests;
#[cfg(test)]
mod fault_tests;
#[cfg(test)]
mod journal_tests;
#[cfg(test)]
mod ownership_fault_tests;
#[cfg(test)]
mod process_death_tests;
#[cfg(test)]
mod publication_fault_tests;
#[cfg(test)]
mod recovery_tests;
#[cfg(test)]
mod sqlite_fault_tests;
#[cfg(test)]
mod tests;

#[cfg(test)]
mod owner_binding_anchor_tests {
    use super::*;

    #[test]
    fn retained_leaf_name_must_remain_bound_to_authenticated_inode_and_bytes() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("snapshot.sqlite3");
        std::fs::write(&path, b"authenticated").unwrap();
        let parent = File::open(root.path()).unwrap();
        let bound = bound_leaf(&parent, "snapshot.sqlite3", true).unwrap();
        assert_eq!(same_bound(&parent, "snapshot.sqlite3", &bound), Ok(()));

        let foreign = root.path().join("foreign");
        std::fs::write(&foreign, b"authenticated").unwrap();
        std::fs::rename(&foreign, &path).unwrap();
        assert_eq!(same_bound(&parent, "snapshot.sqlite3", &bound), Err(()));
        assert_eq!(std::fs::read(&path).unwrap(), b"authenticated");
    }
}
