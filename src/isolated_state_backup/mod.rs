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

use std::{ffi::OsString, path::PathBuf};

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

pub(crate) fn current_source_set_hex() -> Result<String, ()> {
    Ok(backup::hex(backup::source_set(&Paths::production())?.hash))
}

pub(crate) async fn verify_owner_binding_backup(
    snapshot: &str,
    source_set: &str,
) -> Result<std::fs::File, crate::owner_binding_remediation::Failure> {
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
    let namespace = fs::open_dir(&paths.namespace()).map_err(|_| {
        crate::owner_binding_remediation::Failure {
            cause: "backup",
            substage: "namespace",
        }
    })?;
    let manifest = backup::load_manifest(&namespace).map_err(|_| {
        crate::owner_binding_remediation::Failure {
            cause: "backup",
            substage: "manifest",
        }
    })?;
    if backup::hex(manifest.source_set_hash) != source_set {
        return Err(crate::owner_binding_remediation::Failure {
            cause: "backup",
            substage: "source-binding",
        });
    }
    let mut snapshot_file = fs::open_leaf(&namespace, codec::SNAPSHOT, false).map_err(|_| {
        crate::owner_binding_remediation::Failure {
            cause: "backup",
            substage: "snapshot",
        }
    })?;
    let (hash, identity) =
        fs::hash(&mut snapshot_file).map_err(|_| crate::owner_binding_remediation::Failure {
            cause: "backup",
            substage: "snapshot",
        })?;
    if hash != manifest.snapshot_hash || !fs::same(identity, manifest.snapshot) {
        return Err(crate::owner_binding_remediation::Failure {
            cause: "backup",
            substage: "replacement",
        });
    }
    Ok(snapshot_file)
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
