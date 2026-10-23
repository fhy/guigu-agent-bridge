use super::{Failure, RuntimeLock, acquire_runtime_lock};
use std::{ffi::OsString, path::PathBuf};

pub(crate) fn acquire(args: &[OsString]) -> Result<Option<RuntimeLock>, Failure> {
    let Some(command) = args.get(1).and_then(|arg| arg.to_str()) else {
        return Ok(None);
    };
    let fixed = matches!(
        command,
        "backup-isolated-t050-state"
            | "restore-isolated-t050-state"
            | "cleanup-isolated-t050-backup"
            | "remediate-isolated-owner-bindings"
    );
    let explicit = matches!(
        command,
        "historical-terminal-cleanup"
            | "historical-unknown-abandon"
            | "recover-runtime"
            | "reconcile-delivery"
            | "recover-preacceptance"
            | "reconcile-orphaned-preacceptance"
            | "reconcile-lease-owner"
            | "prepare-isolated-legacy-fixture-migration-manifest"
            | "prepare-isolated-legacy-owner-manifest"
    );
    if !fixed && !explicit {
        return Ok(None);
    }
    let database = if fixed {
        PathBuf::from(crate::isolated_state_backup::DATABASE)
    } else {
        let Some(pair) = args.windows(2).find(|pair| pair[0] == "--database") else {
            return Ok(None);
        };
        PathBuf::from(&pair[1])
    };
    if !database.exists() {
        return Ok(None);
    }
    let canonical = std::fs::canonicalize(database).map_err(|_| Failure::Preflight)?;
    acquire_runtime_lock(&canonical)
        .map(Some)
        .map_err(|_| Failure::Busy)
}
