use std::fs::File;

use super::{
    Paths, backup,
    codec::{Identity, RestorePhase, RestoreRecord},
    fs,
    restore::{MAIN, SHM, WAL, next},
};

pub(crate) fn run(paths: &Paths, namespace: &File, record: &mut RestoreRecord) -> Result<(), ()> {
    if !is_rollback(record.phase) {
        next(
            namespace,
            record,
            RestorePhase::RollbackStart,
            record.bitmap,
        )?;
    }
    if record.phase == RestorePhase::RollbackStart {
        next(
            namespace,
            record,
            RestorePhase::RollbackIntentRemoveNew,
            record.bitmap,
        )?;
    }
    if record.phase == RestorePhase::RollbackIntentRemoveNew {
        adopt_remove_new(namespace, record.replacement)?;
        next(
            namespace,
            record,
            RestorePhase::RollbackNewAbsent,
            record.bitmap,
        )?;
    }
    let root = fs::open_dir(&paths.root)?;
    if record.phase == RestorePhase::RollbackNewAbsent {
        if record.bitmap & MAIN != 0 {
            next(
                namespace,
                record,
                RestorePhase::RollbackIntentMain,
                record.bitmap,
            )?;
        } else {
            restore_one(
                &root,
                namespace,
                "state.db",
                "pre-restore.main",
                false,
                record.original_main,
            )?;
            next(namespace, record, RestorePhase::RollbackMain, record.bitmap)?;
        }
    }
    if record.phase == RestorePhase::RollbackIntentMain {
        restore_one(
            &root,
            namespace,
            "state.db",
            "pre-restore.main",
            true,
            record.original_main,
        )?;
        next(namespace, record, RestorePhase::RollbackMain, record.bitmap)?;
    }
    if record.phase == RestorePhase::RollbackMain {
        advance_sidecar(
            namespace,
            record,
            record.original_wal,
            WAL,
            RestorePhase::RollbackIntentWal,
            RestorePhase::RollbackWal,
            &root,
            "state.db-wal",
            "pre-restore.wal",
        )?;
    }
    if record.phase == RestorePhase::RollbackIntentWal {
        restore_one(
            &root,
            namespace,
            "state.db-wal",
            "pre-restore.wal",
            true,
            record.original_wal.ok_or(())?,
        )?;
        next(namespace, record, RestorePhase::RollbackWal, record.bitmap)?;
    }
    if record.phase == RestorePhase::RollbackWal {
        advance_sidecar(
            namespace,
            record,
            record.original_shm,
            SHM,
            RestorePhase::RollbackIntentShm,
            RestorePhase::RollbackShm,
            &root,
            "state.db-shm",
            "pre-restore.shm",
        )?;
    }
    if record.phase == RestorePhase::RollbackIntentShm {
        restore_one(
            &root,
            namespace,
            "state.db-shm",
            "pre-restore.shm",
            true,
            record.original_shm.ok_or(())?,
        )?;
        next(namespace, record, RestorePhase::RollbackShm, record.bitmap)?;
    }
    if record.phase == RestorePhase::RollbackShm {
        next(
            namespace,
            record,
            RestorePhase::RollbackVerifying,
            record.bitmap,
        )?;
    }
    if record.phase == RestorePhase::RollbackVerifying {
        if backup::source_set(paths)?.hash != record.expected_current {
            return Err(());
        }
        next(namespace, record, RestorePhase::Restored, record.bitmap)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn advance_sidecar(
    namespace: &File,
    record: &mut RestoreRecord,
    identity: Option<Identity>,
    bit: u16,
    intent: RestorePhase,
    acknowledged: RestorePhase,
    root: &File,
    fixed: &str,
    quarantine: &str,
) -> Result<(), ()> {
    match identity {
        Some(_) if record.bitmap & bit != 0 => next(namespace, record, intent, record.bitmap),
        Some(identity) => {
            restore_one(root, namespace, fixed, quarantine, false, identity)?;
            next(namespace, record, acknowledged, record.bitmap)
        }
        None => {
            require_absent_pair(root, namespace, fixed, quarantine)?;
            next(namespace, record, acknowledged, record.bitmap)
        }
    }
}

fn is_rollback(phase: RestorePhase) -> bool {
    matches!(
        phase,
        RestorePhase::RollbackStart
            | RestorePhase::RollbackIntentRemoveNew
            | RestorePhase::RollbackNewAbsent
            | RestorePhase::RollbackIntentMain
            | RestorePhase::RollbackMain
            | RestorePhase::RollbackIntentWal
            | RestorePhase::RollbackWal
            | RestorePhase::RollbackIntentShm
            | RestorePhase::RollbackShm
            | RestorePhase::RollbackVerifying
            | RestorePhase::Restored
    )
}

fn restore_one(
    root: &File,
    namespace: &File,
    fixed: &str,
    quarantine: &str,
    moved: bool,
    expected: Identity,
) -> Result<(), ()> {
    let fixed_identity = observed(root, fixed)?;
    let quarantine_identity = observed(namespace, quarantine)?;
    match (moved, fixed_identity, quarantine_identity) {
        (false, Some(value), None) if fs::same(value, expected) => Ok(()),
        (true, None, Some(value)) if fs::same(value, expected) => {
            fs::rename_noreplace(namespace, quarantine, root, fixed)?;
            super::fault::hit("rollback-after-restore-rename")?;
            root.sync_all().map_err(|_| ())?;
            super::fault::hit("rollback-after-restore-fsync")
        }
        (true, Some(value), None) if fs::same(value, expected) => root.sync_all().map_err(|_| ()),
        _ => Err(()),
    }
}

fn observed(dir: &File, name: &str) -> Result<Option<Identity>, ()> {
    if fs::leaf_absent(dir, name)? {
        return Ok(None);
    }
    let mut file = fs::open_leaf(dir, name, false)?;
    let (_, identity) = fs::hash(&mut file)?;
    Ok(Some(identity))
}

fn require_absent_pair(
    root: &File,
    namespace: &File,
    fixed: &str,
    quarantine: &str,
) -> Result<(), ()> {
    if fs::leaf_absent(root, fixed)? && fs::leaf_absent(namespace, quarantine)? {
        Ok(())
    } else {
        Err(())
    }
}

fn adopt_remove_new(namespace: &File, expected: Option<Identity>) -> Result<(), ()> {
    match observed(namespace, "restore.new.sqlite3")? {
        None => namespace.sync_all().map_err(|_| ()),
        Some(observed) if expected.is_some_and(|value| fs::same(value, observed)) => {
            fs::unlink(namespace, "restore.new.sqlite3")?;
            super::fault::hit("rollback-after-new-unlink")?;
            namespace.sync_all().map_err(|_| ())?;
            super::fault::hit("rollback-after-new-fsync")
        }
        Some(_) => Err(()),
    }
}

#[cfg(test)]
pub(crate) fn restore_shape_fixture(
    root: &File,
    namespace: &File,
    moved: bool,
    expected: Identity,
) -> Result<(), ()> {
    restore_one(
        root,
        namespace,
        "state.db",
        "pre-restore.main",
        moved,
        expected,
    )
}
