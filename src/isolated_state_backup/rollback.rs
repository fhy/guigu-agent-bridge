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
    if record.phase == RestorePhase::RollbackStart
        && !fs::leaf_absent(namespace, "restore.new.sqlite3")?
    {
        let mut file = fs::open_leaf(namespace, "restore.new.sqlite3", false)?;
        let (_, observed) = fs::hash(&mut file)?;
        if record
            .replacement
            .is_none_or(|value| !fs::same(value, observed))
        {
            return Err(());
        }
        fs::unlink(namespace, "restore.new.sqlite3")?;
        namespace.sync_all().map_err(|_| ())?;
    }
    if record.phase == RestorePhase::RollbackStart {
        next(
            namespace,
            record,
            RestorePhase::RollbackNewAbsent,
            record.bitmap,
        )?;
    }
    let root = fs::open_dir(&paths.root)?;
    if record.phase == RestorePhase::RollbackNewAbsent {
        restore_one(
            &root,
            namespace,
            "state.db",
            "pre-restore.main",
            record.bitmap & MAIN != 0,
            record.original_main,
        )?;
        next(namespace, record, RestorePhase::RollbackMain, record.bitmap)?;
    }
    if record.phase == RestorePhase::RollbackMain {
        if let Some(identity) = record.original_wal {
            restore_one(
                &root,
                namespace,
                "state.db-wal",
                "pre-restore.wal",
                record.bitmap & WAL != 0,
                identity,
            )?;
        }
        next(namespace, record, RestorePhase::RollbackWal, record.bitmap)?;
    }
    if record.phase == RestorePhase::RollbackWal {
        if let Some(identity) = record.original_shm {
            restore_one(
                &root,
                namespace,
                "state.db-shm",
                "pre-restore.shm",
                record.bitmap & SHM != 0,
                identity,
            )?;
        }
        next(namespace, record, RestorePhase::RollbackShm, record.bitmap)?;
    }
    if record.phase == RestorePhase::RollbackShm {
        if backup::source_set(paths)?.hash != record.expected_current {
            return Err(());
        }
        next(namespace, record, RestorePhase::Restored, record.bitmap)?;
    }
    Ok(())
}

fn is_rollback(phase: RestorePhase) -> bool {
    matches!(
        phase,
        RestorePhase::RollbackStart
            | RestorePhase::RollbackNewAbsent
            | RestorePhase::RollbackMain
            | RestorePhase::RollbackWal
            | RestorePhase::RollbackShm
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
    if moved {
        fs::rename_noreplace(namespace, quarantine, root, fixed)?;
        root.sync_all().map_err(|_| ())?;
    }
    let mut file = fs::open_leaf(root, fixed, false)?;
    let (_, observed) = fs::hash(&mut file)?;
    if fs::same(observed, expected) && fs::leaf_absent(namespace, quarantine)? {
        Ok(())
    } else {
        Err(())
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
