use std::fs::File;

use sha2::{Digest, Sha256};

use super::{
    Failure, NAMESPACE, Paths, backup,
    cleanup_codec::{Kind, Phase, Record},
    cleanup_journal,
    codec::{MANIFEST, RESTORE_JOURNAL, RestorePhase, SNAPSHOT},
    fs, journal,
    restore::{MAIN, SHM, WAL},
};

const COMMAND: &str = "cleanup-isolated-t050-backup";
const MANIFEST_Q: &str = "snapshot.manifest.quarantine";

pub(crate) fn run(paths: Paths, expected: [u8; 32]) -> Result<String, Failure> {
    cleanup(&paths, expected).map_err(|cause| Failure::rejected(COMMAND, cause))
}

fn cleanup(paths: &Paths, expected: [u8; 32]) -> Result<String, &'static str> {
    let root = fs::open_dir(&paths.root).map_err(|_| "identity")?;
    if super::cleanup_witness::resume(&root, expected).map_err(|_| "cleanup")? {
        return Ok(success(0));
    }
    if fs::leaf_absent(&root, NAMESPACE).map_err(|_| "identity")? {
        return Ok("cleanup-isolated-t050-backup result=already-absent status=0 count=0".into());
    }
    let namespace = fs::open_dir(&paths.namespace()).map_err(|_| "identity")?;
    let backup_record = cleanup_journal::adopt(&namespace, Kind::Backup).map_err(|_| "journal")?;
    let restore_record =
        cleanup_journal::adopt(&namespace, Kind::Restore).map_err(|_| "journal")?;
    if backup_record.is_some() && restore_record.is_some() {
        return Err("journal");
    }
    let existing = backup_record.or(restore_record);
    let operation = if existing.is_some() {
        fs::OperationLock::acquire_resume(&namespace).map_err(|_| "cleanup")?
    } else {
        fs::OperationLock::acquire(&namespace).map_err(|_| "cleanup")?
    };
    if existing
        .as_ref()
        .is_some_and(|record| !fs::same(record.operation_lock, operation.identity()))
    {
        operation.preserve();
        return Err("cleanup");
    }
    let mut record = match existing {
        Some(record) => {
            if record.expected_snapshot != expected {
                operation.preserve();
                return Err("manifest");
            }
            record
        }
        None => {
            create_record(&namespace, expected, operation.identity()).map_err(|_| "manifest")?
        }
    };
    let result = drive(&namespace, &mut record);
    if result.is_err() {
        operation.preserve();
        return result.map(|_| unreachable!());
    }
    let count = result?;
    super::cleanup_witness::finish(&root, &namespace, &record, operation).map_err(|_| "cleanup")?;
    Ok(success(count))
}

fn create_record(
    namespace: &File,
    expected: [u8; 32],
    operation_lock: super::codec::Identity,
) -> Result<Record, ()> {
    let manifest = backup::load_manifest(namespace)?;
    if manifest.snapshot_hash != expected {
        return Err(());
    }
    let mut manifest_file = fs::open_leaf(namespace, MANIFEST, false)?;
    let manifest_identity = fs::identity(&manifest_file.metadata().map_err(|_| ())?)?;
    let manifest_bytes = fs::read_small(&mut manifest_file)?;
    let mut snapshot = fs::open_leaf(namespace, SNAPSHOT, false)?;
    let (hash, snapshot_identity) = fs::hash(&mut snapshot)?;
    if hash != expected || !fs::same(snapshot_identity, manifest.snapshot) {
        return Err(());
    }
    let restore = journal::adopt_bound(namespace)?;
    let (kind, old_main, old_wal, old_shm, restore_journal, restore_journal_digest) = match restore
    {
        None => (Kind::Backup, None, None, None, None, None),
        Some((record, identity, digest))
            if record.phase == RestorePhase::Committed && record.snapshot == expected =>
        {
            (
                Kind::Restore,
                (record.bitmap & MAIN != 0).then_some(record.original_main),
                (record.bitmap & WAL != 0)
                    .then_some(record.original_wal)
                    .flatten(),
                (record.bitmap & SHM != 0)
                    .then_some(record.original_shm)
                    .flatten(),
                Some(identity),
                Some(digest),
            )
        }
        Some(_) => return Err(()),
    };
    let record = Record {
        sequence: 0,
        predecessor: [0; 32],
        kind,
        phase: Phase::Ready,
        bitmap: 0,
        expected_snapshot: expected,
        manifest_digest: Sha256::digest(manifest_bytes).into(),
        namespace: fs::dir_identity(&namespace.metadata().map_err(|_| ())?)?,
        snapshot: snapshot_identity,
        manifest: manifest_identity,
        restore_journal,
        restore_journal_digest,
        operation_lock,
        old_main,
        old_wal,
        old_shm,
    };
    cleanup_journal::initial(namespace, &record)?;
    Ok(record)
}

fn drive(namespace: &File, record: &mut Record) -> Result<u32, &'static str> {
    let mut count = 0;
    if record.phase == Phase::Ready {
        transition(namespace, record, Phase::IntentManifest)?;
    }
    if record.phase == Phase::IntentManifest {
        adopt_manifest_disable(namespace, record.manifest).map_err(|_| "cleanup")?;
        transition(namespace, record, Phase::ManifestDisabled)?;
    }
    if record.kind == Kind::Restore {
        count += remove_step(
            namespace,
            record,
            "pre-restore.shm",
            record.old_shm,
            Phase::ManifestDisabled,
            Phase::IntentShm,
            Phase::ShmRemoved,
        )?;
        count += remove_step(
            namespace,
            record,
            "pre-restore.wal",
            record.old_wal,
            Phase::ShmRemoved,
            Phase::IntentWal,
            Phase::WalRemoved,
        )?;
        count += remove_step(
            namespace,
            record,
            "pre-restore.main",
            record.old_main,
            Phase::WalRemoved,
            Phase::IntentMain,
            Phase::MainRemoved,
        )?;
    } else if record.phase == Phase::ManifestDisabled {
        transition(namespace, record, Phase::MainRemoved)?;
    }
    count += remove_step(
        namespace,
        record,
        SNAPSHOT,
        Some(record.snapshot),
        Phase::MainRemoved,
        Phase::IntentSnapshot,
        Phase::SnapshotRemoved,
    )?;
    count += remove_step(
        namespace,
        record,
        MANIFEST_Q,
        Some(record.manifest),
        Phase::SnapshotRemoved,
        Phase::IntentManifestRemove,
        Phase::ManifestRemoved,
    )?;
    if record.phase == Phase::ManifestRemoved {
        if record.kind == Kind::Restore {
            transition(namespace, record, Phase::IntentRestoreJournal)?;
        } else {
            transition(namespace, record, Phase::Terminal)?;
        }
    }
    if record.phase == Phase::IntentRestoreJournal {
        if !fs::leaf_absent(namespace, RESTORE_JOURNAL).map_err(|_| "journal")? {
            super::fault::hit("cleanup-before-restore-journal-delete").map_err(|_| "cleanup")?;
            let mut journal_file =
                fs::open_leaf(namespace, RESTORE_JOURNAL, false).map_err(|_| "journal")?;
            let journal_identity = fs::identity(&journal_file.metadata().map_err(|_| "journal")?)
                .map_err(|_| "journal")?;
            let journal_bytes = fs::read_small(&mut journal_file).map_err(|_| "journal")?;
            let restore =
                super::codec::RestoreRecord::decode(&journal_bytes).map_err(|_| "journal")?;
            if restore.phase != RestorePhase::Committed
                || restore.snapshot != record.expected_snapshot
                || record
                    .restore_journal
                    .is_none_or(|v| !fs::same(v, journal_identity))
                || record.restore_journal_digest != Some(Sha256::digest(&journal_bytes).into())
            {
                return Err("journal");
            }
            remove_exact(namespace, RESTORE_JOURNAL, record.restore_journal)
                .map_err(|_| "cleanup")?;
            count += 1;
        }
        namespace.sync_all().map_err(|_| "cleanup")?;
        transition(namespace, record, Phase::RestoreJournalRemoved)?;
    }
    if record.phase == Phase::RestoreJournalRemoved {
        transition(namespace, record, Phase::Terminal)?;
    }
    Ok(count)
}

fn remove_step(
    namespace: &File,
    record: &mut Record,
    name: &str,
    identity: Option<super::codec::Identity>,
    predecessor: Phase,
    intent: Phase,
    acknowledged: Phase,
) -> Result<u32, &'static str> {
    if record.phase == predecessor {
        transition(namespace, record, intent)?;
    }
    if record.phase == intent {
        match identity {
            Some(identity) => {
                remove_exact(namespace, name, Some(identity)).map_err(|_| "cleanup")?
            }
            None => {
                if !fs::leaf_absent(namespace, name).map_err(|_| "cleanup")? {
                    return Err("cleanup");
                }
            }
        }
        transition(namespace, record, acknowledged)?;
        return Ok(u32::from(identity.is_some()));
    }
    Ok(0)
}

fn transition(namespace: &File, record: &mut Record, phase: Phase) -> Result<(), &'static str> {
    let acknowledged = match phase {
        Phase::ManifestDisabled => 1,
        Phase::ShmRemoved => 2,
        Phase::WalRemoved => 4,
        Phase::MainRemoved => 8,
        Phase::SnapshotRemoved => 16,
        Phase::ManifestRemoved => 32,
        Phase::RestoreJournalRemoved => 64,
        _ => 0,
    };
    cleanup_journal::transition(namespace, record, phase, record.bitmap | acknowledged)
        .map_err(|_| "journal")
}

fn adopt_manifest_disable(namespace: &File, expected: super::codec::Identity) -> Result<(), ()> {
    let original = observed(namespace, MANIFEST)?;
    let quarantine = observed(namespace, MANIFEST_Q)?;
    match (original, quarantine) {
        (Some(value), None) if fs::same(value, expected) => {
            fs::rename_noreplace(namespace, MANIFEST, namespace, MANIFEST_Q)?;
            namespace.sync_all().map_err(|_| ())
        }
        (None, Some(value)) if fs::same(value, expected) => namespace.sync_all().map_err(|_| ()),
        _ => Err(()),
    }
}

fn observed(dir: &File, name: &str) -> Result<Option<super::codec::Identity>, ()> {
    if fs::leaf_absent(dir, name)? {
        return Ok(None);
    }
    let file = fs::open_leaf(dir, name, false)?;
    Ok(Some(fs::identity(&file.metadata().map_err(|_| ())?)?))
}
fn remove_exact(
    dir: &File,
    name: &str,
    expected: Option<super::codec::Identity>,
) -> Result<(), ()> {
    super::fault::hit("cleanup-before-leaf-delete")?;
    match observed(dir, name)? {
        None => dir.sync_all().map_err(|_| ()),
        Some(value) if expected.is_none_or(|e| fs::same(e, value)) => {
            fs::unlink(dir, name)?;
            super::fault::hit("cleanup-after-leaf-unlink")?;
            dir.sync_all().map_err(|_| ())?;
            super::fault::hit("cleanup-after-leaf-fsync")
        }
        Some(_) => Err(()),
    }
}

fn success(count: u32) -> String {
    format!("cleanup-isolated-t050-backup result=cleaned status=0 count={count}")
}

#[cfg(test)]
pub(crate) fn cleanup_fixture(paths: &Paths, expected: [u8; 32]) -> Result<String, &'static str> {
    cleanup(paths, expected)
}
