use std::fs::File;

use super::{
    Failure, Paths, backup,
    codec::{RestorePhase, RestoreRecord, SNAPSHOT},
    fs, journal, sqlite,
};

const COMMAND: &str = "restore-isolated-t050-state";
pub(crate) const SHM: u16 = 1;
pub(crate) const WAL: u16 = 2;
pub(crate) const MAIN: u16 = 4;
const NEW: u16 = 8;
const PUBLISHED: u16 = 16;
const VERIFIED: u16 = 32;

pub(crate) async fn run(
    paths: Paths,
    snapshot: [u8; 32],
    current: [u8; 32],
) -> Result<String, Failure> {
    let guard = crate::runtime_admission::Guard::acquire(&paths.database, &paths.config, "restore")
        .map_err(|_| Failure::rejected(COMMAND, "identity"))?;
    match crate::privileged_proc_proof::prove_runtime_absence(&guard) {
        Ok(crate::privileged_proc_proof::ProofResult::Clean) => {}
        _ => return Err(Failure::rejected(COMMAND, "runtime")),
    }
    restore(&paths, snapshot, current)
        .await
        .map_err(|cause| Failure::rejected(COMMAND, cause))
}

async fn restore(
    paths: &Paths,
    snapshot_hash: [u8; 32],
    expected_current: [u8; 32],
) -> Result<String, &'static str> {
    let namespace = fs::open_dir(&paths.namespace()).map_err(|_| "identity")?;
    let manifest = backup::load_manifest(&namespace).map_err(|_| "manifest")?;
    if manifest.snapshot_hash != snapshot_hash {
        return Err("manifest");
    }
    let mut snapshot = fs::open_leaf(&namespace, SNAPSHOT, false).map_err(|_| "snapshot")?;
    let (observed, snapshot_identity) = fs::hash(&mut snapshot).map_err(|_| "snapshot")?;
    if observed != snapshot_hash || !fs::same(snapshot_identity, manifest.snapshot) {
        return Err("snapshot");
    }
    let existing = journal::adopt(&namespace).map_err(|_| "journal")?;
    let operation = if existing.is_some() {
        fs::OperationLock::acquire_resume(&namespace).map_err(|_| "cleanup")?
    } else {
        fs::OperationLock::acquire(&namespace).map_err(|_| "cleanup")?
    };
    let mut record = if let Some(record) = existing {
        if record.manifest != manifest.digest()
            || record.snapshot != snapshot_hash
            || record.expected_current != expected_current
        {
            operation.preserve();
            return Err("journal");
        }
        record
    } else {
        let current = backup::source_set(paths).map_err(|_| "current")?;
        if current.hash != expected_current {
            return Err("current");
        }
        let record = RestoreRecord {
            sequence: 0,
            predecessor: [0; 32],
            manifest: manifest.digest(),
            snapshot: snapshot_hash,
            expected_current,
            phase: RestorePhase::Prepared,
            bitmap: 0,
            original_main: current.main,
            original_wal: current.wal,
            original_shm: current.shm,
            replacement: None,
        };
        journal::initial(&namespace, &record).map_err(|_| "journal")?;
        record
    };
    let resuming_rollback = matches!(
        record.phase,
        RestorePhase::RollbackStart
            | RestorePhase::RollbackNewAbsent
            | RestorePhase::RollbackMain
            | RestorePhase::RollbackWal
            | RestorePhase::RollbackShm
            | RestorePhase::RollbackIntentRemoveNew
            | RestorePhase::RollbackIntentMain
            | RestorePhase::RollbackIntentWal
            | RestorePhase::RollbackIntentShm
            | RestorePhase::RollbackVerifying
            | RestorePhase::Restored
    );
    if resuming_rollback {
        let outcome = super::rollback::run(paths, &namespace, &mut record);
        operation.preserve();
        return match outcome {
            Ok(()) => Err("pre-publication"),
            Err(()) => Err("rollback"),
        };
    }
    let result = forward(paths, &namespace, &mut record, &mut snapshot).await;
    if result.is_err()
        && record.bitmap & PUBLISHED == 0
        && super::rollback::run(paths, &namespace, &mut record).is_err()
    {
        operation.preserve();
        return Err("rollback");
    }
    if let Err(error) = result {
        operation.preserve();
        return Err(error);
    }
    operation.finish().map_err(|_| "cleanup")?;
    Ok(format!(
        "restore-isolated-t050-state result=committed status=0 snapshot_sha256={} current_set_sha256={}",
        backup::hex(snapshot_hash),
        backup::hex(expected_current)
    ))
}

#[cfg(test)]
pub(crate) async fn restore_fixture(
    paths: &Paths,
    snapshot: [u8; 32],
    current: [u8; 32],
) -> Result<String, &'static str> {
    restore(paths, snapshot, current).await
}

async fn forward(
    paths: &Paths,
    namespace: &File,
    record: &mut RestoreRecord,
    snapshot: &mut File,
) -> Result<(), &'static str> {
    if record.phase == RestorePhase::Committed {
        return Ok(());
    }
    if record.bitmap & SHM == 0 {
        quarantine(
            paths,
            namespace,
            record,
            Quarantine {
                fixed: "state.db-shm",
                quarantine: "pre-restore.shm",
                phase: RestorePhase::IntentShm,
                bit: SHM,
                expected: record.original_shm,
            },
        )?;
    }
    if record.bitmap & WAL == 0 {
        quarantine(
            paths,
            namespace,
            record,
            Quarantine {
                fixed: "state.db-wal",
                quarantine: "pre-restore.wal",
                phase: RestorePhase::IntentWal,
                bit: WAL,
                expected: record.original_wal,
            },
        )?;
    }
    if record.bitmap & MAIN == 0 {
        quarantine(
            paths,
            namespace,
            record,
            Quarantine {
                fixed: "state.db",
                quarantine: "pre-restore.main",
                phase: RestorePhase::IntentMain,
                bit: MAIN,
                expected: Some(record.original_main),
            },
        )?;
    }
    if record.phase != RestorePhase::Quarantined && record.bitmap & NEW == 0 {
        next(namespace, record, RestorePhase::Quarantined, record.bitmap).map_err(|_| "journal")?;
    }
    let manifest = backup::load_manifest(namespace).map_err(|_| "manifest")?;
    if record.bitmap & NEW == 0 {
        if record.phase != RestorePhase::IntentNew {
            next(namespace, record, RestorePhase::IntentNew, record.bitmap)
                .map_err(|_| "journal")?;
        }
        let mut replacement =
            if fs::leaf_absent(namespace, "restore.new.sqlite3").map_err(|_| "publication")? {
                let mut replacement = fs::create_leaf(namespace, "restore.new.sqlite3", 0o600)
                    .map_err(|_| "publication")?;
                super::restore_io::copy(snapshot, &mut replacement).map_err(|_| "publication")?;
                replacement.sync_all().map_err(|_| "publication")?;
                replacement
            } else {
                fs::open_leaf(namespace, "restore.new.sqlite3", false).map_err(|_| "publication")?
            };
        let (replacement_hash, replacement_identity) =
            fs::hash(&mut replacement).map_err(|_| "publication")?;
        let replacement_facts = sqlite::verify_retained(&replacement)
            .await
            .map_err(|_| "snapshot")?;
        if replacement_hash != record.snapshot
            || replacement_identity.size != manifest.snapshot.size
            || replacement_facts.page_size != manifest.page_size
            || replacement_facts.schema_hash != manifest.schema_hash
        {
            return Err("snapshot");
        }
        record.replacement = Some(replacement_identity);
        next(
            namespace,
            record,
            RestorePhase::NewDurable,
            record.bitmap | NEW,
        )
        .map_err(|_| "journal")?;
    }
    let root = fs::open_dir(&paths.root).map_err(|_| "identity")?;
    if record.bitmap & PUBLISHED == 0 {
        if record.phase != RestorePhase::IntentPublish {
            next(
                namespace,
                record,
                RestorePhase::IntentPublish,
                record.bitmap,
            )
            .map_err(|_| "journal")?;
        }
        let replacement = record.replacement.ok_or("journal")?;
        let new_present =
            !fs::leaf_absent(namespace, "restore.new.sqlite3").map_err(|_| "publication")?;
        let fixed_present = !fs::leaf_absent(&root, "state.db").map_err(|_| "publication")?;
        match (new_present, fixed_present) {
            (true, false) => {
                fs::rename_noreplace(namespace, "restore.new.sqlite3", &root, "state.db")
                    .map_err(|_| "publication")?;
                root.sync_all().map_err(|_| "publication")?;
            }
            (false, true) => {
                let file = fs::open_leaf(&root, "state.db", false).map_err(|_| "publication")?;
                let observed = fs::identity(&file.metadata().map_err(|_| "publication")?)
                    .map_err(|_| "publication")?;
                if !fs::same(observed, replacement) {
                    return Err("publication");
                }
            }
            _ => return Err("publication"),
        }
        next(
            namespace,
            record,
            RestorePhase::Published,
            record.bitmap | PUBLISHED,
        )
        .map_err(|_| "journal")?;
    }
    let published_file = fs::open_leaf(&root, "state.db", false).map_err(|_| "verification")?;
    let facts = sqlite::verify_retained(&published_file)
        .await
        .map_err(|_| "verification")?;
    let mut published = published_file.try_clone().map_err(|_| "verification")?;
    let (hash, identity) = fs::hash(&mut published).map_err(|_| "verification")?;
    if hash != record.snapshot
        || record
            .replacement
            .is_none_or(|value| !fs::same(value, identity))
        || facts.schema_hash != manifest.schema_hash
    {
        return Err("verification");
    }
    if record.bitmap & VERIFIED == 0 {
        next(
            namespace,
            record,
            RestorePhase::Verified,
            record.bitmap | VERIFIED,
        )
        .map_err(|_| "journal")?;
    }
    next(namespace, record, RestorePhase::Committed, record.bitmap).map_err(|_| "journal")
}

struct Quarantine {
    fixed: &'static str,
    quarantine: &'static str,
    phase: RestorePhase,
    bit: u16,
    expected: Option<super::codec::Identity>,
}

fn quarantine(
    paths: &Paths,
    namespace: &File,
    record: &mut RestoreRecord,
    step: Quarantine,
) -> Result<(), &'static str> {
    if step.expected.is_none() {
        return Ok(());
    }
    if record.phase != step.phase {
        next(namespace, record, step.phase, record.bitmap).map_err(|_| "journal")?;
    }
    let root = fs::open_dir(&paths.root).map_err(|_| "identity")?;
    let expected = step.expected.ok_or("identity")?;
    let fixed = retained_identity(&root, step.fixed).map_err(|_| "identity")?;
    let moved = retained_identity(namespace, step.quarantine).map_err(|_| "identity")?;
    match (fixed, moved) {
        (Some(observed), None) if fs::same(observed, expected) => {
            fs::rename_noreplace(&root, step.fixed, namespace, step.quarantine)
                .map_err(|_| "publication")?;
            super::fault::hit("restore-after-quarantine-rename").map_err(|_| "publication")?;
            root.sync_all().map_err(|_| "publication")?;
            namespace.sync_all().map_err(|_| "publication")?;
            super::fault::hit("restore-after-quarantine-fsync").map_err(|_| "publication")?;
        }
        (None, Some(observed)) if fs::same(observed, expected) => {
            root.sync_all().map_err(|_| "publication")?;
            namespace.sync_all().map_err(|_| "publication")?;
        }
        _ => return Err("identity"),
    }
    super::fault::hit("restore-before-quarantine-ack").map_err(|_| "journal")?;
    next(namespace, record, step.phase, record.bitmap | step.bit).map_err(|_| "journal")
}

fn retained_identity(dir: &File, name: &str) -> Result<Option<super::codec::Identity>, ()> {
    if fs::leaf_absent(dir, name)? {
        return Ok(None);
    }
    let mut file = fs::open_leaf(dir, name, false)?;
    let (_, identity) = fs::hash(&mut file)?;
    Ok(Some(identity))
}

#[cfg(test)]
pub(crate) fn quarantine_fixture(
    paths: &Paths,
    namespace: &File,
    record: &mut RestoreRecord,
    fixed: &'static str,
    quarantine_name: &'static str,
    phase: RestorePhase,
    bit: u16,
    expected: super::codec::Identity,
) -> Result<(), &'static str> {
    quarantine(
        paths,
        namespace,
        record,
        Quarantine {
            fixed,
            quarantine: quarantine_name,
            phase,
            bit,
            expected: Some(expected),
        },
    )
}

pub(crate) fn next(
    namespace: &File,
    record: &mut RestoreRecord,
    phase: RestorePhase,
    bitmap: u16,
) -> Result<(), ()> {
    let predecessor = record.digest();
    let mut successor = record.clone();
    successor.sequence = successor.sequence.checked_add(1).ok_or(())?;
    successor.predecessor = predecessor;
    successor.phase = phase;
    successor.bitmap = bitmap;
    journal::transition(namespace, &mut successor)?;
    *record = successor;
    Ok(())
}
