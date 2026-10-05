use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

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
        let mut replacement =
            fs::create_leaf(namespace, "restore.new.sqlite3", 0o600).map_err(|_| "publication")?;
        copy(snapshot, &mut replacement).map_err(|_| "publication")?;
        replacement.sync_all().map_err(|_| "publication")?;
        let (_, replacement_identity) = fs::hash(&mut replacement).map_err(|_| "publication")?;
        let replacement_facts = sqlite::verify(&paths.namespace().join("restore.new.sqlite3"))
            .await
            .map_err(|_| "snapshot")?;
        if replacement_identity.size != manifest.snapshot.size
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
    let facts = sqlite::verify(&paths.database)
        .await
        .map_err(|_| "verification")?;
    let mut published = open_path(&paths.database).map_err(|_| "verification")?;
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
    next(namespace, record, step.phase, record.bitmap).map_err(|_| "journal")?;
    let root = fs::open_dir(&paths.root).map_err(|_| "identity")?;
    let mut retained = fs::open_leaf(&root, step.fixed, false).map_err(|_| "identity")?;
    let (_, observed) = fs::hash(&mut retained).map_err(|_| "identity")?;
    if !fs::same(observed, step.expected.ok_or("identity")?) {
        return Err("identity");
    }
    fs::rename_noreplace(&root, step.fixed, namespace, step.quarantine)
        .map_err(|_| "publication")?;
    root.sync_all().map_err(|_| "publication")?;
    namespace.sync_all().map_err(|_| "publication")?;
    next(namespace, record, step.phase, record.bitmap | step.bit).map_err(|_| "journal")
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

fn copy(source: &mut File, destination: &mut File) -> Result<(), ()> {
    source.seek(SeekFrom::Start(0)).map_err(|_| ())?;
    let mut buffer = [0u8; 64 * 1024];
    let mut total = 0u64;
    loop {
        let count = source.read(&mut buffer).map_err(|_| ())?;
        if count == 0 {
            break;
        }
        total = total.checked_add(count as u64).ok_or(())?;
        if total > super::MAX_FILE {
            return Err(());
        }
        destination.write_all(&buffer[..count]).map_err(|_| ())?;
    }
    Ok(())
}
fn open_path(path: &Path) -> Result<File, ()> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| ())
}
