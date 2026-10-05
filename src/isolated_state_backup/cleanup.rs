use std::{ffi::CString, fs::File, os::fd::AsRawFd};

use sha2::{Digest, Sha256};

use super::{
    Failure, NAMESPACE, Paths, backup,
    codec::{
        BACKUP_CLEANUP, MANIFEST, RESTORE_CLEANUP, RESTORE_JOURNAL, RestorePhase, RestoreRecord,
        SNAPSHOT,
    },
    fs,
};

const COMMAND: &str = "cleanup-isolated-t050-backup";
const WITNESS: &str = ".t050-backup-cleanup-owner.v1";

pub(crate) fn run(paths: Paths, expected: [u8; 32]) -> Result<String, Failure> {
    cleanup(&paths, expected).map_err(|cause| Failure::rejected(COMMAND, cause))
}

fn cleanup(paths: &Paths, expected: [u8; 32]) -> Result<String, &'static str> {
    let root = fs::open_dir(&paths.root).map_err(|_| "identity")?;
    if fs::leaf_absent(&root, NAMESPACE).map_err(|_| "identity")? {
        return Ok(
            "cleanup-isolated-t050-backup result=already-absent status=0 count=0".to_owned(),
        );
    }
    let namespace = fs::open_dir(&paths.namespace()).map_err(|_| "identity")?;
    let manifest = backup::load_manifest(&namespace).map_err(|_| "manifest")?;
    if manifest.snapshot_hash != expected {
        return Err("manifest");
    }
    let mut snapshot = fs::open_leaf(&namespace, SNAPSHOT, false).map_err(|_| "snapshot")?;
    let (hash, identity) = fs::hash(&mut snapshot).map_err(|_| "snapshot")?;
    if hash != expected || !fs::same(identity, manifest.snapshot) {
        return Err("snapshot");
    }
    let restore = read_restore(&namespace)?;
    let operation = fs::OperationLock::acquire(&namespace).map_err(|_| "cleanup")?;
    let journal_name = if restore.is_some() {
        RESTORE_CLEANUP
    } else {
        BACKUP_CLEANUP
    };
    write_record(&namespace, journal_name, b"GGT50C01", 0).map_err(|_| "cleanup")?;
    let mut count = 0u32;
    if let Some(record) = restore {
        if record.phase != RestorePhase::Committed || record.snapshot != expected {
            return Err("journal");
        }
        for name in ["pre-restore.shm", "pre-restore.wal", "pre-restore.main"] {
            count += remove_if_present(&namespace, name).map_err(|_| "cleanup")?;
        }
        count += remove_if_present(&namespace, SNAPSHOT).map_err(|_| "cleanup")?;
        disable_manifest(&namespace).map_err(|_| "cleanup")?;
        count +=
            remove_if_present(&namespace, "snapshot.manifest.quarantine").map_err(|_| "cleanup")?;
        count += remove_if_present(&namespace, RESTORE_JOURNAL).map_err(|_| "cleanup")?;
    } else {
        for forbidden in [
            "pre-restore.main",
            "pre-restore.wal",
            "pre-restore.shm",
            "restore.new.sqlite3",
        ] {
            if !fs::leaf_absent(&namespace, forbidden).map_err(|_| "cleanup")? {
                return Err("journal");
            }
        }
        disable_manifest(&namespace).map_err(|_| "cleanup")?;
        count += remove_if_present(&namespace, SNAPSHOT).map_err(|_| "cleanup")?;
        count +=
            remove_if_present(&namespace, "snapshot.manifest.quarantine").map_err(|_| "cleanup")?;
    }
    write_witness(&root, expected, 1).map_err(|_| "cleanup")?;
    count += remove_if_present(&namespace, journal_name).map_err(|_| "cleanup")?;
    operation.finish().map_err(|_| "cleanup")?;
    write_witness_replace(&root, expected, 2).map_err(|_| "cleanup")?;
    remove_namespace(&root).map_err(|_| "cleanup")?;
    write_witness_replace(&root, expected, 3).map_err(|_| "cleanup")?;
    remove_witness(&root).map_err(|_| "cleanup")?;
    Ok(format!(
        "cleanup-isolated-t050-backup result=cleaned status=0 count={count}"
    ))
}

#[cfg(test)]
pub(crate) fn cleanup_fixture(paths: &Paths, expected: [u8; 32]) -> Result<String, &'static str> {
    cleanup(paths, expected)
}

fn read_restore(namespace: &File) -> Result<Option<RestoreRecord>, &'static str> {
    if fs::leaf_absent(namespace, RESTORE_JOURNAL).map_err(|_| "journal")? {
        return Ok(None);
    }
    let mut file = fs::open_leaf(namespace, RESTORE_JOURNAL, false).map_err(|_| "journal")?;
    let bytes = fs::read_small(&mut file).map_err(|_| "journal")?;
    RestoreRecord::decode(&bytes)
        .map(Some)
        .map_err(|_| "journal")
}

fn disable_manifest(namespace: &File) -> Result<(), ()> {
    fs::rename_noreplace(
        namespace,
        MANIFEST,
        namespace,
        "snapshot.manifest.quarantine",
    )?;
    namespace.sync_all().map_err(|_| ())
}

fn remove_if_present(namespace: &File, name: &str) -> Result<u32, ()> {
    if fs::leaf_absent(namespace, name)? {
        return Ok(0);
    }
    let _retained = fs::open_leaf(namespace, name, false)?;
    fs::unlink(namespace, name)?;
    namespace.sync_all().map_err(|_| ())?;
    Ok(1)
}

fn write_record(namespace: &File, name: &str, magic: &[u8; 8], phase: u8) -> Result<(), ()> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(magic);
    bytes.push(1);
    bytes.push(phase);
    let digest = Sha256::digest(&bytes);
    bytes.extend_from_slice(&digest);
    let mut file = fs::create_leaf(namespace, name, 0o600)?;
    fs::write_sync(&mut file, &bytes)?;
    namespace.sync_all().map_err(|_| ())
}

fn witness_bytes(expected: [u8; 32], phase: u8) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"GGT50O01");
    bytes.push(1);
    bytes.push(phase);
    bytes.extend_from_slice(&expected);
    let digest = Sha256::digest(&bytes);
    bytes.extend_from_slice(&digest);
    bytes
}
fn write_witness(root: &File, expected: [u8; 32], phase: u8) -> Result<(), ()> {
    let mut file = fs::create_leaf(root, WITNESS, 0o600)?;
    fs::write_sync(&mut file, &witness_bytes(expected, phase))?;
    root.sync_all().map_err(|_| ())
}
fn write_witness_replace(root: &File, expected: [u8; 32], phase: u8) -> Result<(), ()> {
    let next = ".t050-backup-cleanup-owner.next";
    let previous = ".t050-backup-cleanup-owner.prev";
    let mut file = fs::create_leaf(root, next, 0o600)?;
    fs::write_sync(&mut file, &witness_bytes(expected, phase))?;
    root.sync_all().map_err(|_| ())?;
    fs::rename_noreplace(root, WITNESS, root, previous)?;
    root.sync_all().map_err(|_| ())?;
    fs::rename_noreplace(root, next, root, WITNESS)?;
    root.sync_all().map_err(|_| ())?;
    fs::unlink(root, previous)?;
    root.sync_all().map_err(|_| ())
}
fn remove_namespace(root: &File) -> Result<(), ()> {
    let name = CString::new(NAMESPACE).map_err(|_| ())?;
    if unsafe { libc::unlinkat(root.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) } != 0 {
        return Err(());
    }
    root.sync_all().map_err(|_| ())
}
fn remove_witness(root: &File) -> Result<(), ()> {
    let _file = fs::open_leaf(root, WITNESS, false)?;
    fs::unlink(root, WITNESS)?;
    root.sync_all().map_err(|_| ())
}
