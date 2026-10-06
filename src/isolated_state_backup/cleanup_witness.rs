use super::{
    NAMESPACE,
    cleanup_codec::{Kind, Record},
    codec::{Identity, LOCK},
    fs,
};
use sha2::{Digest, Sha256};
use std::{ffi::CString, fs::File, os::fd::AsRawFd};

const CURRENT: &str = ".t050-backup-cleanup-owner.v1";
const NEXT: &str = ".t050-backup-cleanup-owner.next";
const PREV: &str = ".t050-backup-cleanup-owner.prev";

pub(crate) fn finish(
    root: &File,
    namespace: &File,
    record: &Record,
    operation: fs::OperationLock,
) -> Result<(), ()> {
    write(root, record, 1)?;
    transition(root, record, 2)?;
    remove(namespace, journal(record.kind))?;
    transition(root, record, 3)?;
    transition(root, record, 4)?;
    operation.finish()?;
    transition(root, record, 5)?;
    transition(root, record, 6)?;
    remove_namespace(root, record.namespace)?;
    transition(root, record, 7)?;
    transition(root, record, 8)?;
    remove(root, CURRENT)
}

pub(crate) fn resume(root: &File, expected: [u8; 32]) -> Result<bool, ()> {
    let Some(bytes) = adopt(root)? else {
        return Ok(false);
    };
    let (mut phase, record) = decode(&bytes)?;
    if record.expected_snapshot != expected {
        return Err(());
    }
    let kind = record.kind;
    if phase == 1 {
        transition(root, &record, 2)?;
        phase = 2
    }
    if phase == 2 {
        let namespace = fs::open_child_dir(root, NAMESPACE)?;
        remove(&namespace, journal(kind))?;
        transition(root, &record, 3)?;
        phase = 3
    }
    if phase == 3 {
        transition(root, &record, 4)?;
        phase = 4
    }
    if phase == 4 {
        let namespace = fs::open_child_dir(root, NAMESPACE)?;
        if !fs::leaf_absent(&namespace, LOCK)? {
            super::fault::hit("cleanup-before-lock-delete")?;
            let operation = fs::OperationLock::acquire_resume(&namespace)?;
            if !fs::same(record.operation_lock, operation.identity()) {
                operation.preserve();
                return Err(());
            }
            operation.finish()?
        }
        transition(root, &record, 5)?;
        phase = 5
    }
    if phase == 5 {
        transition(root, &record, 6)?;
        phase = 6
    }
    if phase == 6 {
        if !fs::leaf_absent(root, NAMESPACE)? {
            remove_namespace(root, record.namespace)?
        }
        transition(root, &record, 7)?;
        phase = 7
    }
    if phase == 7 {
        transition(root, &record, 8)?
    }
    remove(root, CURRENT)?;
    Ok(true)
}

fn journal(kind: Kind) -> &'static str {
    match kind {
        Kind::Backup => "cleanup.backup.journal.v1",
        Kind::Restore => "cleanup.restore.journal.v1",
    }
}
fn bytes(record: &Record, phase: u8) -> Vec<u8> {
    let record = record.encode();
    let mut b = Vec::with_capacity(record.len() + 44);
    b.extend_from_slice(b"GGT50O01");
    b.push(1);
    b.push(phase);
    b.extend_from_slice(&(record.len() as u16).to_be_bytes());
    b.extend_from_slice(&record);
    b.extend_from_slice(&Sha256::digest(&b));
    b
}
fn decode(bytes: &[u8]) -> Result<(u8, Record), ()> {
    if bytes.len() < 44 || &bytes[..8] != b"GGT50O01" || bytes[8] != 1 {
        return Err(());
    }
    let length = usize::from(u16::from_be_bytes(
        bytes[10..12].try_into().map_err(|_| ())?,
    ));
    let end = 12usize.checked_add(length).ok_or(())?;
    if end.checked_add(32).ok_or(())? != bytes.len()
        || Sha256::digest(&bytes[..end]).as_slice() != &bytes[end..]
    {
        return Err(());
    }
    Ok((bytes[9], Record::decode(&bytes[12..end])?))
}
fn write(root: &File, record: &Record, phase: u8) -> Result<(), ()> {
    let mut f = fs::create_leaf(root, CURRENT, 0o600)?;
    fs::write_sync(&mut f, &bytes(record, phase))?;
    root.sync_all().map_err(|_| ())
}
fn transition(root: &File, record: &Record, phase: u8) -> Result<(), ()> {
    let mut f = fs::create_leaf(root, NEXT, 0o600)?;
    fs::write_sync(&mut f, &bytes(record, phase))?;
    root.sync_all().map_err(|_| ())?;
    fs::rename_noreplace(root, CURRENT, root, PREV)?;
    root.sync_all().map_err(|_| ())?;
    fs::rename_noreplace(root, NEXT, root, CURRENT)?;
    root.sync_all().map_err(|_| ())?;
    remove(root, PREV)
}
fn remove(root: &File, name: &str) -> Result<(), ()> {
    if fs::leaf_absent(root, name)? {
        return root.sync_all().map_err(|_| ());
    }
    let file = fs::open_leaf(root, name, false)?;
    let retained = fs::identity(&file.metadata().map_err(|_| ())?)?;
    let current = fs::open_leaf(root, name, false)?;
    let observed = fs::identity(&current.metadata().map_err(|_| ())?)?;
    if !fs::same(retained, observed) {
        return Err(());
    }
    fs::unlink(root, name)?;
    root.sync_all().map_err(|_| ())
}
fn remove_namespace(root: &File, expected: Identity) -> Result<(), ()> {
    let name = CString::new(NAMESPACE).map_err(|_| ())?;
    let current = fs::open_child_dir(root, NAMESPACE)?;
    if !fs::same_object(
        expected,
        fs::dir_identity(&current.metadata().map_err(|_| ())?)?,
    ) {
        return Err(());
    }
    if unsafe { libc::unlinkat(root.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) } != 0 {
        return Err(());
    }
    root.sync_all().map_err(|_| ())
}
fn read(root: &File, name: &str) -> Result<Option<(Vec<u8>, Identity)>, ()> {
    if fs::leaf_absent(root, name)? {
        return Ok(None);
    }
    let mut f = fs::open_leaf(root, name, false)?;
    let id = fs::identity(&f.metadata().map_err(|_| ())?)?;
    Ok(Some((fs::read_small(&mut f)?, id)))
}
fn follows(a: &[u8], b: &[u8]) -> bool {
    matches!((decode(a),decode(b)),(Ok((ap,ar)),Ok((bp,br)))if bp==ap.saturating_add(1)&&ar==br)
}
fn adopt(root: &File) -> Result<Option<Vec<u8>>, ()> {
    let c = read(root, CURRENT)?;
    let n = read(root, NEXT)?;
    let p = read(root, PREV)?;
    match (c, n, p) {
        (None, None, None) => Ok(None),
        (Some((c, _)), None, None) => Ok(Some(c)),
        (Some((c, _)), Some((n, _)), None) if follows(&c, &n) => {
            fs::rename_noreplace(root, CURRENT, root, PREV)?;
            root.sync_all().map_err(|_| ())?;
            fs::rename_noreplace(root, NEXT, root, CURRENT)?;
            root.sync_all().map_err(|_| ())?;
            remove(root, PREV)?;
            Ok(Some(n))
        }
        (None, Some((n, _)), Some((p, _))) if follows(&p, &n) => {
            fs::rename_noreplace(root, NEXT, root, CURRENT)?;
            root.sync_all().map_err(|_| ())?;
            remove(root, PREV)?;
            Ok(Some(n))
        }
        (Some((c, _)), None, Some((p, _))) if follows(&p, &c) => {
            remove(root, PREV)?;
            Ok(Some(c))
        }
        _ => Err(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::isolated_state_backup::{cleanup_codec::Phase, fs};
    use std::os::unix::fs::PermissionsExt;
    fn id() -> Identity {
        Identity {
            device: 1,
            inode: 2,
            size: 3,
            uid: 4,
            gid: 5,
            mode: 0o600,
        }
    }
    fn record() -> Record {
        Record {
            sequence: 0,
            predecessor: [0; 32],
            kind: Kind::Backup,
            phase: Phase::Terminal,
            bitmap: 7,
            expected_snapshot: [1; 32],
            manifest_digest: [2; 32],
            namespace: id(),
            snapshot: id(),
            manifest: id(),
            restore_journal: None,
            restore_journal_digest: None,
            operation_lock: id(),
            old_main: None,
            old_wal: None,
            old_shm: None,
        }
    }
    fn dir() -> (tempfile::TempDir, File) {
        let t = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(t.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let d = fs::open_dir(t.path()).unwrap();
        (t, d)
    }
    fn put(d: &File, name: &str, b: &[u8]) {
        let mut f = fs::create_leaf(d, name, 0o600).unwrap();
        fs::write_sync(&mut f, b).unwrap();
        d.sync_all().unwrap()
    }
    #[test]
    fn every_phase_is_canonical_and_identity_bound() {
        let r = record();
        for phase in 1..=8 {
            let b = bytes(&r, phase);
            let (p, decoded) = decode(&b).unwrap();
            assert_eq!(p, phase);
            assert_eq!(decoded, r);
            let mut corrupt = b.clone();
            corrupt[20] ^= 1;
            assert!(decode(&corrupt).is_err())
        }
    }
    #[test]
    fn valid_residues_adopt_and_partial_shapes_refuse() {
        for shape in 0..3 {
            let (_t, d) = dir();
            let r = record();
            let a = bytes(&r, 2);
            let b = bytes(&r, 3);
            match shape {
                0 => {
                    put(&d, CURRENT, &a);
                    put(&d, NEXT, &b)
                }
                1 => {
                    put(&d, PREV, &a);
                    put(&d, NEXT, &b)
                }
                2 => {
                    put(&d, PREV, &a);
                    put(&d, CURRENT, &b)
                }
                _ => unreachable!(),
            }
            assert_eq!(adopt(&d).unwrap(), Some(b))
        }
        let (t, d) = dir();
        put(&d, NEXT, &bytes(&record(), 3));
        let before = std::fs::read_dir(t.path()).unwrap().count();
        assert!(adopt(&d).is_err());
        assert_eq!(std::fs::read_dir(t.path()).unwrap().count(), before)
    }
}
