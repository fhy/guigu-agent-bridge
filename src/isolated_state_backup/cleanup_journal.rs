use super::{
    cleanup_codec::{Kind, Record},
    codec::{BACKUP_CLEANUP, Identity, RESTORE_CLEANUP},
    fs,
};
use std::fs::File;

fn names(kind: Kind) -> (&'static str, &'static str, &'static str) {
    match kind {
        Kind::Backup => (
            BACKUP_CLEANUP,
            "cleanup.backup.journal.next",
            "cleanup.backup.journal.prev",
        ),
        Kind::Restore => (
            RESTORE_CLEANUP,
            "cleanup.restore.journal.next",
            "cleanup.restore.journal.prev",
        ),
    }
}
fn load(dir: &File, name: &str) -> Result<Option<(Record, Identity)>, ()> {
    if fs::leaf_absent(dir, name)? {
        return Ok(None);
    }
    let mut f = fs::open_leaf(dir, name, false)?;
    let id = fs::identity(&f.metadata().map_err(|_| ())?)?;
    Ok(Some((Record::decode(&fs::read_small(&mut f)?)?, id)))
}
fn follows(a: &Record, b: &Record) -> bool {
    b.kind == a.kind
        && b.sequence == a.sequence.checked_add(1).unwrap_or(0)
        && b.predecessor == a.digest()
        && b.expected_snapshot == a.expected_snapshot
        && b.manifest_digest == a.manifest_digest
        && b.namespace == a.namespace
        && b.snapshot == a.snapshot
        && b.manifest == a.manifest
        && b.old_main == a.old_main
        && b.old_wal == a.old_wal
        && b.old_shm == a.old_shm
}
fn remove(dir: &File, name: &str, id: Identity) -> Result<(), ()> {
    let f = fs::open_leaf(dir, name, false)?;
    let got = fs::identity(&f.metadata().map_err(|_| ())?)?;
    if !fs::same(id, got) {
        return Err(());
    }
    fs::unlink(dir, name)?;
    dir.sync_all().map_err(|_| ())
}
pub(crate) fn adopt(dir: &File, kind: Kind) -> Result<Option<Record>, ()> {
    let (c, n, p) = names(kind);
    let current = load(dir, c)?;
    let next = load(dir, n)?;
    let prev = load(dir, p)?;
    match (current, next, prev) {
        (None, None, None) => Ok(None),
        (Some((c, _)), None, None) if c.kind == kind => Ok(Some(c)),
        (Some((c, _)), Some((n, _)), None) if follows(&c, &n) => {
            fs::rename_noreplace(dir, c_name(kind), dir, p)?;
            dir.sync_all().map_err(|_| ())?;
            fs::rename_noreplace(dir, n_name(kind), dir, c_name(kind))?;
            dir.sync_all().map_err(|_| ())?;
            let (_, id) = load(dir, p)?.ok_or(())?;
            remove(dir, p, id)?;
            Ok(Some(n))
        }
        (None, Some((n, _)), Some((p_rec, pid))) if follows(&p_rec, &n) => {
            fs::rename_noreplace(dir, n_name(kind), dir, c_name(kind))?;
            dir.sync_all().map_err(|_| ())?;
            remove(dir, p, pid)?;
            Ok(Some(n))
        }
        (Some((c, _)), None, Some((p_rec, pid))) if follows(&p_rec, &c) => {
            remove(dir, p, pid)?;
            Ok(Some(c))
        }
        _ => Err(()),
    }
}
fn c_name(k: Kind) -> &'static str {
    names(k).0
}
fn n_name(k: Kind) -> &'static str {
    names(k).1
}
pub(crate) fn initial(dir: &File, r: &Record) -> Result<(), ()> {
    if adopt(dir, r.kind)?.is_some() {
        return Err(());
    }
    let mut f = fs::create_leaf(dir, c_name(r.kind), 0o600)?;
    fs::write_sync(&mut f, &r.encode())?;
    dir.sync_all().map_err(|_| ())
}
pub(crate) fn transition(
    dir: &File,
    r: &mut Record,
    phase: super::cleanup_codec::Phase,
    bitmap: u16,
) -> Result<(), ()> {
    let (c, n, p) = names(r.kind);
    if load(dir, n)?.is_some() || load(dir, p)?.is_some() {
        return Err(());
    }
    let mut next = r.clone();
    next.sequence = next.sequence.checked_add(1).ok_or(())?;
    next.predecessor = r.digest();
    next.phase = phase;
    next.bitmap = bitmap;
    let mut f = fs::create_leaf(dir, n, 0o600)?;
    fs::write_sync(&mut f, &next.encode())?;
    dir.sync_all().map_err(|_| ())?;
    fs::rename_noreplace(dir, c, dir, p)?;
    dir.sync_all().map_err(|_| ())?;
    fs::rename_noreplace(dir, n, dir, c)?;
    dir.sync_all().map_err(|_| ())?;
    let (_, id) = load(dir, p)?.ok_or(())?;
    remove(dir, p, id)?;
    *r = next;
    Ok(())
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
    fn record(kind: Kind) -> Record {
        Record {
            sequence: 0,
            predecessor: [0; 32],
            kind,
            phase: Phase::Ready,
            bitmap: 0,
            expected_snapshot: [1; 32],
            manifest_digest: [2; 32],
            namespace: id(),
            snapshot: id(),
            manifest: id(),
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
    fn write(d: &File, name: &str, r: &Record) {
        let mut f = fs::create_leaf(d, name, 0o600).unwrap();
        fs::write_sync(&mut f, &r.encode()).unwrap();
        d.sync_all().unwrap()
    }
    #[test]
    fn both_kinds_adopt_every_valid_three_leaf_shape() {
        for kind in [Kind::Backup, Kind::Restore] {
            for shape in 0..3 {
                let (_t, d) = dir();
                let current = record(kind);
                let mut next = current.clone();
                next.sequence = 1;
                next.predecessor = current.digest();
                next.phase = Phase::IntentManifest;
                let (c, n, p) = names(kind);
                match shape {
                    0 => {
                        write(&d, c, &current);
                        write(&d, n, &next)
                    }
                    1 => {
                        write(&d, p, &current);
                        write(&d, n, &next)
                    }
                    2 => {
                        write(&d, p, &current);
                        write(&d, c, &next)
                    }
                    _ => unreachable!(),
                }
                assert_eq!(adopt(&d, kind).unwrap(), Some(next));
                assert!(fs::leaf_absent(&d, n).unwrap() && fs::leaf_absent(&d, p).unwrap())
            }
        }
    }
    #[test]
    fn invalid_presence_and_cross_kind_records_are_invariant() {
        for bits in [1u8, 2, 4, 7] {
            let (t, d) = dir();
            let (c, n, p) = names(Kind::Backup);
            let wrong = record(Kind::Restore);
            if bits & 1 != 0 {
                write(&d, c, &wrong)
            }
            if bits & 2 != 0 {
                write(&d, n, &wrong)
            }
            if bits & 4 != 0 {
                write(&d, p, &wrong)
            }
            let before = std::fs::read_dir(t.path()).unwrap().count();
            assert!(adopt(&d, Kind::Backup).is_err());
            assert_eq!(std::fs::read_dir(t.path()).unwrap().count(), before)
        }
    }
}
