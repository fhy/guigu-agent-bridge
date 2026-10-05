use std::fs::File;

use super::{codec::RestoreRecord, fs};

const CURRENT: &str = "restore.journal.v1";
const NEXT: &str = "restore.journal.next";
const PREV: &str = "restore.journal.prev";

fn load(dir: &File, name: &str) -> Result<Option<(RestoreRecord, super::codec::Identity)>, ()> {
    if fs::leaf_absent(dir, name)? {
        return Ok(None);
    }
    let mut file = fs::open_leaf(dir, name, false)?;
    let identity = fs::identity(&file.metadata().map_err(|_| ())?)?;
    let record = RestoreRecord::decode(&fs::read_small(&mut file)?)?;
    Ok(Some((record, identity)))
}

fn successor(previous: &RestoreRecord, next: &RestoreRecord) -> bool {
    next.sequence == previous.sequence.checked_add(1).unwrap_or(0)
        && next.predecessor == previous.digest()
        && next.manifest == previous.manifest
        && next.snapshot == previous.snapshot
        && next.expected_current == previous.expected_current
        && next.original_main == previous.original_main
        && next.original_wal == previous.original_wal
        && next.original_shm == previous.original_shm
}

fn remove_exact(dir: &File, name: &str, expected: super::codec::Identity) -> Result<(), ()> {
    let file = fs::open_leaf(dir, name, false)?;
    let observed = fs::identity(&file.metadata().map_err(|_| ())?)?;
    if !fs::same(observed, expected) {
        return Err(());
    }
    fs::unlink(dir, name)?;
    dir.sync_all().map_err(|_| ())
}

pub(crate) fn adopt(dir: &File) -> Result<Option<RestoreRecord>, ()> {
    let current = load(dir, CURRENT)?;
    let next = load(dir, NEXT)?;
    let prev = load(dir, PREV)?;
    match (current, next, prev) {
        (None, None, None) => Ok(None),
        (Some((current, _)), None, None) => Ok(Some(current)),
        (Some((current, _)), Some((next, _)), None) if successor(&current, &next) => {
            fs::rename_noreplace(dir, CURRENT, dir, PREV)?;
            dir.sync_all().map_err(|_| ())?;
            fs::rename_noreplace(dir, NEXT, dir, CURRENT)?;
            dir.sync_all().map_err(|_| ())?;
            let (_, previous_identity) = load(dir, PREV)?.ok_or(())?;
            remove_exact(dir, PREV, previous_identity)?;
            Ok(Some(next))
        }
        (None, Some((next, _)), Some((previous, previous_identity)))
            if successor(&previous, &next) =>
        {
            fs::rename_noreplace(dir, NEXT, dir, CURRENT)?;
            dir.sync_all().map_err(|_| ())?;
            remove_exact(dir, PREV, previous_identity)?;
            Ok(Some(next))
        }
        (Some((current, _)), None, Some((previous, previous_identity)))
            if successor(&previous, &current) =>
        {
            remove_exact(dir, PREV, previous_identity)?;
            Ok(Some(current))
        }
        _ => Err(()),
    }
}

pub(crate) fn initial(dir: &File, record: &RestoreRecord) -> Result<(), ()> {
    if adopt(dir)?.is_some() {
        return Err(());
    }
    let mut file = fs::create_leaf(dir, CURRENT, 0o600)?;
    fs::write_sync(&mut file, &record.encode())?;
    dir.sync_all().map_err(|_| ())
}

pub(crate) fn transition(dir: &File, record: &mut RestoreRecord) -> Result<(), ()> {
    if load(dir, NEXT)?.is_some() || load(dir, PREV)?.is_some() {
        return Err(());
    }
    let mut file = fs::create_leaf(dir, NEXT, 0o600)?;
    fs::write_sync(&mut file, &record.encode())?;
    dir.sync_all().map_err(|_| ())?;
    fs::rename_noreplace(dir, CURRENT, dir, PREV)?;
    dir.sync_all().map_err(|_| ())?;
    fs::rename_noreplace(dir, NEXT, dir, CURRENT)?;
    dir.sync_all().map_err(|_| ())?;
    let (_, previous_identity) = load(dir, PREV)?.ok_or(())?;
    remove_exact(dir, PREV, previous_identity)
}
