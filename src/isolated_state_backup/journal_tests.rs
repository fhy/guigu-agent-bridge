use std::fs::File;
use std::os::unix::fs::PermissionsExt;

use tempfile::TempDir;

use super::{
    codec::{Identity, RestorePhase, RestoreRecord},
    fs, journal,
};

fn identity() -> Identity {
    Identity {
        device: 1,
        inode: 2,
        size: 3,
        uid: 4,
        gid: 5,
        mode: 0o600,
    }
}

fn record() -> RestoreRecord {
    RestoreRecord {
        sequence: 0,
        predecessor: [0; 32],
        manifest: [1; 32],
        snapshot: [2; 32],
        expected_current: [3; 32],
        phase: RestorePhase::Prepared,
        bitmap: 0,
        original_main: identity(),
        original_wal: None,
        original_shm: None,
        replacement: None,
    }
}

fn directory() -> (TempDir, File) {
    let temporary = TempDir::new().unwrap();
    std::fs::set_permissions(temporary.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let directory = fs::open_dir(temporary.path()).unwrap();
    (temporary, directory)
}

fn successor(previous: &RestoreRecord) -> RestoreRecord {
    let mut next = previous.clone();
    next.sequence += 1;
    next.predecessor = previous.digest();
    next.phase = RestorePhase::IntentShm;
    next
}

fn write(directory: &File, name: &str, record: &RestoreRecord) {
    let mut file = fs::create_leaf(directory, name, 0o600).unwrap();
    fs::write_sync(&mut file, &record.encode()).unwrap();
    directory.sync_all().unwrap();
}

#[test]
fn journal_adopts_each_valid_interrupted_transition_shape() {
    for shape in 0..3 {
        let (_temporary, directory) = directory();
        let current = record();
        let next = successor(&current);
        match shape {
            0 => {
                write(&directory, "restore.journal.v1", &current);
                write(&directory, "restore.journal.next", &next);
            }
            1 => {
                write(&directory, "restore.journal.prev", &current);
                write(&directory, "restore.journal.next", &next);
            }
            2 => {
                write(&directory, "restore.journal.prev", &current);
                write(&directory, "restore.journal.v1", &next);
            }
            _ => unreachable!(),
        }
        assert_eq!(journal::adopt(&directory).unwrap(), Some(next));
        assert!(fs::leaf_absent(&directory, "restore.journal.next").unwrap());
        assert!(fs::leaf_absent(&directory, "restore.journal.prev").unwrap());
    }
}

#[test]
fn journal_rejects_all_unbound_presence_shapes_without_mutation() {
    for shape in [
        (false, true, false),
        (false, false, true),
        (true, true, true),
    ] {
        let (_temporary, directory) = directory();
        let current = record();
        if shape.0 {
            write(&directory, "restore.journal.v1", &current);
        }
        if shape.1 {
            write(&directory, "restore.journal.next", &current);
        }
        if shape.2 {
            write(&directory, "restore.journal.prev", &current);
        }
        let before = std::fs::read_dir(_temporary.path()).unwrap().count();
        assert!(journal::adopt(&directory).is_err());
        assert_eq!(
            std::fs::read_dir(_temporary.path()).unwrap().count(),
            before
        );
    }
}
