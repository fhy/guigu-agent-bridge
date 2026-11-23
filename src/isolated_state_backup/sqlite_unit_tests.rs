use super::*;

fn id(inode: u64, size: u64) -> Identity {
    Identity {
        device: 1,
        inode,
        size,
        uid: 1000,
        gid: 1000,
        mode: 0o600,
    }
}

fn set(shm: Option<Identity>) -> SourceSet {
    SourceSet {
        main: id(1, 10),
        wal: Some(id(2, 20)),
        shm,
        hash: [0; 32],
        bytes: 30,
        main_hash: [1; 32],
        wal_hash: Some([2; 32]),
    }
}

#[test]
fn s0_s7_shm_transition_table_is_exhaustive() {
    let absent = set(None);
    assert!(validate_observation(&absent, &set(None), None).is_ok());
    let created = set(Some(id(3, 30)));
    assert!(validate_observation(&absent, &created, None).is_ok());
    assert!(validate_observation(&absent, &created, Some(&created)).is_ok());
    assert!(validate_observation(&absent, &set(None), Some(&created)).is_ok());
    let existing = set(Some(id(4, 40)));
    assert!(validate_observation(&existing, &existing, None).is_ok());
    assert!(validate_observation(&existing, &existing, Some(&existing)).is_ok());
    assert!(validate_observation(&existing, &set(None), Some(&existing)).is_ok());
    assert!(validate_observation(&existing, &set(Some(id(5, 40))), Some(&existing)).is_err());
    assert!(validate_observation(&absent, &set(Some(id(3, 31))), Some(&created)).is_err());
    let mut main_drift = existing.clone();
    main_drift.main_hash = [9; 32];
    assert!(validate_observation(&existing, &main_drift, Some(&existing)).is_err());
    let mut wal_drift = existing.clone();
    wal_drift.wal_hash = Some([9; 32]);
    assert!(validate_observation(&existing, &wal_drift, Some(&existing)).is_err());
}

#[test]
fn initially_absent_wal_and_shm_accept_only_the_sqlite_created_pair() {
    let mut initial = set(None);
    initial.wal = None;
    initial.wal_hash = None;
    initial.bytes = initial.main.size;
    let mut created = initial.clone();
    created.wal = Some(id(2, 0));
    created.wal_hash = Some(Sha256::digest([]).into());
    created.shm = Some(id(3, 32_768));

    assert!(validate_observation(&initial, &created, None).is_ok());
    assert!(validate_observation(&initial, &created, Some(&created)).is_ok());
    assert!(validate_observation(&initial, &initial, Some(&created)).is_ok());

    let mut nonempty = created.clone();
    nonempty.wal = Some(id(2, 1));
    assert!(validate_observation(&initial, &nonempty, None).is_err());
    let mut wal_only = created.clone();
    wal_only.shm = None;
    assert!(validate_observation(&initial, &wal_only, None).is_err());
    let mut replaced = created.clone();
    replaced.shm = Some(id(4, 32_768));
    assert!(validate_observation(&initial, &replaced, Some(&created)).is_err());
}

#[test]
fn snapshot_stage_codes_are_fixed_and_redacted() {
    let stages = [
        SnapshotStage::RetainedSource,
        SnapshotStage::SourceOptions,
        SnapshotStage::SourceOpen,
        SnapshotStage::SourceRevalidation,
        SnapshotStage::SourceBegin,
        SnapshotStage::SourceFacts,
        SnapshotStage::S3Observation,
        SnapshotStage::DestinationName,
        SnapshotStage::DestinationOpen,
        SnapshotStage::CopyOrJournal,
        SnapshotStage::S5Observation,
        SnapshotStage::DestinationClose,
        SnapshotStage::SourceRollback,
        SnapshotStage::SourceClose,
        SnapshotStage::S7Observation,
    ];
    let codes = stages.map(SnapshotStage::code);
    assert_eq!(codes.len(), 15);
    assert!(codes.iter().all(|code| {
        code.bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    }));
    let mut unique = codes.to_vec();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), 15);
}
