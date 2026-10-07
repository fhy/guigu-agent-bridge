use super::super::unit_residue_command::{
    parse_expected as unit_residue_parse_expected, state_is_absent as unit_residue_state_is_absent,
};
use super::*;

fn parent() -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    temp
}

fn evidence() -> (Vec<u8>, [u8; 32], String) {
    let bytes = vec![b'e'; EXPECTED_SIZE as usize];
    let digest: [u8; 32] = Sha256::digest(&bytes).into();
    let encoded = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    (bytes, digest, encoded)
}

fn install(temp: &tempfile::TempDir, bytes: &[u8]) {
    std::fs::write(temp.path().join(UNIT_NAME), bytes).unwrap();
    std::fs::set_permissions(
        temp.path().join(UNIT_NAME),
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
}

#[test]
fn exact_residue_moves_to_private_forensic_leaf() {
    let temp = parent();
    let (bytes, expected, _) = evidence();
    install(&temp, &bytes);
    let mut reloads = 0;
    let mut verifies = 0;
    assert_eq!(
        run_with_hooks(
            temp.path(),
            expected,
            |_| {},
            || {
                reloads += 1;
                true
            },
            || {
                verifies += 1;
                true
            },
        ),
        Ok(())
    );
    assert_eq!((reloads, verifies), (1, 1));
    assert!(!temp.path().join(UNIT_NAME).exists());
    assert_eq!(
        std::fs::read(temp.path().join(FORENSIC_NAME)).unwrap(),
        bytes
    );
    assert_eq!(
        std::fs::metadata(temp.path().join(FORENSIC_NAME))
            .unwrap()
            .mode()
            & 0o7777,
        0o600
    );
}

#[test]
fn identity_hash_and_destination_refusals_are_invariant() {
    let (bytes, expected, _) = evidence();
    for shape in ["hash", "size", "mode", "symlink", "destination"] {
        let temp = parent();
        install(&temp, &bytes);
        match shape {
            "hash" => std::fs::write(temp.path().join(UNIT_NAME), vec![b'x'; 698]).unwrap(),
            "size" => std::fs::write(temp.path().join(UNIT_NAME), b"short").unwrap(),
            "mode" => std::fs::set_permissions(
                temp.path().join(UNIT_NAME),
                std::fs::Permissions::from_mode(0o600),
            )
            .unwrap(),
            "symlink" => {
                std::fs::remove_file(temp.path().join(UNIT_NAME)).unwrap();
                std::os::unix::fs::symlink("outside", temp.path().join(UNIT_NAME)).unwrap();
            }
            "destination" => std::fs::write(temp.path().join(FORENSIC_NAME), b"foreign").unwrap(),
            _ => unreachable!(),
        }
        let unit_before = std::fs::symlink_metadata(temp.path().join(UNIT_NAME)).unwrap();
        let forensic_before = std::fs::symlink_metadata(temp.path().join(FORENSIC_NAME)).ok();
        let mut called = false;
        assert!(
            run_with_hooks(
                temp.path(),
                expected,
                |_| {},
                || {
                    called = true;
                    true
                },
                || true,
            )
            .is_err()
        );
        assert!(!called);
        let unit_after = std::fs::symlink_metadata(temp.path().join(UNIT_NAME)).unwrap();
        assert_eq!(
            (unit_before.dev(), unit_before.ino()),
            (unit_after.dev(), unit_after.ino())
        );
        assert_eq!(
            forensic_before.map(|metadata| (metadata.dev(), metadata.ino())),
            std::fs::symlink_metadata(temp.path().join(FORENSIC_NAME))
                .ok()
                .map(|metadata| (metadata.dev(), metadata.ino()))
        );
    }
}

#[test]
fn parent_and_leaf_replacements_fail_closed_with_foreign_residue() {
    let (bytes, expected, _) = evidence();

    let root = parent();
    let active = root.path().join("active");
    let retained = root.path().join("retained");
    std::fs::create_dir(&active).unwrap();
    std::fs::set_permissions(&active, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(active.join(UNIT_NAME), &bytes).unwrap();
    std::fs::set_permissions(
        active.join(UNIT_NAME),
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    assert_eq!(
        run_with_hooks(
            &active,
            expected,
            |point| {
                if point == HookPoint::ActiveAuthenticated {
                    std::fs::rename(&active, &retained).unwrap();
                    std::fs::create_dir(&active).unwrap();
                    std::fs::set_permissions(&active, std::fs::Permissions::from_mode(0o700))
                        .unwrap();
                }
            },
            || true,
            || true,
        ),
        Err(Cause::Artifact(Stage::ParentBinding))
    );
    assert!(retained.join(UNIT_NAME).exists());

    let root = parent();
    let active = root.path().join("active-after-move");
    let retained = root.path().join("retained-after-move");
    std::fs::create_dir(&active).unwrap();
    std::fs::set_permissions(&active, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(active.join(UNIT_NAME), &bytes).unwrap();
    std::fs::set_permissions(
        active.join(UNIT_NAME),
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    assert_eq!(
        run_with_hooks(
            &active,
            expected,
            |point| {
                if point == HookPoint::Moved {
                    std::fs::rename(&active, &retained).unwrap();
                    std::fs::create_dir(&active).unwrap();
                    std::fs::set_permissions(&active, std::fs::Permissions::from_mode(0o700))
                        .unwrap();
                }
            },
            || true,
            || true,
        ),
        Err(Cause::Rollback)
    );
    assert!(retained.join(FORENSIC_NAME).exists());
    assert!(!active.join(UNIT_NAME).exists());

    let temp = parent();
    install(&temp, &bytes);
    let saved_active = temp.path().join("saved-active");
    let foreign_active = vec![b'f'; EXPECTED_SIZE as usize];
    assert_eq!(
        run_with_hooks(
            temp.path(),
            expected,
            |point| {
                if point == HookPoint::ActiveAuthenticated {
                    std::fs::rename(temp.path().join(UNIT_NAME), &saved_active).unwrap();
                    std::fs::write(temp.path().join(UNIT_NAME), &foreign_active).unwrap();
                    std::fs::set_permissions(
                        temp.path().join(UNIT_NAME),
                        std::fs::Permissions::from_mode(0o644),
                    )
                    .unwrap();
                }
            },
            || true,
            || true,
        ),
        Err(Cause::Rollback)
    );
    assert_eq!(std::fs::read(saved_active).unwrap(), bytes);
    assert_eq!(
        std::fs::read(temp.path().join(FORENSIC_NAME)).unwrap(),
        foreign_active
    );
    assert!(!temp.path().join(UNIT_NAME).exists());

    let temp = parent();
    install(&temp, &bytes);
    let saved = temp.path().join("saved");
    let foreign = b"foreign";
    assert_eq!(
        run_with_hooks(
            temp.path(),
            expected,
            |point| {
                if point == HookPoint::Moved {
                    std::fs::rename(temp.path().join(FORENSIC_NAME), &saved).unwrap();
                    std::fs::write(temp.path().join(FORENSIC_NAME), foreign).unwrap();
                }
            },
            || true,
            || true,
        ),
        Err(Cause::Rollback)
    );
    assert_eq!(
        std::fs::read(temp.path().join(FORENSIC_NAME)).unwrap(),
        foreign
    );
    assert_eq!(std::fs::read(saved).unwrap(), bytes);
    assert!(!temp.path().join(UNIT_NAME).exists());
}

#[test]
fn reload_and_state_failures_restore_name_mode_and_reload() {
    let (bytes, expected, _) = evidence();
    for failure in ["reload", "state"] {
        let temp = parent();
        install(&temp, &bytes);
        let mut reloads = 0;
        let result = run_with_hooks(
            temp.path(),
            expected,
            |_| {},
            || {
                reloads += 1;
                failure != "reload" || reloads == 2
            },
            || failure != "state",
        );
        assert_eq!(
            result,
            Err(if failure == "reload" {
                Cause::Reload
            } else {
                Cause::Verify
            })
        );
        assert_eq!(reloads, 2);
        assert_eq!(std::fs::read(temp.path().join(UNIT_NAME)).unwrap(), bytes);
        assert_eq!(
            std::fs::metadata(temp.path().join(UNIT_NAME))
                .unwrap()
                .mode()
                & 0o7777,
            0o644
        );
        assert!(!temp.path().join(FORENSIC_NAME).exists());
    }
}

#[test]
fn rollback_collision_preserves_both_residues_without_overwrite() {
    let temp = parent();
    let (bytes, expected, _) = evidence();
    install(&temp, &bytes);
    let foreign = b"foreign-active";
    let mut reloads = 0;
    assert_eq!(
        run_with_hooks(
            temp.path(),
            expected,
            |_| {},
            || {
                reloads += 1;
                if reloads == 1 {
                    std::fs::write(temp.path().join(UNIT_NAME), foreign).unwrap();
                }
                false
            },
            || true,
        ),
        Err(Cause::Rollback)
    );
    assert_eq!(reloads, 1);
    assert_eq!(std::fs::read(temp.path().join(UNIT_NAME)).unwrap(), foreign);
    assert_eq!(
        std::fs::read(temp.path().join(FORENSIC_NAME)).unwrap(),
        bytes
    );
    assert_eq!(
        std::fs::metadata(temp.path().join(FORENSIC_NAME))
            .unwrap()
            .mode()
            & 0o7777,
        0o644
    );
}

#[test]
fn grammar_digest_and_state_parser_are_strict() {
    let (_, expected, encoded) = evidence();
    assert_eq!(unit_residue_parse_expected(&encoded), Ok(expected));
    for invalid in ["", "00", &"A".repeat(64), &"z".repeat(64)] {
        assert_eq!(unit_residue_parse_expected(invalid), Err(Cause::Arguments));
    }
    assert!(unit_residue_state_is_absent(
        b"LoadState=not-found\nActiveState=inactive\nSubState=dead\nMainPID=0\n"
    ));
    assert!(!unit_residue_state_is_absent(
        b"LoadState=loaded\nActiveState=inactive\nSubState=dead\nMainPID=0\n"
    ));
    assert!(!unit_residue_state_is_absent(
        b"LoadState=not-found\nActiveState=inactive\nSubState=dead\nMainPID=0\nExtra=x\n"
    ));
    let stages = [
        (Stage::Inspect, "inspect"),
        (Stage::Authenticate, "authenticate"),
        (Stage::Move, "move"),
        (Stage::Chmod, "chmod"),
        (Stage::FileSync, "file-sync"),
        (Stage::ParentSync, "parent-sync"),
        (Stage::ParentBinding, "parent-binding"),
    ];
    for (stage, code) in stages {
        let cause = Cause::Artifact(stage);
        assert_eq!(cause.code(), "artifact");
        assert_eq!(cause.substage(), code);
    }
}
