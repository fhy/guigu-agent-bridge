use super::{Failure, Mode, flow, records, snapshot, test_fixture::Fixture};
use std::{fs, process::Command};

#[tokio::test]
async fn preintent_crashes_classify_residue_without_new_backup_or_operation() {
    for phase in [
        "after-operation",
        "after-backup-bytes",
        "after-backup-record",
        "after-intent",
    ] {
        let fixture = Fixture::new().await;
        let result = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "offline_production_migration::tests::child_entry",
                "--nocapture",
            ])
            .env("R071_TEST_ROOT", fixture.root.path())
            .env("R071_TEST_CRASH", phase)
            .output()
            .unwrap();
        assert_eq!(
            result.status.code(),
            Some(86),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert_eq!(
            snapshot::inspect(&fixture.request.database)
                .await
                .unwrap()
                .version,
            18
        );
        let anchor = fs::read(records::anchor(&fixture.request.database)).unwrap();
        let snapshot = fs::read(fixture.request.manifest_dir.join("snapshot.db")).ok();
        assert!(super::acquire_runtime_lock(&fixture.request.database).is_err());
        let resumed = flow::execute(fixture.mode(Mode::Resume), false).await;
        if matches!(phase, "after-operation" | "after-backup-bytes") {
            assert!(matches!(resumed, Err(Failure::ManualRequired)));
            assert_eq!(
                snapshot::inspect(&fixture.request.database)
                    .await
                    .unwrap()
                    .version,
                18
            );
        } else {
            resumed.unwrap();
        }
        assert_eq!(
            anchor,
            fs::read(records::anchor(&fixture.request.database)).unwrap()
        );
        assert_eq!(
            snapshot,
            fs::read(fixture.request.manifest_dir.join("snapshot.db")).ok()
        );
    }
}
