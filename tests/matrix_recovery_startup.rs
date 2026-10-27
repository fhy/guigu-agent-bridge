#![cfg(feature = "test-support")]

#[path = "support/r084.rs"]
mod support;

use guigu_agent_bridge::storage::{CompatibilityGate, CutoverStore, MatrixCompatibilityStore};
use support::{Fixture, seed_store};
use wiremock::MockServer;

#[tokio::test]
async fn unbound_store_and_identity_only_hold_before_every_owner_enabled_or_disabled() {
    let server = MockServer::start().await;
    for enabled in [true, false] {
        for filename in [
            "identity-only",
            "matrix-sdk-state.sqlite3",
            "matrix-sdk-state.sqlite3-wal",
        ] {
            let fixture = Fixture::new().await;
            std::fs::create_dir(fixture.store()).unwrap();
            std::fs::set_permissions(
                fixture.store(),
                std::os::unix::fs::PermissionsExt::from_mode(0o700),
            )
            .unwrap();
            std::fs::write(fixture.store().join(filename), []).unwrap();
            for _ in 0..2 {
                let runtime = fixture.start(fixture.config(enabled, &server.uri())).await;
                assert_eq!(
                    runtime.health_state().snapshot().await.readiness,
                    guigu_agent_bridge::runtime::Readiness::RecoveryBlocked
                );
                let reason = runtime.health_state().recovery_diagnostic_line().unwrap();
                assert!(matches!(
                    reason.as_str(),
                    "legacy-sdk-store-unbound" | "compatibility-held"
                ));
                fixture.assert_no_owners().await;
                assert_eq!(fixture.count("matrix_recovery_control").await, 0);
                assert_eq!(fixture.count("matrix_recovery_compatibility").await, 1);
                assert_eq!(
                    std::fs::read(fixture.store().join(filename)).unwrap(),
                    Vec::<u8>::new()
                );
                runtime.shutdown().await.unwrap();
            }
        }
    }
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn absent_empty_and_no_locator_disabled_start_without_cutover_writes() {
    for scenario in ["absent", "empty", "no-locator"] {
        let fixture = Fixture::new().await;
        if scenario == "empty" {
            std::fs::create_dir(fixture.store()).unwrap();
            std::fs::set_permissions(
                fixture.store(),
                std::os::unix::fs::PermissionsExt::from_mode(0o700),
            )
            .unwrap();
        }
        let mut config = fixture.config(false, "http://127.0.0.1:9");
        if scenario == "no-locator" {
            config.transports.matrix.crypto_store_path = None;
        }
        let runtime = fixture.start(config).await;
        assert_eq!(fixture.count("matrix_recovery_compatibility").await, 0);
        assert_eq!(fixture.count("matrix_recovery_control").await, 0);
        assert!(fixture.count("runtime_instances").await > 0);
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn unsafe_store_is_durably_held_and_removing_locator_cannot_bypass_it() {
    let fixture = Fixture::new().await;
    let destination = fixture.root.path().join("unrelated");
    std::fs::create_dir(&destination).unwrap();
    std::os::unix::fs::symlink(&destination, fixture.store()).unwrap();
    let runtime = fixture
        .start(fixture.config(false, "http://127.0.0.1:9"))
        .await;
    fixture.assert_no_owners().await;
    runtime.shutdown().await.unwrap();
    let mut config = fixture.config(false, "http://127.0.0.1:9");
    config.transports.matrix.crypto_store_path = None;
    let runtime = fixture.start(config).await;
    fixture.assert_no_owners().await;
    assert!(std::fs::read_dir(destination).unwrap().next().is_none());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn all_legacy_cutover_states_hold_at_real_startup_without_mutation() {
    for enabled in [true, false] {
        for state in [
            "pending",
            "active",
            "processing",
            "complete",
            "manual_required",
        ] {
            let fixture = Fixture::new().await;
            let legacy = CutoverStore::prepare(
                fixture.pool.clone(),
                "legacy",
                &"a".repeat(64),
                &"b".repeat(64),
            )
            .await
            .unwrap();
            if state != "pending" {
                legacy.activate("opaque-legacy-B").await.unwrap();
            }
            if !matches!(state, "pending" | "active") {
                sqlx::query("INSERT INTO matrix_cutover_dispositions(operation_id,room_id,event_id,content_digest,state) VALUES('legacy','!room:test','$old',?,?)").bind("c".repeat(64)).bind(state).execute(&fixture.pool).await.unwrap();
            }
            let before = legacy.state().await.unwrap();
            let runtime = fixture
                .start(fixture.config(enabled, "http://127.0.0.1:9"))
                .await;
            fixture.assert_no_owners().await;
            assert!(legacy.state().await.unwrap() == before);
            assert_eq!(fixture.count("matrix_recovery_control").await, 0);
            runtime.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn native_disabled_restart_preserves_identity_and_b_despite_sdk_progress() {
    let fixture = Fixture::new().await;
    let store = MatrixCompatibilityStore::new(fixture.pool.clone());
    let CompatibilityGate::Native(operation) =
        store.gate(true, Some(&fixture.store())).await.unwrap()
    else {
        panic!("native required")
    };
    sqlx::query(
        "UPDATE matrix_recovery_control SET phase='active',watermark='opaque-B' WHERE control_id=1",
    )
    .execute(&fixture.pool)
    .await
    .unwrap();
    seed_store(&fixture.store());
    let bytes = std::fs::read(fixture.store().join("identity-only")).unwrap();
    for _ in 0..2 {
        let runtime = fixture
            .start(fixture.config(false, "http://127.0.0.1:9"))
            .await;
        let identity: (String, String) =
            sqlx::query_as("SELECT cutover_operation_id,watermark FROM matrix_recovery_control")
                .fetch_one(&fixture.pool)
                .await
                .unwrap();
        assert_eq!(identity, (operation.clone(), "opaque-B".to_owned()));
        assert_eq!(
            std::fs::read(fixture.store().join("identity-only")).unwrap(),
            bytes
        );
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn pending_processing_unknown_refused_hold_at_startup_before_sdk() {
    let server = MockServer::start().await;
    for enabled in [true, false] {
        for state in ["pending", "processing", "unknown", "refused"] {
            let fixture = Fixture::new().await;
            let store = MatrixCompatibilityStore::new(fixture.pool.clone());
            let CompatibilityGate::Native(operation) =
                store.gate(true, Some(&fixture.store())).await.unwrap()
            else {
                panic!("native")
            };
            sqlx::query("UPDATE matrix_recovery_control SET phase='active',watermark='B'")
                .execute(&fixture.pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO matrix_recovery_batches(batch_id,response_token,response_digest,component,owner_token,owner_epoch,expected_events_json,covered_events_json,state,created_at,updated_at) VALUES('batch','C','digest','MatrixSync',?,1,'[\"$event\"]','[]','processing','t','t')").bind(&operation).execute(&fixture.pool).await.unwrap();
            sqlx::query("INSERT INTO matrix_recovery_receipts(account_id,room_id,event_id,cutover_operation_id,payload_digest,state,batch_id,created_at,updated_at) VALUES('@bridge:test','!room:test','$event',?,'digest',?,'batch','t','t')").bind(&operation).bind(state).execute(&fixture.pool).await.unwrap();
            for _ in 0..2 {
                let runtime = fixture.start(fixture.config(enabled, &server.uri())).await;
                fixture.assert_no_owners().await;
                let hold: (String, String) = sqlx::query_as(
                    "SELECT watermark,restart_hold_state FROM matrix_recovery_control",
                )
                .fetch_one(&fixture.pool)
                .await
                .unwrap();
                assert_eq!(hold, ("B".to_owned(), state.to_owned()));
                runtime.shutdown().await.unwrap();
            }
        }
    }
    assert!(server.received_requests().await.unwrap().is_empty());
}
