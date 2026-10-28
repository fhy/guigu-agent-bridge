use chrono::{Duration, Utc};

use crate::offline_identity::{
    CollectorResult, IsolationEvidence, ServerObservation, StoreGeneration,
};

use super::{IdentityEvidence, IdentityIntent, commit, records};

fn intent() -> IdentityIntent {
    IdentityIntent {
        protocol: 1,
        root_digest: "root".into(),
        operation_id: "operation".into(),
        schema_complete_digest: "schema".into(),
        producer_source: "a".repeat(40),
        executable_digest: "b".repeat(64),
        lock_digest: "c".repeat(64),
        sdk: "matrix-sdk=0.13.0".into(),
        created_at: Utc::now() - Duration::seconds(1),
        store_before: StoreGeneration {
            locator_digest: "locator".into(),
            inventory_digest: "before".into(),
            files: 2,
        },
    }
}

fn evidence(intent: &IdentityIntent) -> IdentityEvidence {
    let observation = ServerObservation {
        identity_digest: "tuple".into(),
        whoami_digest: "whoami".into(),
        keys_query_digest: "keys".into(),
        signed_device_digest: "device".into(),
        ed25519: "ed25519".into(),
        curve25519: "curve25519".into(),
    };
    let captured_at = Utc::now();
    IdentityEvidence {
        protocol: 1,
        intent_digest: records::encoded_digest(intent).unwrap(),
        before: observation.clone(),
        after: observation,
        local: CollectorResult {
            ed25519: "ed25519".into(),
            curve25519: "curve25519".into(),
            isolation: IsolationEvidence {
                nonce: "a".repeat(64),
                timeline: crate::offline_identity::CaptureTimeline {
                    setup_started: captured_at,
                    ready_verified: captured_at,
                    go_sent: captured_at,
                    result_received: captured_at,
                    child_exited: Some(captured_at),
                },
                policy: crate::offline_identity::isolation_policy_digest(),
                executable: intent.executable_digest.clone(),
                process: 1,
                generation: "fixture-not-child-proof".into(),
                no_new_privs: 1,
                seccomp: 2,
                denied_probes: [libc::EPERM; 6],
            },
        },
        store_after: StoreGeneration {
            locator_digest: "locator".into(),
            inventory_digest: "after".into(),
            files: 2,
        },
        captured_at,
        expires_at: captured_at + Duration::minutes(15),
    }
}

async fn connection() -> (sqlx::SqliteConnection, tempfile::TempDir) {
    let root = tempfile::Builder::new()
        .prefix("r092-identity-binding-")
        .tempdir_in("/tmp")
        .unwrap();
    let pool = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    sqlx::query("INSERT INTO offline_recovery_binding(singleton,protocol,db_generation,canonical_db_digest,root_digest,admin_operation_id,schema_intent_digest,schema_complete_digest,phase,revision) VALUES(1,2,'generation','database','root','operation','migration','schema','identity_pending',1)")
        .execute(&pool).await.unwrap();
    pool.close().await;
    (
        super::verification::open(&root.path().join("state.db"), false)
            .await
            .unwrap(),
        root,
    )
}

#[test]
fn evidence_expiry_continuity_and_isolation_are_required_without_timestamp_refresh() {
    let intent = intent();
    let mut captured = evidence(&intent);
    let now = Utc::now();
    captured.validate(&intent, now).unwrap();
    assert!(
        captured
            .validate(&intent, now + Duration::minutes(16))
            .is_err()
    );
    assert!(
        captured
            .validate(&intent, intent.created_at - Duration::seconds(1))
            .is_err()
    );
    captured.after.curve25519 = "replacement".into();
    assert!(captured.validate(&intent, now).is_err());
    captured.after.curve25519 = captured.before.curve25519.clone();
    captured.local.isolation.policy = "different-policy".into();
    assert!(captured.validate(&intent, now).is_err());
}

#[test]
fn nonce_and_complete_ordered_handshake_timeline_are_required() {
    let intent = intent();
    for invalid in [
        "nonce",
        "missing-exit",
        "setup",
        "total",
        "ordering",
        "generation",
    ] {
        let mut captured = evidence(&intent);
        match invalid {
            "nonce" => captured.local.isolation.nonce = "not-a-nonce".into(),
            "missing-exit" => captured.local.isolation.timeline.child_exited = None,
            "setup" => captured.local.isolation.timeline.go_sent += Duration::seconds(6),
            "total" => {
                captured.local.isolation.timeline.child_exited =
                    Some(captured.captured_at + Duration::seconds(61));
                captured.captured_at += Duration::seconds(61);
                captured.expires_at = captured.captured_at + Duration::minutes(15);
            }
            "ordering" => captured.local.isolation.timeline.result_received -= Duration::seconds(1),
            _ => captured.local.isolation.generation.clear(),
        }
        assert!(
            captured
                .validate(&intent, Utc::now() + Duration::seconds(62))
                .is_err(),
            "{invalid}"
        );
    }
}

#[tokio::test]
async fn identity_commit_is_exact_operation_bound_idempotent_and_does_not_activate() {
    let (mut connection, _root) = connection().await;
    let intent = intent();
    let captured = evidence(&intent);
    commit(&mut connection, &intent, &captured).await.unwrap();
    let expected: (String, i64, String) =
        sqlx::query_as("SELECT phase,revision,identity_digest FROM offline_recovery_binding")
            .fetch_one(&mut connection)
            .await
            .unwrap();
    assert_eq!(expected.0, "identity_ready");
    assert_eq!(expected.1, 2);
    assert_eq!(expected.2, records::encoded_digest(&captured).unwrap());
    commit(&mut connection, &intent, &captured).await.unwrap();
    let actual: (String, i64, String) =
        sqlx::query_as("SELECT phase,revision,identity_digest FROM offline_recovery_binding")
            .fetch_one(&mut connection)
            .await
            .unwrap();
    assert_eq!(expected, actual);
    let native: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_control")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(native, 0);
}

#[tokio::test]
async fn identity_commit_rejects_wrong_operation_and_different_existing_evidence() {
    let (mut connection, _root) = connection().await;
    let mut intent = intent();
    let captured = evidence(&intent);
    intent.operation_id = "other".into();
    assert!(commit(&mut connection, &intent, &captured).await.is_err());
    intent.operation_id = "operation".into();
    commit(&mut connection, &intent, &captured).await.unwrap();
    let mut changed = captured;
    changed.store_after.inventory_digest = "replacement".into();
    assert!(commit(&mut connection, &intent, &changed).await.is_err());
    let phase: String = sqlx::query_scalar("SELECT phase FROM offline_recovery_binding")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(phase, "identity_ready");
}
