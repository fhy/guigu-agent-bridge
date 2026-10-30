use guigu_agent_bridge::storage::{BatchAdmission, MatrixRecoveryStore, connect, migrate};
use sqlx::SqlitePool;
use uuid::Uuid;

async fn database() -> SqlitePool {
    let path = std::env::temp_dir().join(format!("guigu-r080-{}.db", Uuid::now_v7()));
    let pool = connect(path).await.expect("connect");
    migrate(&pool).await.expect("migrate");
    pool
}

#[tokio::test]
async fn scoped_native_activation_and_checkpoint_keep_authority_and_audit_in_sync() {
    use guigu_agent_bridge::storage::{
        CompatibilityGate, MatrixCompatibilityStore,
        recovery_eligibility::{EffectDomain, RecoveryScope, bootstrap_nonadmin},
    };
    use sha2::{Digest, Sha256};

    let pool = database().await;
    let CompatibilityGate::Native(operation) = MatrixCompatibilityStore::new(pool.clone())
        .gate(true, None)
        .await
        .unwrap()
    else {
        panic!("native required")
    };
    let context = bootstrap_nonadmin(&pool, true).await.unwrap();
    let scope = RecoveryScope::new(context, EffectDomain::Local, None);
    assert!(matches!(
        MatrixCompatibilityStore::new(pool.clone())
            .gate_scoped(true, None, Some(&scope))
            .await
            .unwrap(),
        CompatibilityGate::Native(_)
    ));
    let store = MatrixRecoveryStore::new_scoped(pool.clone(), Some(scope));
    store.reconcile_restart().await.unwrap();
    let before: String = sqlx::query_scalar("SELECT original_digest FROM offline_recovery_rows WHERE table_name='matrix_recovery_control'")
        .fetch_one(&pool).await.unwrap();
    store.activate_cutover(&operation, "B", "t1").await.unwrap();
    store.reconcile_restart().await.unwrap();
    let after: String = sqlx::query_scalar("SELECT original_digest FROM offline_recovery_rows WHERE table_name='matrix_recovery_control'")
        .fetch_one(&pool).await.unwrap();
    assert_ne!(before, after);
    let now = chrono::Utc::now();
    let timestamp = now.to_rfc3339();
    let expiry = (now + chrono::Duration::minutes(1)).to_rfc3339();
    let epoch = store
        .acquire_component_lease("MatrixSync", "owner", &timestamp, &expiry)
        .await
        .unwrap();
    store
        .begin_batch(
            "scoped-batch",
            Some("B"),
            "C",
            "digest",
            "B",
            "owner",
            epoch,
            &[],
            &timestamp,
        )
        .await
        .unwrap();
    store
        .complete_batch("scoped-batch", "C", "owner", epoch, &timestamp)
        .await
        .unwrap();
    store.reconcile_restart().await.unwrap();
    let context: (String, String) =
        sqlx::query_as("SELECT matrix_phase,expected_b_digest FROM runtime_eligibility_context")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        context,
        ("active".into(), format!("{:x}", Sha256::digest(b"C")))
    );
    sqlx::query("UPDATE matrix_recovery_control SET watermark='tampered'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(store.reconcile_restart().await.is_err());
}

#[tokio::test]
async fn scoped_native_activation_refuses_stale_revision_and_changed_control_snapshot() {
    use guigu_agent_bridge::storage::{
        CompatibilityGate, MatrixCompatibilityStore,
        recovery_eligibility::{EffectDomain, RecoveryScope, bootstrap_nonadmin},
    };

    for stale_revision in [false, true] {
        let pool = database().await;
        let CompatibilityGate::Native(operation) = MatrixCompatibilityStore::new(pool.clone())
            .gate(true, None)
            .await
            .unwrap()
        else {
            panic!("native required")
        };
        let context = bootstrap_nonadmin(&pool, true).await.unwrap();
        let store = MatrixRecoveryStore::new_scoped(
            pool.clone(),
            Some(RecoveryScope::new(context, EffectDomain::Local, None)),
        );
        if stale_revision {
            sqlx::query("UPDATE runtime_eligibility_context SET revision=revision+1")
                .execute(&pool)
                .await
                .unwrap();
        } else {
            sqlx::query("UPDATE matrix_recovery_control SET updated_at='unclassified-change'")
                .execute(&pool)
                .await
                .unwrap();
        }
        assert!(store.activate_cutover(&operation, "B", "t1").await.is_err());
        let state: (String, Option<String>) =
            sqlx::query_as("SELECT phase,watermark FROM matrix_recovery_control")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, ("baseline".into(), None));
    }
}

#[tokio::test]
async fn scoped_native_revision_publishes_to_clones_only_after_commit() {
    use guigu_agent_bridge::storage::{
        CompatibilityGate, MatrixCompatibilityStore,
        recovery_eligibility::{EffectDomain, RecoveryScope, bootstrap_nonadmin},
    };

    let pool = database().await;
    let CompatibilityGate::Native(operation) = MatrixCompatibilityStore::new(pool.clone())
        .gate(true, None)
        .await
        .unwrap()
    else {
        panic!("native required")
    };
    let context = bootstrap_nonadmin(&pool, true).await.unwrap();
    let detached = MatrixRecoveryStore::new_scoped(
        pool.clone(),
        Some(RecoveryScope::new(
            context.clone(),
            EffectDomain::Local,
            None,
        )),
    );
    let store = MatrixRecoveryStore::new_scoped(
        pool.clone(),
        Some(RecoveryScope::new(context, EffectDomain::Local, None)),
    );
    let shared = store.clone();
    sqlx::query("CREATE TRIGGER fail_control_audit BEFORE UPDATE ON offline_recovery_rows BEGIN SELECT RAISE(ABORT,'synthetic-control-audit-failure'); END")
        .execute(&pool).await.unwrap();
    let attempt = store.activate_cutover(&operation, "B", "t1").await;
    sqlx::query("DROP TRIGGER fail_control_audit")
        .execute(&pool)
        .await
        .unwrap();
    assert!(attempt.is_err());
    shared.reconcile_restart().await.unwrap();
    let revision: i64 = sqlx::query_scalar("SELECT revision FROM runtime_eligibility_context")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(revision, 0);
    let control = store.control().await.unwrap().unwrap();
    assert_eq!(control.phase, "baseline");
    assert_eq!(control.watermark, None);
    let (activation, readback) = tokio::join!(
        store.activate_cutover(&operation, "B", "t1"),
        shared.reconcile_restart(),
    );
    activation.unwrap();
    readback.unwrap();
    shared.reconcile_restart().await.unwrap();
    assert!(detached.reconcile_restart().await.is_err());
}

#[tokio::test]
async fn partial_batch_keeps_watermark_until_all_receipts_are_dispositioned() {
    let pool = database().await;
    let store = MatrixRecoveryStore::new(pool.clone());
    store.initialize_cutover("op-1", "t0").await.unwrap();
    store.activate_cutover("op-1", "B", "t1").await.unwrap();
    store
        .acquire_component_lease("MatrixSync", "op-1", "a", "z")
        .await
        .unwrap();
    store
        .begin_batch(
            "batch-1",
            Some("B"),
            "C",
            "digest",
            "B",
            "op-1",
            1,
            &["$one".into(), "$two".into()],
            "2026-01-01T00:00:02Z",
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .admit_event(
                "@a:test",
                "!room:test",
                "$one",
                "op-1",
                "p1",
                "batch-1",
                "t3"
            )
            .await
            .unwrap(),
        BatchAdmission::New
    );
    assert!(
        store
            .complete_batch("batch-1", "C", "op-1", 1, "b")
            .await
            .is_err()
    );
    let watermark: Option<String> =
        sqlx::query_scalar("SELECT watermark FROM matrix_recovery_control WHERE control_id=1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(watermark.as_deref(), Some("B"));

    assert_eq!(
        store
            .admit_event(
                "@a:test",
                "!room:test",
                "$one",
                "op-1",
                "p1",
                "batch-1",
                "t5"
            )
            .await
            .unwrap(),
        BatchAdmission::Duplicate
    );
    store
        .admit_event(
            "@a:test",
            "!room:test",
            "$two",
            "op-1",
            "p2",
            "batch-1",
            "t6",
        )
        .await
        .unwrap();
    store
        .complete_batch("batch-1", "C", "op-1", 1, "b")
        .await
        .unwrap();
    let watermark: Option<String> =
        sqlx::query_scalar("SELECT watermark FROM matrix_recovery_control WHERE control_id=1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(watermark.as_deref(), Some("C"));
}

#[tokio::test]
async fn receipt_digest_mismatch_is_fail_closed() {
    let pool = database().await;
    let store = MatrixRecoveryStore::new(pool);
    store.initialize_cutover("op-2", "t0").await.unwrap();
    store.activate_cutover("op-2", "B", "t1").await.unwrap();
    store
        .acquire_component_lease("MatrixSync", "op-2", "a", "z")
        .await
        .unwrap();
    store
        .begin_batch(
            "batch-2",
            Some("B"),
            "C",
            "digest",
            "B",
            "op-2",
            1,
            &["$one".into()],
            "t2",
        )
        .await
        .unwrap();
    store
        .admit_event(
            "@a:test",
            "!room:test",
            "$one",
            "op-2",
            "p1",
            "batch-2",
            "2026-01-01T00:00:03Z",
        )
        .await
        .unwrap();
    let error = store
        .admit_event(
            "@a:test",
            "!room:test",
            "$one",
            "op-2",
            "different",
            "batch-2",
            "t4",
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("digest mismatch"));
}

#[tokio::test]
async fn pending_receipt_is_not_terminal_until_effect_disposition() {
    let pool = database().await;
    let store = MatrixRecoveryStore::new(pool.clone());
    store.ensure_active("op-3", "B", "t0").await.unwrap();
    store
        .acquire_component_lease("MatrixSync", "op-3", "a", "z")
        .await
        .unwrap();
    store
        .begin_batch(
            "batch-3",
            Some("B"),
            "C",
            "digest",
            "B",
            "op-3",
            1,
            &["$one".into()],
            "t1",
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .reserve_event(
                "@a:test",
                "!room:test",
                "$one",
                "op-3",
                "p1",
                "batch-3",
                "t2"
            )
            .await
            .unwrap(),
        BatchAdmission::New
    );
    let state: String =
        sqlx::query_scalar("SELECT state FROM matrix_recovery_receipts WHERE event_id='$one'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(state, "pending");
    assert!(
        store
            .complete_batch("batch-3", "C", "op-3", 1, "b")
            .await
            .is_err()
    );
    store
        .finish_event(
            "@a:test",
            "!room:test",
            "$one",
            "op-3",
            "batch-3",
            "op-3",
            1,
            "t4",
        )
        .await
        .unwrap();
    store
        .complete_batch("batch-3", "C", "op-3", 1, "b")
        .await
        .unwrap();
}

#[tokio::test]
async fn stale_owner_and_restart_selector_fail_closed() {
    let pool = database().await;
    let store = MatrixRecoveryStore::new(pool.clone());
    store.ensure_active("op-4", "B", "t0").await.unwrap();
    store
        .acquire_component_lease(
            "MatrixSync",
            "owner-a",
            "2026-01-01T00:00:00Z",
            "2026-01-02T00:00:00Z",
        )
        .await
        .unwrap();
    store
        .begin_batch(
            "batch-4",
            Some("B"),
            "C",
            "digest",
            "B",
            "owner-a",
            1,
            &["$one".into()],
            "2026-01-01T01:00:00Z",
        )
        .await
        .unwrap();
    store
        .reserve_event(
            "@a:test",
            "!room:test",
            "$one",
            "op-4",
            "p1",
            "batch-4",
            "2026-01-01T02:00:00Z",
        )
        .await
        .unwrap();
    let unresolved = store.unresolved_receipts().await.unwrap();
    assert_eq!(unresolved, vec![("$one".to_owned(), "pending".to_owned())]);
    assert!(store.reconcile_restart().await.is_err());
    let hold: (String, String, String) = sqlx::query_as(
        "SELECT restart_hold_state,restart_hold_event_id,restart_hold_reason FROM matrix_recovery_control WHERE control_id=1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(hold.0, "pending");
    assert_eq!(hold.1, "$one");
    assert!(hold.2.contains("pending admission"));
    assert!(
        store
            .finish_event(
                "@a:test",
                "!room:test",
                "$one",
                "op-4",
                "batch-4",
                "owner-a",
                1,
                "2026-01-03T00:00:00Z",
            )
            .await
            .is_err()
    );
    assert!(
        store
            .refuse_event(
                "@a:test",
                "!room:test",
                "$one",
                "op-4",
                "batch-4",
                "owner-a",
                1,
                "expired",
                "2026-01-03T00:00:00Z",
            )
            .await
            .is_err()
    );
    assert_eq!(store.unresolved_receipts().await.unwrap(), unresolved);
    store
        .acquire_component_lease(
            "MatrixSync",
            "owner-b",
            "2026-01-03T00:00:00Z",
            "2026-01-04T00:00:00Z",
        )
        .await
        .unwrap();
    assert!(
        store
            .complete_batch("batch-4", "C", "owner-a", 1, "2026-01-03T01:00:00Z")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn activation_requires_operation_provenance_and_reads_back_marker() {
    let pool = database().await;
    let store = MatrixRecoveryStore::new(pool.clone());
    store.initialize_cutover("op-5", "t0").await.unwrap();
    assert!(store.activate_cutover("wrong-op", "B", "t1").await.is_err());
    assert!(store.activate_cutover("op-5", "B", "t1").await.is_ok());
    let marker: (String, String, String) = sqlx::query_as(
        "SELECT phase,cutover_operation_id,watermark FROM matrix_recovery_control WHERE control_id=1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(marker, ("active".into(), "op-5".into(), "B".into()));
}

#[tokio::test]
async fn administrative_activation_intent_prepared_recovery_and_stale_owner_refuse() {
    use guigu_agent_bridge::storage::cutover_digest;
    let pool = database().await;
    let now = chrono::Utc::now();
    let now_text = now.to_rfc3339();
    sqlx::query("INSERT INTO offline_recovery_binding(singleton,protocol,db_generation,canonical_db_digest,root_digest,admin_operation_id,schema_intent_digest,schema_complete_digest,plan_digest,identity_digest,classification_digest,transition_intent_digest,transition_complete_digest,native_operation_id,phase,revision) VALUES(1,2,'generation','db','root','operation','schema','complete','plan','identity','class','intent','complete','native-op','baseline_ready',4)")
        .execute(&pool).await.unwrap();
    sqlx::query(
        "UPDATE runtime_eligibility_bootstrap SET state='admin_reserved' WHERE singleton=1",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO matrix_recovery_control(control_id,cutover_operation_id,phase,watermark,updated_at) VALUES(1,'native-op','baseline',NULL,'t0')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO matrix_recovery_compatibility(singleton,decision,source_schema_version,source_state_digest,native_operation_id,reason_code,created_at) VALUES(1,'native',24,'fixture','native-op','administrative-transition','fixture')").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO matrix_component_leases(component,owner_token,epoch,state,expires_at,updated_at) VALUES('MatrixSync','owner',1,'active',?,?)")
        .bind((now+chrono::Duration::minutes(1)).to_rfc3339()).bind(&now_text).execute(&pool).await.unwrap();
    let store = MatrixRecoveryStore::new(pool.clone());
    assert!(
        store
            .begin_activation_intent(
                "owner",
                1,
                "attempt",
                &cutover_digest(b"request"),
                &now_text
            )
            .await
            .unwrap()
    );
    let state: String = sqlx::query_scalar(
        "SELECT state FROM matrix_activation_intent WHERE native_operation_id='native-op'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(state, "inflight");
    assert!(
        store
            .commit_activation_intent("native-op", "owner", 1, "B", &now_text)
            .await
            .is_err()
    );
    store
        .prepare_activation_intent("owner", 1, "B", &cutover_digest(b"B"), &now_text)
        .await
        .unwrap();
    let prepared: (String, String, String) = sqlx::query_as("SELECT control.phase,binding.phase,intent.state FROM matrix_recovery_control control JOIN offline_recovery_binding binding ON binding.singleton=1 JOIN matrix_activation_intent intent ON intent.native_operation_id=binding.native_operation_id")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(
        prepared,
        (
            "baseline".into(),
            "activation_prepared".into(),
            "prepared".into()
        )
    );
    assert!(
        store
            .prepare_activation_intent("stale", 1, "B", &cutover_digest(b"B"), &now_text)
            .await
            .is_err()
    );
    store
        .commit_activation_intent("native-op", "owner", 1, "B", &now_text)
        .await
        .unwrap();
    let rows: (String, String, String) = sqlx::query_as("SELECT control.phase,binding.phase,intent.state FROM matrix_recovery_control control JOIN offline_recovery_binding binding ON binding.singleton=1 JOIN matrix_activation_intent intent ON intent.native_operation_id=binding.native_operation_id")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(rows, ("active".into(), "active".into(), "committed".into()));
}
