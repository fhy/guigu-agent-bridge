use super::*;
use crate::storage::recovery_eligibility::{
    EffectDomain, RecoveryScope, bootstrap_nonadmin, writer,
};
use crate::storage::{
    CompatibilityGate, MatrixCompatibilityStore, MatrixRecoveryStore, ReliabilityStore,
    RuntimeOwnerEvidence, connect, migrate,
};

#[tokio::test]
async fn matrix_scoped_publication_uses_lifecycle_writer_and_fence() {
    let root = tempfile::tempdir().unwrap();
    let pool = connect(root.path().join("state.db")).await.unwrap();
    migrate(&pool).await.unwrap();
    let CompatibilityGate::Native(operation) = MatrixCompatibilityStore::new(pool.clone())
        .gate(true, None)
        .await
        .unwrap()
    else {
        panic!("native");
    };
    let context = bootstrap_nonadmin(&pool, true).await.unwrap();
    let scope = RecoveryScope::new(context, EffectDomain::LocalHold, None);
    let recovery = MatrixRecoveryStore::new_scoped(pool.clone(), Some(scope.clone()));
    recovery
        .activate_cutover(&operation, "B", "activation")
        .await
        .unwrap();
    let epoch = crate::admission_hold::acquire_fence(&recovery, &scope, "collector", true)
        .await
        .unwrap()
        .unwrap();
    let store = ReliabilityStore::with_recovery(pool.clone(), scope.clone());
    let evidence = RuntimeOwnerEvidence::capture().unwrap();
    let now = Utc::now().to_rfc3339();
    assert!(
        store
            .begin_runtime_with_evidence("owner", "fp", &now, &evidence)
            .await
            .unwrap()
    );
    let binding = "{\"systemd_capsule_v1\":\"test\"}".to_owned();
    let mut transaction = writer::begin(&pool, Some(&scope)).await.unwrap();
    let key: String = sqlx::query_scalar("SELECT json_array('text:'||hex(CAST(instance_token AS BLOB)),'integer:'||hex(CAST(runtime_generation AS BLOB)),'integer:'||hex(CAST(evidence_revision AS BLOB))) FROM runtime_owner_evidence WHERE instance_token='owner'").fetch_one(&mut *transaction).await.unwrap();
    let identity = crate::storage::recovery_eligibility::RowIdentity {
        table: "runtime_owner_evidence".into(),
        key,
    };
    let mutation = writer::prepare_mutation(&mut transaction, Some(&scope), &identity)
        .await
        .unwrap();
    sqlx::query("UPDATE runtime_owner_evidence SET child_snapshot=? WHERE instance_token='owner'")
        .bind(&binding)
        .execute(&mut *transaction)
        .await
        .unwrap();
    writer::complete_mutation(&mut transaction, Some(&scope), mutation)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(store.stopping_runtime("owner", &now).await.unwrap());
    assert!(store.stop_runtime("owner", &now).await.unwrap());
    let mut capsule = super::tests::modeled();
    capsule.pid = evidence.pid;
    capsule.starttime = evidence.proc_starttime;
    capsule.boot = evidence.boot_id;
    capsule.boottime = evidence.boottime_ns;
    let terminal = terminal(&capsule, &super::tests::terminal_fields(&capsule)).unwrap();
    let proof = Proof {
        capsule,
        binding,
        terminal,
        observed_at: now,
        boottime: evidence.boottime_ns,
    };
    assert!(store.finalize_systemd_owner(&proof, 2).await.unwrap());
    recovery
        .release_component_lease("MatrixSync", "collector", epoch, &Utc::now().to_rfc3339())
        .await
        .unwrap();
    let state: String = sqlx::query_scalar(
        "SELECT state FROM runtime_owner_evidence WHERE instance_token='owner' AND is_current=1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(state, "reaped");
}
