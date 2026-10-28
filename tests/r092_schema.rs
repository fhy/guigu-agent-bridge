use guigu_agent_bridge::storage;
use sqlx::Row;

async fn pool() -> (sqlx::SqlitePool, tempfile::TempDir) {
    let root = tempfile::Builder::new()
        .prefix("r092-schema-")
        .tempdir_in("/tmp")
        .unwrap();
    let pool = storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    storage::migrate(&pool).await.unwrap();
    (pool, root)
}

#[tokio::test]
async fn schema25_is_additive_and_creates_only_pending_authority() {
    let (pool, _root) = pool().await;
    let version: i64 = sqlx::query_scalar("SELECT max(version) FROM _sqlx_migrations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(version, 25);
    let marker = sqlx::query("SELECT state,bootstrap_nonce,generation,inventory_digest FROM runtime_eligibility_bootstrap")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(marker.get::<String, _>("state"), "pending");
    assert_eq!(marker.get::<String, _>("bootstrap_nonce").len(), 32);
    assert!(marker.get::<Option<String>, _>("generation").is_none());
    assert!(
        marker
            .get::<Option<String>, _>("inventory_digest")
            .is_none()
    );
    for table in [
        "offline_recovery_binding",
        "runtime_eligibility_context",
        "offline_recovery_rows",
        "matrix_activation_intent",
    ] {
        let query = sqlx::AssertSqlSafe(format!("SELECT count(*) FROM {table}"));
        let count: i64 = sqlx::query_scalar(query).fetch_one(&pool).await.unwrap();
        assert_eq!(count, 0);
    }
}

#[tokio::test]
async fn incomplete_active_binding_and_unbound_matrix_activation_refuse() {
    let (pool, _root) = pool().await;
    let incomplete = sqlx::query("INSERT INTO offline_recovery_binding(singleton,protocol,db_generation,canonical_db_digest,root_digest,admin_operation_id,schema_intent_digest,phase,revision) VALUES (1,2,'generation','database','root','operation','intent','active',0)")
        .execute(&pool).await;
    assert!(incomplete.is_err());
    let unbound = sqlx::query("INSERT INTO matrix_activation_intent(native_operation_id,plan_digest,identity_digest,attempt_id,component_owner,component_epoch,request_digest,state,revision,created_at) VALUES ('native','plan','identity','attempt','owner',1,'request','inflight',0,'2026-10-28T00:00:00Z')")
        .execute(&pool).await;
    assert!(unbound.is_err());
}

#[tokio::test]
async fn admin_root_identity_is_immutable_and_revisions_are_cas_fenced() {
    let (pool, _root) = pool().await;
    sqlx::query("INSERT INTO offline_recovery_binding(singleton,protocol,db_generation,canonical_db_digest,root_digest,admin_operation_id,schema_intent_digest,phase,revision) VALUES (1,2,'generation','database','root','operation','intent','schema_pending',0)")
        .execute(&pool).await.unwrap();
    assert!(
        sqlx::query(
            "UPDATE offline_recovery_binding SET root_digest='replacement',revision=revision+1"
        )
        .execute(&pool)
        .await
        .is_err()
    );
    assert!(
        sqlx::query("UPDATE offline_recovery_binding SET revision=revision+2")
            .execute(&pool)
            .await
            .is_err()
    );
    sqlx::query("UPDATE offline_recovery_binding SET schema_complete_digest='complete',phase='schema_ready',revision=revision+1 WHERE revision=0")
        .execute(&pool).await.unwrap();
    let stale =
        sqlx::query("UPDATE offline_recovery_binding SET revision=revision+1 WHERE revision=0")
            .execute(&pool)
            .await
            .unwrap();
    assert_eq!(stale.rows_affected(), 0);
}
