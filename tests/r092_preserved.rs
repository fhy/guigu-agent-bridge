use guigu_agent_bridge::storage;
use sqlx::{Row, SqliteConnection, SqlitePool};

async fn database() -> (SqlitePool, tempfile::TempDir) {
    let directory = tempfile::Builder::new()
        .prefix("r092-preserved-")
        .tempdir_in("/tmp")
        .unwrap();
    let pool = storage::connect(directory.path().join("state.db"))
        .await
        .unwrap();
    storage::migrate(&pool).await.unwrap();
    (pool, directory)
}

async fn context(connection: &mut SqliteConnection) {
    sqlx::query("INSERT INTO runtime_eligibility_context(singleton,generation,bootstrap_nonce,provenance_kind,matrix_phase,state,revision,created_at,bootstrap_inventory_digest) SELECT 1,'generation',bootstrap_nonce,'pristine_local','absent','ready',0,'2026-10-28T00:00:00Z','inventory' FROM runtime_eligibility_bootstrap WHERE state='pending'")
        .execute(connection).await.unwrap();
}

async fn classification(
    connection: &mut SqliteConnection,
    key: &str,
    disposition: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO offline_recovery_rows(table_name,canonical_pk,origin_generation,original_revision,original_digest,parent_relations,disposition,evidence_digest,context_kind,context_generation) VALUES('tasks',?,'generation','0','original','[]',?,'verified-inventory','nonadmin','generation')")
        .bind(key).bind(disposition).execute(connection).await?;
    Ok(())
}

async fn complete(connection: &mut SqliteConnection) {
    let changed = sqlx::query("UPDATE runtime_eligibility_bootstrap SET state='complete',generation='generation',inventory_digest='inventory' WHERE state='pending'")
        .execute(connection).await.unwrap();
    assert_eq!(changed.rows_affected(), 1);
}

#[tokio::test]
async fn preserved_origin_is_only_created_in_the_original_pending_bootstrap() {
    let (pool, directory) = database().await;
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    context(&mut tx).await;
    classification(&mut tx, "existing-task", "eligible_preserved")
        .await
        .unwrap();
    complete(&mut tx).await;
    tx.commit().await.unwrap();
    pool.close().await;
    let pool = storage::connect(directory.path().join("state.db"))
        .await
        .unwrap();
    let row = sqlx::query("SELECT bootstrap.state,bootstrap.generation,row.disposition,row.canonical_pk FROM runtime_eligibility_bootstrap AS bootstrap JOIN offline_recovery_rows AS row ON row.context_generation=bootstrap.generation")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(row.get::<String, _>(0), "complete");
    assert_eq!(row.get::<String, _>(1), "generation");
    assert_eq!(row.get::<String, _>(2), "eligible_preserved");
    assert_eq!(row.get::<String, _>(3), "existing-task");
    let mut connection = pool.acquire().await.unwrap();
    assert!(
        classification(&mut connection, "late-task", "eligible_preserved")
            .await
            .is_err()
    );
    classification(&mut connection, "new-child", "eligible_new")
        .await
        .unwrap();
    assert!(sqlx::query("UPDATE runtime_eligibility_bootstrap SET state='pending',generation=NULL,inventory_digest=NULL")
        .execute(&mut *connection).await.is_err());
    assert!(
        sqlx::query("DELETE FROM runtime_eligibility_bootstrap")
            .execute(&mut *connection)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn historical_holds_and_preserved_origins_cannot_be_promoted_or_relabelled() {
    let (pool, _directory) = database().await;
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    context(&mut tx).await;
    for disposition in [
        "terminal_evidenced",
        "quarantined_no_effect",
        "uncertain_hold",
        "refused_hold",
        "eligible_preserved",
    ] {
        classification(&mut tx, disposition, disposition)
            .await
            .unwrap();
    }
    complete(&mut tx).await;
    tx.commit().await.unwrap();
    for disposition in [
        "terminal_evidenced",
        "quarantined_no_effect",
        "uncertain_hold",
        "refused_hold",
        "eligible_preserved",
    ] {
        for next in ["eligible_new", "eligible_preserved"] {
            if disposition == next {
                continue;
            }
            assert!(
                sqlx::query("UPDATE offline_recovery_rows SET disposition=? WHERE canonical_pk=?")
                    .bind(next)
                    .bind(disposition)
                    .execute(&pool)
                    .await
                    .is_err()
            );
        }
    }
    assert!(sqlx::query("UPDATE offline_recovery_rows SET context_generation='grafted',origin_generation='grafted' WHERE canonical_pk='eligible_preserved'")
        .execute(&pool).await.is_err());
}

#[tokio::test]
async fn bootstrap_transaction_rollback_keeps_pending_nonce_without_partial_classification() {
    let (pool, _directory) = database().await;
    let before: String =
        sqlx::query_scalar("SELECT bootstrap_nonce FROM runtime_eligibility_bootstrap")
            .fetch_one(&pool)
            .await
            .unwrap();
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    context(&mut tx).await;
    classification(&mut tx, "first", "eligible_preserved")
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let row = sqlx::query("SELECT state,bootstrap_nonce FROM runtime_eligibility_bootstrap")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>(0), "pending");
    assert_eq!(row.get::<String, _>(1), before);
    for table in ["runtime_eligibility_context", "offline_recovery_rows"] {
        let count: i64 =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT count(*) FROM {table}")))
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 0);
    }
    assert!(sqlx::query("UPDATE runtime_eligibility_bootstrap SET state='complete',generation='forged',inventory_digest='fake'")
        .execute(&pool).await.is_err());
}

#[tokio::test]
async fn admin_inventory_cannot_use_preserved_or_mix_with_nonadmin_context() {
    let (pool, _directory) = database().await;
    assert!(sqlx::query("INSERT INTO offline_recovery_rows(table_name,canonical_pk,origin_generation,original_revision,original_digest,parent_relations,disposition,evidence_digest,context_kind,admin_operation_id,plan_digest) VALUES('tasks','old','admin-generation','0','digest','[]','eligible_preserved','evidence','admin','admin-operation','plan')")
        .execute(&pool).await.is_err());
    let mut tx = pool.begin().await.unwrap();
    context(&mut tx).await;
    assert!(sqlx::query("INSERT INTO offline_recovery_binding(singleton,protocol,db_generation,canonical_db_digest,root_digest,admin_operation_id,schema_intent_digest,phase,revision) VALUES(1,2,'admin-generation','db','root','admin-operation','intent','schema_pending',0)")
        .execute(&mut *tx).await.is_err());
}
