use sqlx::SqlitePool;

use super::{EffectDomain, EligibilityContext, RowIdentity, bootstrap_nonadmin, require_row};

#[path = "bootstrap_history_tests.rs"]
mod history;

pub(super) async fn database() -> (SqlitePool, tempfile::TempDir) {
    let root = tempfile::Builder::new()
        .prefix("r092-bootstrap-")
        .tempdir_in("/tmp")
        .unwrap();
    let pool = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    (pool, root)
}

async fn conversation(pool: &SqlitePool) {
    sqlx::query(
        "INSERT INTO conversations(conversation_id,participants_json) VALUES('local','[]')",
    )
    .execute(pool)
    .await
    .unwrap();
}

fn generation(context: &EligibilityContext) -> &str {
    match context {
        EligibilityContext::NonAdmin { generation, .. } => generation,
        _ => panic!("nonadmin context required"),
    }
}

#[tokio::test]
async fn pristine_disabled_bootstrap_is_atomic_and_does_not_create_matrix_state() {
    let (pool, _root) = database().await;
    let context = bootstrap_nonadmin(&pool, false).await.unwrap();
    let restarted = bootstrap_nonadmin(&pool, false).await.unwrap();
    assert_eq!(generation(&context), generation(&restarted));
    let count: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM matrix_recovery_control)+(SELECT count(*) FROM matrix_recovery_compatibility)+(SELECT count(*) FROM offline_recovery_rows)")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(count, 0);
    let state: String = sqlx::query_scalar("SELECT state FROM runtime_eligibility_bootstrap")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "complete");
}

#[tokio::test]
async fn proven_local_inventory_is_preserved_without_business_row_changes() {
    let (pool, _root) = database().await;
    conversation(&pool).await;
    sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES('local-task','local-task','from','to','local','never sent',1,0,0,0)")
        .execute(&pool).await.unwrap();
    let before: Vec<(String, i64)> = sqlx::query_as("SELECT text,version FROM tasks")
        .fetch_all(&pool)
        .await
        .unwrap();
    let context = bootstrap_nonadmin(&pool, false).await.unwrap();
    let identity = RowIdentity {
        table: "tasks".into(),
        key: "[\"text:6C6F63616C2D7461736B\"]".into(),
    };
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    require_row(
        &mut transaction,
        &context,
        EffectDomain::Local,
        None,
        &identity,
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();
    let disposition: Vec<String> =
        sqlx::query_scalar("SELECT disposition FROM offline_recovery_rows")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(disposition, vec!["eligible_preserved"; 2]);
    let after: Vec<(String, i64)> = sqlx::query_as("SELECT text,version FROM tasks")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(before, after);
    bootstrap_nonadmin(&pool, false).await.unwrap();
}

#[tokio::test]
async fn restart_does_not_bulk_adopt_new_rows_or_replace_a_generation() {
    let (pool, _root) = database().await;
    let initial = bootstrap_nonadmin(&pool, false).await.unwrap();
    conversation(&pool).await;
    assert!(bootstrap_nonadmin(&pool, false).await.is_err());
    let stored: String = sqlx::query_scalar("SELECT generation FROM runtime_eligibility_context")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(generation(&initial), stored);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM offline_recovery_rows")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn restart_rejects_changed_bytes_even_with_unchanged_primary_key() {
    let (pool, _root) = database().await;
    conversation(&pool).await;
    bootstrap_nonadmin(&pool, false).await.unwrap();
    sqlx::query("UPDATE conversations SET participants_json='[\"drift\"]'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(bootstrap_nonadmin(&pool, false).await.is_err());
}

#[tokio::test]
async fn uncertain_pre25_work_rolls_back_context_and_all_classifications() {
    let (pool, _root) = database().await;
    conversation(&pool).await;
    sqlx::query("INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES('old','now','now','active','unknown-owner')")
        .execute(&pool).await.unwrap();
    assert!(bootstrap_nonadmin(&pool, false).await.is_err());
    let count: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM offline_recovery_rows)+(SELECT count(*) FROM runtime_eligibility_context)")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(count, 0);
    let state: String = sqlx::query_scalar("SELECT state FROM runtime_eligibility_bootstrap")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "pending");
}

#[tokio::test]
async fn enabled_without_validated_native_authority_does_not_fabricate_an_operation() {
    let (pool, _root) = database().await;
    assert!(bootstrap_nonadmin(&pool, true).await.is_err());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_eligibility_context")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn disabled_native_baseline_preserves_operation_and_null_watermark() {
    let (pool, _root) = database().await;
    let gate = crate::storage::MatrixCompatibilityStore::new(pool.clone())
        .gate(true, None)
        .await
        .unwrap();
    let crate::storage::CompatibilityGate::Native(operation) = gate else {
        panic!("native required");
    };
    let context = bootstrap_nonadmin(&pool, false).await.unwrap();
    assert_eq!(context.matrix_operation(), Some(operation.as_str()));
    let state: (String, Option<String>) =
        sqlx::query_as("SELECT phase,watermark FROM matrix_recovery_control")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(state, ("baseline".into(), None));
    bootstrap_nonadmin(&pool, false).await.unwrap();
}

#[tokio::test]
async fn native_mode_revision_fences_a_previous_context_without_relabeling_rows() {
    let (pool, _root) = database().await;
    crate::storage::MatrixCompatibilityStore::new(pool.clone())
        .gate(true, None)
        .await
        .unwrap();
    conversation(&pool).await;
    let old = bootstrap_nonadmin(&pool, false).await.unwrap();
    let current = bootstrap_nonadmin(&pool, true).await.unwrap();
    assert_eq!(generation(&old), generation(&current));
    let identity = RowIdentity {
        table: "conversations".into(),
        key: "[\"text:6C6F63616C\"]".into(),
    };
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    assert!(
        require_row(&mut transaction, &old, EffectDomain::Local, None, &identity)
            .await
            .is_err()
    );
    require_row(
        &mut transaction,
        &current,
        EffectDomain::Local,
        None,
        &identity,
    )
    .await
    .unwrap();
}
