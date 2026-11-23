use super::{
    EffectDomain, RowIdentity, bootstrap_nonadmin, conversation, database, generation, require_row,
};

#[tokio::test]
async fn local_disabled_scope_cannot_bypass_a_native_restart_hold() {
    let (pool, _root) = database().await;
    crate::storage::MatrixCompatibilityStore::new(pool.clone())
        .gate(true, None)
        .await
        .unwrap();
    conversation(&pool).await;
    let context = bootstrap_nonadmin(&pool, false).await.unwrap();
    sqlx::query("UPDATE matrix_recovery_control SET restart_hold_state='pending',restart_hold_reason='synthetic-unknown'")
        .execute(&pool).await.unwrap();
    let identity = RowIdentity {
        table: "conversations".into(),
        key: "[\"text:6C6F63616C\"]".into(),
    };
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    assert!(
        require_row(
            &mut transaction,
            &context,
            EffectDomain::Local,
            None,
            &identity
        )
        .await
        .is_err()
    );
    transaction.rollback().await.unwrap();
    assert!(bootstrap_nonadmin(&pool, false).await.is_err());
}

#[tokio::test]
async fn semantic_outbox_parent_cannot_be_omitted_merely_because_no_foreign_key_exists() {
    let (pool, _root) = database().await;
    sqlx::query("INSERT INTO projection_outbox(source_kind,source_id,projection,transport,stable_txn_id,room_id,render_version,body,body_hash,state,attempt_count,created_at,updated_at) VALUES('task_event','missing-event','terminal_reply','http','synthetic-txn','room','v1','no effect','hash','pending',0,'now','now')")
        .execute(&pool).await.unwrap();
    assert!(bootstrap_nonadmin(&pool, false).await.is_err());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM runtime_eligibility_context")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn disabled_active_native_restart_retains_exact_b_and_detects_uncommitted_drift() {
    let (pool, _root) = database().await;
    crate::storage::MatrixCompatibilityStore::new(pool.clone())
        .gate(true, None)
        .await
        .unwrap();
    sqlx::query("UPDATE matrix_recovery_control SET phase='active',watermark='opaque-B'")
        .execute(&pool)
        .await
        .unwrap();
    let initial = bootstrap_nonadmin(&pool, false).await.unwrap();
    let restarted = bootstrap_nonadmin(&pool, false).await.unwrap();
    assert_eq!(generation(&initial), generation(&restarted));
    sqlx::query("UPDATE matrix_recovery_control SET watermark='unbound-C'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(bootstrap_nonadmin(&pool, false).await.is_err());
    let revision: i64 = sqlx::query_scalar("SELECT revision FROM runtime_eligibility_context")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(revision, 0);
}

#[tokio::test]
async fn unreceipted_matrix_task_cannot_gain_provenance_from_current_operation_alone() {
    let (pool, _root) = database().await;
    crate::storage::MatrixCompatibilityStore::new(pool.clone())
        .gate(true, None)
        .await
        .unwrap();
    sqlx::query("UPDATE matrix_recovery_control SET phase='active',watermark='opaque-B'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO conversations(conversation_id,transport,external_id,participants_json) VALUES('matrix','matrix','!synthetic:example.test','[]')")
        .execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES('unbound','unbound','from','to','matrix','not covered',1,0,0,0)")
        .execute(&pool).await.unwrap();
    assert!(bootstrap_nonadmin(&pool, false).await.is_err());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM offline_recovery_rows")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn completed_local_task_and_prior_events_remain_immutable_terminal_history() {
    let (pool, _root) = database().await;
    conversation(&pool).await;
    sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES('done','done','from','to','local','completed',1,0,0,0)")
        .execute(&pool).await.unwrap();
    for (sequence, status) in [(1, "queued"), (2, "dispatched"), (3, "completed")] {
        sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,'done',?,?,'now','{}')")
            .bind(format!("event-{sequence}")).bind(sequence).bind(status).execute(&pool).await.unwrap();
    }
    let context = bootstrap_nonadmin(&pool, false).await.unwrap();
    let dispositions: Vec<String> = sqlx::query_scalar(
        "SELECT disposition FROM offline_recovery_rows WHERE table_name IN ('tasks','task_events')",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(dispositions, vec!["terminal_evidenced"; 4]);
    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let task = RowIdentity {
        table: "tasks".into(),
        key: "[\"text:646F6E65\"]".into(),
    };
    assert!(
        require_row(&mut transaction, &context, EffectDomain::Local, None, &task)
            .await
            .is_err()
    );
    transaction.rollback().await.unwrap();
    bootstrap_nonadmin(&pool, false).await.unwrap();
}
