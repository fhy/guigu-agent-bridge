use super::*;
use crate::storage::recovery_eligibility::{EffectDomain, RecoveryScope, bootstrap_nonadmin};

#[tokio::test]
async fn scoped_session_composite_identity_preserves_updates_and_refuses_drift() {
    let root = tempfile::Builder::new()
        .prefix("r092-session-identity-")
        .tempdir_in("/tmp")
        .unwrap();
    let pool = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    let first = EndpointId::generate();
    let second = EndpointId::generate();
    let conversation = ConversationId::generate();
    for (endpoint, name) in [(first, "first"), (second, "second")] {
        sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,?,'acp',1,'[]')").bind(encode_id(endpoint)).bind(name).execute(&pool).await.unwrap();
    }
    sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES(?,'[]')")
        .bind(encode_id(conversation))
        .execute(&pool)
        .await
        .unwrap();
    let context = bootstrap_nonadmin(&pool, false).await.unwrap();
    let store = SqliteSessionStore::new_scoped(
        pool.clone(),
        Some(RecoveryScope::new(context, EffectDomain::Local, None)),
    );
    let now = Utc::now();
    assert!(matches!(
        store
            .set_state("missing", first, SessionState::Ready, now)
            .await,
        Err(StorageError::NotFound { .. })
    ));
    let session = StoredSession::new(
        "shared-session-id",
        first,
        conversation,
        "/tmp",
        "synthetic/1",
        now,
    )
    .unwrap();
    let other = StoredSession::new(
        "shared-session-id",
        second,
        conversation,
        "/tmp",
        "synthetic/1",
        now,
    )
    .unwrap();
    store.upsert_session(&session).await.unwrap();
    store.upsert_session(&other).await.unwrap();
    store
        .set_state("shared-session-id", first, SessionState::Prompting, now)
        .await
        .unwrap();
    store.upsert_session(&session).await.unwrap();
    assert_eq!(
        store
            .live_session(first, conversation, "/tmp")
            .await
            .unwrap()
            .unwrap()
            .state(),
        SessionState::Ready
    );
    assert_eq!(
        store
            .live_session(second, conversation, "/tmp")
            .await
            .unwrap()
            .unwrap()
            .state(),
        SessionState::Ready
    );
    let classified: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM offline_recovery_rows WHERE table_name='sessions'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(classified, 2);
    sqlx::query("UPDATE sessions SET backend_id='tampered' WHERE endpoint_id=?")
        .bind(encode_id(first))
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        store
            .live_session(first, conversation, "/tmp")
            .await
            .is_err()
    );
    assert!(
        store
            .live_session(second, conversation, "/tmp")
            .await
            .unwrap()
            .is_some()
    );
}
