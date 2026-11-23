use guigu_agent_bridge::{
    matrix::MatrixError,
    storage::{CutoverStore, connect, cutover_digest, cutover_event_identity, migrate},
};
use sqlx::SqlitePool;

async fn fixture() -> (tempfile::TempDir, SqlitePool, CutoverStore) {
    let root = tempfile::tempdir().unwrap();
    let pool = connect(&root.path().join("state.db")).await.unwrap();
    migrate(&pool).await.unwrap();
    let store = CutoverStore::prepare(
        pool.clone(),
        "operation",
        &cutover_digest(b"binding"),
        &cutover_digest(b"archive"),
    )
    .await
    .unwrap();
    (root, pool, store)
}

#[tokio::test]
async fn pending_activation_reopen_and_watermark_are_separate() {
    let (root, pool, store) = fixture().await;
    assert!(store.state().await.unwrap().baseline.is_none());
    assert!(store.activate("").await.is_err());
    store.activate("A").await.unwrap();
    let state = store.state().await.unwrap();
    store.commit_batch(&state, "B").await.unwrap();
    assert!(store.commit_batch(&state, "C").await.is_err());
    pool.close().await;
    let reopened = connect(&root.path().join("state.db")).await.unwrap();
    let store = CutoverStore::open(reopened, &cutover_digest(b"binding"))
        .await
        .unwrap()
        .unwrap();
    let state = store.state().await.unwrap();
    assert_eq!(state.baseline.as_deref(), Some("A"));
    assert_eq!(state.watermark.as_deref(), Some("B"));
    assert_eq!(state.revision, 3);
}

#[tokio::test]
async fn binding_operation_archive_and_baseline_drift_refuse() {
    let (_root, pool, store) = fixture().await;
    let digest = cutover_digest(b"binding");
    assert!(
        CutoverStore::open(pool.clone(), &cutover_digest(b"wrong"))
            .await
            .is_err()
    );
    assert!(
        CutoverStore::prepare(pool.clone(), "other", &digest, &cutover_digest(b"archive"))
            .await
            .is_err()
    );
    assert!(
        CutoverStore::prepare(
            pool.clone(),
            "operation",
            &digest,
            &cutover_digest(b"other archive")
        )
        .await
        .is_err()
    );
    assert!(
        CutoverStore::prepare(
            pool.clone(),
            "operation",
            &digest,
            &cutover_digest(b"archive")
        )
        .await
        .is_ok()
    );
    store.activate("A").await.unwrap();
    assert!(store.activate("B").await.is_err());
    sqlx::query("UPDATE matrix_cutover SET baseline='drift'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(store.state().await.is_err());
}

#[tokio::test]
async fn completed_receipts_dedup_after_reopen_and_partial_batch() {
    let (root, pool, store) = fixture().await;
    store.activate("A").await.unwrap();
    assert!(
        store
            .begin_event("!room", "$first", "digest1")
            .await
            .unwrap()
    );
    store
        .finish_event("!room", "$first", "digest1")
        .await
        .unwrap();
    pool.close().await;
    let pool = connect(&root.path().join("state.db")).await.unwrap();
    let store = CutoverStore::open(pool.clone(), &cutover_digest(b"binding"))
        .await
        .unwrap()
        .unwrap();
    assert!(
        !store
            .begin_event("!room", "$first", "digest1")
            .await
            .unwrap()
    );
    assert!(
        store
            .begin_event("!room", "$second", "digest2")
            .await
            .unwrap()
    );
    let state = store.state().await.unwrap();
    assert!(store.commit_batch(&state, "B").await.is_err());
    store
        .finish_event("!room", "$second", "digest2")
        .await
        .unwrap();
    store.commit_batch(&state, "B").await.unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM matrix_cutover_dispositions WHERE state='complete'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn interrupted_unknown_effect_is_durably_manual_and_never_reclaimed() {
    let (root, pool, store) = fixture().await;
    store.activate("A").await.unwrap();
    assert!(
        store
            .begin_event("!room", "$admin", "digest")
            .await
            .unwrap()
    );
    pool.close().await;
    let pool = connect(&root.path().join("state.db")).await.unwrap();
    let store = CutoverStore::open(pool.clone(), &cutover_digest(b"binding"))
        .await
        .unwrap()
        .unwrap();
    for _attempt in 0..2 {
        assert!(matches!(
            store.begin_event("!room", "$admin", "digest").await,
            Err(MatrixError::CutoverRefused)
        ));
    }
    let state: String = sqlx::query_scalar("SELECT state FROM matrix_cutover_dispositions")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "manual_required");
    assert_eq!(store.state().await.unwrap().watermark.as_deref(), Some("A"));
}

#[tokio::test]
async fn content_drift_never_reexecutes_a_completed_event() {
    let (_root, pool, store) = fixture().await;
    store.activate("A").await.unwrap();
    store
        .begin_event("!room", "$event", "original")
        .await
        .unwrap();
    store
        .finish_event("!room", "$event", "original")
        .await
        .unwrap();
    assert!(
        store
            .begin_event("!room", "$event", "changed")
            .await
            .is_err()
    );
    assert!(
        store
            .begin_event("!room", "$event", "original")
            .await
            .is_err()
    );
    let state: String = sqlx::query_scalar("SELECT state FROM matrix_cutover_dispositions")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "manual_required");
}

#[tokio::test]
async fn activation_failure_leaves_pending_and_cas_failure_preserves_watermark() {
    let (_root, pool, store) = fixture().await;
    sqlx::query("CREATE TRIGGER refuse_activation BEFORE UPDATE ON matrix_cutover BEGIN SELECT RAISE(ABORT,'fixture'); END")
        .execute(&pool).await.unwrap();
    assert!(store.activate("A").await.is_err());
    assert!(store.state().await.unwrap().baseline.is_none());
    sqlx::query("DROP TRIGGER refuse_activation")
        .execute(&pool)
        .await
        .unwrap();
    store.activate("A").await.unwrap();
    let mut stale = store.state().await.unwrap();
    stale.revision += 1;
    assert!(store.commit_batch(&stale, "B").await.is_err());
    assert_eq!(store.state().await.unwrap().watermark.as_deref(), Some("A"));
}

#[test]
fn event_digest_ignores_only_unsigned_data_and_refuses_missing_identity() {
    let first = r#"{"event_id":"$event","content":{"body":"one"},"unsigned":{"age":1}}"#;
    let replay = r#"{"event_id":"$event","content":{"body":"one"},"unsigned":{"age":2}}"#;
    assert_eq!(
        cutover_event_identity(first).unwrap(),
        cutover_event_identity(replay).unwrap()
    );
    assert_ne!(
        cutover_event_identity(first).unwrap(),
        cutover_event_identity(&first.replace("one", "two")).unwrap()
    );
    assert!(cutover_event_identity("{}").is_err());
}
