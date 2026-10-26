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
async fn partial_batch_keeps_watermark_until_all_receipts_are_dispositioned() {
    let pool = database().await;
    let store = MatrixRecoveryStore::new(pool.clone());
    store.initialize_cutover("op-1", "t0").await.unwrap();
    store.activate_cutover("B", "t1").await.unwrap();
    store
        .begin_batch(
            "batch-1",
            Some("B"),
            "C",
            "digest",
            &["$one".into(), "$two".into()],
            "t2",
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
    assert!(store.complete_batch("batch-1", "C", "t4").await.is_err());
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
    store.complete_batch("batch-1", "C", "t7").await.unwrap();
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
    store.activate_cutover("B", "t1").await.unwrap();
    store
        .begin_batch("batch-2", Some("B"), "C", "digest", &["$one".into()], "t2")
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
            "t3",
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
