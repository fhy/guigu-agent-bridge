use chrono::{Duration, Utc};
use guigu_agent_bridge::a2a::wire::{Message, Part, Role};
use guigu_agent_bridge::a2a::{A2aStore, InboundReservation};
use guigu_agent_bridge::storage::{connect, migrate};

async fn database() -> (sqlx::SqlitePool, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!("a2a-store-{}.db", uuid::Uuid::now_v7()));
    let pool = connect(&path).await.unwrap();
    migrate(&pool).await.unwrap();
    (pool, path)
}

#[tokio::test]
async fn reservation_is_durable_idempotent_and_conflict_detecting() {
    let (pool, path) = database().await;
    let store = A2aStore::new(pool.clone());
    let first = store
        .reserve_inbound("peer-a", "req-1", "hash-a", "ctx", Utc::now())
        .await
        .unwrap();
    let id = match first {
        InboundReservation::New(row) => row.exchange_id,
        _ => panic!("new"),
    };
    let replay = store
        .reserve_inbound("peer-a", "req-1", "hash-a", "ctx", Utc::now())
        .await
        .unwrap();
    assert!(matches!(replay, InboundReservation::Replay(row) if row.exchange_id == id));
    assert!(
        store
            .reserve_inbound("peer-a", "req-1", "different", "ctx", Utc::now())
            .await
            .is_err()
    );
    drop(store);
    pool.close().await;
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn cleanup_is_fenced_by_active_runtime_and_revision() {
    let (pool, path) = database().await;
    let store = A2aStore::new(pool.clone());
    let now = Utc::now();
    let row = match store
        .reserve_inbound("peer", "req", "hash", "ctx", now)
        .await
        .unwrap()
    {
        InboundReservation::New(row) => row,
        _ => unreachable!(),
    };
    let bytes = store
        .store_message(
            &row.exchange_id,
            &Message {
                message_id: "message".into(),
                role: Role::User,
                parts: vec![Part::Text {
                    text: "retained payload".into(),
                }],
            },
            now,
        )
        .await
        .unwrap();
    assert_eq!(bytes, 16);
    assert!(
        store
            .mark_terminal(&row.exchange_id, 0, "failed", now - Duration::hours(2))
            .await
            .unwrap()
    );
    sqlx::query("INSERT INTO runtime_instances VALUES (?,?,?,?,?)")
        .bind("runtime-a")
        .bind(now.to_rfc3339())
        .bind(now.to_rfc3339())
        .bind("active")
        .bind("test")
        .execute(&pool)
        .await
        .unwrap();
    let claims = store
        .claim_cleanup("runtime-a", now - Duration::hours(1), 32, now)
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert!(
        !store
            .clean_claim(
                "runtime-a",
                &guigu_agent_bridge::a2a::CleanupClaim {
                    exchange_id: row.exchange_id.clone(),
                    revision: claims[0].revision + 1
                },
                now
            )
            .await
            .unwrap()
    );
    assert!(
        store
            .clean_claim("runtime-a", &claims[0], now)
            .await
            .unwrap()
    );
    let parts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM a2a_message_parts")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        parts, 0,
        "claimed content is removed but the exchange tombstone remains"
    );
    assert!(
        store
            .exchange(&row.exchange_id)
            .await
            .unwrap()
            .unwrap()
            .content_cleaned
    );
    assert_eq!(store.retained_terminal_bytes().await.unwrap(), 0);
    drop(store);
    pool.close().await;
    std::fs::remove_file(path).unwrap();
}
