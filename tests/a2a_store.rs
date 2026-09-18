use chrono::{Duration, Utc};
use guigu_agent_bridge::a2a::wire::{Message, Part, Role};
use guigu_agent_bridge::a2a::{A2aStore, A2aTerminalProjection, InboundReservation};
use guigu_agent_bridge::bus::EventConsumer;
use guigu_agent_bridge::models::{EventId, TaskEvent, TaskEventPayload, TaskId, TaskStatus};
use guigu_agent_bridge::storage::{connect, migrate};

async fn database() -> (sqlx::SqlitePool, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!("a2a-store-{}.db", uuid::Uuid::now_v7()));
    let pool = connect(&path).await.unwrap();
    migrate(&pool).await.unwrap();
    (pool, path)
}

async fn insert_task(pool: &sqlx::SqlitePool, status: &str) -> String {
    let task_id = TaskId::generate().to_string();
    let conversation = uuid::Uuid::now_v7().to_string();
    sqlx::query("INSERT INTO conversations VALUES (?,NULL,NULL,NULL,'[]')")
        .bind(&conversation)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO tasks VALUES (?,?,NULL,?,?,?,NULL,'test',5,0,0,NULL,0)")
        .bind(&task_id)
        .bind(&task_id)
        .bind(uuid::Uuid::now_v7().to_string())
        .bind(uuid::Uuid::now_v7().to_string())
        .bind(conversation)
        .execute(pool)
        .await
        .unwrap();
    insert_event(pool, &task_id, 1, status).await;
    task_id
}

async fn insert_event(pool: &sqlx::SqlitePool, task_id: &str, seq: i64, status: &str) {
    sqlx::query("INSERT INTO task_events VALUES (?,?,?,?,?,?)")
        .bind(uuid::Uuid::now_v7().to_string())
        .bind(task_id)
        .bind(seq)
        .bind(status)
        .bind(Utc::now().to_rfc3339())
        .bind("{}")
        .execute(pool)
        .await
        .unwrap();
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
    let task_id = insert_task(&pool, "running").await;
    assert!(
        store
            .bind_task(&row.exchange_id, &task_id, "external", bytes, now)
            .await
            .unwrap()
    );
    assert!(
        store
            .mark_terminal(&row.exchange_id, 1, "failed", now - Duration::hours(2))
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
    assert!(
        claims.is_empty(),
        "active authoritative task cannot be claimed"
    );
    insert_event(&pool, &task_id, 2, "failed").await;
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
        {
            insert_event(&pool, &task_id, 3, "running").await;
            !store
                .clean_claim("runtime-a", &claims[0], now)
                .await
                .unwrap()
        },
        "delete rechecks the authoritative latest event"
    );
    insert_event(&pool, &task_id, 4, "failed").await;
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

#[tokio::test]
async fn terminal_projection_advances_inbound_and_outbound_from_authoritative_events() {
    let (pool, path) = database().await;
    let store = A2aStore::new(pool.clone());
    let task_id = insert_task(&pool, "running").await;
    let now = Utc::now();
    let inbound = match store
        .reserve_inbound("peer", "in", "hash", "ctx", now)
        .await
        .unwrap()
    {
        InboundReservation::New(row) => row,
        _ => unreachable!(),
    };
    store
        .bind_task(&inbound.exchange_id, &task_id, "remote-in", 0, now)
        .await
        .unwrap();
    let outbound = match store
        .reserve_outbound("peer", "out", &task_id, "ctx", now)
        .await
        .unwrap()
    {
        InboundReservation::New(row) => row,
        _ => unreachable!(),
    };
    store
        .acknowledge_outbound(&outbound.exchange_id, "remote-out", "working", now)
        .await
        .unwrap();

    insert_event(&pool, &task_id, 2, "completed").await;
    let projection = A2aTerminalProjection::new(store.clone());
    projection
        .consume(&TaskEvent {
            id: EventId::generate(),
            task_id: task_id.parse().unwrap(),
            seq: 2,
            status: TaskStatus::Completed,
            timestamp: now,
            payload: TaskEventPayload::Completed {
                output: "done".into(),
            },
        })
        .await
        .unwrap();
    for exchange_id in [inbound.exchange_id, outbound.exchange_id] {
        let row = store.exchange(&exchange_id).await.unwrap().unwrap();
        assert_eq!(row.state, "completed");
        assert_eq!(row.revision, 2);
    }
    drop(store);
    pool.close().await;
    std::fs::remove_file(path).unwrap();
}
