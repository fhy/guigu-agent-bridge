use crate::{
    models::{
        AgentTask, EndpointId, EventId, Message, MessageId, Priority, TaskEvent, TaskEventPayload,
        TaskId, TaskStatus,
    },
    storage::{Repository, SqliteRepository, recovery_eligibility::RecoveryScope},
};

use super::{EffectDomain, Fixture};

async fn new_message(fixture: &Fixture) -> Message {
    let conversation: String = sqlx::query_scalar("SELECT conversation_id FROM conversations")
        .fetch_one(&fixture.pool)
        .await
        .unwrap();
    Message {
        id: MessageId::generate(),
        conversation: conversation.parse().unwrap(),
        sender: EndpointId::generate(),
        recipient: EndpointId::generate(),
        body: "fresh independent message".into(),
        reply_to: None,
        metadata: Default::default(),
    }
}

fn scoped(fixture: &Fixture, anchor: bool) -> SqliteRepository {
    let scope = RecoveryScope::new(
        fixture.context.clone(),
        EffectDomain::Matrix,
        Some(fixture.fence.clone()),
    );
    let scope = if anchor {
        scope.with_metadata_anchor(fixture.parent.identity.clone())
    } else {
        scope
    };
    SqliteRepository::with_recovery(fixture.pool.clone(), scope)
}

#[tokio::test]
async fn repository_inserts_new_message_and_classification_atomically_through_exact_anchor() {
    let fixture = Fixture::create().await;
    let message = new_message(&fixture).await;
    let repository = scoped(&fixture, true);
    repository.insert_message(&message).await.unwrap();
    repository.insert_message(&message).await.unwrap();
    assert_eq!(
        repository.get_message(message.id).await.unwrap(),
        Some(message.clone())
    );
    let rows: (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM messages),(SELECT count(*) FROM offline_recovery_rows WHERE table_name='messages' AND disposition='eligible_new' AND matrix_operation_id='native')")
        .fetch_one(&fixture.pool).await.unwrap();
    assert_eq!(rows, (1, 1));
    let original: String = sqlx::query_scalar(
        "SELECT disposition FROM offline_recovery_rows WHERE table_name='conversations'",
    )
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert_eq!(original, "terminal_evidenced");
    assert!(
        SqliteRepository::new(fixture.pool.clone())
            .insert_message(&new_message(&fixture).await)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn repository_without_explicit_metadata_role_rolls_back_business_insert() {
    let fixture = Fixture::create().await;
    assert!(
        scoped(&fixture, false)
            .insert_message(&new_message(&fixture).await)
            .await
            .is_err()
    );
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM messages")
        .fetch_one(&fixture.pool)
        .await
        .unwrap();
    assert_eq!(rows, 0);
}

async fn task_and_event(fixture: &Fixture) -> (SqliteRepository, AgentTask, TaskEvent) {
    let message = new_message(fixture).await;
    let repository = scoped(fixture, true);
    repository.insert_message(&message).await.unwrap();
    let id = TaskId::generate();
    let task = AgentTask {
        task_id: id,
        root_task_id: id,
        parent_task_id: None,
        from_agent: message.sender,
        to_agent: message.recipient,
        conversation_id: message.conversation,
        reply_to: Some(message.id),
        text: "independent new task".into(),
        priority: Priority::DEFAULT,
        depth: 0,
        hops: 0,
        deadline: None,
        version: 0,
    };
    let event = TaskEvent {
        id: EventId::generate(),
        task_id: id,
        seq: 1,
        status: TaskStatus::Queued,
        timestamp: chrono::Utc::now(),
        payload: TaskEventPayload::Queued,
    };
    (repository, task, event)
}

#[tokio::test]
async fn repository_task_event_atomic_pair_cannot_replay_a_terminal_administrative_parent() {
    let fixture = Fixture::create().await;
    let (repository, task, event) = task_and_event(&fixture).await;
    repository
        .insert_task_and_event(&task, &event)
        .await
        .unwrap();
    repository
        .insert_task_and_event(&task, &event)
        .await
        .unwrap();
    assert_eq!(
        repository
            .compare_and_increment_version(task.task_id, 0)
            .await
            .unwrap(),
        1
    );
    assert!(
        repository
            .compare_and_increment_version(task.task_id, 0)
            .await
            .is_err()
    );
    let rows: (i64, i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM tasks),(SELECT count(*) FROM task_events),(SELECT count(*) FROM offline_recovery_rows WHERE table_name IN ('tasks','task_events'))")
        .fetch_one(&fixture.pool).await.unwrap();
    assert_eq!(rows, (1, 1, 2));
    sqlx::query("UPDATE offline_recovery_rows SET disposition='terminal_evidenced' WHERE table_name='tasks'")
        .execute(&fixture.pool).await.unwrap();
    assert!(
        repository
            .insert_task_and_event(&task, &event)
            .await
            .is_err()
    );
    assert!(
        repository
            .append_event(&TaskEvent {
                id: EventId::generate(),
                seq: 2,
                ..event
            })
            .await
            .is_err()
    );
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM task_events")
        .fetch_one(&fixture.pool)
        .await
        .unwrap();
    assert_eq!(events, 1);
}

#[tokio::test]
async fn expired_owner_rolls_back_real_repository_admission_and_preserves_anchor() {
    let fixture = Fixture::create().await;
    let repository = scoped(&fixture, true);
    sqlx::query("UPDATE matrix_component_leases SET expires_at='2000-01-01T00:00:00Z'")
        .execute(&fixture.pool)
        .await
        .unwrap();
    assert!(
        repository
            .insert_message(&new_message(&fixture).await)
            .await
            .is_err()
    );
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM messages")
        .fetch_one(&fixture.pool)
        .await
        .unwrap();
    assert_eq!(rows, 0);
    let snapshot = super::inventory::snapshot(
        &mut fixture.pool.acquire().await.unwrap(),
        &fixture.parent.identity,
    )
    .await
    .unwrap();
    assert_eq!(snapshot.digest, fixture.parent.digest);
}
