use chrono::Utc;
use guigu_agent_bridge::models::{DeliveryId, EndpointId, TaskId};
use guigu_agent_bridge::storage::{
    BusinessStoreOwner, Delivery, Repository, SqliteRepository, StorageError,
};

#[tokio::test]
async fn owner_delivery_domain_methods_are_consistent() {
    let path = std::env::temp_dir().join(format!(
        "guigu-owner-domain-delivery-{}.db",
        uuid::Uuid::now_v7()
    ));
    let owner = BusinessStoreOwner::open(&path).expect("owner");
    owner
        .execute(|connection| {
            connection
                .execute_batch("CREATE TABLE deliveries(delivery_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, attempt INTEGER NOT NULL, target_endpoint_id TEXT NOT NULL, dispatched_at TEXT NOT NULL, acknowledged_at TEXT); CREATE TABLE task_events(event_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, seq INTEGER NOT NULL, status TEXT NOT NULL, timestamp TEXT NOT NULL, payload TEXT NOT NULL)")
                .map_err(|error| StorageError::OwnerQuery(error.to_string()))
        })
        .expect("schema");
    let repository = SqliteRepository::new_owner(owner);
    let delivery = Delivery::new(
        DeliveryId::generate(),
        TaskId::generate(),
        1,
        EndpointId::generate(),
        Utc::now(),
    );
    repository.record_delivery(&delivery).await.expect("record");
    assert!(
        repository
            .get_delivery(delivery.delivery_id())
            .await
            .expect("get")
            .is_some()
    );
    assert_eq!(
        repository
            .unacknowledged_deliveries()
            .await
            .expect("unack")
            .len(),
        1
    );
    assert!(
        repository
            .deliveries_awaiting_outcome()
            .await
            .expect("pending")
            .is_empty()
    );
    assert_eq!(
        repository
            .acknowledge_delivery(delivery.delivery_id(), Utc::now())
            .await
            .expect("ack"),
        guigu_agent_bridge::storage::AckOutcome::Recorded
    );
    assert_eq!(
        repository
            .acknowledge_delivery(delivery.delivery_id(), Utc::now())
            .await
            .expect("replay"),
        guigu_agent_bridge::storage::AckOutcome::AlreadyAcknowledged
    );
    assert!(
        repository
            .unacknowledged_deliveries()
            .await
            .expect("unack after")
            .is_empty()
    );
}
