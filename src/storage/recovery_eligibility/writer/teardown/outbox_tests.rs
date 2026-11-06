use super::*;

#[tokio::test]
async fn refused_stop_samples_current_owner_after_heartbeat_expires() {
    let fixture = fixture(true).await;
    let future = (Utc::now() + chrono::Duration::minutes(5)).to_rfc3339();
    assert!(
        fixture
            .owner
            .stopping_runtime("owner", &future)
            .await
            .is_err()
    );
    let before: String =
        sqlx::query_scalar("SELECT state FROM runtime_instances WHERE instance_token='owner'")
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
    assert_eq!(before, "active");
    tokio::time::sleep(std::time::Duration::from_secs(31)).await;
    let now = Utc::now().to_rfc3339();
    assert!(fixture.owner.stopping_runtime("owner", &now).await.unwrap());
    let captured: String = sqlx::query_scalar("SELECT captured_at FROM runtime_owner_evidence WHERE instance_token='owner' AND is_current=1")
        .fetch_one(&fixture.pool).await.unwrap();
    assert_eq!(captured, now);
    let (outbox, epoch) = fixture.outbox.as_ref().unwrap();
    outbox
        .release_component_lease("OutboxDrain", "owner", *epoch, &now)
        .await
        .unwrap();
    fixture
        .sync
        .release_component_lease("MatrixSync", "owner", fixture.epoch, &now)
        .await
        .unwrap();
    let gate = crate::acp::AcpSpawnGate::for_generation("owner");
    gate.close_and_join().await;
    gate.reap_transports().await;
    assert!(
        fixture
            .owner
            .finish_controlled_stop(
                "owner",
                &Utc::now().to_rfc3339(),
                &RuntimeOwnerEvidence::capture().unwrap(),
                &gate.joined_children().unwrap()
            )
            .await
            .unwrap()
    );
    let state: String =
        sqlx::query_scalar("SELECT state FROM runtime_instances WHERE instance_token='owner'")
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
    assert_eq!(state, "stopped");
    let reaped: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime_owner_evidence WHERE state='reaped'")
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
    assert_eq!(reaped, 0);
    fixture.pool.close().await;
}

#[tokio::test]
async fn refused_outbox_release_and_controlled_stop_do_not_release_another_component() {
    let fixture = fixture(true).await;
    let before = preserved(&fixture.pool).await;
    let (outbox, epoch) = fixture.outbox.as_ref().unwrap();
    let now = Utc::now().to_rfc3339();
    assert!(
        outbox
            .release_component_lease("MatrixSync", "owner", fixture.epoch, &now)
            .await
            .is_err()
    );
    fixture.owner.stopping_runtime("owner", &now).await.unwrap();
    outbox
        .release_component_lease("OutboxDrain", "owner", *epoch, &now)
        .await
        .unwrap();
    let active: i64 =
        sqlx::query_scalar("SELECT count(*) FROM matrix_component_leases WHERE state='active'")
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
    assert_eq!(active, 1);
    fixture
        .sync
        .release_component_lease("MatrixSync", "owner", fixture.epoch, &now)
        .await
        .unwrap();
    let gate = crate::acp::AcpSpawnGate::for_generation("owner");
    gate.close_and_join().await;
    gate.reap_transports().await;
    for _attempt in 0..2 {
        assert!(
            fixture
                .owner
                .finish_controlled_stop(
                    "owner",
                    &Utc::now().to_rfc3339(),
                    &RuntimeOwnerEvidence::capture().unwrap(),
                    &gate.joined_children().unwrap()
                )
                .await
                .unwrap()
        );
    }
    assert_eq!(preserved(&fixture.pool).await, before);
    fixture.pool.close().await;
}
