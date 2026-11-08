use super::*;

struct OverlapSender {
    pool: sqlx::SqlitePool,
    calls: std::sync::atomic::AtomicUsize,
}
impl crate::matrix::MatrixOutboxSender for OverlapSender {
    fn send_stable<'a>(
        &'a self,
        _: &'a str,
        _: Option<&'a str>,
        _: Option<&'a str>,
        _: &'a str,
        _: &'a str,
    ) -> crate::matrix::ReplyFuture<'a> {
        Box::pin(async move {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            sqlx::query("INSERT INTO matrix_recovery_batches(batch_id,request_token,previous_watermark,response_token,response_digest,component,owner_token,owner_epoch,expected_events_json,covered_events_json,state,created_at,updated_at) VALUES('overlap','B','B','C','digest','MatrixSync','owner',1,'[]','[]','processing','t','t')")
                .execute(&self.pool).await.unwrap();
            Ok(())
        })
    }
}

#[tokio::test]
async fn projection_sync_overlap_retries_only_completion_without_resending() {
    let (fixture, store) = seeded().await;
    let sender = std::sync::Arc::new(OverlapSender {
        pool: fixture.pool.clone(),
        calls: Default::default(),
    });
    let drain = crate::app::OutboxDrain::new(store, sender.clone(), 4);
    assert!(drain.drain_once().await.is_err());
    assert_eq!(state(&fixture).await, ("sending".into(), 0, 1));
    sqlx::query("UPDATE matrix_recovery_batches SET state='terminal' WHERE batch_id='overlap'")
        .execute(&fixture.pool)
        .await
        .unwrap();
    assert_eq!(drain.clone().drain_once().await.unwrap(), 1);
    assert_eq!(state(&fixture).await, ("sent".into(), 1, 1));
    assert_eq!(drain.drain_once().await.unwrap(), 0);
    assert_eq!(sender.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

struct SuspendedSender {
    entered: tokio::sync::Notify,
    calls: std::sync::atomic::AtomicUsize,
}
impl crate::matrix::MatrixOutboxSender for SuspendedSender {
    fn send_stable<'a>(
        &'a self,
        _: &'a str,
        _: Option<&'a str>,
        _: Option<&'a str>,
        _: &'a str,
        _: &'a str,
    ) -> crate::matrix::ReplyFuture<'a> {
        Box::pin(async move {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.entered.notify_one();
            std::future::pending().await
        })
    }
}

#[tokio::test]
async fn projection_cancelled_send_stays_unknown_and_is_never_repeated_by_clone() {
    let (fixture, store) = seeded().await;
    let sender = std::sync::Arc::new(SuspendedSender {
        entered: Default::default(),
        calls: Default::default(),
    });
    let drain = crate::app::OutboxDrain::new(store, sender.clone(), 4);
    let other = drain.clone();
    let task = tokio::spawn(async move { other.drain_once().await });
    sender.entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(drain.drain_once().await.is_err());
    assert_eq!(sender.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(state(&fixture).await, ("sending".into(), 0, 1));
}

#[tokio::test]
async fn lease_maintenance_during_foreign_batch_does_not_authorize_projection_effects() {
    let (fixture, store) = seeded().await;
    let sender = OverlapSender {
        pool: fixture.pool.clone(),
        calls: Default::default(),
    };
    crate::matrix::MatrixOutboxSender::send_stable(&sender, "", None, None, "", "")
        .await
        .unwrap();
    let now = chrono::Utc::now().to_rfc3339();
    store.renew_outbox_component(&now).await.unwrap();
    assert!(
        store
            .claim_projections("claim", &now, "unused", 1)
            .await
            .is_err()
    );
    assert_eq!(state(&fixture).await, ("pending".into(), 0, 0));
    let future = (chrono::Utc::now() + chrono::Duration::minutes(2)).to_rfc3339();
    assert!(store.renew_outbox_component(&future).await.is_err());
    // An invalid scope cannot be converted into maintenance authority.
    for fence in [
        None,
        Some(crate::storage::recovery_eligibility::LiveFence {
            component: "OutboxDrain".into(),
            owner: "different".into(),
            epoch: 1,
        }),
    ] {
        let scoped = crate::storage::MatrixRecoveryStore::new_scoped(
            fixture.pool.clone(),
            Some(RecoveryScope::new(
                fixture.context.clone(),
                EffectDomain::Matrix,
                fence,
            )),
        );
        assert!(
            scoped
                .renew_component_lease("OutboxDrain", "outbox-owner", 1, &now)
                .await
                .is_err()
        );
    }
    // A genuine checkpoint hold still forbids even maintenance.
    sqlx::query("UPDATE matrix_recovery_control SET restart_hold_state='processing'")
        .execute(&fixture.pool)
        .await
        .unwrap();
    assert!(store.renew_outbox_component(&now).await.is_err());
    fixture.pool.close().await;
}
