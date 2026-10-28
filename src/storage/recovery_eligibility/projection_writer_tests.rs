use crate::storage::{
    ReliabilityStore,
    recovery_eligibility::{RecoveryScope, RowIdentity, writer},
};

use super::{EffectDomain, Fixture, task_and_event};

async fn seeded() -> (Fixture, ReliabilityStore) {
    let fixture = Fixture::create().await;
    let (repository, task, event) = task_and_event(&fixture).await;
    repository
        .insert_task_and_event(&task, &event)
        .await
        .unwrap();
    sqlx::query("INSERT INTO matrix_component_leases(component,owner_token,epoch,state,expires_at,updated_at) VALUES('OutboxDrain','outbox-owner',1,'active','2099-01-01T00:00:00Z','2026-10-28T00:00:00Z')")
        .execute(&fixture.pool).await.unwrap();
    let scope = RecoveryScope::new(
        fixture.context.clone(),
        EffectDomain::Matrix,
        Some(crate::storage::recovery_eligibility::LiveFence {
            component: "OutboxDrain".into(),
            owner: "outbox-owner".into(),
            epoch: 1,
        }),
    );
    let mut transaction = writer::begin(&fixture.pool, Some(&scope)).await.unwrap();
    let event_id = event.id.to_string();
    sqlx::query("INSERT INTO projection_outbox(source_kind,source_id,projection,transport,stable_txn_id,room_id,render_version,body,body_hash,state,attempt_count,created_at,updated_at) VALUES('task_event',?,'terminal_reply','matrix','fresh-txn','!synthetic:example.test','v1','new output','hash','pending',0,'2026-10-28T00:00:00Z','2026-10-28T00:00:00Z')")
        .bind(&event_id).execute(&mut *transaction).await.unwrap();
    let keys = ["task_event", &event_id, "terminal_reply"].map(|value| {
        format!(
            "text:{}",
            value
                .as_bytes()
                .iter()
                .map(|byte| format!("{byte:02X}"))
                .collect::<String>()
        )
    });
    writer::inserted(
        &mut transaction,
        Some(&scope),
        &RowIdentity {
            table: "projection_outbox".into(),
            key: serde_json::to_string(&keys).unwrap(),
        },
    )
    .await
    .unwrap();
    transaction.commit().await.unwrap();
    let store = ReliabilityStore::with_recovery(fixture.pool.clone(), scope);
    (fixture, store)
}

async fn state(fixture: &Fixture) -> (String, i64, i64) {
    sqlx::query_as("SELECT state,attempt_count,COALESCE(claim_revision,0) FROM projection_outbox")
        .fetch_one(&fixture.pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn scoped_projection_claim_and_sent_update_business_and_classification_without_replay() {
    let (fixture, store) = seeded().await;
    let claimed = store
        .claim_projections("outbox-owner", "2026-10-28T01:00:00Z", "unused", 1)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(state(&fixture).await, ("sending".into(), 0, 1));
    assert!(store.mark_projection_sent(&claimed[0]).await.unwrap());
    assert!(!store.mark_projection_sent(&claimed[0]).await.unwrap());
    assert_eq!(state(&fixture).await, ("sent".into(), 1, 1));
    assert!(
        store
            .claim_projections("another", "2026-10-28T02:00:00Z", "unused", 1)
            .await
            .unwrap()
            .is_empty()
    );
    let row: (String, String, String) = sqlx::query_as("SELECT canonical_pk,original_revision,original_digest FROM offline_recovery_rows WHERE table_name='projection_outbox'")
        .fetch_one(&fixture.pool).await.unwrap();
    let snapshot = super::super::super::inventory::snapshot(
        &mut fixture.pool.acquire().await.unwrap(),
        &RowIdentity {
            table: "projection_outbox".into(),
            key: row.0,
        },
    )
    .await
    .unwrap();
    assert_eq!((row.1, row.2), (snapshot.revision, snapshot.digest));
}

#[tokio::test]
async fn projection_history_and_unscoped_entrypoint_cannot_claim() {
    let (fixture, store) = seeded().await;
    assert!(
        ReliabilityStore::new(fixture.pool.clone())
            .claim_projections("bypass", "now", "unused", 1)
            .await
            .is_err()
    );
    sqlx::query("UPDATE offline_recovery_rows SET disposition='terminal_evidenced' WHERE table_name='projection_outbox'")
        .execute(&fixture.pool).await.unwrap();
    assert!(
        store
            .claim_projections("owner", "now", "unused", 1)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(state(&fixture).await, ("pending".into(), 0, 0));
}

#[tokio::test]
async fn projection_post_send_expiry_preserves_nonreplayable_sending_and_claim_digest() {
    let (fixture, store) = seeded().await;
    let claimed = store
        .claim_projections("owner", "now", "unused", 1)
        .await
        .unwrap();
    let digest: String = sqlx::query_scalar(
        "SELECT original_digest FROM offline_recovery_rows WHERE table_name='projection_outbox'",
    )
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    sqlx::query("UPDATE matrix_component_leases SET expires_at='2000-01-01T00:00:00Z'")
        .execute(&fixture.pool)
        .await
        .unwrap();
    assert!(store.mark_projection_sent(&claimed[0]).await.is_err());
    assert!(store.mark_projection_abandoned(&claimed[0]).await.is_err());
    assert!(store.authorize_projection_send(&claimed[0]).await.is_err());
    assert_eq!(state(&fixture).await, ("sending".into(), 0, 1));
    let after: String = sqlx::query_scalar(
        "SELECT original_digest FROM offline_recovery_rows WHERE table_name='projection_outbox'",
    )
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert_eq!(after, digest);
}

#[tokio::test]
async fn projection_abandonment_rejects_wrong_claim_and_never_requeues() {
    let (fixture, store) = seeded().await;
    let mut claimed = store
        .claim_projections("owner", "now", "unused", 1)
        .await
        .unwrap();
    claimed[0].claim_revision += 1;
    assert!(!store.mark_projection_abandoned(&claimed[0]).await.unwrap());
    assert_eq!(state(&fixture).await, ("sending".into(), 0, 1));
    claimed[0].claim_revision -= 1;
    assert!(store.mark_projection_abandoned(&claimed[0]).await.unwrap());
    assert_eq!(state(&fixture).await, ("abandoned".into(), 1, 1));
    assert!(
        store
            .claim_projections("again", "now", "unused", 1)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn projection_missing_classification_is_refusal_not_idle() {
    let (fixture, store) = seeded().await;
    sqlx::query("INSERT INTO projection_outbox(source_kind,source_id,projection,transport,stable_txn_id,room_id,render_version,body,body_hash,state,attempt_count,created_at,updated_at) SELECT source_kind,source_id,'observer','matrix','unclassified-txn',room_id,render_version,body,body_hash,'pending',0,'2020-01-01T00:00:00Z','2020-01-01T00:00:00Z' FROM projection_outbox")
        .execute(&fixture.pool)
        .await
        .unwrap();
    assert!(
        store
            .claim_projections("owner", "now", "unused", 1)
            .await
            .is_err()
    );
    assert_eq!(state(&fixture).await, ("pending".into(), 0, 0));
}

#[tokio::test]
async fn projection_terminal_write_error_rolls_back_classification_and_leaves_sending() {
    let (fixture, store) = seeded().await;
    let claimed = store
        .claim_projections("owner", "now", "unused", 1)
        .await
        .unwrap();
    let digest: String = sqlx::query_scalar(
        "SELECT original_digest FROM offline_recovery_rows WHERE table_name='projection_outbox'",
    )
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    sqlx::query("CREATE TRIGGER synthetic_terminal_failure BEFORE UPDATE ON projection_outbox WHEN NEW.state IN ('sent','abandoned') BEGIN SELECT RAISE(ABORT,'synthetic terminal write failure'); END")
        .execute(&fixture.pool).await.unwrap();
    assert!(store.mark_projection_sent(&claimed[0]).await.is_err());
    assert!(store.mark_projection_abandoned(&claimed[0]).await.is_err());
    assert_eq!(state(&fixture).await, ("sending".into(), 0, 1));
    let after: String = sqlx::query_scalar(
        "SELECT original_digest FROM offline_recovery_rows WHERE table_name='projection_outbox'",
    )
    .fetch_one(&fixture.pool)
    .await
    .unwrap();
    assert_eq!(after, digest);
    assert!(
        store
            .claim_projections("another", "now", "unused", 1)
            .await
            .unwrap()
            .is_empty()
    );
}

struct Sender {
    pool: sqlx::SqlitePool,
    calls: std::sync::atomic::AtomicUsize,
    expire: bool,
}

impl crate::matrix::MatrixOutboxSender for Sender {
    fn send_stable<'a>(
        &'a self,
        _room: &'a str,
        _thread: Option<&'a str>,
        _reply: Option<&'a str>,
        _body: &'a str,
        _txn: &'a str,
    ) -> crate::matrix::ReplyFuture<'a> {
        Box::pin(async move {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.expire {
                sqlx::query("UPDATE matrix_component_leases SET expires_at='2000-01-01T00:00:00Z' WHERE component='OutboxDrain'")
                    .execute(&self.pool).await.unwrap();
            }
            Ok(())
        })
    }
}

#[tokio::test]
async fn projection_real_drain_entrypoint_sends_only_new_eligible_work_once() {
    let (fixture, store) = seeded().await;
    let sender = std::sync::Arc::new(Sender {
        pool: fixture.pool.clone(),
        calls: Default::default(),
        expire: false,
    });
    let drain = crate::app::OutboxDrain::new(store, sender.clone(), 4);
    assert_eq!(drain.drain_once().await.unwrap(), 1);
    assert_eq!(drain.drain_once().await.unwrap(), 0);
    assert_eq!(sender.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(state(&fixture).await, ("sent".into(), 1, 1));
}

#[tokio::test]
async fn projection_real_drain_post_send_expiry_keeps_unknown_effect_nonreplayable() {
    let (fixture, store) = seeded().await;
    let sender = std::sync::Arc::new(Sender {
        pool: fixture.pool.clone(),
        calls: Default::default(),
        expire: true,
    });
    let drain = crate::app::OutboxDrain::new(store, sender.clone(), 4);
    assert!(drain.drain_once().await.is_err());
    assert!(drain.drain_once().await.is_err());
    assert_eq!(sender.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(state(&fixture).await, ("sending".into(), 0, 1));
}

#[tokio::test]
async fn projection_wrong_component_and_local_domain_cannot_claim_matrix_send() {
    let (fixture, _) = seeded().await;
    for (domain, fence) in [
        (EffectDomain::Matrix, Some(fixture.fence.clone())),
        (EffectDomain::Local, None),
    ] {
        let store = ReliabilityStore::with_recovery(
            fixture.pool.clone(),
            RecoveryScope::new(fixture.context.clone(), domain, fence),
        );
        assert!(
            store
                .claim_projections("owner", "now", "unused", 1)
                .await
                .is_err()
        );
    }
    assert_eq!(state(&fixture).await, ("pending".into(), 0, 0));
}
