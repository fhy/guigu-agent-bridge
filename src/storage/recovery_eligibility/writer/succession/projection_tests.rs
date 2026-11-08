use super::{
    tests::{Fixture, fixture},
    *,
};

async fn seed(f: &Fixture) {
    let scope = RecoveryScope::new(
        crate::storage::recovery_eligibility::readback_nonadmin(&f.pool, true)
            .await
            .unwrap(),
        EffectDomain::Matrix,
        Some(crate::storage::recovery_eligibility::LiveFence {
            component: "MatrixSync".into(),
            owner: "owner".into(),
            epoch: f.epoch,
        }),
    );
    let mut tx = super::super::begin(&f.pool, Some(&scope)).await.unwrap();
    for (query, table, key) in [
        (
            "INSERT INTO conversations(conversation_id,participants_json) VALUES('conversation','[]')",
            "conversations",
            "conversation",
        ),
        (
            "INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES('task','task','from','to','conversation','fixture',1,0,0,0)",
            "tasks",
            "task",
        ),
        (
            "INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES('event','task',1,'completed','t','{}')",
            "task_events",
            "event",
        ),
        (
            "INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES('task','terminal',1,'owner','t','t')",
            "task_admissions",
            "task",
        ),
    ] {
        sqlx::query(query).execute(&mut *tx).await.unwrap();
        super::super::inserted(&mut tx, Some(&scope), &text_identity(table, key).unwrap())
            .await
            .unwrap();
    }
    sqlx::query("INSERT INTO projection_outbox(source_kind,source_id,projection,transport,stable_txn_id,room_id,render_version,body,body_hash,state,attempt_count,claim_owner,claim_revision,created_at,updated_at) VALUES('task_event','event','observer','matrix','txn','!room:example.test','v1','notice','hash','sending',0,'claim',1,'t','t')")
        .execute(&mut *tx).await.unwrap();
    super::super::inserted(
        &mut tx,
        Some(&scope),
        &composite_text_identity("projection_outbox", &["task_event", "event", "observer"])
            .unwrap(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn succession_preserves_isolated_terminal_notification_without_replay() {
    let f = fixture(false).await;
    seed(&f).await;
    let mut c = f.pool.acquire().await.unwrap();
    let state = state::read(&mut c).await.unwrap().unwrap();
    drop(c);
    assert_eq!(state.isolated_projections.len(), 1);
    let proof = Exclusion::modeled(state.owner.clone());
    publish(&f.pool, &state, &proof, &[], || Ok(()))
        .await
        .unwrap();
    assert!(
        confirmed(&f.root.path().join("state.db"), &state, &proof)
            .await
            .unwrap()
    );
    let row: (String, String, i64, i64) = sqlx::query_as(
        "SELECT state,claim_owner,claim_revision,attempt_count FROM projection_outbox",
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(row, ("sending".into(), "claim".into(), 1, 0));
    let record:String=sqlx::query_scalar("SELECT child_snapshot FROM runtime_owner_evidence WHERE instance_token='owner' AND is_current=1").fetch_one(&f.pool).await.unwrap();
    let record: Value = serde_json::from_str(&record).unwrap();
    assert_eq!(
        record["source"]["isolated_projections"][0]["stable_txn_id"],
        "txn"
    );
    assert_eq!(record["historical_joined"], false);
    f.pool.close().await;
}

#[tokio::test]
async fn succession_refuses_pending_projection_missing_claim_or_nonterminal_parent() {
    for change in [
        "UPDATE projection_outbox SET state='pending'",
        "UPDATE projection_outbox SET claim_owner=NULL",
        "UPDATE task_admissions SET state='running'",
        "UPDATE task_events SET status='running'",
    ] {
        let f = fixture(false).await;
        seed(&f).await;
        sqlx::query(change).execute(&f.pool).await.unwrap();
        let mut c = f.pool.acquire().await.unwrap();
        assert!(state::read(&mut c).await.is_err());
        drop(c);
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT state FROM runtime_instances WHERE instance_token='owner'"
            )
            .fetch_one(&f.pool)
            .await
            .unwrap(),
            "stopping"
        );
        f.pool.close().await;
    }
}

#[tokio::test]
async fn succession_never_combines_unresolved_notification_with_interrupted_input() {
    let f = fixture(false).await;
    seed(&f).await;
    f.store
        .begin_batch(
            "interrupted",
            Some("B"),
            "C",
            "digest",
            "B",
            "owner",
            f.epoch,
            &[],
            &chrono::Utc::now().to_rfc3339(),
        )
        .await
        .unwrap();
    let mut c = f.pool.acquire().await.unwrap();
    assert!(
        state::read(&mut c)
            .await
            .unwrap_err()
            .to_string()
            .contains("input-and-output-uncertainty")
    );
    drop(c);
    f.pool.close().await;
}
