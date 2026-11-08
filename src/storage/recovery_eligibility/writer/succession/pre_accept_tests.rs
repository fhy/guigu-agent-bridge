use super::{
    tests::{Fixture, fixture},
    *,
};
const TASK: &str = "00000000-0000-4000-8000-000000000001";
const DELIVERY: &str = "00000000-0000-4000-8000-000000000002";
// SQL below interpolates only fixed fixture UUIDs and a typed UUID payload; no external input.
async fn seeded() -> Fixture {
    let f = fixture(false).await;
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
    let queued = serde_json::to_string(&crate::models::TaskEventPayload::Queued).unwrap();
    let payload = serde_json::to_string(&crate::models::TaskEventPayload::Dispatched {
        delivery_id: DELIVERY.parse().unwrap(),
        attempt: 1,
    })
    .unwrap();
    for (query,table,key) in [
        ("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES('agent','agent','acp',1,'[]')".into(),"agents","agent"),
        ("INSERT INTO conversations(conversation_id,transport,external_id,participants_json) VALUES('conversation','matrix','!room:example.test','[]')".into(),"conversations","conversation"),
        (format!("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES('{TASK}','{TASK}','agent','agent','conversation','harmless',1,0,0,1)"),"tasks",TASK),
        (format!("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES('queued','{TASK}',1,'queued','t','{queued}')"),"task_events","queued"),
        (format!("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES('dispatched','{TASK}',2,'dispatched','t','{payload}')"),"task_events","dispatched"),
        (format!("INSERT INTO task_admissions(task_id,state,revision,runtime_instance,reply_room,reply_event_id,render_version,created_at,updated_at) VALUES('{TASK}','dispatching',1,'owner','!room:example.test','$request','v1','t','t')"),"task_admissions",TASK),
        (format!("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES('{DELIVERY}','{TASK}',1,'agent','2000-01-01T00:00:00Z')"),"deliveries",DELIVERY),
        (format!("INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state) VALUES('{DELIVERY}','{TASK}',1,'prepared')"),"delivery_dispositions",DELIVERY),
        (format!("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES('resource','{TASK}','lease-owner',1,'active','t','t','2000-01-01T00:00:00Z')"),"execution_leases","resource"),
        (format!("INSERT INTO workspace_claims(claim_id,runtime_owner,task_id,canonical_path,path_components_json,owner_fence,revision,state,created_at,updated_at) VALUES('claim','lease-owner','{TASK}','/tmp','[\"/\",\"tmp\"]',1,1,'active','t','t')"),"workspace_claims","claim"),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(query)).execute(&mut *tx).await.unwrap();
        super::super::inserted(&mut tx,Some(&scope),&text_identity(table,key).unwrap()).await.unwrap();
    }
    tx.commit().await.unwrap();
    f
}
async fn input(f: &Fixture) -> (State, Exclusion) {
    let mut c = f.pool.acquire().await.unwrap();
    let s = state::read(&mut c).await.unwrap().unwrap();
    let p = Exclusion::modeled(s.owner.clone());
    (s, p)
}

#[tokio::test]
async fn pre_accept_succession_closes_only_unaccepted_task_without_historical_reap() {
    let f = seeded().await;
    let (s, p) = input(&f).await;
    publish(&f.pool, &s, &p, &[], || Ok(())).await.unwrap();
    assert!(
        confirmed(&f.root.path().join("state.db"), &s, &p)
            .await
            .unwrap()
    );
    let row: (String, Option<String>, Option<String>) =
        sqlx::query_as("SELECT state,reap_status,reap_at FROM delivery_dispositions")
            .fetch_one(&f.pool)
            .await
            .unwrap();
    assert_eq!(row, ("terminal".into(), None, None));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM offline_recovery_workspace_deletions")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        1
    );
    assert!(publish(&f.pool, &s, &p, &[], || Ok(())).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM task_events WHERE status='failed'")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM projection_outbox WHERE state='pending'"
        )
        .fetch_one(&f.pool)
        .await
        .unwrap(),
        1
    );
    f.pool.close().await;
}

#[tokio::test]
async fn pre_accept_succession_refuses_acknowledged_output_workspace_or_revision_drift() {
    for query in [
        "UPDATE agents SET transport='http'".to_owned(),
        "UPDATE deliveries SET acknowledged_at='z'".to_owned(),
        "UPDATE task_events SET status='running' WHERE seq=2".into(),
        "UPDATE workspace_claims SET owner_fence=2".into(),
        "UPDATE task_admissions SET revision=revision+1".into(),
        format!(
            "INSERT INTO acp_output_sets(set_id,task_id,attempt,turn,runtime_fence,room_id,render_version,state,created_at,updated_at) VALUES('set','{TASK}',1,1,'runtime','!room:example.test','v1','assembling','t','t')"
        ),
    ] {
        let f = seeded().await;
        let (s, p) = input(&f).await;
        sqlx::query(sqlx::AssertSqlSafe(query))
            .execute(&f.pool)
            .await
            .unwrap();
        assert!(publish(&f.pool, &s, &p, &[], || Ok(())).await.is_err());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM task_events WHERE status='failed'")
                .fetch_one(&f.pool)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT state FROM execution_leases")
                .fetch_one(&f.pool)
                .await
                .unwrap(),
            "active"
        );
        f.pool.close().await;
    }
}

#[tokio::test]
async fn pre_accept_succession_notification_failure_rolls_back_task_lease_and_workspace() {
    let f = seeded().await;
    let (s, p) = input(&f).await;
    sqlx::query("CREATE TRIGGER interrupted_notice BEFORE INSERT ON projection_outbox BEGIN SELECT RAISE(ABORT,'interrupted'); END").execute(&f.pool).await.unwrap();
    assert!(publish(&f.pool, &s, &p, &[], || Ok(())).await.is_err());
    assert_eq!(input(&f).await.0, s);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM offline_recovery_workspace_deletions")
            .fetch_one(&f.pool)
            .await
            .unwrap(),
        0
    );
    sqlx::query("DROP TRIGGER interrupted_notice")
        .execute(&f.pool)
        .await
        .unwrap();
    publish(&f.pool, &s, &p, &[], || Ok(())).await.unwrap();
    assert!(
        confirmed(&f.root.path().join("state.db"), &s, &p)
            .await
            .unwrap()
    );
    f.pool.close().await;
}
