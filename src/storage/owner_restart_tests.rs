use super::*;

async fn fixture() -> (sqlx::SqlitePool, ReliabilityStore, crate::acp::AcpSpawnGate) {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    let store = ReliabilityStore::new(pool.clone());
    let now = Utc::now().to_rfc3339();
    assert!(
        store
            .begin_runtime_with_evidence(
                "owner",
                &RuntimeOwnerEvidence::fingerprint().unwrap(),
                &now,
                &RuntimeOwnerEvidence::capture().unwrap()
            )
            .await
            .unwrap()
    );
    assert!(store.stopping_runtime("owner", &now).await.unwrap());
    let gate = crate::acp::AcpSpawnGate::for_generation("owner");
    gate.close_and_join().await;
    gate.reap_transports().await;
    (pool, store, gate)
}

async fn finish(
    store: &ReliabilityStore,
    gate: &crate::acp::AcpSpawnGate,
) -> Result<bool, ReliabilityError> {
    store
        .finish_controlled_stop(
            "owner",
            &Utc::now().to_rfc3339(),
            &RuntimeOwnerEvidence::capture().unwrap(),
            &gate.joined_children().unwrap(),
        )
        .await
}

#[tokio::test]
async fn controlled_stop_bootstrap_and_next_owner_never_authorize_reaped() {
    let (pool, store, gate) = fixture().await;
    assert!(finish(&store, &gate).await.unwrap());
    assert!(finish(&store, &gate).await.unwrap());
    let states: (String, String, String, i64) = sqlx::query_as("SELECT r.state,e.state,e.child_scan_state,e.evidence_revision FROM runtime_instances r JOIN runtime_owner_evidence e USING(instance_token) WHERE e.is_current=1").fetch_one(&pool).await.unwrap();
    assert_eq!(
        states,
        (
            "stopped".into(),
            "observing".into(),
            "inaccessible".into(),
            3
        )
    );
    let context = crate::storage::recovery_eligibility::bootstrap_nonadmin(&pool, false)
        .await
        .unwrap();
    let store = ReliabilityStore::with_recovery(
        pool.clone(),
        crate::storage::recovery_eligibility::RecoveryScope::new(
            context,
            crate::storage::recovery_eligibility::EffectDomain::Local,
            None,
        ),
    );
    assert!(
        store
            .begin_runtime_with_evidence(
                "next-owner",
                &RuntimeOwnerEvidence::fingerprint().unwrap(),
                &Utc::now().to_rfc3339(),
                &RuntimeOwnerEvidence::capture().unwrap()
            )
            .await
            .unwrap()
    );
    let reaped: i64 =
        sqlx::query_scalar("SELECT count(*) FROM runtime_owner_evidence WHERE state='reaped'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(reaped, 0);
    pool.close().await;
}

#[tokio::test]
async fn controlled_stop_unknown_identity_clock_and_foreign_owner_refuse() {
    for query in [
        "UPDATE runtime_owner_evidence SET state='unknown' WHERE is_current=1",
        "UPDATE runtime_owner_evidence SET process_fingerprint='foreign' WHERE is_current=1",
        "UPDATE runtime_owner_evidence SET proc_starttime=0 WHERE is_current=1",
        "UPDATE runtime_owner_evidence SET captured_at='2000-01-01T00:00:00Z' WHERE is_current=1",
        "UPDATE runtime_owner_evidence SET evidence_revision=8 WHERE is_current=1",
        "UPDATE runtime_owner_evidence SET runtime_generation=2 WHERE is_current=0",
        "INSERT INTO runtime_instances VALUES('foreign','t','t','active','foreign')",
        "INSERT INTO matrix_component_leases VALUES('MatrixSync','owner',1,'unknown','t','t')",
        "INSERT INTO workspace_claims VALUES('claim','owner','task','/fixture','[]',1,1,'active','t','t')",
    ] {
        let (pool, store, gate) = fixture().await;
        sqlx::query(query).execute(&pool).await.unwrap();
        assert!(!finish(&store, &gate).await.unwrap(), "{query}");
        let state: String =
            sqlx::query_scalar("SELECT state FROM runtime_instances WHERE instance_token='owner'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, "stopping");
        pool.close().await;
    }
}

#[tokio::test]
async fn controlled_stop_writer_failure_rolls_back_and_receipt_drift_refuses() {
    let (pool, store, gate) = fixture().await;
    sqlx::query("CREATE TRIGGER fail_finish BEFORE UPDATE ON runtime_instances WHEN NEW.state='stopped' BEGIN SELECT RAISE(ABORT,'injected'); END").execute(&pool).await.unwrap();
    assert!(finish(&store, &gate).await.is_err());
    let state: (String, i64) = sqlx::query_as(
        "SELECT state,evidence_revision FROM runtime_owner_evidence WHERE is_current=1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(state, ("scan_pending".into(), 2));
    sqlx::query("DROP TRIGGER fail_finish")
        .execute(&pool)
        .await
        .unwrap();
    assert!(finish(&store, &gate).await.unwrap());
    sqlx::query("UPDATE runtime_owner_evidence SET child_snapshot='{}' WHERE is_current=1")
        .execute(&pool)
        .await
        .unwrap();
    let mut connection = pool.acquire().await.unwrap();
    assert!(!restart_eligible(&mut connection, "owner").await.unwrap());
    drop(connection);
    pool.close().await;
}

#[tokio::test]
async fn controlled_stop_gate_requires_real_owned_child_wait() {
    let gate = crate::acp::AcpSpawnGate::for_generation("owner");
    assert!(gate.joined_children().is_none());
    let mut reservation = gate.reserve().unwrap();
    let transport = std::sync::Arc::new(
        crate::acp::transport::AcpTransport::spawn(
            &crate::models::EndpointAddress::Acp {
                command: "/bin/cat".into(),
                args: vec![],
            },
            None,
            crate::acp::transport::TransportLimits::default(),
        )
        .unwrap(),
    );
    assert!(crate::acp::AcpSpawnGate::register_child(
        &mut reservation,
        transport.child_pid()
    ));
    assert!(crate::acp::AcpSpawnGate::retain_transport(
        &reservation,
        transport.clone()
    ));
    drop(reservation);
    gate.close_and_join().await;
    gate.reap_transports().await;
    assert!(transport.shutdown_reaped().await);
    assert!(gate.joined_children().is_some());
    assert!(gate.reserve().is_none());
    assert!(!gate.final_observation().qualifies(1));
}

#[tokio::test]
async fn controlled_stop_pending_work_fence_and_effects_remain_fail_closed() {
    for query in [
        "INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES('task','running',1,'owner','t','t')",
        "INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES('resource','task','owner',7,'active','t','t','t')",
        "INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES('resource','task','foreign',99,'recovery_needed','t','t','t')",
        "INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at) VALUES('delivery','task',1,'agent','t')",
        "INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES('pending','task',2,'running','t','{}')",
        "INSERT INTO acp_output_sets(set_id,task_id,attempt,turn,runtime_fence,room_id,render_version,state,created_at,updated_at) VALUES('output','task',1,1,'owner','room','v1','assembling','t','t')",
        "INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at,acknowledged_at) VALUES('delivery','task',1,'agent','t','t'); INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state) VALUES('delivery','task',1,'terminal'); INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES('resource','task','owner',7,'released','t','t','t'); INSERT INTO task_continuations(task_id,resource_key,delivery_id,lease_fence,revision,state,next_turn,completed_turns,consecutive_no_progress,next_prompt,started_at,heartbeat_at,last_progress_at,observed_output_bytes) VALUES('task','resource','delivery',7,1,'ready',1,0,0,'pending','t','t','t',0)",
    ] {
        let (pool, store, gate) = fixture().await;
        sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES('conversation','[]')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES('agent','agent','acp',1,'[]')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES('task','task','sender','agent','conversation','fixture',1,0,0,0)").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES('terminal','task',1,'completed','t','{}')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::raw_sql(query).execute(&pool).await.unwrap();
        assert!(!finish(&store, &gate).await.unwrap(), "{query}");
        pool.close().await;
    }
}

#[tokio::test]
async fn controlled_stop_concurrent_identity_change_rolls_back_publication() {
    let (pool, store, gate) = fixture().await;
    sqlx::query("CREATE TRIGGER mutate_owner AFTER INSERT ON runtime_owner_evidence WHEN NEW.state='observing' BEGIN UPDATE runtime_instances SET process_fingerprint='foreign' WHERE instance_token=NEW.instance_token; END").execute(&pool).await.unwrap();
    assert!(finish(&store, &gate).await.is_err());
    let state: (String, String) = sqlx::query_as(
        "SELECT state,process_fingerprint FROM runtime_instances WHERE instance_token='owner'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        state,
        (
            "stopping".into(),
            RuntimeOwnerEvidence::fingerprint().unwrap()
        )
    );
    pool.close().await;
}

#[tokio::test]
async fn controlled_stop_allows_only_fully_acknowledged_output_cleanup() {
    for mutation in [
        None,
        Some("UPDATE acp_output_sets SET state='delivering'"),
        Some("UPDATE acp_output_sets SET acknowledged_cursor=-1"),
        Some("DELETE FROM acp_output_delivery_segments"),
        Some("UPDATE acp_output_delivery_segments SET delivery_index=1"),
        Some("UPDATE acp_output_segments SET state='reserved'"),
        Some("UPDATE acp_output_segments SET state='quarantined'"),
    ] {
        let (pool, store, gate) = fixture().await;
        sqlx::raw_sql(
            "INSERT INTO conversations(conversation_id,participants_json) VALUES('conversation','[]');
             INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES('agent','agent','acp',1,'[]');
             INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES('task','task','sender','agent','conversation','fixture',1,0,0,0);
             INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES('terminal','task',1,'completed','t','{}');
             INSERT INTO acp_output_sets(set_id,task_id,attempt,turn,runtime_fence,room_id,render_version,total_bytes,total_hash,segment_count,acknowledged_cursor,state,created_at,updated_at) VALUES('output','task',1,1,'owner','room','v1',5,'hash',1,0,'cleaning','t','t');
             INSERT INTO acp_output_segments(set_id,segment_index,byte_start,byte_end,byte_length,content_hash,relative_name,state,reservation_owner,created_at,updated_at) VALUES('output',0,0,5,5,'hash','part','landed','owner','t','t');
             INSERT INTO acp_output_delivery_segments(set_id,delivery_index,spool_index,byte_start,byte_end,content_hash,stable_txn_id) VALUES('output',0,0,0,5,'hash','txn');",
        ).execute(&pool).await.unwrap();
        if let Some(query) = mutation {
            sqlx::query(query).execute(&pool).await.unwrap();
        }
        assert_eq!(
            finish(&store, &gate).await.unwrap(),
            mutation.is_none(),
            "{mutation:?}"
        );
        let mut connection = pool.acquire().await.unwrap();
        assert_eq!(
            restart_eligible(&mut connection, "owner").await.unwrap(),
            mutation.is_none()
        );
        drop(connection);
        if mutation.is_none() {
            assert!(
                store
                    .begin_runtime_with_evidence(
                        "next-owner",
                        &RuntimeOwnerEvidence::fingerprint().unwrap(),
                        &Utc::now().to_rfc3339(),
                        &RuntimeOwnerEvidence::capture().unwrap(),
                    )
                    .await
                    .unwrap()
            );
            let state: String =
                sqlx::query_scalar("SELECT state FROM acp_output_sets WHERE set_id='output'")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(state, "cleaning");
        }
        pool.close().await;
    }
}
