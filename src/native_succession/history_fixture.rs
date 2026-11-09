use super::{process::Exclusion, state};
use crate::storage::{
    CompatibilityGate, MatrixCompatibilityStore, MatrixRecoveryStore, ReliabilityStore,
    RuntimeOwnerEvidence,
    recovery_eligibility::{EffectDomain, RecoveryScope, bootstrap_nonadmin, writer},
};
use sqlx::SqlitePool;
use std::path::Path;

pub(super) async fn scope(pool: &SqlitePool) -> RecoveryScope {
    RecoveryScope::new(
        bootstrap_nonadmin(pool, true).await.unwrap(),
        EffectDomain::Local,
        None,
    )
}

pub(super) async fn seed(path: &Path) -> SqlitePool {
    let pool = crate::storage::connect(path).await.unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    let CompatibilityGate::Native(operation) = MatrixCompatibilityStore::new(pool.clone())
        .gate(true, None)
        .await
        .unwrap()
    else {
        panic!("native")
    };
    let s = scope(&pool).await;
    MatrixRecoveryStore::new_scoped(pool.clone(), Some(s))
        .activate_cutover(&operation, "B", &now())
        .await
        .unwrap();
    // Redacted I1 order, evidence revisions and terminal-admission distribution.
    for (token, revision) in [
        ("succession-0", 17),
        ("succession-1", 3),
        ("controlled-0", 22),
        ("controlled-1", 25),
        ("controlled-2", 37),
        ("controlled-3", 42),
        ("succession-2", 16),
        ("controlled-4", 26),
        ("succession-3", 8),
        ("succession-4", 43),
        ("controlled-5", 46),
    ] {
        let owner = start(&pool, token).await;
        heartbeat(&owner, token, revision - 2).await;
        if token.starts_with("controlled") {
            controlled(&owner, token).await;
        } else {
            succeeded(&pool, &owner, token).await;
        }
    }
    let owner = start(&pool, "unknown").await;
    leases(&pool, "unknown").await;
    terminal_work(&pool).await;
    older_records(&pool).await;
    heartbeat(&owner, "unknown", 36).await;
    let mut invalid = RuntimeOwnerEvidence::capture().unwrap();
    invalid.boot_id.push_str("-fault");
    assert!(
        !owner
            .publish_owner_heartbeat(
                "unknown",
                &RuntimeOwnerEvidence::fingerprint().unwrap(),
                &now(),
                &invalid
            )
            .await
            .unwrap()
    );
    assert!(owner.stop_runtime("unknown", &now()).await.unwrap());
    pool
}

async fn heartbeat(owner: &ReliabilityStore, token: &str, count: usize) {
    for _ in 0..count {
        assert!(
            owner
                .publish_owner_heartbeat(
                    token,
                    &RuntimeOwnerEvidence::fingerprint().unwrap(),
                    &now(),
                    &RuntimeOwnerEvidence::capture().unwrap()
                )
                .await
                .unwrap()
        );
    }
}

async fn controlled(owner: &ReliabilityStore, token: &str) {
    assert!(owner.stopping_runtime(token, &now()).await.unwrap());
    let gate = crate::acp::AcpSpawnGate::for_generation(token);
    gate.close_and_join().await;
    gate.reap_transports().await;
    assert!(
        owner
            .finish_controlled_stop(
                token,
                &now(),
                &RuntimeOwnerEvidence::capture().unwrap(),
                &gate.joined_children().unwrap()
            )
            .await
            .unwrap()
    );
}

async fn succeeded(pool: &SqlitePool, owner: &ReliabilityStore, token: &str) {
    leases(pool, token).await;
    assert!(owner.stopping_runtime(token, &now()).await.unwrap());
    let mut c = pool.acquire().await.unwrap();
    let state = state::read(&mut c).await.unwrap().unwrap();
    drop(c);
    let proof = Exclusion::modeled(state.owner.clone());
    crate::storage::recovery_eligibility::writer::succession::publish(
        pool,
        &state,
        &proof,
        &[],
        || Ok(()),
    )
    .await
    .unwrap();
}

fn now() -> String {
    chrono::Utc::now().to_rfc3339()
}
pub(super) async fn start(pool: &SqlitePool, token: &str) -> ReliabilityStore {
    let owner = ReliabilityStore::with_recovery(pool.clone(), scope(pool).await);
    assert!(
        owner
            .begin_runtime_with_evidence(
                token,
                &RuntimeOwnerEvidence::fingerprint().unwrap(),
                &now(),
                &RuntimeOwnerEvidence::capture().unwrap()
            )
            .await
            .unwrap()
    );
    owner
}
async fn leases(pool: &SqlitePool, token: &str) {
    let store = MatrixRecoveryStore::new_scoped(pool.clone(), Some(scope(pool).await));
    for component in ["MatrixSync", "OutboxDrain"] {
        store
            .acquire_component_lease(
                component,
                token,
                &now(),
                &(chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
            )
            .await
            .unwrap();
    }
}
async fn terminal_work(pool: &SqlitePool) {
    let s = scope(pool).await;
    let mut tx = writer::begin(pool, Some(&s)).await.unwrap();
    sqlx::query(
        "INSERT INTO conversations(conversation_id,participants_json) VALUES('conversation','[]')",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    writer::inserted(
        &mut tx,
        Some(&s),
        &writer::text_identity("conversations", "conversation").unwrap(),
    )
    .await
    .unwrap();
    let owners = [
        "succession-0",
        "controlled-0",
        "controlled-1",
        "controlled-1",
        "controlled-2",
        "controlled-2",
        "controlled-2",
        "controlled-3",
        "succession-2",
        "controlled-4",
        "succession-3",
        "controlled-5",
        "controlled-5",
        "unknown",
    ];
    for (i, owner) in owners.iter().enumerate() {
        let id = format!("task-{i}");
        sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,'from','to','conversation','fixture',1,0,0,0)").bind(&id).bind(&id).execute(&mut *tx).await.unwrap();
        writer::inserted(
            &mut tx,
            Some(&s),
            &writer::text_identity("tasks", &id).unwrap(),
        )
        .await
        .unwrap();
        sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,1,'completed','t','{}')").bind(&id).bind(&id).execute(&mut *tx).await.unwrap();
        writer::inserted(
            &mut tx,
            Some(&s),
            &writer::text_identity("task_events", &id).unwrap(),
        )
        .await
        .unwrap();
        sqlx::query("INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES(?,'terminal',1,?,'t','t')").bind(&id).bind(owner).execute(&mut *tx).await.unwrap();
        writer::inserted(
            &mut tx,
            Some(&s),
            &writer::text_identity("task_admissions", &id).unwrap(),
        )
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    terminal_deliveries(pool).await;
    projections(pool).await;
}

async fn projections(pool: &SqlitePool) {
    let epoch: i64 = sqlx::query_scalar(
        "SELECT epoch FROM matrix_component_leases WHERE component='MatrixSync'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    let s = scope(pool)
        .await
        .for_component(crate::storage::recovery_eligibility::LiveFence {
            component: "MatrixSync".into(),
            owner: "unknown".into(),
            epoch,
        });
    let mut tx = writer::begin(pool, Some(&s)).await.unwrap();
    for i in 0..5 {
        let id = format!("task-{i}");
        sqlx::query("INSERT INTO projection_outbox(source_kind,source_id,projection,transport,stable_txn_id,room_id,render_version,body,body_hash,state,attempt_count,claim_owner,claim_revision,created_at,updated_at) VALUES('task_event',?,'observer','matrix',?,'!room:example.test','v1','notice','hash','sending',2,'claim',3,'t','t')").bind(&id).bind(format!("txn-{i}")).execute(&mut *tx).await.unwrap();
        writer::inserted(
            &mut tx,
            Some(&s),
            &writer::composite_text_identity("projection_outbox", &["task_event", &id, "observer"])
                .unwrap(),
        )
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
}

pub(super) async fn snapshot(pool: &SqlitePool) -> Vec<String> {
    let mut c = pool.acquire().await.unwrap();
    let mut values = Vec::new();
    for id in crate::storage::recovery_eligibility::inventory::identities(&mut c)
        .await
        .unwrap()
    {
        values.push(
            crate::storage::recovery_eligibility::inventory::snapshot(&mut c, &id)
                .await
                .unwrap()
                .digest,
        );
    }
    values
}

pub(super) async fn notices(pool: &SqlitePool) -> Vec<String> {
    sqlx::query_scalar("SELECT json_array(source_id,state,claim_owner,claim_revision,attempt_count,stable_txn_id,body,body_hash) FROM projection_outbox ORDER BY source_id").fetch_all(pool).await.unwrap()
}

async fn terminal_deliveries(pool: &SqlitePool) {
    let s = scope(pool).await;
    let mut tx = writer::begin(pool, Some(&s)).await.unwrap();
    let endpoint = "00000000-0000-0000-0000-000000000001";
    sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES(?,'retired-fixture','acp',0,'[]')").bind(endpoint).execute(&mut *tx).await.unwrap();
    writer::inserted(
        &mut tx,
        Some(&s),
        &writer::text_identity("agents", endpoint).unwrap(),
    )
    .await
    .unwrap();
    for i in 0..14 {
        let id = format!("task-{i}");
        sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at,acknowledged_at) VALUES(?,?,1,?,'t','t')").bind(&id).bind(&id).bind(endpoint).execute(&mut *tx).await.unwrap();
        writer::inserted(
            &mut tx,
            Some(&s),
            &writer::text_identity("deliveries", &id).unwrap(),
        )
        .await
        .unwrap();
        sqlx::query("INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state,reason_code) VALUES(?,?,1,'terminal','fixture-completed')").bind(&id).bind(&id).execute(&mut *tx).await.unwrap();
        writer::inserted(
            &mut tx,
            Some(&s),
            &writer::text_identity("delivery_dispositions", &id).unwrap(),
        )
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
}

// Only disposable fixtures are rewritten, through the classified writer, to
// reproduce the optional-field shapes emitted by earlier R105 revisions.
async fn older_records(pool: &SqlitePool) {
    let epoch = sqlx::query_scalar(
        "SELECT epoch FROM matrix_component_leases WHERE component='MatrixSync'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    let s = scope(pool)
        .await
        .for_component(crate::storage::recovery_eligibility::LiveFence {
            component: "MatrixSync".into(),
            owner: "unknown".into(),
            epoch,
        });
    let mut tx = writer::begin(pool, Some(&s)).await.unwrap();
    for token in [
        "succession-0",
        "succession-1",
        "succession-2",
        "succession-3",
        "succession-4",
    ] {
        let (key, record): (String, String) = sqlx::query_as("SELECT json_array('text:'||hex(CAST(instance_token AS BLOB)),'integer:'||hex(CAST(runtime_generation AS BLOB)),'integer:'||hex(CAST(evidence_revision AS BLOB))),child_snapshot FROM runtime_owner_evidence WHERE instance_token=? AND is_current=1")
            .bind(token).fetch_one(&mut *tx).await.unwrap();
        let id = crate::storage::recovery_eligibility::RowIdentity {
            table: "runtime_owner_evidence".into(),
            key,
        };
        let mutation = writer::prepare_mutation(&mut tx, Some(&s), &id)
            .await
            .unwrap();
        let mut record: serde_json::Value = serde_json::from_str(&record).unwrap();
        let source = record["source"].as_object_mut().unwrap();
        source.remove("history");
        if matches!(token, "succession-0" | "succession-1" | "succession-2") {
            source.remove("pre_accept");
        }
        if matches!(token, "succession-0" | "succession-1") {
            source.remove("isolated_projections");
        }
        sqlx::query("UPDATE runtime_owner_evidence SET child_snapshot=? WHERE instance_token=? AND is_current=1")
            .bind(record.to_string()).bind(token).execute(&mut *tx).await.unwrap();
        writer::complete_mutation(&mut tx, Some(&s), mutation)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
}
