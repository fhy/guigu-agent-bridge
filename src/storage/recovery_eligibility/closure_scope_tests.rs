use super::{EffectDomain, RecoveryScope, bootstrap_nonadmin, inventory, writer};
use crate::models::{EventId, TaskEvent, TaskEventPayload, TaskStatus};
use sqlx::{Row, SqlitePool};

struct Fixture {
    root: tempfile::TempDir,
    pool: SqlitePool,
    scope: RecoveryScope,
    task: String,
    delivery: String,
}

async fn fixture() -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let pool = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    let scope = RecoveryScope::new(
        bootstrap_nonadmin(&pool, false).await.unwrap(),
        EffectDomain::Local,
        None,
    );
    let task = uuid::Uuid::now_v7().to_string();
    let delivery = uuid::Uuid::now_v7().to_string();
    let mut tx = writer::begin(&pool, Some(&scope)).await.unwrap();
    for sql in [
        "INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES('ep','worker','acp',1,'[]')",
        "INSERT INTO conversations(conversation_id,participants_json) VALUES('local','[]')",
        "INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES('old','t','t','stopped','fp')",
    ] {
        sqlx::query(sql).execute(&mut *tx).await.unwrap();
    }
    sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,'ep','ep','local','synthetic',5,0,0,1)").bind(&task).bind(&task).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES(?,'dispatching',1,'old','t','t')").bind(&task).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at,acknowledged_at) VALUES(?,?,1,'ep','t','t')").bind(&delivery).bind(&task).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state,session_id) VALUES(?,?,1,'acknowledged','setup-session')").bind(&delivery).bind(&task).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES('resource',?,'lease-owner',4,'recovery_needed','t','t','later')").bind(&task).execute(&mut *tx).await.unwrap();
    for (seq, status, payload) in [
        (1, "queued", serde_json::json!("queued")),
        (
            2,
            "dispatched",
            serde_json::json!({"dispatched":{"delivery_id":delivery,"attempt":1}}),
        ),
    ] {
        sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,?,?,'t',?)").bind(uuid::Uuid::now_v7().to_string()).bind(&task).bind(seq).bind(status).bind(payload.to_string()).execute(&mut *tx).await.unwrap();
    }
    let identities = inventory::identities(&mut tx).await.unwrap();
    for table in [
        "agents",
        "conversations",
        "runtime_instances",
        "tasks",
        "task_events",
        "deliveries",
        "task_admissions",
        "execution_leases",
        "delivery_dispositions",
    ] {
        for identity in identities.iter().filter(|identity| identity.table == table) {
            writer::inserted(&mut tx, Some(&scope), identity)
                .await
                .unwrap();
        }
    }
    tx.commit().await.unwrap();
    bootstrap_nonadmin(&pool, false).await.unwrap();
    Fixture {
        root,
        pool,
        scope,
        task,
        delivery,
    }
}

async fn snapshot(pool: &SqlitePool) -> serde_json::Value {
    let mut c = pool.acquire().await.unwrap();
    let mut values = Vec::new();
    for identity in inventory::identities(&mut c).await.unwrap() {
        values.push(serde_json::json!([
            identity,
            inventory::values(&mut c, &identity).await.unwrap()
        ]));
    }
    let classes: Vec<String> = sqlx::query_scalar("SELECT json_array(table_name,canonical_pk,origin_generation,original_revision,original_digest,parent_relations,disposition,evidence_digest,context_kind,context_generation,admin_operation_id,plan_digest,matrix_operation_id) FROM offline_recovery_rows ORDER BY table_name,canonical_pk").fetch_all(&mut *c).await.unwrap();
    serde_json::json!([values, classes])
}

async fn close(
    f: &Fixture,
    scope: Option<&RecoveryScope>,
) -> Result<(), crate::terminal_closure::ClosureError> {
    use crate::terminal_closure::*;
    let now = "2026-11-05T18:26:54.986797753Z";
    let event = TaskEvent {
        id: EventId::generate(),
        task_id: f.task.parse().unwrap(),
        seq: 3,
        status: TaskStatus::Failed,
        timestamp: now.parse().unwrap(),
        payload: TaskEventPayload::Failed {
            error: "failed_before_prompt".into(),
        },
    };
    let mut tx = f.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    apply_scoped(
        &mut tx,
        ClosureContext {
            task_id: &f.task,
            resource_key: "resource",
            event: &event,
            now,
            delivery: DeliveryBinding::OfflinePrePrompt {
                delivery_id: &f.delivery,
                attempt: 1,
            },
            admission: AdmissionMode::Offline {
                runtime_owner: Some("old"),
                revision: 1,
            },
            task_version: TaskVersionMode::OfflineExpected(1),
            disposition_reason: Some("failed_before_prompt"),
        },
        LeaseMode::OfflineRecoveryNeeded {
            owner: "lease-owner",
            fence: 4,
            expires_at: "later",
        },
        scope,
    )
    .await?;
    tx.commit().await.unwrap();
    Ok(())
}

#[tokio::test]
async fn classified_old_entry_refuses_without_changing_rows_or_classifications() {
    let f = fixture().await;
    let before = snapshot(&f.pool).await;
    assert_eq!(
        crate::failed_before_prompt::reconcile(
            f.root.path().join("state.db"),
            "2026-11-05T18:26:54.986797753Z"
        )
        .await
        .unwrap_err(),
        crate::failed_before_prompt::Error::Database
    );
    assert_eq!(snapshot(&f.pool).await, before);
    bootstrap_nonadmin(&f.pool, false).await.unwrap();
}

#[tokio::test]
async fn shared_closure_valid_scope_preserves_complete_bootstrap_inventory() {
    let f = fixture().await;
    close(&f, Some(&f.scope)).await.unwrap();
    bootstrap_nonadmin(&f.pool, false).await.unwrap();
    let terminal:(i64,String,String)=sqlx::query_as("SELECT t.version,a.state,l.state FROM tasks t JOIN task_admissions a USING(task_id) JOIN execution_leases l USING(task_id) WHERE t.task_id=?").bind(&f.task).fetch_one(&f.pool).await.unwrap();
    assert_eq!(terminal, (2, "terminal".into(), "released".into()));
}

#[tokio::test]
async fn shared_closure_missing_and_stale_scope_refuse_before_any_write() {
    let f = fixture().await;
    let before = snapshot(&f.pool).await;
    assert!(close(&f, None).await.is_err());
    assert_eq!(snapshot(&f.pool).await, before);
    let mut context = bootstrap_nonadmin(&f.pool, false).await.unwrap();
    if let super::EligibilityContext::NonAdmin { revision, .. } = &mut context {
        *revision += 1;
    }
    let stale = RecoveryScope::new(context, EffectDomain::Local, None);
    assert!(close(&f, Some(&stale)).await.is_err());
    assert_eq!(snapshot(&f.pool).await, before);
}

#[tokio::test]
async fn existing_deletion_trigger_refuses_recovery_needed_claim_even_with_released_lease() {
    let f = fixture().await;
    close(&f, Some(&f.scope)).await.unwrap();
    let mut tx = writer::begin(&f.pool, Some(&f.scope)).await.unwrap();
    sqlx::query("INSERT INTO workspace_claims(claim_id,runtime_owner,task_id,canonical_path,path_components_json,owner_fence,revision,state,created_at,updated_at) VALUES('claim','lease-owner',?,'/synthetic','[\"synthetic\"]',4,2,'recovery_needed','t','t')").bind(&f.task).execute(&mut *tx).await.unwrap();
    let id = writer::text_identity("workspace_claims", "claim").unwrap();
    writer::inserted(&mut tx, Some(&f.scope), &id)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let before = snapshot(&f.pool).await;
    let context = bootstrap_nonadmin(&f.pool, false).await.unwrap();
    let mut tx = f.pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let error = super::deletion::record(
        &mut tx,
        &context,
        &id,
        super::deletion::ReleaseEvidence {
            owner: "lease-owner",
            task: &f.task,
            fence: 4,
            authority: None,
        },
    )
    .await
    .unwrap_err();
    let db = error
        .as_database_error()
        .expect("actual SQLite trigger rejection");
    assert_eq!(db.message(), "offline-recovery-deletion-without-claim");
    println!(
        "recovery_needed_deletion=refused sqlite_code={} trigger=offline_recovery_workspace_deletion_requires_live_claim",
        db.code().unwrap_or_default()
    );
    tx.rollback().await.unwrap();
    assert_eq!(snapshot(&f.pool).await, before);
    let receipt_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM offline_recovery_workspace_deletions")
            .fetch_one(&f.pool)
            .await
            .unwrap();
    assert_eq!(receipt_count, 0);
    let row =
        sqlx::query("SELECT state,runtime_owner FROM workspace_claims WHERE claim_id='claim'")
            .fetch_one(&f.pool)
            .await
            .unwrap();
    assert_eq!(row.get::<&str, _>("state"), "recovery_needed");
    assert_eq!(row.get::<&str, _>("runtime_owner"), "lease-owner");
}
