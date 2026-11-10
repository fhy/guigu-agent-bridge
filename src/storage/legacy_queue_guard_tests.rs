//! W5: valid-parent legacy APIs must reject classified stores before business SQL.
use super::*;

type Image = Vec<(String, Vec<String>)>;
struct Parents {
    task: String,
    delivery: String,
    endpoint: String,
}

async fn classified() -> (Fixture, Parents) {
    let f = fixture().await;
    route(&f, &envelope()).await.unwrap();
    let row = sqlx::query("SELECT task_id,delivery_id,target_endpoint_id FROM agent_work_queue")
        .fetch_one(&f.pool)
        .await
        .unwrap();
    let p = Parents {
        task: row.get(0),
        delivery: row.get(1),
        endpoint: row.get(2),
    };
    valid_parents(&f.pool, &p).await;
    assert_inventory(&f.pool).await;
    (f, p)
}

async fn valid_parents(pool: &SqlitePool, p: &Parents) {
    let mut c = pool.acquire().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
            .fetch_one(&mut *c)
            .await
            .unwrap(),
        1
    );
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&mut *c)
            .await
            .unwrap()
            .is_empty()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM tasks t JOIN deliveries d ON d.task_id=t.task_id JOIN agents a ON a.endpoint_id=d.target_endpoint_id WHERE t.task_id=? AND d.delivery_id=? AND a.endpoint_id=?")
        .bind(&p.task).bind(&p.delivery).bind(&p.endpoint).fetch_one(&mut *c).await.unwrap();
    assert_eq!(count, 1, "all reserve/claim parents must be valid");
}

async fn reserve(
    store: &ReliabilityStore,
    p: &Parents,
) -> Result<QueueReservation, ReliabilityError> {
    store
        .reserve_queue(
            &p.task,
            &p.delivery,
            &p.endpoint,
            "room",
            None,
            &p.endpoint,
            "legacy-request",
            "body",
            4,
            "now",
        )
        .await
}

fn authority(error: ReliabilityError) {
    match error {
        ReliabilityError::Sql(sqlx::Error::Protocol(reason)) => {
            assert_eq!(reason, "offline-recovery-eligibility-refused");
        }
        other => panic!("expected begin(None) classified guard, not FK/lease/CAS: {other:?}"),
    }
}

// Quote schema identifiers and encode every cell (including NULL/BLOB) distinctly.
// Sorting the complete rows makes the image independent of query/physical page order.
async fn image(pool: &SqlitePool) -> Image {
    let mut tx = pool.begin().await.unwrap();
    let tables: Vec<String> = sqlx::query_scalar("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        .fetch_all(&mut *tx).await.unwrap();
    let mut image = Vec::new();
    for table in tables {
        let columns: Vec<String> =
            sqlx::query_scalar("SELECT name FROM pragma_table_info(?) ORDER BY cid")
                .bind(&table)
                .fetch_all(&mut *tx)
                .await
                .unwrap();
        let cells = columns
            .iter()
            .map(|c| format!("quote(\"{}\")", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(",");
        // Identifiers come only from this disposable database's schema and are escaped.
        let query = sqlx::AssertSqlSafe(format!(
            "SELECT json_array({cells}) FROM \"{}\"",
            table.replace('"', "\"\"")
        ));
        let mut rows: Vec<String> = sqlx::query_scalar(query).fetch_all(&mut *tx).await.unwrap();
        rows.sort();
        image.push((table, rows));
    }
    tx.rollback().await.unwrap();
    image
}

async fn unchanged(f: &Fixture, before: Image) {
    assert_eq!(image(&f.pool).await, before);
    assert_inventory(&f.pool).await;
    f.pool.close().await;
    let reopened = crate::storage::connect(f.root.path().join("state.db"))
        .await
        .unwrap();
    assert_eq!(image(&reopened).await, before);
    assert_inventory(&reopened).await;
    reopened.close().await;
}

async fn lease(pool: &SqlitePool, scope: Option<&RecoveryScope>, p: &Parents) -> String {
    let now = chrono::Utc::now();
    let mut tx = writer::begin(pool, scope).await.unwrap();
    sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES('legacy-resource',?,'lease-owner',7,'active',?,?,?)")
        .bind(&p.task).bind(now.to_rfc3339()).bind(now.to_rfc3339())
        .bind((now + chrono::Duration::minutes(10)).to_rfc3339()).execute(&mut *tx).await.unwrap();
    writer::inserted(
        &mut tx,
        scope,
        &writer::text_identity("execution_leases", "legacy-resource").unwrap(),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_leases l JOIN agent_work_queue q ON q.task_id=l.task_id WHERE q.target_endpoint_id=? AND q.state='queued' AND l.resource_key='legacy-resource' AND l.owner_token='lease-owner' AND l.fence=7 AND l.state='active' AND l.expires_at>?")
        .bind(&p.endpoint).bind(now.to_rfc3339()).fetch_one(pool).await.unwrap();
    assert_eq!(
        count, 1,
        "claim must have a queued task and matching live lease"
    );
    now.to_rfc3339()
}

#[tokio::test]
async fn classified_reserve_reports_guard_before_any_write_with_valid_parents() {
    let (f, p) = classified().await;
    let before = image(&f.pool).await;
    authority(reserve(&f.store, &p).await.unwrap_err());
    // The guard is a DB property; omitting the in-memory scope cannot bypass it.
    authority(
        reserve(&ReliabilityStore::new(f.pool.clone()), &p)
            .await
            .unwrap_err(),
    );
    unchanged(&f, before).await;
    println!(
        "W5/F1 classified-reserve: exact-authority-error, valid-parents, zero-write, reopened-inventory-equal"
    );
}

#[tokio::test]
async fn held_reserve_preserves_control_and_classifications_with_valid_parents() {
    let (f, p) = classified().await;
    let scope = f.store.recovery.as_ref().unwrap();
    let fence = scope.component_fence().unwrap();
    let recovery = MatrixRecoveryStore::new_scoped(f.pool.clone(), Some(scope.clone()));
    let operation = recovery.control().await.unwrap().unwrap().operation_id;
    recovery
        .refuse_response(
            &operation,
            &fence.owner,
            fence.epoch,
            "rebuild-history-gap",
            &chrono::Utc::now().to_rfc3339(),
        )
        .await
        .unwrap();
    assert_eq!(
        recovery
            .control()
            .await
            .unwrap()
            .unwrap()
            .restart_hold_state
            .as_deref(),
        Some("refused")
    );
    valid_parents(&f.pool, &p).await;
    let before = image(&f.pool).await;
    authority(reserve(&f.store, &p).await.unwrap_err());
    unchanged(&f, before).await;
    println!(
        "W5/F1 held-reserve: exact-authority-error, hold-retained, zero-write, reopened-inventory-equal"
    );
}

#[tokio::test]
async fn classified_claim_reports_guard_before_valid_live_lease_and_queue_mutation() {
    let (f, p) = classified().await;
    let now = lease(&f.pool, f.store.recovery.as_ref(), &p).await;
    valid_parents(&f.pool, &p).await;
    let before = image(&f.pool).await;
    authority(
        f.store
            .claim_next(&p.endpoint, "legacy-resource", "lease-owner", 7, &now)
            .await
            .unwrap_err(),
    );
    unchanged(&f, before).await;
    println!(
        "W5/F1 classified-claim: exact-authority-error, matching-live-lease, zero-write, reopened-inventory-equal"
    );
}

async fn unclassified() -> (tempfile::TempDir, SqlitePool, Parents) {
    let root = tempfile::tempdir().unwrap();
    let pool = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,capabilities_json) VALUES('endpoint','legacy','acp',1,'[]')").execute(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO conversations(conversation_id,participants_json) VALUES('conversation','[]')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES('task','task','endpoint','endpoint','conversation','synthetic',5,0,0,0)").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO deliveries(delivery_id,task_id,target_endpoint_id,attempt,dispatched_at) VALUES('delivery','task','endpoint',1,'now')").execute(&pool).await.unwrap();
    let p = Parents {
        task: "task".into(),
        delivery: "delivery".into(),
        endpoint: "endpoint".into(),
    };
    valid_parents(&pool, &p).await;
    (root, pool, p)
}

async fn reject_bad_leases(store: &ReliabilityStore, pool: &SqlitePool, p: &Parents, now: &str) {
    let before = image(pool).await;
    for (owner, fence, clock) in [
        ("wrong", 7, now),
        ("lease-owner", 8, now),
        ("lease-owner", 7, "9999-01-01T00:00:00Z"),
    ] {
        assert!(matches!(
            store
                .claim_next(&p.endpoint, "legacy-resource", owner, fence, clock)
                .await,
            Err(ReliabilityError::WorkflowConflict)
        ));
        assert_eq!(image(pool).await, before);
    }
}

#[tokio::test]
async fn unclassified_legacy_success_preserves_dedup_fence_and_stale_cas() {
    let (root, pool, p) = unclassified().await;
    let store = ReliabilityStore::new(pool.clone());
    let reserved = reserve(&store, &p).await.unwrap();
    let duplicate = reserve(&store, &p).await.unwrap();
    assert_eq!(
        (reserved.queue_id.as_str(), reserved.sequence),
        (duplicate.queue_id.as_str(), duplicate.sequence)
    );
    let now = lease(&pool, None, &p).await;
    reject_bad_leases(&store, &pool, &p, &now).await;
    let claim = store
        .claim_next(&p.endpoint, "legacy-resource", "lease-owner", 7, &now)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claim.queue_id, reserved.queue_id);
    assert_eq!((claim.revision, claim.owner_fence), (1, 7));
    assert!(
        store
            .claim_next(&p.endpoint, "legacy-resource", "lease-owner", 7, &now)
            .await
            .unwrap()
            .is_none()
    );
    let mut stale = claim.clone();
    stale.revision += 1;
    let before = image(&pool).await;
    assert!(matches!(
        store
            .mark_send_started(&stale, "legacy-resource", "lease-owner", &now)
            .await,
        Err(ReliabilityError::WorkflowConflict)
    ));
    assert_eq!(image(&pool).await, before);
    store
        .mark_send_started(&claim, "legacy-resource", "lease-owner", &now)
        .await
        .unwrap();
    let before = image(&pool).await;
    assert!(matches!(
        store
            .mark_send_started(&claim, "legacy-resource", "lease-owner", &now)
            .await,
        Err(ReliabilityError::WorkflowConflict)
    ));
    assert_eq!(image(&pool).await, before);
    let actual: (String, i64, String, i64, i64) = sqlx::query_as(
        "SELECT state,revision,runtime_owner,owner_fence,send_started FROM agent_work_queue",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(actual, ("running".into(), 2, "lease-owner".into(), 7, 1));
    pool.close().await;
    let reopened = crate::storage::connect(root.path().join("state.db"))
        .await
        .unwrap();
    assert_eq!(image(&reopened).await, before);
    valid_parents(&reopened, &p).await;
    assert_eq!(sqlx::query_scalar::<_, i64>("SELECT (SELECT count(*) FROM runtime_eligibility_context)+(SELECT count(*) FROM offline_recovery_binding)+(SELECT count(*) FROM offline_recovery_rows)").fetch_one(&reopened).await.unwrap(), 0);
    println!(
        "W5/F1 unclassified: reserve/claim-success, duplicate-stable, wrong-owner/fence/expiry-and-stale-CAS-refused, reopened-state-equal"
    );
}
