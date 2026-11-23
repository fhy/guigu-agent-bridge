use super::{AppRuntime, Arc, Fixture, MockServer, history_fixture};
use crate::storage::recovery_eligibility::{self as eligibility, EffectDomain, writer};
use sqlx::SqlitePool;

pub(super) async fn refuse_completed_history_effects(server: &MockServer, pool: &SqlitePool) {
    let mut outcomes = Vec::new();
    for effect in ["workspace", "continuation", "queue", "output"] {
        let f = Fixture::new();
        sqlx::query("VACUUM INTO ?")
            .bind(f.root.path().join("state.db").to_str().unwrap())
            .execute(pool)
            .await
            .unwrap();
        let copy = crate::storage::connect(f.root.path().join("state.db"))
            .await
            .unwrap();
        let mut c = copy.acquire().await.unwrap();
        assert!(
            crate::native_succession::state::read(&mut c)
                .await
                .unwrap()
                .is_none()
        );
        drop(c);
        insert_effect(&copy, effect).await;
        let before = snapshot(&copy).await;
        let notices = history_fixture::notices(&copy).await;
        let requests = server.received_requests().await.unwrap().len();
        let runtime = AppRuntime::start_with_config_and_a2a_faults(
            f.root.path().join("bridge.toml"),
            f.config(true, &server.uri()),
            Arc::new(crate::a2a::NoFault),
        )
        .await;
        let (held, stopped) = match runtime {
            Ok(runtime) => {
                let held = runtime.health_state().snapshot().await.readiness
                    == crate::runtime::Readiness::RecoveryBlocked
                    && runtime
                        .health_state()
                        .recovery_diagnostic_line()
                        .is_some_and(|line| line.contains("native-succession-refused"));
                (held, runtime.shutdown().await.is_ok())
            }
            Err(error) => {
                eprintln!("R114 F1 {effect}: unexpected startup error: {error:?}");
                (false, false)
            }
        };
        let unchanged = snapshot(&copy).await == before;
        let no_replay = history_fixture::notices(&copy).await == notices
            && server.received_requests().await.unwrap().len() == requests;
        eprintln!(
            "R114 F1 {effect}: native_hold={held} stopped={stopped} unchanged={unchanged} no_replay={no_replay}"
        );
        outcomes.push((effect, held && stopped && unchanged && no_replay));
        copy.close().await;
    }
    assert!(outcomes.iter().all(|(_, pass)| *pass), "{outcomes:?}");
}

async fn insert_effect(pool: &SqlitePool, effect: &str) {
    let s = history_fixture::scope(pool).await;
    let mut tx = writer::begin(pool, Some(&s)).await.unwrap();
    let terminal: i64 = sqlx::query_scalar("SELECT count(*) FROM task_admissions a JOIN deliveries d USING(task_id) JOIN delivery_dispositions p USING(delivery_id) WHERE a.task_id='task-0' AND a.state='terminal' AND d.acknowledged_at IS NOT NULL AND p.state='terminal'")
        .fetch_one(&mut *tx).await.unwrap();
    assert_eq!(terminal, 1);
    if effect == "continuation" {
        insert(&mut tx, &s, "execution_leases", "resource", "INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES('resource','task-0','succession-0',1,'released','t','t','t')").await;
    }
    let (table, key, query) = match effect {
        "workspace" => (
            "workspace_claims",
            "claim",
            "INSERT INTO workspace_claims(claim_id,runtime_owner,task_id,canonical_path,path_components_json,owner_fence,revision,state,created_at,updated_at) VALUES('claim','succession-0','task-0','/fixture','[\"/\",\"fixture\"]',1,1,'active','t','t')",
        ),
        "continuation" => (
            "task_continuations",
            "task-0",
            "INSERT INTO task_continuations(task_id,resource_key,delivery_id,lease_fence,revision,state,next_turn,completed_turns,consecutive_no_progress,next_prompt,started_at,heartbeat_at,last_progress_at,observed_output_bytes) VALUES('task-0','resource','task-0',1,1,'ready',1,0,0,'pending','t','t','t',0)",
        ),
        "queue" => (
            "agent_work_queue",
            "queue",
            "INSERT INTO agent_work_queue(queue_id,task_id,delivery_id,target_endpoint_id,room_id,sender_endpoint_id,idempotency_key,body_hash,lane,state,sequence,revision,created_at,updated_at) VALUES('queue','task-0','task-0','00000000-0000-0000-0000-000000000001','!room:example.test','from','key','hash','ordinary','queued',1,1,'t','t')",
        ),
        "output" => (
            "acp_output_sets",
            "output",
            "INSERT INTO acp_output_sets(set_id,task_id,attempt,turn,runtime_fence,room_id,render_version,state,created_at,updated_at) VALUES('output','task-0',1,1,'succession-0','!room:example.test','v1','assembling','t','t')",
        ),
        _ => unreachable!(),
    };
    insert(&mut tx, &s, table, key, query).await;
    assert!(
        !crate::storage::owner_restart::guards::work_clear(&mut tx)
            .await
            .unwrap()
    );
    tx.commit().await.unwrap();
    let context = eligibility::readback_nonadmin(pool, true).await.unwrap();
    let mut c = pool.acquire().await.unwrap();
    eligibility::require_lineage(
        &mut c,
        &context,
        EffectDomain::Matrix,
        &writer::text_identity(table, key).unwrap(),
    )
    .await
    .unwrap();
}

async fn insert(
    c: &mut sqlx::SqliteConnection,
    s: &eligibility::RecoveryScope,
    table: &str,
    key: &str,
    query: &'static str,
) {
    sqlx::query(query).execute(&mut *c).await.unwrap();
    let id = writer::text_identity(table, key).unwrap();
    writer::inserted(c, Some(s), &id).await.unwrap();
}

async fn snapshot(pool: &SqlitePool) -> (Vec<String>, Vec<String>) {
    let domain = history_fixture::snapshot(pool).await;
    let classifications = sqlx::query_scalar("SELECT json_array(table_name,canonical_pk,origin_generation,original_revision,original_digest,parent_relations,disposition,evidence_digest,context_kind,context_generation,admin_operation_id,plan_digest,matrix_operation_id) FROM offline_recovery_rows ORDER BY table_name,canonical_pk")
        .fetch_all(pool).await.unwrap();
    (domain, classifications)
}
