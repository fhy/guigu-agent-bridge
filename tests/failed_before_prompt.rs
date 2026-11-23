use guigu_agent_bridge::{
    bus::derive_endpoint_id,
    failed_before_prompt,
    models::{DeliveryId, TaskId},
    storage::{connect, migrate},
};
use sqlx::SqlitePool;
use std::path::PathBuf;

struct Fixture {
    pool: SqlitePool,
    path: PathBuf,
    task: TaskId,
    delivery: DeliveryId,
}
async fn fixture() -> Fixture {
    let path =
        std::env::temp_dir().join(format!("failed-before-prompt-{}.db", uuid::Uuid::now_v7()));
    let pool = connect(&path).await.unwrap();
    migrate(&pool).await.unwrap();
    let from = derive_endpoint_id("from");
    let to = derive_endpoint_id("to");
    for (id, name) in [(from, "from"), (to, "to")] {
        sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,address_json,capabilities_json) VALUES(?,?,'acp',1,'{\"acp\":{\"command\":\"mock\",\"args\":[]}}','[]')").bind(id.to_string()).bind(name).execute(&pool).await.unwrap();
    }
    let conversation = uuid::Uuid::now_v7().to_string();
    sqlx::query("INSERT INTO conversations(conversation_id,participants_json) VALUES(?,'[]')")
        .bind(&conversation)
        .execute(&pool)
        .await
        .unwrap();
    let task = TaskId::generate();
    let delivery = DeliveryId::generate();
    let owner = format!("owner-{task}");
    sqlx::query("INSERT INTO tasks(task_id,root_task_id,from_agent,to_agent,conversation_id,text,priority,depth,hops,version) VALUES(?,?,?,?,?,'prompt',5,0,0,0)").bind(task.to_string()).bind(task.to_string()).bind(from.to_string()).bind(to.to_string()).bind(conversation).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO runtime_instances(instance_token,started_at,heartbeat_at,state,process_fingerprint) VALUES(?,'t','t','stopped','fp')").bind(&owner).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES(?,'dispatching',1,?,'t','t')").bind(task.to_string()).bind(&owner).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO deliveries(delivery_id,task_id,attempt,target_endpoint_id,dispatched_at,acknowledged_at) VALUES(?,?,1,?,?,?)").bind(delivery.to_string()).bind(task.to_string()).bind(to.to_string()).bind("t").bind("t").execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state,session_id) VALUES(?,?,1,'acknowledged','setup-session')").bind(delivery.to_string()).bind(task.to_string()).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO execution_leases(resource_key,task_id,owner_token,fence,state,acquired_at,heartbeat_at,expires_at) VALUES('resource',?,?,4,'recovery_needed','t','t','later')").bind(task.to_string()).bind(&owner).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,1,'queued','t','\"queued\"')").bind(uuid::Uuid::now_v7().to_string()).bind(task.to_string()).execute(&pool).await.unwrap();
    let payload = format!(
        "{{\"dispatched\":{{\"delivery_id\":\"{}\",\"attempt\":1}}}}",
        delivery
    );
    sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,2,'dispatched','t',?)").bind(uuid::Uuid::now_v7().to_string()).bind(task.to_string()).bind(payload).execute(&pool).await.unwrap();
    Fixture {
        pool,
        path,
        task,
        delivery,
    }
}
impl Fixture {
    async fn close(self) {
        self.pool.close().await;
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{}", self.path.display(), suffix));
        }
    }
}

#[tokio::test]
async fn closes_only_acknowledged_pre_prompt_dispatch() {
    let f = fixture().await;
    failed_before_prompt::reconcile(&f.path, "2026-11-06T00:00:00Z")
        .await
        .unwrap();
    let state: (String, String, String, String) = sqlx::query_as("SELECT a.state,p.state,l.state,e.status FROM task_admissions a JOIN delivery_dispositions p ON p.task_id=a.task_id JOIN execution_leases l ON l.task_id=a.task_id JOIN task_events e ON e.task_id=a.task_id ORDER BY e.seq DESC LIMIT 1").fetch_one(&f.pool).await.unwrap();
    assert_eq!(
        state,
        (
            "terminal".into(),
            "terminal".into(),
            "released".into(),
            "failed".into()
        )
    );
    let reason: String =
        sqlx::query_scalar("SELECT reason_code FROM delivery_dispositions WHERE delivery_id=?")
            .bind(f.delivery.to_string())
            .fetch_one(&f.pool)
            .await
            .unwrap();
    assert_eq!(reason, "failed_before_prompt");
    f.close().await;
}

#[tokio::test]
async fn rejects_when_prompt_evidence_exists() {
    let f = fixture().await;
    sqlx::query("INSERT INTO task_continuations(task_id,resource_key,delivery_id,lease_fence,revision,state,next_turn,completed_turns,consecutive_no_progress,next_prompt,started_at,heartbeat_at,last_progress_at,observed_output_bytes) VALUES(?,?,?,4,1,'ready',1,0,0,'prompt','t','t','t',0)").bind(f.task.to_string()).bind("resource").bind(f.delivery.to_string()).execute(&f.pool).await.unwrap();
    assert_eq!(
        failed_before_prompt::reconcile(&f.path, "2026-11-06T00:00:00Z")
            .await
            .unwrap_err(),
        failed_before_prompt::Error::NotEligible
    );
    f.close().await;
}
