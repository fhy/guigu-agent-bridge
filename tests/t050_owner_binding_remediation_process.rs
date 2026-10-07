use std::{ffi::OsString, os::unix::ffi::OsStringExt, process::Command};

use guigu_agent_bridge::storage::{connect, migrate};
use sqlx::Executor;

const COMMAND: &str = "remediate-isolated-owner-bindings";
const REJECTED: &str =
    "remediate-isolated-owner-bindings result=rejected status=3 cause=arguments substage=none\n";

fn run(args: Vec<OsString>) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn malformed_cli_matrix_is_fixed_redacted_and_terminal() {
    let digest = "01".repeat(32);
    let cases = vec![
        vec![COMMAND.into()],
        vec![COMMAND.into(), "--expected-backup-sha256".into()],
        vec![
            COMMAND.into(),
            "--wrong".into(),
            digest.clone().into(),
            "--expected-source-set-sha256".into(),
            digest.clone().into(),
        ],
        vec![
            COMMAND.into(),
            "--expected-backup-sha256".into(),
            "bad-digest".into(),
            "--expected-source-set-sha256".into(),
            digest.clone().into(),
        ],
        vec![
            COMMAND.into(),
            "--expected-backup-sha256".into(),
            "AA".repeat(32).into(),
            "--expected-source-set-sha256".into(),
            digest.clone().into(),
        ],
        vec![
            COMMAND.into(),
            "--expected-backup-sha256".into(),
            digest.clone().into(),
            "--expected-source-set-sha256".into(),
            digest.clone().into(),
            "extra".into(),
        ],
        vec![
            COMMAND.into(),
            "--expected-backup-sha256".into(),
            OsString::from_vec(vec![0xff]),
            "--expected-source-set-sha256".into(),
            digest.into(),
        ],
    ];
    for args in cases {
        let output = run(args);
        assert_eq!(output.status.code(), Some(3));
        assert!(output.stderr.is_empty());
        assert_eq!(output.stdout, REJECTED.as_bytes());
    }
}

#[tokio::test]
async fn synthetic_child_success_and_repeat_are_canonical() {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("live.db");
    let retained = root.path().join("retained.db");
    let pool = connect(&database).await.unwrap();
    migrate(&pool).await.unwrap();
    for sql in [
        "INSERT INTO agents VALUES('agent-a','a','acp',1,NULL,'[]'),('agent-b','b','acp',1,NULL,'[]')",
        "INSERT INTO conversations VALUES('conversation',NULL,NULL,NULL,'[]')",
        "INSERT INTO tasks VALUES('task','task',NULL,'agent-a','agent-b','conversation',NULL,'closed fixture',5,0,0,NULL,1)",
        "INSERT INTO task_events VALUES('event-1','task',1,'running','2026-10-01T00:00:00.000000000Z','{}'),('event-2','task',2,'failed','2026-10-01T00:00:03.000000000Z','{}')",
        "INSERT INTO deliveries VALUES('delivery','task',1,'agent-b','2026-10-01T00:00:01.000000000Z','2026-10-01T00:00:03.000000000Z')",
        "INSERT INTO runtime_instances VALUES('owner-s','2026-09-30T23:00:00.000000000Z','2026-09-30T23:30:00.000000000Z','stopping','legacy-stopping'),('owner-a','2026-10-01T00:00:00.000000000Z','2026-10-01T00:00:03.000000000Z','active','legacy-active')",
        "INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES('task','terminal',4,'owner-s','2026-10-01T00:00:00.000000000Z','2026-10-01T00:00:03.000000000Z')",
        "INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state) VALUES('delivery','task',1,'terminal')",
        "INSERT INTO sessions(session_id,endpoint_id,conversation_id,cwd,backend_id,protocol_version,state,created_at,updated_at) VALUES('closed-session','agent-b','conversation','/tmp','agent-b@1',1,'closed','2026-10-01T00:00:00.000000000Z','2026-10-01T00:00:03.000000000Z')",
        "INSERT INTO execution_leases VALUES('resource','task','owner-s',7,'released','2026-10-01T00:00:00.000000000Z','2026-10-01T00:00:03.000000000Z','2026-10-01T00:00:03.000000000Z')",
        "INSERT INTO task_continuations(task_id,resource_key,delivery_id,lease_fence,revision,state,next_turn,completed_turns,consecutive_no_progress,next_prompt,started_at,heartbeat_at,last_progress_at,observed_output_bytes,runtime_generation,runtime_generation_version) VALUES('task','resource','delivery',7,2,'blocked',1,1,0,'','2026-10-01T00:00:00.000000000Z','2026-10-01T00:00:03.000000000Z','2026-10-01T00:00:03.000000000Z',12,'generation.v1:strict',1)",
    ] {
        pool.execute(sql).await.unwrap();
    }
    let response_hash = uuid::Uuid::new_v5(
        &uuid::Uuid::from_u128(0x7297ec04_1338_57b4_9bc4_f0ff9f84fa5a),
        b"{}",
    )
    .to_string();
    sqlx::query("INSERT INTO continuation_response_receipts VALUES('receipt','task','delivery','resource',7,1,?,'structured',2,'generation.v1:strict','2026-10-01T00:00:03.000000000Z')")
        .bind(response_hash).execute(&pool).await.unwrap();
    pool.close().await;
    std::fs::copy(&database, &retained).unwrap();
    let digest = "01".repeat(32);
    let invoke = || {
        Command::new(env!("CARGO_BIN_EXE_guigu-agent-bridge"))
            .args([
                COMMAND,
                "--expected-backup-sha256",
                &digest,
                "--expected-source-set-sha256",
                &digest,
            ])
            .env("T050_OWNER_BINDING_TEST_DATABASE", &database)
            .env("T050_OWNER_BINDING_TEST_RETAINED", &retained)
            .output()
            .unwrap()
    };
    let first = invoke();
    assert_eq!(first.status.code(), Some(0));
    assert!(first.stderr.is_empty());
    assert_eq!(
        String::from_utf8(first.stdout).unwrap(),
        "remediate-isolated-owner-bindings result=remediated status=0 cause=none substage=none\n"
    );
    let repeat = invoke();
    assert_eq!(repeat.status.code(), Some(0));
    assert!(repeat.stderr.is_empty());
    assert_eq!(
        String::from_utf8(repeat.stdout).unwrap(),
        "remediate-isolated-owner-bindings result=already-remediated status=0 cause=none substage=none\n"
    );
}
