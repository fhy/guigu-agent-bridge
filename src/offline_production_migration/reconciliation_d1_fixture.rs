use sqlx::{Connection, SqliteConnection};

use super::{
    reconciliation_records::{Action, Decision, Plan, RowImage},
    records,
};
use crate::storage::recovery_eligibility::inventory;

pub(super) fn text(value: &str) -> String {
    format!(
        "text:{}",
        value
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect::<String>()
    )
}

pub(super) async fn seed(connection: &mut SqliteConnection, states: [&str; 2]) {
    for sql in [
        "INSERT INTO agents VALUES('agent-a','a','acp',1,NULL,'[]'),('agent-b','b','acp',1,NULL,'[]')",
        "INSERT INTO conversations VALUES('conversation',NULL,NULL,NULL,'[]')",
        "INSERT INTO tasks VALUES('task-a','task-a',NULL,'agent-a','agent-b','conversation',NULL,'fixture',5,0,0,NULL,11),('task-b','task-b',NULL,'agent-a','agent-b','conversation',NULL,'fixture',5,0,0,NULL,12)",
        "INSERT INTO deliveries VALUES('delivery-a','task-a',2,'agent-b','t','t'),('delivery-b','task-b',1,'agent-b','t','t')",
        "INSERT INTO runtime_instances VALUES('runtime-a','t','t','stopped','fingerprint-a'),('runtime-b','t','t','stopped','fingerprint-b')",
        "INSERT INTO execution_leases VALUES('resource-a','task-a','owner-a',7,'recovery_needed','t','t','later'),('resource-b','task-b','owner-b',8,'recovery_needed','t','t','later')",
        "INSERT INTO task_continuations(task_id,resource_key,delivery_id,lease_fence,revision,state,next_turn,completed_turns,consecutive_no_progress,next_prompt,started_at,heartbeat_at,last_progress_at,observed_output_bytes,runtime_generation,runtime_generation_version) VALUES('task-a','resource-a','delivery-a',7,17,'recovery_needed',1,0,0,'','t','t','t',0,'generation-a',1),('task-b','resource-b','delivery-b',8,18,'recovery_needed',1,0,0,'','t','t','t',0,'generation-b',1)",
        "INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state,session_id,child_fingerprint) VALUES('delivery-a','task-a',2,'outcome_unknown','session-a','child-a'),('delivery-b','task-b',1,'outcome_unknown','session-b','child-b')",
        "INSERT INTO sessions(session_id,endpoint_id,conversation_id,cwd,backend_id,protocol_version,state,created_at,updated_at) VALUES('session-a','agent-b','conversation','/a','fixture',1,'closed','t','t'),('session-b','agent-b','conversation','/b','fixture',1,'closed','t','t')",
    ] {
        sqlx::query(sql).execute(&mut *connection).await.unwrap();
    }
    for (suffix, state, revision) in [("a", states[0], 13), ("b", states[1], 14)] {
        sqlx::query("INSERT INTO task_admissions(task_id,state,revision,runtime_instance,created_at,updated_at) VALUES(?,?,?,?, 't','t')")
            .bind(format!("task-{suffix}")).bind(state).bind(revision)
            .bind(format!("runtime-{suffix}")).execute(&mut *connection).await.unwrap();
        sqlx::query("INSERT INTO runtime_owner_evidence(instance_token,runtime_generation,evidence_revision,state,pid,proc_starttime,process_fingerprint,boot_id,boottime_ns,captured_at,barrier_epoch,barrier_state,in_flight_spawns,child_scan_state,child_snapshot,is_current,max_age_seconds) VALUES(?,1,1,'live',123,456,?,'synthetic-boot',789,'t',0,'open',0,'inaccessible','{}',1,30)")
            .bind(format!("runtime-{suffix}")).bind(format!("fingerprint-{suffix}"))
            .execute(&mut *connection).await.unwrap();
    }
}

pub(super) fn decision(selected: &str, operation: &str) -> Decision {
    let fields = if selected == "a" {
        [
            "resource-a",
            "task-a",
            "delivery-a",
            "owner-a",
            "7",
            "11",
            "13",
            "17",
            "generation-a",
            "session-a",
            "2",
        ]
    } else {
        [
            "resource-b",
            "task-b",
            "delivery-b",
            "owner-b",
            "8",
            "12",
            "14",
            "18",
            "generation-b",
            "session-b",
            "1",
        ]
    };
    Decision {
        decision_id: operation.into(),
        evidence_digest: "2".repeat(64),
        references: fields.map(String::from).to_vec(),
    }
}

pub(super) fn update(action: &mut Action, selected: &str, at: &str) {
    let identity = format!(
        "{}-{selected}",
        if action.table_name == "delivery_dispositions" {
            "delivery"
        } else if action.table_name == "execution_leases" {
            "resource"
        } else {
            "task"
        }
    );
    if action.canonical_pk != serde_json::to_string(&vec![text(&identity)]).unwrap() {
        return;
    }
    let changes = match action.table_name.as_str() {
        "execution_leases" => vec![("state", text("released")), ("heartbeat_at", text(at))],
        "task_continuations" => vec![
            ("state", text("terminal")),
            (
                "revision",
                format!("integer:{}", if selected == "a" { "3138" } else { "3139" }),
            ),
            ("heartbeat_at", text(at)),
        ],
        "delivery_dispositions" => vec![
            ("state", text("terminal")),
            ("reason_code", text("historical-unknown-abandoned")),
        ],
        _ => return,
    };
    action.action_type = "update".into();
    action.set_columns = changes.iter().map(|(name, _)| (*name).into()).collect();
    action
        .unchanged_columns
        .retain(|name| !action.set_columns.contains(name));
    let after = action.after.as_mut().unwrap();
    for (name, value) in changes {
        let index = after
            .names
            .iter()
            .position(|column| column == name)
            .unwrap();
        after.values[index] = value;
    }
}

pub(super) async fn populate(connection: &mut SqliteConnection, plan: &mut Plan, selected: &str) {
    plan.d1_unknown_chain = Some(decision(selected, &plan.native_operation_id));
    for identity in inventory::identities(connection).await.unwrap() {
        let snapshot = inventory::snapshot(connection, &identity).await.unwrap();
        let (names, values) = inventory::values(connection, &identity).await.unwrap();
        let image = RowImage { names, values };
        let mut action = Action {
            action_id: format!("{}:{}", identity.table, identity.key),
            action_type: "preserve".into(),
            table_name: identity.table,
            canonical_pk: identity.key,
            original_revision: snapshot.revision,
            original_digest: snapshot.digest,
            before: Some(image.clone()),
            after: Some(image.clone()),
            set_columns: vec![],
            unchanged_columns: image.names,
            parents: vec![],
            evidence_digest: "2".repeat(64),
        };
        update(&mut action, selected, &plan.abandonment_at);
        plan.actions.push(action);
    }
    plan.action_digest = records::encoded_digest(&plan.actions).unwrap();
}

pub(super) async fn snapshot(connection: &mut SqliteConnection) -> Vec<(String, String, RowImage)> {
    let mut rows = Vec::new();
    for identity in inventory::identities(connection).await.unwrap() {
        let (names, values) = inventory::values(connection, &identity).await.unwrap();
        rows.push((identity.table, identity.key, RowImage { names, values }));
    }
    rows.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    rows
}

pub(super) async fn database() -> (tempfile::TempDir, SqliteConnection) {
    let root = tempfile::tempdir().unwrap();
    let pool = crate::storage::connect(root.path().join("d1.db"))
        .await
        .unwrap();
    crate::storage::migrate(&pool).await.unwrap();
    pool.close().await;
    let connection = SqliteConnection::connect(&format!(
        "sqlite://{}?mode=rw",
        root.path().join("d1.db").display()
    ))
    .await
    .unwrap();
    (root, connection)
}

pub(super) async fn audits(connection: &mut SqliteConnection) -> Vec<String> {
    sqlx::query_scalar("SELECT json_array(decision_id,task_id,delivery_id,resource_key,owner_token,fence,before_task_version,before_admission_revision,before_continuation_revision,before_generation,post_continuation_revision,disposition,reason,policy_ref,authorization_ref,not_replayed,created_at,session_id_digest,before_state_digest,post_state_digest,abandonment_kind,termination_proven) FROM historical_unknown_abandonments ORDER BY decision_id")
        .fetch_all(connection).await.unwrap()
}
