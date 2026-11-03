use std::path::{Path, PathBuf};

use sqlx::{Connection, Row, SqliteConnection};

use super::{
    Failure, RuntimeLock, TransitionRequest,
    arguments::Mode,
    evidence::{self, Evidence},
    launcher, reconciliation_graph,
    reconciliation_records::{self as rec, Binding, Completion, Grant, Intent, Plan},
    reconciliation_sql, records, version2_records,
};

pub(super) fn discriminator(path: &Path) -> Result<bool, Failure> {
    rec::discriminator(path)
}

pub(super) async fn execute(request: TransitionRequest) -> Result<String, Failure> {
    execute_with_launcher(request, true).await
}

#[cfg(test)]
pub(super) async fn execute_fixture(request: TransitionRequest) -> Result<String, Failure> {
    execute_with_launcher(request, false).await
}

async fn execute_with_launcher(
    request: TransitionRequest,
    live_launcher: bool,
) -> Result<String, Failure> {
    let migration = super::Request {
        mode: request.mode,
        target_schema: 25,
        config: request.config.clone(),
        database: request.database.clone(),
        operation_id: request.operation_id.clone(),
        manifest_dir: request.manifest_dir.clone(),
        release_provenance: request.release_provenance.clone(),
        executable: request.executable.clone(),
    };
    let protocol2 = rec::discriminator(&rec::grant_path(&request.operation_id))?;
    let mut evidence = if protocol2 {
        Evidence::read_original(&migration)?
    } else {
        Evidence::read(&migration)?
    };
    launcher::check(&evidence.listeners, live_launcher, "transition-v2-pre-lock").await?;
    let lock = RuntimeLock::acquire(&request.database).map_err(|_| Failure::Busy)?;
    launcher::check(
        &evidence.listeners,
        live_launcher,
        "transition-v2-post-lock",
    )
    .await?;
    evidence.recheck(&migration)?;
    let authenticated = authenticate(&request, &migration, &evidence)?;
    evidence.recheck(&migration)?;
    launcher::check(
        &evidence.listeners,
        live_launcher,
        "transition-v2-pre-write",
    )
    .await?;
    if request.mode == Mode::Preflight || request.mode == Mode::Verify {
        let result = verify_readonly(&request, &authenticated).await;
        drop(lock);
        return result;
    }
    run_locked(request, authenticated, lock).await
}

struct Authenticated {
    plan: Plan,
    intent: Intent,
    intent_path: PathBuf,
    complete_path: PathBuf,
}

fn authenticate(
    request: &TransitionRequest,
    migration: &super::Request,
    evidence: &Evidence,
) -> Result<Authenticated, Failure> {
    let grant_path = rec::grant_path(&request.operation_id);
    let grant: Grant = rec::load_grant(&grant_path)?;
    validate_grant(&grant, request)?;
    let plan: Plan = rec::load(&request.transition_plan)?;
    if rec::plan_digest(&plan)? != plan.plan_digest
        || plan.operation_id != request.operation_id
        || plan.protocol != 2
        || plan.grant_digest != records::encoded_digest(&grant)?
    {
        return Err(Failure::ManualRequired);
    }
    let root_path = records::anchor(&request.database);
    let root: version2_records::Root = rec::load(&root_path)?;
    version2_records::validate_root(migration, evidence, &root)?;
    let root_digest = records::encoded_digest(&root)?;
    if plan.root_digest != root_digest
        || grant.root_digest != root_digest
        || plan.original_provenance_digest != root.operation.provenance_digest
        || plan.original_provenance_digest != grant.original_provenance_digest
        || plan.original_executable_digest != grant.original_executable_digest
        || plan.schema_complete_digest != grant.schema_complete_digest
        || plan.identity_digest != grant.identity_digest
        || grant.protocol != 2
        || grant.entry != "offline-transition-matrix-compatibility"
    {
        return Err(Failure::ManualRequired);
    }
    let successor_digest = evidence::hash_file(&request.executable)?;
    if successor_digest != grant.successor_executable_digest
        || plan.successor_executable_digest != successor_digest
        || grant.successor_package_digest.is_empty()
        || grant.successor_source.is_empty()
        || grant.successor_review.is_empty()
    {
        return Err(Failure::ManualRequired);
    }
    if !grant.allowed_modes.iter().any(|mode| {
        mode == match request.mode {
            Mode::Apply => "apply",
            Mode::Resume => "resume",
            Mode::Verify => "verify",
            Mode::Preflight => "preflight",
        }
    }) {
        return Err(Failure::ManualRequired);
    }
    if plan.original_binding.root_digest != root_digest
        || plan.original_binding.admin_operation_id != request.operation_id
        || plan.original_binding.phase != "identity_ready"
    {
        return Err(Failure::ManualRequired);
    }
    let intent_path = request.manifest_dir.join("reconciliation-intent-v2.json");
    let intent = rec::intent(&plan);
    if intent_path.exists() && rec::load::<Intent>(&intent_path)? != intent {
        return Err(Failure::ManualRequired);
    }
    Ok(Authenticated {
        plan,
        intent,
        intent_path,
        complete_path: request.manifest_dir.join("reconciliation-complete-v2.json"),
    })
}

fn validate_grant(grant: &Grant, request: &TransitionRequest) -> Result<(), Failure> {
    if grant.version != 1
        || grant.operation_id != request.operation_id
        || grant.protocol != 2
        || grant.entry != "offline-transition-matrix-compatibility"
        || grant.allowed_modes.is_empty()
        || !reconciliation_graph::hex_digest(&grant.root_digest)
        || !reconciliation_graph::hex_digest(&grant.original_provenance_digest)
        || !reconciliation_graph::hex_digest(&grant.original_executable_digest)
        || !reconciliation_graph::hex_digest(&grant.archive_digest)
        || !reconciliation_graph::hex_digest(&grant.schema_complete_digest)
        || !reconciliation_graph::hex_digest(&grant.identity_digest)
        || !reconciliation_graph::hex_digest(&grant.successor_executable_digest)
        || !reconciliation_graph::hex_digest(&grant.successor_package_digest)
    {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

async fn verify_readonly(
    request: &TransitionRequest,
    authenticated: &Authenticated,
) -> Result<String, Failure> {
    let mut connection = super::verification::open(&request.database, true).await?;
    let binding = load_binding(&mut connection).await?;
    if binding.root_digest != authenticated.plan.root_digest
        || binding.admin_operation_id != authenticated.plan.operation_id
        || binding.native_operation_id.as_deref()
            != Some(authenticated.plan.native_operation_id.as_str())
            && binding.phase != "identity_ready"
    {
        return Err(Failure::ManualRequired);
    }
    if reconciliation_sql::schema_state(&mut connection).await?
        == reconciliation_sql::SchemaState::Partial
    {
        return Err(Failure::ManualRequired);
    }
    if binding.phase == "identity_ready" {
        reconciliation_graph::validate_plan(&mut connection, &authenticated.plan).await?;
        connection.close().await.map_err(|_| Failure::Migration)?;
        return Ok("offline-transition-matrix-compatibility result=preflight".into());
    }
    reconciliation_graph::verify_classifications(
        &mut connection,
        &authenticated.plan,
        &authenticated.plan.operation_id,
    )
    .await?;
    reconciliation_graph::verify_actions(&mut connection, &authenticated.plan).await?;
    connection.close().await.map_err(|_| Failure::Migration)?;
    Ok(format!(
        "offline-transition-matrix-compatibility result=phase-{}",
        binding.phase
    ))
}

async fn run_locked(
    request: TransitionRequest,
    authenticated: Authenticated,
    _lock: RuntimeLock,
) -> Result<String, Failure> {
    let intent_bytes = serde_json::to_vec(&authenticated.intent).map_err(|_| Failure::Preflight)?;
    if !authenticated.intent_path.exists() {
        rec::publish(&authenticated.intent_path, &authenticated.intent)?;
    } else if rec::protected_bytes(&authenticated.intent_path)? != intent_bytes {
        return Err(Failure::ManualRequired);
    }
    super::flow::fault("reconciliation-after-intent");
    let mut connection = super::verification::open(&request.database, false).await?;
    let mut tx = connection
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(|_| Failure::Migration)?;
    let binding = load_binding(&mut tx).await?;
    let marker = reconciliation_sql::schema_state(&mut tx).await?;
    let state = match marker {
        reconciliation_sql::SchemaState::None => None,
        reconciliation_sql::SchemaState::Partial => return Err(Failure::ManualRequired),
        reconciliation_sql::SchemaState::Complete => {
            reconciliation_sql::marker(&mut tx, &authenticated.plan.operation_id).await?
        }
    };
    let mut expected_graph: Option<reconciliation_graph::ExpectedPostGraph> = None;
    if binding.phase == "identity_ready" && state.is_none() {
        if marker == reconciliation_sql::SchemaState::None {
            reconciliation_sql::create_schema(&mut tx).await?;
        }
        let pre_graph = reconciliation_graph::PreTransitionGraph::capture(&mut tx).await?;
        reconciliation_graph::validate_plan(&mut tx, &authenticated.plan).await?;
        reconciliation_sql::insert_audit(&mut tx, &authenticated.plan).await?;
        apply_chain(&mut tx, &authenticated.plan).await?;
        let original = super::transition_native::original(&mut tx).await?;
        super::transition_native::apply(
            &mut tx,
            &authenticated.plan.native_operation_id,
            &original,
            &authenticated.plan.source_state_digest,
        )
        .await?;
        reconciliation_sql::insert_classifications(&mut tx, &authenticated.plan).await?;
        let intent_digest = records::encoded_digest(&authenticated.intent)?;
        reconciliation_sql::initial_binding(&mut tx, &authenticated.plan, &intent_digest).await?;
        reconciliation_sql::insert_anchors(&mut tx, &authenticated.plan).await?;
        reconciliation_sql::insert_workspaces(&mut tx, &authenticated.plan.workspaces).await?;
        reconciliation_sql::delete_workspaces(&mut tx, &authenticated.plan.workspaces).await?;
        reconciliation_graph::verify_classifications(
            &mut tx,
            &authenticated.plan,
            &authenticated.plan.operation_id,
        )
        .await?;
        reconciliation_graph::verify_actions(&mut tx, &authenticated.plan).await?;
        expected_graph = Some(reconciliation_graph::ExpectedPostGraph::from_pre_state(
            &pre_graph,
        ));
        reconciliation_graph::verify_expected_graph(
            &mut tx,
            &authenticated.plan,
            expected_graph.as_ref(),
        )
        .await?;
        let graph_digest = reconciliation_graph::graph_digest(&mut tx, &authenticated.plan).await?;
        let commit_digest = records::encoded_digest(&(
            &authenticated.intent,
            &graph_digest,
            authenticated.plan.original_binding.revision + 1,
        ))?;
        reconciliation_sql::insert_marker(
            &mut tx,
            &authenticated.plan,
            &intent_digest,
            &graph_digest,
            &commit_digest,
            authenticated.plan.original_binding.revision + 1,
        )
        .await?;
        tx.commit().await.map_err(|_| Failure::Migration)?;
        connection.close().await.map_err(|_| Failure::Migration)?;
        super::flow::fault("reconciliation-after-first-commit");
    } else {
        tx.rollback().await.map_err(|_| Failure::Migration)?;
        connection.close().await.map_err(|_| Failure::Migration)?;
        if binding.phase != "transition_pending" && binding.phase != "baseline_ready" {
            return Err(Failure::ManualRequired);
        }
    }
    let mut reader = super::verification::open(&request.database, true).await?;
    let binding = load_binding(&mut reader).await?;
    reconciliation_graph::verify_classifications(
        &mut reader,
        &authenticated.plan,
        &authenticated.plan.operation_id,
    )
    .await?;
    reconciliation_graph::verify_expected_graph(
        &mut reader,
        &authenticated.plan,
        expected_graph.as_ref(),
    )
    .await?;
    let graph_digest = reconciliation_graph::graph_digest(&mut reader, &authenticated.plan).await?;
    reader.close().await.map_err(|_| Failure::Migration)?;
    let completion = Completion {
        protocol: 2,
        operation_id: authenticated.plan.operation_id.clone(),
        intent_digest: records::encoded_digest(&authenticated.intent)?,
        plan_digest: authenticated.plan.plan_digest.clone(),
        action_digest: authenticated.plan.action_digest.clone(),
        classification_digest: authenticated.plan.classification_digest.clone(),
        native_operation_id: authenticated.plan.native_operation_id.clone(),
        committed_graph_digest: graph_digest.clone(),
        committed_binding_revision: state
            .as_ref()
            .map(|marker| marker.3)
            .unwrap_or(binding.revision),
    };
    if authenticated.complete_path.exists() {
        if rec::load::<Completion>(&authenticated.complete_path)? != completion {
            return Err(Failure::ManualRequired);
        }
    } else {
        rec::publish(&authenticated.complete_path, &completion)?;
    }
    super::flow::fault("reconciliation-after-completion");
    let completion_digest = records::encoded_digest(&completion)?;
    let final_graph = graph_digest;
    let final_cas = records::encoded_digest(&(
        &authenticated.plan.root_digest,
        &completion_digest,
        &final_graph,
    ))?;
    let mut connection = super::verification::open(&request.database, false).await?;
    let mut tx = connection
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(|_| Failure::Migration)?;
    let binding = load_binding(&mut tx).await?;
    if binding.phase == "transition_pending" {
        reconciliation_sql::final_binding(
            &mut tx,
            &authenticated.plan,
            &completion.intent_digest,
            &completion_digest,
        )
        .await?;
    } else if binding.phase != "baseline_ready" {
        return Err(Failure::ManualRequired);
    }
    let marker = reconciliation_sql::marker(&mut tx, &authenticated.plan.operation_id)
        .await?
        .ok_or(Failure::ManualRequired)?;
    if marker.0 == "committed" {
        reconciliation_sql::finalize_marker(
            &mut tx,
            &authenticated.plan,
            &completion,
            &completion_digest,
            &final_graph,
            &final_cas,
            &marker.4,
        )
        .await?;
    } else if marker.0 != "finalized" {
        return Err(Failure::ManualRequired);
    }
    tx.commit().await.map_err(|_| Failure::Migration)?;
    connection.close().await.map_err(|_| Failure::Migration)?;
    super::flow::fault("reconciliation-after-final-commit");
    Ok("offline-transition-matrix-compatibility result=phase-baseline_ready".into())
}

async fn load_binding(connection: &mut SqliteConnection) -> Result<Binding, Failure> {
    sqlx::query(
        "SELECT db_generation,canonical_db_digest,root_digest,admin_operation_id,
                schema_intent_digest,schema_complete_digest,plan_digest,identity_digest,
                classification_digest,transition_intent_digest,transition_complete_digest,
                native_operation_id,phase,revision
         FROM offline_recovery_binding WHERE singleton=1",
    )
    .fetch_one(connection)
    .await
    .map_err(|_| Failure::ManualRequired)
    .and_then(|row| {
        Ok(Binding {
            db_generation: row
                .try_get("db_generation")
                .map_err(|_| Failure::ManualRequired)?,
            canonical_db_digest: row
                .try_get("canonical_db_digest")
                .map_err(|_| Failure::ManualRequired)?,
            root_digest: row
                .try_get("root_digest")
                .map_err(|_| Failure::ManualRequired)?,
            admin_operation_id: row
                .try_get("admin_operation_id")
                .map_err(|_| Failure::ManualRequired)?,
            schema_intent_digest: row
                .try_get("schema_intent_digest")
                .map_err(|_| Failure::ManualRequired)?,
            schema_complete_digest: row
                .try_get("schema_complete_digest")
                .map_err(|_| Failure::ManualRequired)?,
            plan_digest: row
                .try_get("plan_digest")
                .map_err(|_| Failure::ManualRequired)?,
            identity_digest: row
                .try_get("identity_digest")
                .map_err(|_| Failure::ManualRequired)?,
            classification_digest: row
                .try_get("classification_digest")
                .map_err(|_| Failure::ManualRequired)?,
            transition_intent_digest: row
                .try_get("transition_intent_digest")
                .map_err(|_| Failure::ManualRequired)?,
            transition_complete_digest: row
                .try_get("transition_complete_digest")
                .map_err(|_| Failure::ManualRequired)?,
            native_operation_id: row
                .try_get("native_operation_id")
                .map_err(|_| Failure::ManualRequired)?,
            phase: row.try_get("phase").map_err(|_| Failure::ManualRequired)?,
            revision: row
                .try_get("revision")
                .map_err(|_| Failure::ManualRequired)?,
        })
    })
}

async fn apply_chain(connection: &mut SqliteConnection, plan: &Plan) -> Result<(), Failure> {
    let decision = match plan.d1_unknown_chain.as_ref() {
        None => return Ok(()),
        Some(decision) if decision.references.len() == 11 => decision,
        Some(_) => return Err(Failure::ManualRequired),
    };
    if decision.evidence_digest.is_empty() {
        return Err(Failure::ManualRequired);
    }
    let tuple = D1ChainTuple::parse(&decision.references)?;
    let lease = sqlx::query(
        "SELECT l.resource_key,l.task_id,l.owner_token,l.fence,
                t.version AS task_version,a.revision AS admission_revision,
                c.delivery_id,c.revision AS continuation_revision,c.lease_fence,
                c.runtime_generation,d.session_id,d.attempt
         FROM execution_leases l
         JOIN tasks t ON t.task_id=l.task_id
         JOIN task_admissions a ON a.task_id=l.task_id
         JOIN task_continuations c ON c.task_id=l.task_id
         JOIN delivery_dispositions d ON d.delivery_id=c.delivery_id AND d.task_id=c.task_id
         WHERE l.resource_key=? AND l.task_id=? AND l.owner_token=? AND l.fence=?
           AND t.version=? AND a.revision=? AND c.delivery_id=? AND c.revision=?
           AND c.lease_fence=? AND c.runtime_generation=? AND d.session_id=? AND d.attempt=?
           AND l.state='recovery_needed' AND a.state='recovery_needed'
           AND c.state='recovery_needed' AND d.state='outcome_unknown'
           AND c.resource_key=l.resource_key AND c.lease_fence=l.fence
           AND d.session_id IS NOT NULL
           AND NOT EXISTS (SELECT 1 FROM historical_unknown_abandonments h
                           WHERE h.task_id=l.task_id AND h.delivery_id=c.delivery_id
                             AND h.resource_key=l.resource_key)",
    )
    .bind(&tuple.resource)
    .bind(&tuple.task)
    .bind(&tuple.owner)
    .bind(tuple.fence)
    .bind(tuple.task_version)
    .bind(tuple.admission_revision)
    .bind(&tuple.delivery)
    .bind(tuple.continuation_revision)
    .bind(tuple.fence)
    .bind(&tuple.generation)
    .bind(&tuple.session)
    .bind(tuple.attempt)
    .fetch_one(&mut *connection)
    .await
    .map_err(|_| Failure::ManualRequired)?;
    let resource: String = lease
        .try_get("resource_key")
        .map_err(|_| Failure::ManualRequired)?;
    let task: String = lease
        .try_get("task_id")
        .map_err(|_| Failure::ManualRequired)?;
    let owner: String = lease
        .try_get("owner_token")
        .map_err(|_| Failure::ManualRequired)?;
    let fence: i64 = lease
        .try_get("fence")
        .map_err(|_| Failure::ManualRequired)?;
    let delivery: String = lease
        .try_get("delivery_id")
        .map_err(|_| Failure::ManualRequired)?;
    let task_version: i64 = lease
        .try_get("task_version")
        .map_err(|_| Failure::ManualRequired)?;
    let admission_revision: i64 = lease
        .try_get("admission_revision")
        .map_err(|_| Failure::ManualRequired)?;
    let continuation_revision: i64 = lease
        .try_get("continuation_revision")
        .map_err(|_| Failure::ManualRequired)?;
    let generation: String = lease
        .try_get("runtime_generation")
        .map_err(|_| Failure::ManualRequired)?;
    let session_id: String = lease
        .try_get("session_id")
        .map_err(|_| Failure::ManualRequired)?;
    let attempt: i64 = lease
        .try_get("attempt")
        .map_err(|_| Failure::ManualRequired)?;
    let frozen = [
        resource.clone(),
        task.clone(),
        delivery.clone(),
        owner.clone(),
        fence.to_string(),
        task_version.to_string(),
        admission_revision.to_string(),
        continuation_revision.to_string(),
        generation.clone(),
        session_id.clone(),
        attempt.to_string(),
    ];
    if !tuple.matches(&frozen) || decision.decision_id != plan.native_operation_id {
        return Err(Failure::ManualRequired);
    }
    let session_digest = records::encoded_digest(&session_id)?;
    let before_digest = records::encoded_digest(&(
        task_version,
        admission_revision,
        continuation_revision,
        &generation,
        &resource,
        &task,
        &delivery,
    ))?;
    sqlx::query(
        "INSERT INTO historical_unknown_abandonments
         (decision_id,task_id,delivery_id,resource_key,owner_token,fence,
          before_task_version,before_admission_revision,before_continuation_revision,
          before_generation,post_continuation_revision,disposition,reason,policy_ref,
          authorization_ref,not_replayed,created_at,session_id_digest,before_state_digest,
          post_state_digest,abandonment_kind)
         VALUES(?,?,?,?,?,?,?,?,?,?,?,'manual_abandonment',?,?,?,1,datetime('now'),?,?,?,'historical_unknown_abandoned')",
    )
    .bind(&plan.native_operation_id)
    .bind(&task).bind(&delivery).bind(&resource).bind(&owner).bind(fence)
    .bind(task_version).bind(admission_revision).bind(continuation_revision)
    .bind(&generation).bind(continuation_revision + 1)
    .bind("offline protocol-2 D1 reconciliation")
    .bind("R093-D1").bind(&plan.operation_id).bind(&plan.native_operation_id)
    .bind(&session_digest).bind(&before_digest)
    .bind(&records::encoded_digest(&(task_version, admission_revision, continuation_revision + 1, &generation, &resource, &task, &delivery, "terminal"))?)
    .execute(&mut *connection).await.map_err(|_| Failure::ManualRequired)?;
    let changed = sqlx::query(
        "UPDATE execution_leases SET state='released',heartbeat_at=?
         WHERE resource_key=? AND task_id=? AND owner_token=? AND fence=?
           AND state='recovery_needed'
           AND EXISTS (SELECT 1 FROM tasks WHERE task_id=execution_leases.task_id AND version=?)",
    )
    .bind(&plan.abandonment_at)
    .bind(&resource)
    .bind(&task)
    .bind(&owner)
    .bind(fence)
    .bind(task_version)
    .execute(&mut *connection)
    .await
    .map_err(|_| Failure::ManualRequired)?;
    if changed.rows_affected() != 1 {
        return Err(Failure::ManualRequired);
    }
    let changed = sqlx::query(
        "UPDATE task_continuations
         SET state='terminal',revision=revision+1,heartbeat_at=?
         WHERE task_id=? AND resource_key=? AND delivery_id=? AND lease_fence=? AND runtime_generation=?
           AND state='recovery_needed' AND revision=?
           AND EXISTS (SELECT 1 FROM task_admissions WHERE task_id=task_continuations.task_id AND revision=?)",
    )
    .bind(&plan.abandonment_at)
    .bind(&task)
    .bind(&resource)
    .bind(&delivery)
    .bind(fence)
    .bind(&generation)
    .bind(continuation_revision)
    .bind(admission_revision)
    .execute(&mut *connection)
    .await
    .map_err(|_| Failure::ManualRequired)?;
    if changed.rows_affected() != 1 {
        return Err(Failure::ManualRequired);
    }
    let changed = sqlx::query(
        "UPDATE delivery_dispositions
         SET state='terminal',reason_code='historical-unknown-abandoned'
         WHERE task_id=? AND delivery_id=? AND attempt=? AND session_id=? AND state='outcome_unknown'",
    )
    .bind(&task)
    .bind(&delivery)
    .bind(attempt)
    .bind(&session_id)
    .execute(&mut *connection)
    .await
    .map_err(|_| Failure::ManualRequired)?;
    if changed.rows_affected() != 1 {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

struct D1ChainTuple {
    resource: String,
    task: String,
    delivery: String,
    owner: String,
    fence: i64,
    task_version: i64,
    admission_revision: i64,
    continuation_revision: i64,
    generation: String,
    session: String,
    attempt: i64,
}

impl D1ChainTuple {
    fn parse(values: &[String]) -> Result<Self, Failure> {
        if values.len() != 11 {
            return Err(Failure::ManualRequired);
        }
        Ok(Self {
            resource: values[0].clone(),
            task: values[1].clone(),
            delivery: values[2].clone(),
            owner: values[3].clone(),
            fence: values[4].parse().map_err(|_| Failure::ManualRequired)?,
            task_version: values[5].parse().map_err(|_| Failure::ManualRequired)?,
            admission_revision: values[6].parse().map_err(|_| Failure::ManualRequired)?,
            continuation_revision: values[7].parse().map_err(|_| Failure::ManualRequired)?,
            generation: values[8].clone(),
            session: values[9].clone(),
            attempt: values[10].parse().map_err(|_| Failure::ManualRequired)?,
        })
    }

    fn matches(&self, values: &[String; 11]) -> bool {
        self.resource == values[0]
            && self.task == values[1]
            && self.delivery == values[2]
            && self.owner == values[3]
            && self.fence.to_string() == values[4]
            && self.task_version.to_string() == values[5]
            && self.admission_revision.to_string() == values[6]
            && self.continuation_revision.to_string() == values[7]
            && self.generation == values[8]
            && self.session == values[9]
            && self.attempt.to_string() == values[10]
    }
}

#[cfg(test)]
mod d1_tuple_tests {
    use super::{D1ChainTuple, apply_chain};
    use crate::offline_production_migration::reconciliation_records::{Binding, Decision, Plan};
    use sqlx::Executor;

    fn fixture_plan() -> Plan {
        Plan {
            protocol: 2,
            plan_digest: "plan".into(),
            operation_id: "operation".into(),
            root_digest: "root".into(),
            original_provenance_digest: "prov".into(),
            original_executable_digest: "orig".into(),
            archive_digest: "archive".into(),
            schema_complete_digest: "schema".into(),
            identity_digest: "identity".into(),
            grant_digest: "grant".into(),
            successor_executable_digest: "succ".into(),
            successor_package_digest: "pkg".into(),
            successor_source: "source".into(),
            successor_review: "review".into(),
            native_operation_id: "native".into(),
            source_state_digest: "state".into(),
            original_binding: Binding {
                db_generation: "generation".into(),
                canonical_db_digest: "db".into(),
                root_digest: "root".into(),
                admin_operation_id: "operation".into(),
                schema_intent_digest: "intent".into(),
                schema_complete_digest: None,
                plan_digest: None,
                identity_digest: None,
                classification_digest: None,
                transition_intent_digest: None,
                transition_complete_digest: None,
                native_operation_id: None,
                phase: "identity_ready".into(),
                revision: 4,
            },
            classifications: vec![],
            inserted_classifications: vec![],
            actions: vec![],
            d1_unknown_chain: None,
            d1_quarantined: None,
            d2_owner: vec![],
            anchors: vec![],
            workspaces: vec![],
            native_at: "now".into(),
            abandonment_at: "t".into(),
            action_digest: "action".into(),
            classification_digest: "classification".into(),
        }
    }

    #[test]
    fn rejects_non_selected_chain_tuple() {
        let selected = [
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
        .map(String::from);
        let non_selected = [
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
        .map(String::from);
        let selected = D1ChainTuple::parse(&selected).unwrap();
        let other = D1ChainTuple::parse(&non_selected).unwrap();
        assert_ne!(selected.task, other.task);
    }

    #[tokio::test]
    async fn migration_two_chain_apply_preserves_non_selected() {
        let root = tempfile::tempdir().unwrap();
        let pool = crate::storage::connect(root.path().join("d1.db"))
            .await
            .unwrap();
        crate::storage::migrate(&pool).await.unwrap();
        for sql in [
            "INSERT INTO agents VALUES('agent-a','a','acp',1,NULL,'[]'),('agent-b','b','acp',1,NULL,'[]')",
            "INSERT INTO conversations VALUES('conversation',NULL,NULL,NULL,'[]')",
            "INSERT INTO tasks VALUES('task-a','task-a',NULL,'agent-a','agent-b','conversation',NULL,'fixture',5,0,0,NULL,11),('task-b','task-b',NULL,'agent-a','agent-b','conversation',NULL,'fixture',5,0,0,NULL,12)",
            "INSERT INTO deliveries VALUES('delivery-a','task-a',1,'agent-b','t','t'),('delivery-b','task-b',1,'agent-b','t','t')",
            "INSERT INTO task_admissions(task_id,state,revision,created_at,updated_at) VALUES('task-a','recovery_needed',13,'t','t'),('task-b','recovery_needed',14,'t','t')",
            "INSERT INTO execution_leases VALUES('resource-a','task-a','owner-a',7,'recovery_needed','t','t','later'),('resource-b','task-b','owner-b',8,'recovery_needed','t','t','later')",
            "INSERT INTO task_continuations(task_id,resource_key,delivery_id,lease_fence,revision,state,next_turn,completed_turns,consecutive_no_progress,next_prompt,started_at,heartbeat_at,last_progress_at,observed_output_bytes,runtime_generation,runtime_generation_version) VALUES('task-a','resource-a','delivery-a',7,17,'recovery_needed',1,0,0,'','t','t','t',0,'generation-a',1),('task-b','resource-b','delivery-b',8,18,'recovery_needed',1,0,0,'','t','t','t',0,'generation-b',1)",
            "INSERT INTO delivery_dispositions(delivery_id,task_id,attempt,state,session_id) VALUES('delivery-a','task-a',2,'outcome_unknown','session-a'),('delivery-b','task-b',1,'outcome_unknown','session-b')",
        ] {
            pool.execute(sql).await.unwrap();
        }
        let snapshot_pool = pool.clone();
        let snapshot = move |task: &str| {
            let task = task.to_owned();
            let pool = snapshot_pool.clone();
            async move {
                sqlx::query_as::<_, (String, String, String)>("SELECT json_array(l.resource_key,l.task_id,l.owner_token,l.fence,l.state,l.acquired_at,l.heartbeat_at,l.expires_at), json_array(c.task_id,c.resource_key,c.delivery_id,c.lease_fence,c.revision,c.state,c.next_turn,c.completed_turns,c.consecutive_no_progress,c.next_prompt,c.started_at,c.heartbeat_at,c.last_progress_at,c.observed_output_bytes,c.runtime_generation,c.runtime_generation_version), json_array(d.delivery_id,d.task_id,d.attempt,d.state,d.session_id,d.child_fingerprint,d.reap_status,d.reap_at,d.reason_code) FROM execution_leases l JOIN task_continuations c ON c.task_id=l.task_id JOIN delivery_dispositions d ON d.task_id=l.task_id WHERE l.task_id=?").bind(task).fetch_one(&pool).await.unwrap()
            }
        };
        let before_other = snapshot("task-b").await;
        let before_selected = snapshot("task-a").await;
        let audit_before: Vec<String> = sqlx::query_scalar("SELECT json_array(decision_id,task_id,delivery_id,resource_key,owner_token,fence,before_task_version,before_admission_revision,before_continuation_revision,before_generation,post_continuation_revision,disposition,reason,policy_ref,authorization_ref,not_replayed,created_at,session_id_digest,before_state_digest,post_state_digest,abandonment_kind) FROM historical_unknown_abandonments ORDER BY decision_id").fetch_all(&pool).await.unwrap();
        let mut connection = pool.acquire().await.unwrap();
        let refs = [
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
        .into_iter()
        .map(String::from)
        .collect();
        let mut plan = fixture_plan();
        plan.d1_unknown_chain = Some(Decision {
            decision_id: "native".into(),
            evidence_digest: "evidence".into(),
            references: refs,
        });
        apply_chain(&mut connection, &plan).await.unwrap();
        let after_other = snapshot("task-b").await;
        let after_selected = snapshot("task-a").await;
        let audit_after: Vec<String> = sqlx::query_scalar("SELECT json_array(decision_id,task_id,delivery_id,resource_key,owner_token,fence,before_task_version,before_admission_revision,before_continuation_revision,before_generation,post_continuation_revision,disposition,reason,policy_ref,authorization_ref,not_replayed,created_at,session_id_digest,before_state_digest,post_state_digest,abandonment_kind) FROM historical_unknown_abandonments ORDER BY decision_id").fetch_all(&pool).await.unwrap();
        assert_eq!(before_other, after_other);
        assert_eq!(
            before_selected.0.replace("recovery_needed", "released"),
            after_selected.0
        );
        assert_ne!(before_selected.1, after_selected.1);
        assert_eq!(audit_before, Vec::<String>::new());
        assert_eq!(audit_after.len(), 1);
        let audit: i64 = sqlx::query_scalar("SELECT count(*) FROM historical_unknown_abandonments WHERE task_id='task-a' AND delivery_id='delivery-a'").fetch_one(&pool).await.unwrap();
        let other_audit: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM historical_unknown_abandonments WHERE task_id='task-b'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(audit, 1);
        assert_eq!(other_audit, 0);
    }
}
