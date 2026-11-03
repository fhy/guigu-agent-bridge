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
    let grant: Grant = rec::load(&grant_path)?;
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
    if binding.phase == "identity_ready" && state.is_none() {
        if marker == reconciliation_sql::SchemaState::None {
            reconciliation_sql::create_schema(&mut tx).await?;
        }
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
        reconciliation_sql::final_binding(&mut tx, &authenticated.plan, &completion_digest).await?;
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
    if plan.d1_unknown_chain.is_none() {
        return Ok(());
    }
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM execution_leases l
         JOIN task_admissions a ON a.task_id=l.task_id
         JOIN task_continuations c ON c.task_id=l.task_id
         JOIN delivery_dispositions d ON d.delivery_id=c.delivery_id
         WHERE l.state='recovery_needed' AND a.state='recovery_needed'
           AND c.state='recovery_needed' AND d.state='outcome_unknown'",
    )
    .fetch_one(&mut *connection)
    .await
    .map_err(|_| Failure::ManualRequired)?;
    if count != 1 {
        return Err(Failure::ManualRequired);
    }
    let lease = sqlx::query(
        "SELECT resource_key,task_id,owner_token,fence FROM execution_leases
         WHERE state='recovery_needed'",
    )
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
    let changed = sqlx::query(
        "UPDATE execution_leases SET state='released',heartbeat_at=?
         WHERE resource_key=? AND task_id=? AND owner_token=? AND fence=?
           AND state='recovery_needed'",
    )
    .bind(&plan.abandonment_at)
    .bind(&resource)
    .bind(&task)
    .bind(&owner)
    .bind(fence)
    .execute(&mut *connection)
    .await
    .map_err(|_| Failure::ManualRequired)?;
    if changed.rows_affected() != 1 {
        return Err(Failure::ManualRequired);
    }
    let changed = sqlx::query(
        "UPDATE task_continuations
         SET state='terminal',revision=revision+1,heartbeat_at=?
         WHERE task_id=? AND resource_key=? AND lease_fence=? AND state='recovery_needed'",
    )
    .bind(&plan.abandonment_at)
    .bind(&task)
    .bind(&resource)
    .bind(fence)
    .execute(&mut *connection)
    .await
    .map_err(|_| Failure::ManualRequired)?;
    if changed.rows_affected() != 1 {
        return Err(Failure::ManualRequired);
    }
    let changed = sqlx::query(
        "UPDATE delivery_dispositions
         SET state='terminal',reason_code='historical-unknown-abandoned'
         WHERE task_id=? AND state='outcome_unknown'",
    )
    .bind(&task)
    .execute(&mut *connection)
    .await
    .map_err(|_| Failure::ManualRequired)?;
    if changed.rows_affected() != 1 {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}
