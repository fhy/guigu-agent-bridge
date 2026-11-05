use std::{ffi::OsString, path::PathBuf};

use chrono::Utc;
use sqlx::Row;

use crate::storage::{
    MatrixRecoveryStore,
    recovery_eligibility::{EffectDomain, LiveFence, RecoveryScope, bootstrap_nonadmin, writer},
};

pub(crate) struct Request {
    database: PathBuf,
    config: PathBuf,
    task: String,
    owner: String,
    revision: i64,
}

pub(crate) fn parse(arguments: &[OsString]) -> Result<Request, ()> {
    if arguments.len() != 11 || arguments[0] != "hold-unresolved-admission" {
        return Err(());
    }
    let mut fields = std::collections::BTreeMap::new();
    for pair in arguments[1..].chunks_exact(2) {
        let key = pair[0].to_str().ok_or(())?;
        let value = pair[1]
            .to_str()
            .filter(|value| !value.is_empty())
            .ok_or(())?;
        if !["--database", "--config", "--task", "--owner", "--revision"].contains(&key)
            || fields.insert(key, value).is_some()
        {
            return Err(());
        }
    }
    let revision = fields["--revision"].parse::<i64>().map_err(|_| ())?;
    if revision < 0 {
        return Err(());
    }
    Ok(Request {
        database: fields["--database"].into(),
        config: fields["--config"].into(),
        task: fields["--task"].into(),
        owner: fields["--owner"].into(),
        revision,
    })
}

pub(crate) async fn run(request: Request) -> Result<(), ()> {
    let config = crate::config::load(&request.config).map_err(|_| ())?;
    if config.bridge.database.canonicalize().map_err(|_| ())?
        != request.database.canonicalize().map_err(|_| ())?
    {
        return Err(());
    }
    let guard =
        crate::runtime_admission::Guard::acquire(&request.database, &request.config, "startup")
            .map_err(|_| ())?;
    let pool = crate::storage::connect(&request.database)
        .await
        .map_err(|_| ())?;
    let administrative: i64 = sqlx::query_scalar("SELECT count(*) FROM offline_recovery_binding")
        .fetch_one(&pool)
        .await
        .map_err(|_| ())?;
    if administrative != 0 {
        return Err(());
    }
    let context = bootstrap_nonadmin(&pool, config.transports.matrix.enabled)
        .await
        .map_err(|_| ())?;
    let scope = RecoveryScope::new(context, EffectDomain::Local, None);
    let recovery = MatrixRecoveryStore::new_scoped(pool.clone(), Some(scope.clone()));
    let owner = uuid::Uuid::now_v7().to_string();
    let now = Utc::now();
    let epoch = if config.transports.matrix.enabled {
        let epoch = recovery
            .acquire_component_lease(
                "MatrixSync",
                &owner,
                &now.to_rfc3339(),
                &(now + chrono::Duration::seconds(60)).to_rfc3339(),
            )
            .await
            .map_err(|_| ())?;
        scope
            .install_matrix_fence(LiveFence {
                component: "MatrixSync".into(),
                owner: owner.clone(),
                epoch,
            })
            .map_err(|_| ())?;
        Some(epoch)
    } else {
        None
    };
    let result = hold(&pool, &scope, &request).await;
    let released = if let Some(epoch) = epoch {
        recovery
            .release_component_lease("MatrixSync", &owner, epoch, &Utc::now().to_rfc3339())
            .await
            .map_err(|_| ())
    } else {
        Ok(())
    };
    pool.close().await;
    guard.revalidate().map_err(|_| ())?;
    result.and(released)
}

async fn hold(pool: &sqlx::SqlitePool, scope: &RecoveryScope, request: &Request) -> Result<(), ()> {
    let mut transaction = writer::begin(pool, Some(scope)).await.map_err(|_| ())?;
    let row = sqlx::query("SELECT admission.state,admission.revision,admission.runtime_instance,owner.state AS owner_state FROM task_admissions admission JOIN runtime_instances owner ON owner.instance_token=admission.runtime_instance WHERE admission.task_id=?")
        .bind(&request.task).fetch_one(&mut *transaction).await.map_err(|_| ())?;
    let state: String = row.try_get("state").map_err(|_| ())?;
    let revision: i64 = row.try_get("revision").map_err(|_| ())?;
    if state != "dispatching"
        || revision != request.revision
        || row
            .try_get::<String, _>("runtime_instance")
            .map_err(|_| ())?
            != request.owner
        || row.try_get::<String, _>("owner_state").map_err(|_| ())? != "stopped"
    {
        return Err(());
    }
    let conflicts: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM runtime_instances WHERE state!='stopped')+(SELECT count(*) FROM deliveries WHERE task_id=?)+(SELECT count(*) FROM execution_leases WHERE task_id=?)+(SELECT count(*) FROM task_continuations WHERE task_id=?)+(SELECT count(*) FROM session_owner_bindings WHERE task_id=?)+(SELECT count(*) FROM agent_work_queue WHERE task_id=?)+(SELECT count(*) FROM workspace_claims WHERE task_id=?)")
        .bind(&request.task).bind(&request.task).bind(&request.task).bind(&request.task).bind(&request.task).bind(&request.task).fetch_one(&mut *transaction).await.map_err(|_| ())?;
    let statuses: Vec<String> =
        sqlx::query_scalar("SELECT status FROM task_events WHERE task_id=? ORDER BY seq")
            .bind(&request.task)
            .fetch_all(&mut *transaction)
            .await
            .map_err(|_| ())?;
    if conflicts != 0 || statuses != ["queued", "dispatched"] {
        return Err(());
    }
    record_hold(&mut transaction, scope, request).await?;
    let identity = writer::text_identity("task_admissions", &request.task).map_err(|_| ())?;
    let mutation = writer::prepare_mutation(&mut transaction, Some(scope), &identity)
        .await
        .map_err(|_| ())?;
    let changed = sqlx::query("UPDATE task_admissions SET state='recovery_needed',revision=revision+1,updated_at=? WHERE task_id=? AND state='dispatching' AND revision=? AND runtime_instance=?")
        .bind(Utc::now().to_rfc3339()).bind(&request.task).bind(request.revision).bind(&request.owner).execute(&mut *transaction).await.map_err(|_| ())?;
    if changed.rows_affected() != 1 {
        return Err(());
    }
    writer::complete_mutation(&mut transaction, Some(scope), mutation)
        .await
        .map_err(|_| ())?;
    transaction.commit().await.map_err(|_| ())?;
    tracing::warn!(task_id = %request.task, category = "unresolved-admission-held", "unknown task retained without replay or terminal verdict");
    Ok(())
}

async fn record_hold(
    connection: &mut sqlx::SqliteConnection,
    scope: &RecoveryScope,
    request: &Request,
) -> Result<(), ()> {
    let revision = request.revision.checked_add(1).ok_or(())?;
    let evidence = format!("unknown-preserved:{}:{revision}", request.owner);
    let now = Utc::now().to_rfc3339();
    sqlx::query("INSERT INTO transport_receipts(transport,external_event_id,room_id,task_id,receipt_kind,route_kind,state,result_code,admin_actor,created_at,updated_at) VALUES('offline-admission-hold',?,'',?,'admin','unknown-dispatch-hold-v1','rejected',?,'offline-operator',?,?)")
        .bind(&request.task).bind(&request.task).bind(evidence).bind(&now).bind(&now).execute(&mut *connection).await.map_err(|_| ())?;
    let identity = writer::composite_text_identity(
        "transport_receipts",
        &["offline-admission-hold", &request.task],
    )
    .map_err(|_| ())?;
    writer::inserted(connection, Some(scope), &identity)
        .await
        .map_err(|_| ())
}

#[cfg(test)]
#[path = "admission_hold_tests.rs"]
mod tests;
