use sqlx::{Row, SqliteConnection};

use super::{
    Failure,
    reconciliation_records::{Completion, Plan, Workspace},
};

#[cfg(test)]
use super::reconciliation_schema::MARKER_DDL;
pub(super) use super::reconciliation_schema::{SchemaState, create_schema, schema_state};

pub(super) async fn insert_audit(
    connection: &mut SqliteConnection,
    plan: &Plan,
) -> Result<(), Failure> {
    for action in &plan.actions {
        let pre_digest = image_digest(action.before.as_ref())?;
        let post_digest = image_digest(action.after.as_ref())?;
        let changed = sqlx::query(
            "INSERT INTO r093_reconciliation_audit
             (operation_id,action_id,action_type,table_name,canonical_pk,
              original_revision,original_digest,pre_digest,post_digest,evidence_digest)
             VALUES(?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&plan.operation_id)
        .bind(&action.action_id)
        .bind(&action.action_type)
        .bind(&action.table_name)
        .bind(&action.canonical_pk)
        .bind(&action.original_revision)
        .bind(&action.original_digest)
        .bind(pre_digest)
        .bind(post_digest)
        .bind(&action.evidence_digest)
        .execute(&mut *connection)
        .await
        .map_err(|_| Failure::ManualRequired)?;
        if changed.rows_affected() != 1 {
            return Err(Failure::ManualRequired);
        }
    }
    Ok(())
}

pub(super) async fn insert_classifications(
    connection: &mut SqliteConnection,
    plan: &Plan,
) -> Result<(), Failure> {
    for item in plan
        .classifications
        .iter()
        .chain(&plan.inserted_classifications)
    {
        let changed = sqlx::query(
            "INSERT INTO offline_recovery_rows
             (table_name,canonical_pk,origin_generation,original_revision,
              original_digest,parent_relations,disposition,evidence_digest,
              context_kind,context_generation,admin_operation_id,plan_digest,
              matrix_operation_id)
             VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(&item.table_name)
        .bind(&item.canonical_pk)
        .bind(&item.origin_generation)
        .bind(&item.original_revision)
        .bind(&item.original_digest)
        .bind(serde_json::to_string(&item.parent_relations).map_err(|_| Failure::Preflight)?)
        .bind(&item.disposition)
        .bind(&item.evidence_digest)
        .bind(&item.context_kind)
        .bind(&item.context_generation)
        .bind(&item.admin_operation_id)
        .bind(&plan.plan_digest)
        .bind(&item.matrix_operation_id)
        .execute(&mut *connection)
        .await
        .map_err(|_| Failure::ManualRequired)?;
        if changed.rows_affected() != 1 {
            return Err(Failure::ManualRequired);
        }
    }
    Ok(())
}

pub(super) async fn insert_anchors(
    connection: &mut SqliteConnection,
    plan: &Plan,
) -> Result<(), Failure> {
    if plan.anchors.len() != 3 {
        return Err(Failure::ManualRequired);
    }
    let mut seen = std::collections::BTreeSet::new();
    for anchor in &plan.anchors {
        if !seen.insert((&anchor.table_name, &anchor.canonical_pk))
            || !matches!(anchor.table_name.as_str(), "agents" | "conversations")
        {
            return Err(Failure::ManualRequired);
        }
        let changed = sqlx::query(
            "INSERT INTO offline_recovery_metadata_anchors
             (table_name,canonical_pk,role,db_generation,root_digest,
              admin_operation_id,plan_digest,original_revision,original_digest)
             SELECT ?,?,'metadata_anchor',db_generation,root_digest,
                    admin_operation_id,plan_digest,?,?
             FROM offline_recovery_binding
             WHERE singleton=1 AND phase='transition_pending'
               AND admin_operation_id=? AND plan_digest=?",
        )
        .bind(&anchor.table_name)
        .bind(&anchor.canonical_pk)
        .bind(&anchor.original_revision)
        .bind(&anchor.original_digest)
        .bind(&plan.operation_id)
        .bind(&plan.plan_digest)
        .execute(&mut *connection)
        .await
        .map_err(|_| Failure::ManualRequired)?;
        if changed.rows_affected() != 1 {
            return Err(Failure::ManualRequired);
        }
    }
    Ok(())
}

pub(super) async fn insert_workspaces(
    connection: &mut SqliteConnection,
    workspaces: &[Workspace],
) -> Result<(), Failure> {
    for item in workspaces {
        let changed = sqlx::query(
            "INSERT INTO offline_recovery_workspace_deletions
             (table_name,canonical_pk,classification_json,snapshot_json,
              context_json,authority_fence_json,receipt_digest,runtime_owner,
              task_id,owner_fence)
             VALUES('workspace_claims',?,?,?,?,?,?,?,?,?)",
        )
        .bind(&item.canonical_pk)
        .bind(&item.classification_json)
        .bind(&item.snapshot_json)
        .bind(&item.context_json)
        .bind(&item.authority_fence_json)
        .bind(&item.receipt_digest)
        .bind(&item.runtime_owner)
        .bind(&item.task_id)
        .bind(item.owner_fence)
        .execute(&mut *connection)
        .await
        .map_err(|_| Failure::ManualRequired)?;
        if changed.rows_affected() != 1 {
            return Err(Failure::ManualRequired);
        }
    }
    Ok(())
}

pub(super) async fn delete_workspaces(
    connection: &mut SqliteConnection,
    workspaces: &[Workspace],
) -> Result<(), Failure> {
    for item in workspaces {
        let changed = sqlx::query(
            "DELETE FROM workspace_claims
             WHERE claim_id=json_extract(?, '$[0]')
               AND runtime_owner=? AND task_id=? AND owner_fence=? AND state='active'",
        )
        .bind(&item.canonical_pk)
        .bind(&item.runtime_owner)
        .bind(&item.task_id)
        .bind(item.owner_fence)
        .execute(&mut *connection)
        .await
        .map_err(|_| Failure::ManualRequired)?;
        if changed.rows_affected() != 1 {
            return Err(Failure::ManualRequired);
        }
    }
    Ok(())
}

pub(super) async fn initial_binding(
    connection: &mut SqliteConnection,
    plan: &Plan,
    intent_digest: &str,
) -> Result<(), Failure> {
    let b = &plan.original_binding;
    let changed = sqlx::query(
        "UPDATE offline_recovery_binding
         SET schema_complete_digest=?,plan_digest=?,identity_digest=?,
             classification_digest=?,transition_intent_digest=?,
             native_operation_id=?,phase='transition_pending',revision=revision+1
         WHERE singleton=1 AND protocol=2 AND phase='identity_ready'
           AND db_generation=? AND canonical_db_digest=? AND root_digest=?
           AND admin_operation_id=? AND schema_intent_digest=?
           AND schema_complete_digest IS ? AND plan_digest IS ?
           AND identity_digest IS ? AND classification_digest IS ?
           AND transition_intent_digest IS ? AND transition_complete_digest IS ?
           AND native_operation_id IS ? AND revision=?",
    )
    .bind(&plan.schema_complete_digest)
    .bind(&plan.plan_digest)
    .bind(&plan.identity_digest)
    .bind(&plan.classification_digest)
    .bind(intent_digest)
    .bind(&plan.native_operation_id)
    .bind(&b.db_generation)
    .bind(&b.canonical_db_digest)
    .bind(&b.root_digest)
    .bind(&b.admin_operation_id)
    .bind(&b.schema_intent_digest)
    .bind(&b.schema_complete_digest)
    .bind(&b.plan_digest)
    .bind(&b.identity_digest)
    .bind(&b.classification_digest)
    .bind(&b.transition_intent_digest)
    .bind(&b.transition_complete_digest)
    .bind(&b.native_operation_id)
    .bind(b.revision)
    .execute(&mut *connection)
    .await
    .map_err(|_| Failure::Migration)?;
    if changed.rows_affected() != 1 {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

pub(super) async fn insert_marker(
    connection: &mut SqliteConnection,
    plan: &Plan,
    intent_digest: &str,
    graph_digest: &str,
    commit_digest: &str,
    committed_revision: i64,
) -> Result<(), Failure> {
    let changed = sqlx::query(
        "INSERT INTO r093_reconciliation_marker
         (singleton,protocol,operation_id,root_digest,state,intent_digest,
          grant_digest,plan_digest,action_digest,archive_digest,schema_digest,
         identity_digest,committed_graph_digest,committed_binding_revision,
         commit_digest,completion_digest,final_graph_digest,final_cas_digest)
         VALUES(1,2,?,?, 'committed',?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL)",
    )
    .bind(&plan.operation_id)
    .bind(&plan.root_digest)
    .bind(intent_digest)
    .bind(&plan.grant_digest)
    .bind(&plan.plan_digest)
    .bind(&plan.action_digest)
    .bind(&plan.archive_digest)
    .bind(&plan.schema_complete_digest)
    .bind(&plan.identity_digest)
    .bind(graph_digest)
    .bind(committed_revision)
    .bind(commit_digest)
    .execute(&mut *connection)
    .await
    .map_err(|_| Failure::Migration)?;
    if changed.rows_affected() != 1 {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

pub(super) async fn final_binding(
    connection: &mut SqliteConnection,
    plan: &Plan,
    intent_digest: &str,
    completion_digest: &str,
) -> Result<(), Failure> {
    let changed = sqlx::query(
        "UPDATE offline_recovery_binding
         SET transition_complete_digest=?,phase='baseline_ready',revision=revision+1
         WHERE singleton=1 AND protocol=2 AND phase='transition_pending'
           AND db_generation=? AND canonical_db_digest=?
           AND root_digest=? AND admin_operation_id=? AND schema_intent_digest=?
           AND schema_complete_digest=? AND identity_digest=? AND plan_digest=?
           AND classification_digest=? AND transition_intent_digest IS ?
           AND native_operation_id=? AND revision=?
           AND transition_complete_digest IS NULL",
    )
    .bind(completion_digest)
    .bind(&plan.original_binding.db_generation)
    .bind(&plan.original_binding.canonical_db_digest)
    .bind(&plan.root_digest)
    .bind(&plan.operation_id)
    .bind(&plan.original_binding.schema_intent_digest)
    .bind(&plan.schema_complete_digest)
    .bind(&plan.identity_digest)
    .bind(&plan.plan_digest)
    .bind(&plan.classification_digest)
    .bind(intent_digest)
    .bind(&plan.native_operation_id)
    .bind(plan.original_binding.revision + 1)
    .execute(&mut *connection)
    .await
    .map_err(|_| Failure::Migration)?;
    if changed.rows_affected() != 1 {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

pub(super) async fn finalize_marker(
    connection: &mut SqliteConnection,
    plan: &Plan,
    completion: &Completion,
    completion_digest: &str,
    final_graph: &str,
    final_cas: &str,
    commit_digest: &str,
) -> Result<(), Failure> {
    let changed = sqlx::query(
        "UPDATE r093_reconciliation_marker
         SET state='finalized',completion_digest=?,final_graph_digest=?,
             final_cas_digest=?
         WHERE singleton=1 AND protocol=2 AND operation_id=?
           AND state='committed' AND completion_digest IS NULL
           AND final_graph_digest IS NULL AND final_cas_digest IS NULL
           AND root_digest=? AND intent_digest=? AND grant_digest=?
           AND plan_digest=? AND action_digest=? AND archive_digest=?
           AND schema_digest=? AND identity_digest=?
           AND committed_graph_digest=? AND committed_binding_revision=?
           AND commit_digest=?",
    )
    .bind(completion_digest)
    .bind(final_graph)
    .bind(final_cas)
    .bind(&plan.operation_id)
    .bind(&plan.root_digest)
    .bind(&completion.intent_digest)
    .bind(&plan.grant_digest)
    .bind(&plan.plan_digest)
    .bind(&plan.action_digest)
    .bind(&plan.archive_digest)
    .bind(&plan.schema_complete_digest)
    .bind(&plan.identity_digest)
    .bind(&completion.committed_graph_digest)
    .bind(completion.committed_binding_revision)
    .bind(commit_digest)
    .execute(&mut *connection)
    .await
    .map_err(|_| Failure::Migration)?;
    if changed.rows_affected() != 1 {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

pub(super) fn image_digest(
    image: Option<&super::reconciliation_records::RowImage>,
) -> Result<String, Failure> {
    super::records::encoded_digest(&image)
}

pub(super) async fn marker(
    connection: &mut SqliteConnection,
    operation: &str,
) -> Result<Option<(String, String, String, i64, String)>, Failure> {
    sqlx::query(
        "SELECT state,intent_digest,committed_graph_digest,committed_binding_revision,commit_digest
         FROM r093_reconciliation_marker WHERE singleton=1 AND operation_id=?",
    )
    .bind(operation)
    .fetch_optional(connection)
    .await
    .map_err(|_| Failure::ManualRequired)?
    .map(|row| {
        Ok((
            row.try_get("state").map_err(|_| Failure::ManualRequired)?,
            row.try_get("intent_digest")
                .map_err(|_| Failure::ManualRequired)?,
            row.try_get("committed_graph_digest")
                .map_err(|_| Failure::ManualRequired)?,
            row.try_get("committed_binding_revision")
                .map_err(|_| Failure::ManualRequired)?,
            row.try_get("commit_digest")
                .map_err(|_| Failure::ManualRequired)?,
        ))
    })
    .transpose()
}

#[cfg(test)]
#[path = "reconciliation_sql_tests.rs"]
mod tests;
