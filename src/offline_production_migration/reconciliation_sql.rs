use sqlx::{Row, SqliteConnection};

use super::{
    Failure,
    reconciliation_records::{Completion, Plan, Workspace},
};

const MARKER_DDL: &str = r#"
CREATE TABLE r093_reconciliation_marker (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  protocol INTEGER NOT NULL CHECK (protocol = 2),
  operation_id TEXT NOT NULL UNIQUE,
  root_digest TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('committed','finalized')),
  intent_digest TEXT NOT NULL,
  grant_digest TEXT NOT NULL,
  plan_digest TEXT NOT NULL,
  action_digest TEXT NOT NULL,
  archive_digest TEXT NOT NULL,
  schema_digest TEXT NOT NULL,
  identity_digest TEXT NOT NULL,
  committed_graph_digest TEXT NOT NULL,
  committed_binding_revision INTEGER NOT NULL,
  commit_digest TEXT NOT NULL,
  completion_digest TEXT,
  final_graph_digest TEXT,
  final_cas_digest TEXT,
  UNIQUE(operation_id, intent_digest),
  FOREIGN KEY(singleton) REFERENCES offline_recovery_binding(singleton),
  CHECK ((state='committed' AND completion_digest IS NULL
      AND final_graph_digest IS NULL AND final_cas_digest IS NULL)
    OR (state='finalized' AND completion_digest IS NOT NULL
      AND final_graph_digest IS NOT NULL AND final_cas_digest IS NOT NULL))
) STRICT;
CREATE TABLE r093_reconciliation_audit (
  operation_id TEXT NOT NULL,
  action_id TEXT NOT NULL,
  action_type TEXT NOT NULL,
  table_name TEXT NOT NULL,
  canonical_pk TEXT NOT NULL,
  original_revision TEXT NOT NULL,
  original_digest TEXT NOT NULL,
  pre_digest TEXT NOT NULL,
  post_digest TEXT NOT NULL,
  evidence_digest TEXT NOT NULL,
  PRIMARY KEY(operation_id, action_id),
  FOREIGN KEY(operation_id) REFERENCES r093_reconciliation_marker(operation_id)
    DEFERRABLE INITIALLY DEFERRED
) STRICT;
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SchemaState {
    None,
    Complete,
    Partial,
}

pub(super) async fn schema_state(
    connection: &mut SqliteConnection,
) -> Result<SchemaState, Failure> {
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_schema WHERE type='table'
         AND name IN ('r093_reconciliation_marker','r093_reconciliation_audit')
         ORDER BY name",
    )
    .fetch_all(&mut *connection)
    .await
    .map_err(|_| Failure::Migration)?;
    if names.is_empty() {
        Ok(SchemaState::None)
    } else if names.len() == 2
        && names.iter().any(|name| name == "r093_reconciliation_audit")
        && names
            .iter()
            .any(|name| name == "r093_reconciliation_marker")
    {
        let marker_columns: Vec<(String, String, i64)> = sqlx::query_as(
            "SELECT name,type,\"notnull\" FROM pragma_table_info('r093_reconciliation_marker')",
        )
        .fetch_all(&mut *connection)
        .await
        .map_err(|_| Failure::Migration)?;
        let audit_columns: Vec<(String, String, i64)> = sqlx::query_as(
            "SELECT name,type,\"notnull\" FROM pragma_table_info('r093_reconciliation_audit')",
        )
        .fetch_all(&mut *connection)
        .await
        .map_err(|_| Failure::Migration)?;
        let marker_required = [
            ("singleton", "INTEGER"),
            ("protocol", "INTEGER"),
            ("operation_id", "TEXT"),
            ("state", "TEXT"),
            ("committed_graph_digest", "TEXT"),
            ("committed_binding_revision", "INTEGER"),
            ("commit_digest", "TEXT"),
        ];
        let audit_required = [
            ("operation_id", "TEXT"),
            ("action_id", "TEXT"),
            ("pre_digest", "TEXT"),
            ("post_digest", "TEXT"),
        ];
        let has = |columns: &[(String, String, i64)], required: &[(&str, &str)]| {
            required.iter().all(|(name, ty)| {
                columns.iter().any(|(actual, actual_type, _notnull)| {
                    actual == name && actual_type.eq_ignore_ascii_case(ty)
                })
            })
        };
        if has(&marker_columns, &marker_required) && has(&audit_columns, &audit_required) {
            Ok(SchemaState::Complete)
        } else {
            Ok(SchemaState::Partial)
        }
    } else {
        Ok(SchemaState::Partial)
    }
}

pub(super) async fn create_schema(connection: &mut SqliteConnection) -> Result<(), Failure> {
    if schema_state(connection).await? != SchemaState::None {
        return Err(Failure::ManualRequired);
    }
    sqlx::raw_sql(MARKER_DDL)
        .execute(&mut *connection)
        .await
        .map_err(|_| Failure::Migration)?;
    if schema_state(connection).await? != SchemaState::Complete {
        return Err(Failure::Migration);
    }
    Ok(())
}

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
           AND root_digest=? AND admin_operation_id=? AND schema_intent_digest=?
           AND schema_complete_digest=? AND identity_digest=? AND plan_digest=?
           AND classification_digest=? AND transition_intent_digest IS ?
           AND native_operation_id=? AND revision=?
           AND transition_complete_digest IS NULL",
    )
    .bind(completion_digest)
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
mod tests {
    use super::*;
    use crate::offline_production_migration::reconciliation_records::{Binding, Completion};
    use sqlx::{Connection, Row, SqliteConnection};

    fn plan() -> Plan {
        Plan {
            protocol: 2,
            plan_digest: "plan".into(),
            operation_id: "operation".into(),
            root_digest: "root".into(),
            original_provenance_digest: "provenance".into(),
            original_executable_digest: "original-executable".into(),
            archive_digest: "archive".into(),
            schema_complete_digest: "schema".into(),
            identity_digest: "identity".into(),
            grant_digest: "grant".into(),
            successor_executable_digest: "successor".into(),
            successor_package_digest: "package".into(),
            successor_source: "source".into(),
            successor_review: "review".into(),
            native_operation_id: "native".into(),
            source_state_digest: "source-state".into(),
            original_binding: Binding {
                db_generation: "generation".into(),
                canonical_db_digest: "database".into(),
                root_digest: "root".into(),
                admin_operation_id: "operation".into(),
                schema_intent_digest: "schema-intent".into(),
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
            classifications: Vec::new(),
            inserted_classifications: Vec::new(),
            actions: Vec::new(),
            d1_unknown_chain: None,
            d1_quarantined: None,
            d2_owner: Vec::new(),
            anchors: Vec::new(),
            workspaces: Vec::new(),
            native_at: "2026-01-01T00:00:00Z".into(),
            abandonment_at: "2026-01-01T00:00:00Z".into(),
            action_digest: "action".into(),
            classification_digest: "classification".into(),
        }
    }

    #[tokio::test]
    async fn marker_finalization_preserves_completion_digest() {
        let mut connection = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            "CREATE TABLE offline_recovery_binding (
                singleton INTEGER PRIMARY KEY,
                protocol INTEGER NOT NULL,
                phase TEXT NOT NULL
            )",
        )
        .execute(&mut connection)
        .await
        .unwrap();
        sqlx::query("INSERT INTO offline_recovery_binding(singleton,protocol,phase) VALUES(1,2,'transition_pending')")
            .execute(&mut connection)
            .await
            .unwrap();
        create_schema(&mut connection).await.unwrap();

        let plan = plan();
        insert_marker(
            &mut connection,
            &plan,
            "intent",
            "committed-graph",
            "commit",
            5,
        )
        .await
        .unwrap();
        let completion = Completion {
            protocol: 2,
            operation_id: plan.operation_id.clone(),
            intent_digest: "intent".into(),
            plan_digest: plan.plan_digest.clone(),
            action_digest: plan.action_digest.clone(),
            classification_digest: plan.classification_digest.clone(),
            native_operation_id: plan.native_operation_id.clone(),
            committed_graph_digest: "committed-graph".into(),
            committed_binding_revision: 5,
        };
        finalize_marker(
            &mut connection,
            &plan,
            &completion,
            "completion",
            "final-graph",
            "final-cas",
            "commit",
        )
        .await
        .unwrap();
        let row = sqlx::query(
            "SELECT state,completion_digest,final_graph_digest,final_cas_digest
             FROM r093_reconciliation_marker WHERE singleton=1",
        )
        .fetch_one(&mut connection)
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>("state"), "finalized");
        assert_eq!(row.get::<String, _>("completion_digest"), "completion");
        assert_eq!(row.get::<String, _>("final_graph_digest"), "final-graph");
        assert_eq!(row.get::<String, _>("final_cas_digest"), "final-cas");
    }
}
