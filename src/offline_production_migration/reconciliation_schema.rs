use super::Failure;
use sqlx::{Connection, SqliteConnection};

pub(super) const MARKER_DDL: &str = r#"
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
    let actual = schema_objects(connection).await?;
    if actual.is_empty() {
        return Ok(SchemaState::None);
    }
    let mut reference = SqliteConnection::connect("sqlite::memory:")
        .await
        .map_err(|_| Failure::Migration)?;
    let result = async {
        sqlx::raw_sql(MARKER_DDL)
            .execute(&mut reference)
            .await
            .map_err(|_| Failure::Migration)?;
        let expected = schema_objects(&mut reference).await?;
        Ok(if actual == expected {
            SchemaState::Complete
        } else {
            SchemaState::Partial
        })
    }
    .await;
    reference.close().await.map_err(|_| Failure::Migration)?;
    result
}

async fn schema_objects(
    connection: &mut SqliteConnection,
) -> Result<Vec<(String, String, String, Option<String>)>, Failure> {
    sqlx::query_as(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema
         WHERE name GLOB 'r093_*' OR tbl_name GLOB 'r093_*'
            OR lower(coalesce(sql,'')) LIKE '%r093_reconciliation_%'
         ORDER BY type,name,tbl_name",
    )
    .fetch_all(connection)
    .await
    .map_err(|_| Failure::Migration)
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
