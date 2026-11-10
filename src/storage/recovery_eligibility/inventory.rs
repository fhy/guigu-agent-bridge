use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{AssertSqlSafe, Row, SqliteConnection};

use super::refused;

pub(super) const TABLES: &[&str] = &[
    "agents",
    "conversations",
    "messages",
    "tasks",
    "task_events",
    "deliveries",
    "sessions",
    "execution_leases",
    "task_continuations",
    "transport_receipts",
    "runtime_instances",
    "task_admissions",
    "delivery_dispositions",
    "projection_outbox",
    "gateway_envelopes",
    "gateway_deliveries",
    "gateway_artifacts",
    "workflow_envelopes",
    "continuation_response_receipts",
    "agent_queue_counters",
    "agent_work_queue",
    "acp_output_sets",
    "acp_output_segments",
    "acp_output_delivery_segments",
    "runtime_owner_evidence",
    "session_owner_bindings",
    "matrix_recovery_batches",
    "matrix_recovery_coverage",
    "matrix_recovery_receipts",
    "matrix_component_leases",
    "matrix_recovery_control",
    "a2a_exchanges",
    "a2a_messages",
    "a2a_message_parts",
    "a2a_artifacts",
    "a2a_artifact_parts",
    "workspace_claims",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowIdentity {
    pub table: String,
    pub key: String,
}

pub(crate) struct Snapshot {
    pub revision: String,
    pub digest: String,
    pub matrix: bool,
    pub parents: Vec<RowIdentity>,
}

pub(super) fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(super) fn encoded(name: &str) -> String {
    let column = quoted(name);
    format!("typeof({column})||':'||hex(CAST({column} AS BLOB))")
}

pub(crate) async fn snapshot(
    connection: &mut SqliteConnection,
    identity: &RowIdentity,
) -> Result<Snapshot, sqlx::Error> {
    let (names, values) = values(connection, identity).await?;
    let bytes = serde_json::to_vec(&(&names, &values)).map_err(|_| refused())?;
    let digest = format!("{:x}", Sha256::digest(bytes));
    let revision = names
        .iter()
        .position(|name| matches!(name.as_str(), "revision" | "version" | "claim_revision"))
        .map(|index| values[index].clone())
        .unwrap_or_else(|| digest.clone());
    let matrix = identity.table.starts_with("matrix_")
        || names.iter().zip(&values).any(|(name, value)| {
            matches!(name.as_str(), "transport" | "source_transport")
                && value == "text:6D6174726978"
        });
    let mut parents = parents(connection, identity, &names, &values).await?;
    for parent in super::semantic::parents(connection, identity, &names, &values).await? {
        if !parents.contains(&parent) {
            parents.push(parent);
        }
    }
    Ok(Snapshot {
        revision,
        digest,
        matrix,
        parents,
    })
}

pub(crate) async fn values(
    connection: &mut SqliteConnection,
    identity: &RowIdentity,
) -> Result<(Vec<String>, Vec<String>), sqlx::Error> {
    if !TABLES.contains(&identity.table.as_str()) {
        return Err(refused());
    }
    let keys: Vec<String> = serde_json::from_str(&identity.key).map_err(|_| refused())?;
    if serde_json::to_string(&keys).map_err(|_| refused())? != identity.key {
        return Err(refused());
    }
    let columns = sqlx::query(AssertSqlSafe(format!(
        "PRAGMA table_info({})",
        quoted(&identity.table)
    )))
    .fetch_all(&mut *connection)
    .await?;
    let mut primary = columns
        .iter()
        .filter_map(|column| {
            let ordinal: i64 = column.get("pk");
            (ordinal > 0).then(|| (ordinal, column.get::<String, _>("name")))
        })
        .collect::<Vec<_>>();
    primary.sort();
    if keys.len() != primary.len() || keys.is_empty() {
        return Err(refused());
    }
    let expressions = columns
        .iter()
        .map(|column| encoded(column.get("name")))
        .collect::<Vec<_>>();
    // Preserve exact encoded-key comparison, while letting the receipt primary key
    // avoid an O(n) scan for each member of a bounded recovery batch.
    let indexed = (identity.table == "matrix_recovery_receipts")
        .then(|| keys.iter().map(|k| text_key(k)).collect::<Option<Vec<_>>>())
        .flatten();
    let mut predicates = primary
        .iter()
        .map(|(_, column)| format!("{}=?", encoded(column)))
        .collect::<Vec<_>>();
    if indexed.is_some() {
        predicates.extend(
            primary
                .iter()
                .map(|(_, name)| format!("{}=?", quoted(name))),
        );
    }
    let query = AssertSqlSafe(format!(
        "SELECT {} FROM {} WHERE {}",
        expressions.join(","),
        quoted(&identity.table),
        predicates.join(" AND ")
    ));
    let mut query = sqlx::query(query);
    for key in keys {
        query = query.bind(key);
    }
    for value in indexed.into_iter().flatten() {
        query = query.bind(value);
    }
    let row = query
        .fetch_optional(&mut *connection)
        .await?
        .ok_or_else(refused)?;
    let values = (0..columns.len())
        .map(|index| row.get::<String, _>(index))
        .collect::<Vec<_>>();
    let names = columns
        .iter()
        .map(|column| column.get::<String, _>("name"))
        .collect::<Vec<_>>();
    Ok((names, values))
}

pub(crate) async fn identities(
    connection: &mut SqliteConnection,
) -> Result<Vec<RowIdentity>, sqlx::Error> {
    let mut identities = Vec::new();
    for table in TABLES {
        let columns = sqlx::query(AssertSqlSafe(format!(
            "PRAGMA table_info({})",
            quoted(table)
        )))
        .fetch_all(&mut *connection)
        .await?;
        let mut primary = columns
            .iter()
            .filter_map(|column| {
                let ordinal: i64 = column.get("pk");
                (ordinal > 0).then(|| (ordinal, column.get::<String, _>("name")))
            })
            .collect::<Vec<_>>();
        primary.sort();
        if primary.is_empty() {
            // Explicit schema-25 offline operations retain their original inventory.
            // An applied 0026 ledger with a missing table is always corruption.
            if *table == "matrix_recovery_coverage" {
                let applied: i64 =
                    sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations WHERE version>=26")
                        .fetch_one(&mut *connection)
                        .await?;
                if applied == 0 {
                    continue;
                }
            }
            return Err(refused());
        }
        let selection = primary
            .iter()
            .map(|(_, name)| encoded(name))
            .collect::<Vec<_>>()
            .join(",");
        let rows = sqlx::query(AssertSqlSafe(format!(
            "SELECT {selection} FROM {} ORDER BY {selection}",
            quoted(table)
        )))
        .fetch_all(&mut *connection)
        .await?;
        for row in rows {
            let keys = (0..primary.len())
                .map(|index| row.get::<String, _>(index))
                .collect::<Vec<_>>();
            identities.push(RowIdentity {
                table: (*table).into(),
                key: serde_json::to_string(&keys).map_err(|_| refused())?,
            });
        }
    }
    Ok(identities)
}

async fn parents(
    connection: &mut SqliteConnection,
    identity: &RowIdentity,
    names: &[String],
    values: &[String],
) -> Result<Vec<RowIdentity>, sqlx::Error> {
    let foreign = foreign_keys(connection, &identity.table).await?;
    let mut groups = std::collections::BTreeMap::<i64, Vec<_>>::new();
    for row in foreign {
        groups.entry(row.get("id")).or_default().push(row);
    }
    let mut parents = Vec::new();
    for rows in groups.values() {
        let table: String = rows[0].get("table");
        if !TABLES.contains(&table.as_str()) {
            return Err(refused());
        }
        let mut bindings = Vec::new();
        for row in rows {
            let source: String = row.get("from");
            let index = names
                .iter()
                .position(|name| *name == source)
                .ok_or_else(refused)?;
            bindings.push((row.get::<String, _>("to"), values[index].clone()));
        }
        if bindings.iter().any(|(_, value)| value == "null:") {
            continue;
        }
        let info = sqlx::query(AssertSqlSafe(format!(
            "PRAGMA table_info({})",
            quoted(&table)
        )))
        .fetch_all(&mut *connection)
        .await?;
        let mut primary = info
            .iter()
            .filter_map(|column| {
                let ordinal: i64 = column.get("pk");
                (ordinal > 0).then(|| (ordinal, column.get::<String, _>("name")))
            })
            .collect::<Vec<_>>();
        primary.sort();
        if primary.is_empty() {
            return Err(refused());
        }
        let selection = primary
            .iter()
            .map(|(_, name)| encoded(name))
            .collect::<Vec<_>>()
            .join(",");
        let predicates = bindings
            .iter()
            .map(|(name, _)| format!("{}=?", encoded(name)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let mut query = sqlx::query(AssertSqlSafe(format!(
            "SELECT {selection} FROM {} WHERE {predicates}",
            quoted(&table)
        )));
        for (_, value) in bindings {
            query = query.bind(value);
        }
        let rows = query.fetch_all(&mut *connection).await?;
        if rows.len() != 1 {
            return Err(refused());
        }
        let key = (0..primary.len())
            .map(|index| rows[0].get::<String, _>(index))
            .collect::<Vec<_>>();
        parents.push(RowIdentity {
            table,
            key: serde_json::to_string(&key).map_err(|_| refused())?,
        });
    }
    Ok(parents)
}

async fn foreign_keys(
    connection: &mut SqliteConnection,
    table: &str,
) -> Result<Vec<sqlx::sqlite::SqliteRow>, sqlx::Error> {
    sqlx::query(AssertSqlSafe(format!(
        "PRAGMA foreign_key_list({})",
        quoted(table)
    )))
    .fetch_all(connection)
    .await
}

fn text_key(key: &str) -> Option<String> {
    let hex = key.strip_prefix("text:")?;
    if hex.len() % 2 != 0 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let bytes = (0..hex.len())
        .step_by(2)
        .map(|n| u8::from_str_radix(&hex[n..n + 2], 16).ok())
        .collect::<Option<Vec<_>>>()?;
    String::from_utf8(bytes).ok()
}
