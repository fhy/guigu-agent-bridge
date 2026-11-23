use sqlx::{AssertSqlSafe, Row, SqliteConnection};

use super::{
    RowIdentity,
    inventory::{encoded, quoted},
    refused,
};

pub(super) async fn parents(
    connection: &mut SqliteConnection,
    identity: &RowIdentity,
    names: &[String],
    values: &[String],
) -> Result<Vec<RowIdentity>, sqlx::Error> {
    let value = |name: &str| {
        names
            .iter()
            .position(|column| column == name)
            .map(|index| values[index].as_str())
            .ok_or_else(refused)
    };
    let parent = match identity.table.as_str() {
        "workflow_envelopes" => Some(lookup(connection, "tasks", &encoded("task_id"), value("task_id")?).await?),
        "continuation_response_receipts" => Some(lookup(connection, "execution_leases", &encoded("resource_key"), value("resource_key")?).await?),
        "projection_outbox" => match value("source_kind")? {
            "text:7461736B5F6576656E74" => Some(lookup(connection, "task_events", &encoded("event_id"), value("source_id")?).await?),
            "text:7472616E73706F72745F72656365697074" => Some(lookup(connection, "transport_receipts",
                "typeof(transport||':'||external_event_id)||':'||hex(CAST(transport||':'||external_event_id AS BLOB))", value("source_id")?).await?),
            _ => return Err(refused()),
        },
        _ => None,
    };
    Ok(parent.into_iter().collect())
}

async fn lookup(
    connection: &mut SqliteConnection,
    table: &str,
    predicate: &str,
    value: &str,
) -> Result<RowIdentity, sqlx::Error> {
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
        return Err(refused());
    }
    let selection = primary
        .iter()
        .map(|(_, name)| encoded(name))
        .collect::<Vec<_>>()
        .join(",");
    let rows = sqlx::query(AssertSqlSafe(format!(
        "SELECT {selection} FROM {} WHERE ({predicate})=?",
        quoted(table)
    )))
    .bind(value)
    .fetch_all(connection)
    .await?;
    if rows.len() != 1 {
        return Err(refused());
    }
    let keys = (0..primary.len())
        .map(|index| rows[0].get::<String, _>(index))
        .collect::<Vec<_>>();
    Ok(RowIdentity {
        table: table.into(),
        key: serde_json::to_string(&keys).map_err(|_| refused())?,
    })
}
