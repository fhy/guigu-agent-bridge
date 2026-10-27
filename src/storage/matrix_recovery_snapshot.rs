use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{Row, SqliteConnection};

pub(super) type LegacyCutover = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    i64,
    String,
    String,
);

pub(super) struct LegacySnapshot {
    pub digest: String,
    pub cutover: Option<LegacyCutover>,
    pub legacy_rows: i64,
    pub work_rows: i64,
}

pub(super) async fn snapshot(
    connection: &mut SqliteConnection,
) -> Result<LegacySnapshot, sqlx::Error> {
    let required: Vec<String> =
        sqlx::query_scalar("SELECT operation_id FROM matrix_cutover_required ORDER BY singleton")
            .fetch_all(&mut *connection)
            .await?;
    let cutover: Option<LegacyCutover> =
        sqlx::query_as("SELECT operation_id,phase,baseline,baseline_digest,watermark,revision,binding,archive_digest FROM matrix_cutover WHERE singleton=1")
            .fetch_optional(&mut *connection).await?;
    let dispositions = sqlx::query("SELECT operation_id,room_id,event_id,content_digest,state FROM matrix_cutover_dispositions ORDER BY operation_id,room_id,event_id")
        .fetch_all(&mut *connection).await?;
    let mut records = Vec::<Value>::new();
    for row in &dispositions {
        records.push(serde_json::json!([
            row.try_get::<String, _>("operation_id")?,
            row.try_get::<String, _>("room_id")?,
            row.try_get::<String, _>("event_id")?,
            row.try_get::<String, _>("content_digest")?,
            row.try_get::<String, _>("state")?
        ]));
    }
    let bytes = serde_json::to_vec(&(required.clone(), &cutover, &records))
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;
    let digest = format!("{:x}", Sha256::digest(bytes));
    let work_rows: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM transport_receipts WHERE transport='matrix') + (SELECT count(*) FROM agent_work_queue queue JOIN tasks task ON task.task_id=queue.task_id JOIN conversations conversation ON conversation.conversation_id=task.conversation_id WHERE conversation.transport='matrix') + (SELECT count(*) FROM projection_outbox WHERE transport='matrix') + (SELECT count(*) FROM gateway_deliveries WHERE transport='matrix') + (SELECT count(*) FROM workflow_envelopes WHERE transport='matrix')")
        .fetch_one(&mut *connection).await?;
    Ok(LegacySnapshot {
        digest,
        legacy_rows: required.len() as i64 + i64::from(cutover.is_some()) + records.len() as i64,
        cutover,
        work_rows,
    })
}
