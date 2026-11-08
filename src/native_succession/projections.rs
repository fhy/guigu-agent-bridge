//! Preserve terminal-task notification uncertainty without reusing its claim.
use super::state::{Result, refuse};
use serde::{Deserialize, Serialize};
use sqlx::SqliteConnection;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub(crate) struct Projection {
    source_kind: String,
    source_id: String,
    projection: String,
    stable_txn_id: String,
    room_id: String,
    body_hash: String,
    claim_owner: String,
    claim_revision: i64,
}

pub(crate) async fn read(c: &mut SqliteConnection) -> Result<Vec<Projection>> {
    // Only notification rows whose business parent is terminal can be isolated.
    // They stay sending: claim_projections admits pending only. No success,
    // abandonment, replay, receipt, lease or historical cleanup is inferred.
    let total: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM projection_outbox WHERE state NOT IN ('sent','abandoned')",
    )
    .fetch_one(&mut *c)
    .await?;
    if total > 64 {
        return Err(refuse("projection-bound"));
    }
    let rows: Vec<Projection> = sqlx::query_as("SELECT p.source_kind,p.source_id,p.projection,p.stable_txn_id,p.room_id,p.body_hash,p.claim_owner,p.claim_revision FROM projection_outbox p JOIN task_events e ON p.source_kind='task_event' AND e.event_id=p.source_id JOIN task_admissions a ON a.task_id=e.task_id AND a.state='terminal' WHERE p.state='sending' AND p.transport='matrix' AND p.projection IN ('observer','terminal_reply') AND e.status IN ('completed','failed','timed_out','cancelled') AND e.seq=(SELECT max(seq) FROM task_events WHERE task_id=e.task_id) AND length(p.claim_owner)>0 AND p.claim_revision>0 AND length(p.body_hash)>0 AND length(p.stable_txn_id)>0 AND length(p.room_id)>0 AND (SELECT count(*) FROM projection_outbox other WHERE other.stable_txn_id=p.stable_txn_id)=1 ORDER BY p.source_kind,p.source_id,p.projection")
        .fetch_all(c).await?;
    if rows.len() as i64 != total {
        return Err(refuse("unresolved-projection-effect"));
    }
    Ok(rows)
}
