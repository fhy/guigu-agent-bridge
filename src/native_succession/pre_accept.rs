//! Exact durable evidence that an interrupted delivery never reached prompt.
use super::state::{Result, refuse};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqliteConnection};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub(crate) struct Workspace {
    pub claim_id: String,
    pub canonical_path: String,
    pub revision: i64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub(crate) struct Delivery {
    pub task: String,
    pub version: i64,
    pub admission_revision: i64,
    pub delivery: String,
    pub attempt: i64,
    pub resource: String,
    pub lease_owner: String,
    pub fence: i64,
    pub expiry: String,
    pub reply_room: String,
    pub reply_thread: Option<String>,
    pub reply_event: String,
    pub render_version: String,
    #[sqlx(skip)]
    pub workspaces: Vec<Workspace>,
}
impl Delivery {
    pub fn event(&self) -> String {
        uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            format!(
                "bridge-native-pre-acceptance-v1\0{}\0{}",
                self.task, self.delivery
            )
            .as_bytes(),
        )
        .to_string()
    }
}

pub(crate) async fn read(c: &mut SqliteConnection, owner: &str) -> Result<Option<Delivery>> {
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM task_admissions WHERE runtime_instance=? AND state='dispatching'",
    )
    .bind(owner)
    .fetch_one(&mut *c)
    .await?;
    if count == 0 {
        return Ok(None);
    }
    if count != 1 {
        return Err(refuse("pre-acceptance-count"));
    }
    let mut p:Delivery=sqlx::query_as("SELECT a.task_id AS task,t.version,a.revision AS admission_revision,d.delivery_id AS delivery,d.attempt,l.resource_key AS resource,l.owner_token AS lease_owner,l.fence,l.expires_at AS expiry,a.reply_room,a.reply_thread_root AS reply_thread,a.reply_event_id AS reply_event,a.render_version FROM task_admissions a JOIN tasks t USING(task_id) JOIN deliveries d USING(task_id) JOIN agents agent ON agent.endpoint_id=d.target_endpoint_id AND agent.endpoint_id=t.to_agent AND agent.transport='acp' JOIN delivery_dispositions x USING(delivery_id) JOIN execution_leases l ON l.task_id=a.task_id WHERE a.runtime_instance=? AND a.state='dispatching' AND d.attempt=1 AND d.acknowledged_at IS NULL AND x.state='prepared' AND x.session_id IS NULL AND x.child_fingerprint IS NULL AND x.reap_status IS NULL AND x.reap_at IS NULL AND l.state='active' AND length(l.owner_token)>0 AND l.fence>0 AND length(a.reply_room)>0 AND length(a.reply_event_id)>0 AND a.render_version='v1' AND (SELECT count(*) FROM deliveries WHERE task_id=a.task_id)=1 AND (SELECT count(*) FROM execution_leases WHERE task_id=a.task_id)=1")
        .bind(owner).fetch_optional(&mut *c).await?.ok_or_else(||refuse("pre-acceptance-shape"))?;
    for query in [
        "SELECT count(*) FROM task_continuations WHERE task_id=?",
        "SELECT count(*) FROM session_owner_bindings WHERE task_id=?",
        "SELECT count(*) FROM acp_output_sets WHERE task_id=?",
        "SELECT count(*) FROM agent_work_queue WHERE task_id=?",
        "SELECT count(*) FROM projection_outbox WHERE source_kind='task_event' AND source_id IN (SELECT event_id FROM task_events WHERE task_id=?)",
    ] {
        if sqlx::query_scalar::<_, i64>(query)
            .bind(&p.task)
            .fetch_one(&mut *c)
            .await?
            != 0
        {
            return Err(refuse("pre-acceptance-effect"));
        }
    }
    let events =
        sqlx::query("SELECT seq,status,payload FROM task_events WHERE task_id=? ORDER BY seq")
            .bind(&p.task)
            .fetch_all(&mut *c)
            .await?;
    if events.len() != 2
        || events[0].get::<i64, _>("seq") != 1
        || events[0].get::<&str, _>("status") != "queued"
        || events[1].get::<i64, _>("seq") != 2
        || events[1].get::<&str, _>("status") != "dispatched"
    {
        return Err(refuse("pre-acceptance-events"));
    }
    if !matches!(
        serde_json::from_str::<crate::models::TaskEventPayload>(events[0].get("payload")),
        Ok(crate::models::TaskEventPayload::Queued)
    ) {
        return Err(refuse("pre-acceptance-queued-event"));
    }
    if !matches!(serde_json::from_str::<crate::models::TaskEventPayload>(events[1].get("payload")),Ok(crate::models::TaskEventPayload::Dispatched {delivery_id,attempt}) if delivery_id.to_string()==p.delivery && i64::from(attempt)==p.attempt)
    {
        return Err(refuse("pre-acceptance-dispatch"));
    }
    let total: i64 = sqlx::query_scalar("SELECT count(*) FROM workspace_claims WHERE task_id=?")
        .bind(&p.task)
        .fetch_one(&mut *c)
        .await?;
    p.workspaces=sqlx::query_as("SELECT claim_id,canonical_path,revision FROM workspace_claims WHERE task_id=? AND runtime_owner=? AND owner_fence=? AND state='active' ORDER BY claim_id")
        .bind(&p.task).bind(&p.lease_owner).bind(p.fence).fetch_all(c).await?;
    if total > 16 || total != p.workspaces.len() as i64 {
        return Err(refuse("pre-acceptance-workspace"));
    }
    Ok(Some(p))
}

pub(crate) fn config_covers(p: &Delivery, config: &crate::config::Config) -> Result<()> {
    let paths = config
        .agents
        .values()
        .filter(|a| a.enabled)
        .flat_map(|a| a.workspace.iter().chain(&a.additional_workspaces))
        .collect::<Vec<_>>();
    if p.workspaces.iter().any(|w| {
        !paths
            .iter()
            .any(|p| p.as_path() == std::path::Path::new(&w.canonical_path))
    }) {
        return Err(refuse("pre-acceptance-workspace-exclusion"));
    }
    Ok(())
}
