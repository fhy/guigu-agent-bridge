use super::*;
use crate::native_succession::pre_accept::Delivery;
use sqlx::{Sqlite, sqlite::SqliteArguments};

const REASON: &str = "interrupted_before_acceptance_current_exclusion";
const BODY: &str = "任务在 Agent 接受前被中断，本次请求不会自动重试。请发送新请求。";

pub(super) async fn close(
    c: &mut SqliteConnection,
    s: &RecoveryScope,
    state: &State,
    now: &str,
) -> Result<(), sqlx::Error> {
    let Some(p) = &state.pre_accept else {
        return Ok(());
    };
    change(
        c,
        s,
        text_identity("tasks", &p.task)?,
        sqlx::query("UPDATE tasks SET version=version+1 WHERE task_id=? AND version=?")
            .bind(&p.task)
            .bind(p.version),
    )
    .await?;
    let event = p.event();
    let payload = serde_json::to_string(&crate::models::TaskEventPayload::Failed {
        error: REASON.into(),
    })
    .map_err(|_| refused())?;
    sqlx::query("INSERT INTO task_events(event_id,task_id,seq,status,timestamp,payload) VALUES(?,?,3,'failed',?,?)").bind(&event).bind(&p.task).bind(now).bind(payload).execute(&mut *c).await?;
    lineage::inserted(c, s, &text_identity("task_events", &event)?).await?;
    change(c,s,text_identity("task_admissions",&p.task)?,sqlx::query("UPDATE task_admissions SET state='terminal',revision=revision+1,updated_at=? WHERE task_id=? AND runtime_instance=? AND revision=? AND state='dispatching'").bind(now).bind(&p.task).bind(&state.owner.token).bind(p.admission_revision)).await?;
    change(c,s,text_identity("deliveries",&p.delivery)?,sqlx::query("UPDATE deliveries SET acknowledged_at=? WHERE delivery_id=? AND task_id=? AND attempt=? AND acknowledged_at IS NULL").bind(now).bind(&p.delivery).bind(&p.task).bind(p.attempt)).await?;
    change(c,s,text_identity("delivery_dispositions",&p.delivery)?,sqlx::query("UPDATE delivery_dispositions SET state='terminal',reason_code=? WHERE delivery_id=? AND task_id=? AND attempt=? AND state='prepared' AND session_id IS NULL AND child_fingerprint IS NULL AND reap_status IS NULL AND reap_at IS NULL").bind(REASON).bind(&p.delivery).bind(&p.task).bind(p.attempt)).await?;
    change(c,s,text_identity("execution_leases",&p.resource)?,sqlx::query("UPDATE execution_leases SET state='released',heartbeat_at=?,expires_at=? WHERE resource_key=? AND task_id=? AND owner_token=? AND fence=? AND expires_at=? AND state='active'").bind(now).bind(now).bind(&p.resource).bind(&p.task).bind(&p.lease_owner).bind(p.fence).bind(&p.expiry)).await?;
    for w in &p.workspaces {
        let id = text_identity("workspace_claims", &w.claim_id)?;
        super::super::super::require_lineage(c, &s.context(), EffectDomain::Matrix, &id).await?;
        // The exclusive current-exclusion proof is in the same committed owner
        // record. None does not assert a historical live component authority.
        super::super::super::deletion::record(
            c,
            &s.context(),
            &id,
            super::super::super::deletion::ReleaseEvidence {
                owner: &p.lease_owner,
                task: &p.task,
                fence: p.fence,
                authority: None,
            },
        )
        .await?;
        let deleted=sqlx::query("DELETE FROM workspace_claims WHERE claim_id=? AND runtime_owner=? AND task_id=? AND owner_fence=? AND revision=? AND state='active'")
            .bind(&w.claim_id).bind(&p.lease_owner).bind(&p.task).bind(p.fence).bind(w.revision).execute(&mut *c).await?;
        if deleted.rows_affected() != 1 {
            return Err(refused());
        }
    }
    notification(c, s, p, now).await
}

async fn change<'q>(
    c: &mut SqliteConnection,
    s: &RecoveryScope,
    id: super::super::RowIdentity,
    query: sqlx::query::Query<'q, Sqlite, SqliteArguments>,
) -> Result<(), sqlx::Error> {
    let mutation = lineage::prepare(c, s, &id).await?;
    if query.execute(&mut *c).await?.rows_affected() != 1 {
        return Err(refused());
    }
    lineage::complete(c, s, mutation).await
}

async fn notification(
    c: &mut SqliteConnection,
    s: &RecoveryScope,
    p: &Delivery,
    now: &str,
) -> Result<(), sqlx::Error> {
    let event = p.event();
    let txn = uuid::Uuid::new_v5(
        &crate::runtime::TERMINAL_OUTBOX_NAMESPACE,
        format!("task_event\0{event}\0terminal_reply").as_bytes(),
    )
    .to_string();
    let hash =
        uuid::Uuid::new_v5(&crate::runtime::OUTBOX_BODY_NAMESPACE, BODY.as_bytes()).to_string();
    sqlx::query("INSERT INTO projection_outbox(source_kind,source_id,projection,transport,stable_txn_id,room_id,thread_root,reply_event_id,render_version,body,body_hash,state,attempt_count,created_at,updated_at) VALUES('task_event',?,'terminal_reply','matrix',?,?,?,?,?,?,?,'pending',0,?,?)")
        .bind(&event).bind(txn).bind(&p.reply_room).bind(&p.reply_thread).bind(&p.reply_event).bind(&p.render_version).bind(BODY).bind(hash).bind(now).bind(now).execute(&mut *c).await?;
    lineage::inserted(
        c,
        s,
        &composite_text_identity(
            "projection_outbox",
            &["task_event", &event, "terminal_reply"],
        )?,
    )
    .await
}

pub(super) async fn confirmed(
    c: &mut SqliteConnection,
    state: &State,
) -> Result<bool, sqlx::Error> {
    let Some(p) = &state.pre_accept else {
        return Ok(true);
    };
    let count:i64=sqlx::query_scalar("SELECT count(*) FROM tasks t JOIN task_admissions a USING(task_id) JOIN deliveries d USING(task_id) JOIN delivery_dispositions x USING(delivery_id) JOIN execution_leases l ON l.task_id=t.task_id JOIN task_events e ON e.task_id=t.task_id JOIN projection_outbox o ON o.source_id=e.event_id AND o.source_kind='task_event' AND o.projection='terminal_reply' WHERE t.task_id=? AND t.version=? AND a.state='terminal' AND a.revision=? AND d.delivery_id=? AND d.acknowledged_at IS NOT NULL AND x.state='terminal' AND x.reason_code=? AND x.reap_status IS NULL AND x.reap_at IS NULL AND l.resource_key=? AND l.owner_token=? AND l.fence=? AND l.state='released' AND e.event_id=? AND e.seq=3 AND e.status='failed' AND o.state='pending' AND NOT EXISTS(SELECT 1 FROM workspace_claims WHERE task_id=t.task_id)")
        .bind(&p.task).bind(p.version.checked_add(1).ok_or_else(refused)?).bind(p.admission_revision.checked_add(1).ok_or_else(refused)?).bind(&p.delivery).bind(REASON).bind(&p.resource).bind(&p.lease_owner).bind(p.fence).bind(p.event()).fetch_one(&mut *c).await?;
    Ok(count == 1)
}
