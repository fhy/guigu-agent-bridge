use super::*;

pub(super) async fn record(
    connection: &mut sqlx::SqliteConnection,
    event: &crate::models::TaskEvent,
    recovery: Option<&crate::storage::recovery_eligibility::RecoveryScope>,
) -> Result<(), RuntimeError> {
    if !matches!(
        event.status,
        crate::models::TaskStatus::Failed
            | crate::models::TaskStatus::TimedOut
            | crate::models::TaskStatus::Cancelled
    ) {
        return Ok(());
    }
    let row = sqlx::query("SELECT reply_room,reply_thread_root,reply_event_id,monitor_generation,COALESCE(render_version,'v1') AS render_version FROM task_admissions WHERE task_id=? AND state='terminal'")
        .bind(event.task_id.to_string()).fetch_one(&mut *connection).await?;
    let Some(room) = row.try_get::<Option<String>, _>("reply_room")? else {
        return Ok(());
    };
    let body = match event.status {
        crate::models::TaskStatus::Failed => {
            "The Agent request failed. Check the correlated task diagnostics before retrying."
        }
        crate::models::TaskStatus::TimedOut => "The task timed out.",
        crate::models::TaskStatus::Cancelled => "The task was cancelled.",
        _ => return Ok(()),
    };
    insert_terminal_projection(
        connection,
        event,
        "terminal_reply",
        &room,
        row.try_get::<Option<String>, _>("reply_thread_root")?
            .as_deref(),
        row.try_get::<Option<String>, _>("reply_event_id")?
            .as_deref(),
        row.try_get("monitor_generation")?,
        &row.try_get::<String, _>("render_version")?,
        body,
        event.timestamp,
        recovery,
    )
    .await
}
