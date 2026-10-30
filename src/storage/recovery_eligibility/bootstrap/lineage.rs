use sqlx::{Row, SqliteConnection};

use super::{Entry, Native, inventory, refused};

pub(super) async fn native(
    connection: &mut SqliteConnection,
    enabled: bool,
) -> Result<Native, sqlx::Error> {
    let legacy = crate::storage::matrix_recovery_snapshot::snapshot(connection).await?;
    let row = sqlx::query("SELECT decision,native_operation_id,source_state_digest FROM matrix_recovery_compatibility WHERE singleton=1")
        .fetch_optional(&mut *connection).await?;
    let Some(row) = row else {
        let control: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_control")
            .fetch_one(connection)
            .await?;
        if enabled || legacy.legacy_rows != 0 || legacy.work_rows != 0 || control != 0 {
            return Err(refused());
        }
        return Ok(Native {
            operation: None,
            source: None,
            phase: "absent".into(),
            watermark: None,
        });
    };
    let operation: Option<String> = row.try_get("native_operation_id")?;
    let source: String = row.try_get("source_state_digest")?;
    if row.try_get::<&str, _>("decision")? != "native"
        || source != legacy.digest
        || operation
            .as_ref()
            .is_none_or(|value| value.trim().is_empty())
    {
        return Err(refused());
    }
    let control = sqlx::query("SELECT cutover_operation_id,phase,watermark,restart_hold_state FROM matrix_recovery_control WHERE control_id=1")
        .fetch_optional(&mut *connection).await?.ok_or_else(refused)?;
    let phase: String = control.try_get("phase")?;
    let watermark: Option<String> = control.try_get("watermark")?;
    if operation.as_deref() != Some(control.try_get::<&str, _>("cutover_operation_id")?)
        || control
            .try_get::<Option<String>, _>("restart_hold_state")?
            .is_some()
        || !match phase.as_str() {
            "baseline" => watermark.is_none(),
            "active" => watermark.as_ref().is_some_and(|token| !token.is_empty()),
            _ => false,
        }
    {
        return Err(refused());
    }
    Ok(Native {
        operation,
        source: Some(source),
        phase,
        watermark,
    })
}

pub(super) async fn inventory(
    connection: &mut SqliteConnection,
    native: &Native,
) -> Result<Vec<Entry>, sqlx::Error> {
    let mut entries = Vec::new();
    for identity in inventory::identities(connection).await? {
        let snapshot = inventory::snapshot(connection, &identity).await?;
        let (names, values) = inventory::values(connection, &identity).await?;
        let disposition = row_disposition(connection, &identity, &names, &values).await?;
        entries.push(Entry {
            identity,
            matrix: snapshot.matrix,
            snapshot,
            disposition,
        });
    }
    loop {
        let mut changed = false;
        for index in 0..entries.len() {
            let mut inherited = false;
            for parent in &entries[index].snapshot.parents {
                inherited |= entries
                    .iter()
                    .find(|entry| entry.identity == *parent)
                    .ok_or_else(refused)?
                    .matrix;
            }
            if inherited && !entries[index].matrix {
                entries[index].matrix = true;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    for entry in &entries {
        if entry.matrix
            && (native.operation.is_none()
                || (native.phase != "active" && entry.identity.table != "matrix_recovery_control"))
        {
            return Err(refused());
        }
        if entry.matrix && entry.identity.table == "tasks" {
            let rooted: i64 = sqlx::query_scalar("SELECT count(*) FROM tasks AS task JOIN transport_receipts AS receipt ON receipt.task_id=task.root_task_id JOIN matrix_recovery_receipts AS native ON native.event_id=receipt.external_event_id AND native.room_id=receipt.room_id JOIN matrix_recovery_batches AS batch ON batch.batch_id=native.batch_id WHERE json_array(typeof(task.task_id)||':'||hex(CAST(task.task_id AS BLOB)))=? AND receipt.transport='matrix' AND receipt.state='admitted' AND native.cutover_operation_id=? AND native.state='dispositioned' AND batch.state='terminal'")
                .bind(&entry.identity.key).bind(&native.operation).fetch_one(&mut *connection).await?;
            if rooted != 1 {
                return Err(refused());
            }
        }
    }
    native_receipts(connection, native).await?;
    Ok(entries)
}

fn disposition(
    table: &str,
    names: &[String],
    values: &[String],
) -> Result<&'static str, sqlx::Error> {
    let state = names
        .iter()
        .position(|name| matches!(name.as_str(), "state" | "phase"))
        .map(|index| values[index].as_str());
    let Some(state) = state else {
        return Ok("eligible_preserved");
    };
    let terminal = [
        "completed",
        "failed",
        "timed_out",
        "cancelled",
        "canceled",
        "rejected",
        "terminal",
        "stopped",
        "closed",
        "sent",
        "abandoned",
        "expired",
        "superseded",
        "delivered",
        "cleaned",
        "quarantined",
        "stale",
        "released",
        "reaped",
        "dispositioned",
    ];
    if terminal.iter().any(|value| encoded(value) == state) {
        return Ok("terminal_evidenced");
    }
    let ready = match table {
        "sessions" => &["ready"][..],
        "transport_receipts" => &["admitted"][..],
        "task_admissions" => &["ready", "enqueued"][..],
        "agent_work_queue" => &["queued", "paused"][..],
        "projection_outbox" => &["pending"][..],
        "task_continuations" => &["ready", "blocked"][..],
        "gateway_envelopes" => &["received", "acked", "accepted"][..],
        "gateway_deliveries" => &["pending", "transport_acked", "task_accepted"][..],
        "gateway_artifacts" => &["live"][..],
        "a2a_exchanges" => &["reserved", "submitted"][..],
        "matrix_recovery_control" => &["baseline", "active"][..],
        _ => &[][..],
    };
    if ready.iter().any(|value| encoded(value) == state) {
        Ok("eligible_preserved")
    } else {
        Err(refused())
    }
}

pub(crate) async fn row_disposition(
    connection: &mut SqliteConnection,
    identity: &super::RowIdentity,
    names: &[String],
    values: &[String],
) -> Result<&'static str, sqlx::Error> {
    if identity.table == "tasks" {
        let status: Option<String> = sqlx::query_scalar("SELECT status FROM task_events WHERE json_array(typeof(task_id)||':'||hex(CAST(task_id AS BLOB)))=? ORDER BY seq DESC LIMIT 1")
            .bind(&identity.key).fetch_optional(connection).await?;
        return match status.as_deref() {
            None | Some("queued") => Ok("eligible_preserved"),
            Some("completed" | "failed" | "timed_out" | "cancelled") => Ok("terminal_evidenced"),
            _ => Err(refused()),
        };
    }
    if identity.table == "task_events" {
        let latest: Option<String> = sqlx::query_scalar("SELECT latest.status FROM task_events AS event JOIN task_events AS latest ON latest.task_id=event.task_id WHERE json_array(typeof(event.event_id)||':'||hex(CAST(event.event_id AS BLOB)))=? ORDER BY latest.seq DESC LIMIT 1")
            .bind(&identity.key).fetch_optional(connection).await?;
        if matches!(
            latest.as_deref(),
            Some("completed" | "failed" | "timed_out" | "cancelled")
        ) {
            return Ok("terminal_evidenced");
        }
        let state = names
            .iter()
            .position(|name| name == "status")
            .ok_or_else(refused)?;
        return if ["completed", "failed", "timed_out", "cancelled"]
            .iter()
            .any(|status| encoded(status) == values[state])
        {
            Ok("terminal_evidenced")
        } else if ["queued"]
            .iter()
            .any(|status| encoded(status) == values[state])
        {
            Ok("eligible_preserved")
        } else {
            Err(refused())
        };
    }
    disposition(&identity.table, names, values)
}

fn encoded(value: &str) -> String {
    format!(
        "text:{}",
        value
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect::<String>()
    )
}

async fn native_receipts(
    connection: &mut SqliteConnection,
    native: &Native,
) -> Result<(), sqlx::Error> {
    let unexplained: i64 = sqlx::query_scalar("SELECT count(*) FROM transport_receipts AS receipt WHERE receipt.transport='matrix' AND NOT EXISTS(SELECT 1 FROM matrix_recovery_receipts AS native JOIN matrix_recovery_batches AS batch ON batch.batch_id=native.batch_id WHERE native.event_id=receipt.external_event_id AND native.room_id=receipt.room_id AND native.cutover_operation_id=? AND native.state='dispositioned' AND batch.state='terminal')")
        .bind(&native.operation).fetch_one(&mut *connection).await?;
    let foreign: i64 = sqlx::query_scalar("SELECT count(*) FROM matrix_recovery_receipts WHERE cutover_operation_id IS NOT ? OR state!='dispositioned'")
        .bind(&native.operation).fetch_one(connection).await?;
    if unexplained != 0 || foreign != 0 {
        return Err(refused());
    }
    Ok(())
}
