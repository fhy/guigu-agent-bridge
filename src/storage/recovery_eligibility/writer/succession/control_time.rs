//! Authenticate a control preimage; never elect an owner by timestamp.
use super::*;

pub(super) async fn authenticated(
    c: &mut SqliteConnection,
    state: &State,
) -> Result<String, sqlx::Error> {
    let mut times: Vec<String> = sqlx::query_scalar("SELECT DISTINCT updated_at FROM matrix_recovery_batches WHERE response_token=? AND state='terminal'")
        .bind(&state.watermark).fetch_all(&mut *c).await?;
    // An idle native succession also writes the control timestamp, without
    // advancing the watermark or changing any terminal batch. Only the histories
    // fully authenticated by state::read may supply its publication timestamp.
    for (owner, _) in &state.history {
        let at: Option<String> = sqlx::query_scalar("SELECT r.heartbeat_at FROM runtime_instances r JOIN runtime_owner_evidence e USING(instance_token) WHERE r.instance_token=? AND r.state='stopped' AND e.is_current=1 AND e.state='observing' AND e.barrier_state='open' AND json_extract(e.child_snapshot,'$.kind')='safe-succession-v1' AND json_extract(e.child_snapshot,'$.source.native_generation')=? AND json_extract(e.child_snapshot,'$.source.operation')=?")
            .bind(owner).bind(&state.native_generation).bind(&state.operation).fetch_optional(&mut *c).await?;
        if let Some(at) = at {
            times.push(at);
        }
    }
    times.sort();
    times.dedup();
    let id = lineage::control();
    let (names, mut values) = super::super::super::inventory::values(c, &id).await?;
    let (revision, digest): (String, String) = sqlx::query_as("SELECT original_revision,original_digest FROM offline_recovery_rows WHERE table_name='matrix_recovery_control' AND canonical_pk=?")
        .bind(&id.key).fetch_one(&mut *c).await?;
    if revision != digest {
        return Err(state::refuse("control-preimage"));
    }
    for (name, value) in [
        ("restart_hold_state", None),
        ("restart_hold_event_id", None),
        ("restart_hold_reason", None),
    ] {
        replace(&names, &mut values, name, value)?;
    }
    if state.hold.as_deref() == Some("refused") {
        replace(&names, &mut values, "phase", Some("active"))?;
    }
    let mut matching = Vec::new();
    for time in times {
        replace(&names, &mut values, "updated_at", Some(&time))?;
        let bytes = serde_json::to_vec(&(&names, &values)).map_err(|_| refused())?;
        if format!("{:x}", Sha256::digest(bytes)) == digest {
            matching.push(time);
        }
    }
    if matching.len() != 1 {
        return Err(state::refuse("control-preimage"));
    }
    Ok(matching.remove(0))
}

fn replace(
    names: &[String],
    values: &mut [String],
    name: &str,
    value: Option<&str>,
) -> Result<(), sqlx::Error> {
    let index = names.iter().position(|n| n == name).ok_or_else(refused)?;
    values[index] = match value {
        Some(value) => format!(
            "text:{}",
            value
                .as_bytes()
                .iter()
                .map(|b| format!("{b:02X}"))
                .collect::<String>()
        ),
        None => "null:".into(),
    };
    Ok(())
}
