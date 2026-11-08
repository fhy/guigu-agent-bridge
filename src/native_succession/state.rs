use serde::{Deserialize, Serialize};
use sqlx::{Row, SqliteConnection};

pub(super) type Result<T> = std::result::Result<T, sqlx::Error>;
pub(crate) fn refuse(reason: &str) -> sqlx::Error {
    sqlx::Error::Protocol(format!("native-succession/{reason}"))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Owner {
    pub token: String,
    pub generation: i64,
    pub revision: i64,
    pub pid: i64,
    pub starttime: i64,
    pub boot: String,
    pub fingerprint: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Batch {
    pub id: String,
    pub request: Option<String>,
    pub previous: String,
    pub response: String,
    pub digest: String,
    pub owner: String,
    pub epoch: i64,
    pub expected: Vec<String>,
    pub covered: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct State {
    pub owner: Owner,
    pub native_generation: String,
    pub revision: i64,
    pub operation: String,
    pub watermark: String,
    pub hold: Option<String>,
    pub batch: Option<Batch>,
    pub leases: Vec<(String, String, i64, String, String)>,
}

pub(crate) async fn read(connection: &mut SqliteConnection) -> Result<Option<State>> {
    let rows = sqlx::query("SELECT e.*,r.process_fingerprint AS runtime_fingerprint FROM runtime_instances r JOIN runtime_owner_evidence e USING(instance_token) WHERE r.state IN ('active','stopping') AND e.is_current=1")
        .fetch_all(&mut *connection).await?;
    if rows.is_empty() {
        return Ok(None);
    }
    if rows.len() != 1 {
        return Err(refuse("competing-owner"));
    }
    let row = &rows[0];
    if row.get::<String, _>("process_fingerprint") != row.get::<String, _>("runtime_fingerprint")
        || !matches!(row.get::<&str, _>("state"), "live" | "scan_pending")
    {
        return Err(refuse("owner-identity"));
    }
    let owner = Owner {
        token: row.get("instance_token"),
        generation: row.get("runtime_generation"),
        revision: row.get("evidence_revision"),
        pid: row.get("pid"),
        starttime: row.get("proc_starttime"),
        boot: row.get("boot_id"),
        fingerprint: row.get("process_fingerprint"),
    };
    let control=sqlx::query("SELECT x.generation,x.revision,c.* FROM runtime_eligibility_context x JOIN runtime_eligibility_bootstrap b ON b.generation=x.generation AND b.bootstrap_nonce=x.bootstrap_nonce AND b.inventory_digest=x.bootstrap_inventory_digest JOIN matrix_recovery_control c ON c.cutover_operation_id=x.compatibility_operation_id JOIN matrix_recovery_compatibility k ON k.native_operation_id=c.cutover_operation_id AND k.source_state_digest=x.compatibility_source_digest WHERE x.state='ready' AND x.matrix_enabled=1 AND x.provenance_kind='validated_native' AND b.state='complete' AND (c.phase='active' OR (c.phase='refused' AND c.restart_hold_state='refused' AND c.restart_hold_reason='rebuild-history-gap' AND c.restart_hold_event_id IS NULL)) AND k.decision='native' AND NOT EXISTS(SELECT 1 FROM offline_recovery_binding)")
        .fetch_optional(&mut *connection).await?.ok_or_else(||refuse("not-native"))?;
    let batch = read_batch(connection).await?;
    let hold: Option<String> = control.get("restart_hold_state");
    let held_id: Option<String> = control.get("restart_hold_event_id");
    let rebuild = hold.as_deref() == Some("refused")
        && control.get::<Option<&str>, _>("restart_hold_reason") == Some("rebuild-history-gap")
        && held_id.is_none()
        && batch.is_none();
    if hold.is_some()
        && !rebuild
        && (hold.as_deref() != Some("processing")
            || batch.as_ref().map(|b| format!("batch:{}", b.id)) != held_id
            || control.get::<Option<&str>, _>("restart_hold_reason")
                != Some("incomplete response coverage requires reconciliation"))
    {
        return Err(refuse("unsupported-hold"));
    }
    let watermark: String = control.get("watermark");
    if batch.as_ref().is_some_and(|b| {
        b.owner != owner.token
            || b.previous != watermark
            || b.request.as_deref() != Some(watermark.as_str())
    }) {
        return Err(refuse("batch-binding"));
    }
    let leases=sqlx::query_as("SELECT component,owner_token,epoch,state,expires_at FROM matrix_component_leases ORDER BY component").fetch_all(&mut *connection).await?;
    let state = State {
        owner,
        native_generation: control.get("generation"),
        revision: control.get("revision"),
        operation: control.get("cutover_operation_id"),
        watermark,
        hold,
        batch,
        leases,
    };
    for (_, lease_owner, _, status, _) in &state.leases {
        if lease_owner != &state.owner.token {
            let stopped: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM runtime_instances WHERE instance_token=? AND state='stopped'",
            )
            .bind(lease_owner)
            .fetch_one(&mut *connection)
            .await?;
            if status != "expired" || stopped != 1 {
                return Err(refuse("foreign-component-owner"));
            }
        }
    }
    validate_leases(&state)?;
    Ok(Some(state))
}

fn validate_leases(state: &State) -> Result<()> {
    if state
        .leases
        .iter()
        .any(|(component, owner, epoch, status, _)| {
            !matches!(component.as_str(), "MatrixSync" | "OutboxDrain")
                || (owner != &state.owner.token && status != "expired")
                || *epoch < 1
                || !matches!(status.as_str(), "active" | "expired")
        })
    {
        return Err(refuse("component-fence"));
    }
    if let Some(batch) = &state.batch
        && !state
            .leases
            .iter()
            .any(|(c, o, e, _, _)| c == "MatrixSync" && o == &batch.owner && *e == batch.epoch)
    {
        return Err(refuse("batch-fence"));
    }
    Ok(())
}

async fn read_batch(connection: &mut SqliteConnection) -> Result<Option<Batch>> {
    let rows = sqlx::query("SELECT * FROM matrix_recovery_batches WHERE state!='terminal'")
        .fetch_all(connection)
        .await?;
    if rows.is_empty() {
        return Ok(None);
    }
    if rows.len() != 1 {
        return Err(refuse("multiple-batches"));
    }
    let row = &rows[0];
    if row.get::<&str, _>("state") != "processing"
        || row.get::<&str, _>("component") != "MatrixSync"
        || row.get::<Option<String>, _>("refusal_code").is_some()
    {
        return Err(refuse("batch-state"));
    }
    let expected: Vec<String> =
        serde_json::from_str(row.get("expected_events_json")).map_err(|_| refuse("event-set"))?;
    let covered: Vec<String> =
        serde_json::from_str(row.get("covered_events_json")).map_err(|_| refuse("coverage"))?;
    let unique = expected.iter().collect::<std::collections::BTreeSet<_>>();
    if expected.len() > 64
        || unique.len() != expected.len()
        || covered.iter().any(|e| !unique.contains(e))
    {
        return Err(refuse("coverage"));
    }
    Ok(Some(Batch {
        id: row.get("batch_id"),
        request: row.get("request_token"),
        previous: row.get("previous_watermark"),
        response: row.get("response_token"),
        digest: row.get("response_digest"),
        owner: row.get("owner_token"),
        epoch: row.get("owner_epoch"),
        expected,
        covered,
    }))
}
