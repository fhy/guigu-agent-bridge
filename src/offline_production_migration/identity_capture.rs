use chrono::{Duration, Utc};
use sqlx::{Connection, Row};

#[cfg(test)]
#[path = "identity_tests.rs"]
mod tests;

use crate::offline_identity::{self, CollectorRequest};

use super::{
    Failure, Mode, Request, RuntimeLock,
    evidence::{self, Evidence, Provenance},
    identity_records::{IdentityEvidence, IdentityFailure, IdentityIntent},
    launcher, records, verification,
    version2_records::{self as graph, Root},
};

pub async fn run(req: Request) -> Result<String, Failure> {
    execute(req, true).await
}

pub(super) async fn execute(req: Request, live_launcher: bool) -> Result<String, Failure> {
    if req.target_schema != 25 || !matches!(req.mode, Mode::Apply | Mode::Verify) {
        return Err(Failure::InvalidArguments);
    }
    let mut provenance = Evidence::read(&req)?;
    launcher::check(&provenance.listeners, live_launcher, "identity-pre-lock").await?;
    let _lock = RuntimeLock::acquire(&req.database).map_err(|_| Failure::Busy)?;
    launcher::check(&provenance.listeners, live_launcher, "identity-post-lock").await?;
    provenance.recheck(&req)?;
    records::no_pending(&req)?;
    let root = graph::root(&req, &provenance, 25)?;
    let backup = super::version2_archive::validate(&req, &root).await?;
    let migration = graph::load(&req.manifest_dir.join("migration-intent-v2.json"))?;
    super::version2::verify_complete(&req, &root, &backup, &migration).await?;
    match std::fs::symlink_metadata(req.manifest_dir.join("identity-failure-v1.json")) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        _ => return Err(Failure::ManualRequired),
    }
    let config = crate::config::load(&req.config).map_err(|_| Failure::Preflight)?;
    let matrix = config.transports.matrix;
    if !matrix.device_trusted || matrix.access_token.expose().is_empty() {
        return Err(Failure::Preflight);
    }
    let store = matrix.crypto_store_path.ok_or(Failure::Preflight)?;
    let capture = CollectorRequest {
        homeserver: matrix.homeserver,
        user_id: matrix.user_id,
        device_id: matrix.device_id,
        access_token: matrix.access_token.expose().to_owned(),
        store,
        passphrase: None,
    };
    let mut connection = verification::open(&req.database, false).await?;
    let schema = schema_binding(&mut connection, &req, &root).await?;
    let store = capture.store.clone();
    let (intent, captured) = if req.mode == Mode::Verify {
        verify(&req, &root, &schema, &capture).await?
    } else {
        capture_identity(&req, &root, &schema, capture, &mut connection).await?
    };
    if let Err(failure) = finalize_capture(
        &req,
        &mut provenance,
        &store,
        live_launcher,
        &mut connection,
        &intent,
        &captured,
    )
    .await
    {
        if req.mode == Mode::Apply {
            IdentityFailure::publish(&req, &intent, "final-validation-and-binding", &failure)?;
        }
        return Err(failure);
    }
    connection.close().await.map_err(|_| Failure::Migration)?;
    Ok("offline-capture-matrix-identity result=identity-ready".into())
}

async fn finalize_capture(
    req: &Request,
    provenance: &mut Evidence,
    store: &std::path::Path,
    live_launcher: bool,
    connection: &mut sqlx::SqliteConnection,
    intent: &IdentityIntent,
    captured: &IdentityEvidence,
) -> Result<(), Failure> {
    provenance.recheck(req)?;
    launcher::check(
        &provenance.listeners,
        live_launcher,
        "identity-post-capture",
    )
    .await?;
    if offline_identity::store_generation(store).map_err(|_| Failure::Preflight)?
        != captured.store_after
    {
        return Err(Failure::ManualRequired);
    }
    captured.validate(intent, Utc::now())?;
    commit(connection, intent, captured).await
}

async fn schema_binding(
    connection: &mut sqlx::SqliteConnection,
    req: &Request,
    root: &Root,
) -> Result<String, Failure> {
    let complete: graph::Complete = graph::load(&req.manifest_dir.join("schema-complete-v2.json"))?;
    let digest = records::encoded_digest(&complete)?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM offline_recovery_binding WHERE singleton=1 AND protocol=2 AND db_generation=? AND root_digest=? AND admin_operation_id=? AND schema_complete_digest=? AND phase IN ('schema_ready','identity_pending','identity_ready') AND NOT EXISTS(SELECT 1 FROM runtime_eligibility_context) AND EXISTS(SELECT 1 FROM runtime_eligibility_bootstrap WHERE state='admin_reserved')")
        .bind(&root.db_generation).bind(records::encoded_digest(root)?)
        .bind(&req.operation_id).bind(&digest).fetch_one(connection).await
        .map_err(|_| Failure::ManualRequired)?;
    if count != 1 || complete.actual_schema != 25 {
        return Err(Failure::ManualRequired);
    }
    Ok(digest)
}

async fn capture_identity(
    req: &Request,
    root: &Root,
    schema: &str,
    capture: CollectorRequest,
    connection: &mut sqlx::SqliteConnection,
) -> Result<(IdentityIntent, IdentityEvidence), Failure> {
    let source: Provenance = records::load(&req.release_provenance)?;
    let intent = IdentityIntent {
        protocol: 1,
        root_digest: records::encoded_digest(root)?,
        operation_id: req.operation_id.clone(),
        schema_complete_digest: schema.to_owned(),
        producer_source: source.source_commit,
        executable_digest: source.executable_sha256,
        lock_digest: evidence::digest(include_bytes!("../../Cargo.lock")),
        sdk: "matrix-sdk=0.13.0".into(),
        store_before: offline_identity::store_generation(&capture.store)
            .map_err(|_| Failure::Preflight)?,
        created_at: Utc::now(),
    };
    intent.validate(root, schema)?;
    records::publish(&req.manifest_dir.join("identity-intent-v1.json"), &intent)?;
    let changed = sqlx::query("UPDATE offline_recovery_binding SET phase='identity_pending',revision=revision+1 WHERE singleton=1 AND phase='schema_ready' AND root_digest=? AND admin_operation_id=? AND schema_complete_digest=? AND identity_digest IS NULL")
        .bind(&intent.root_digest).bind(&req.operation_id).bind(schema)
        .execute(&mut *connection).await.map_err(|_| Failure::Migration)?;
    if changed.rows_affected() != 1 {
        return Err(Failure::ManualRequired);
    }
    let mut stage = "before-observation";
    let outcome = perform_capture(req, &intent, capture, &mut stage).await;
    match outcome {
        Ok(evidence) => Ok((intent, evidence)),
        Err(failure) => {
            IdentityFailure::publish(req, &intent, stage, &failure)?;
            Err(failure)
        }
    }
}

async fn perform_capture(
    req: &Request,
    intent: &IdentityIntent,
    capture: CollectorRequest,
    stage: &mut &'static str,
) -> Result<IdentityEvidence, Failure> {
    let before = offline_identity::observe_server(&capture)
        .await
        .map_err(|_| Failure::Preflight)?;
    *stage = "before-store-recheck";
    if offline_identity::store_generation(&capture.store).map_err(|_| Failure::Preflight)?
        != intent.store_before
    {
        return Err(Failure::ManualRequired);
    }
    let store = capture.store.clone();
    let after_request = CollectorRequest {
        homeserver: capture.homeserver.clone(),
        user_id: capture.user_id.clone(),
        device_id: capture.device_id.clone(),
        access_token: capture.access_token.clone(),
        store: store.clone(),
        passphrase: None,
    };
    *stage = "isolated-collector";
    let local = offline_identity::collect(capture)
        .await
        .map_err(|_| Failure::Preflight)?;
    *stage = "after-observation";
    let after = offline_identity::observe_server(&after_request)
        .await
        .map_err(|_| Failure::Preflight)?;
    *stage = "evidence-validation";
    let captured_at = Utc::now();
    let evidence = IdentityEvidence {
        protocol: 1,
        intent_digest: records::encoded_digest(&intent)?,
        before,
        local,
        after,
        store_after: offline_identity::store_generation(&store).map_err(|_| Failure::Preflight)?,
        captured_at,
        expires_at: captured_at + Duration::minutes(15),
    };
    evidence.validate(intent, Utc::now())?;
    *stage = "evidence-publication";
    records::publish(
        &req.manifest_dir.join("identity-evidence-v1.json"),
        &evidence,
    )?;
    Ok(evidence)
}

async fn verify(
    req: &Request,
    root: &Root,
    schema: &str,
    capture: &CollectorRequest,
) -> Result<(IdentityIntent, IdentityEvidence), Failure> {
    let intent: IdentityIntent = graph::load(&req.manifest_dir.join("identity-intent-v1.json"))?;
    let evidence: IdentityEvidence =
        graph::load(&req.manifest_dir.join("identity-evidence-v1.json"))?;
    intent.validate(root, schema)?;
    evidence.validate(&intent, Utc::now())?;
    if offline_identity::store_generation(&capture.store).map_err(|_| Failure::Preflight)?
        != evidence.store_after
    {
        return Err(Failure::ManualRequired);
    }
    Ok((intent, evidence))
}

async fn commit(
    connection: &mut sqlx::SqliteConnection,
    intent: &IdentityIntent,
    evidence: &IdentityEvidence,
) -> Result<(), Failure> {
    let digest = records::encoded_digest(evidence)?;
    let mut tx = connection
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(|_| Failure::Migration)?;
    let row = sqlx::query("SELECT phase,identity_digest,revision FROM offline_recovery_binding WHERE singleton=1 AND root_digest=? AND admin_operation_id=? AND schema_complete_digest=?")
        .bind(&intent.root_digest).bind(&intent.operation_id).bind(&intent.schema_complete_digest)
        .fetch_one(&mut *tx).await.map_err(|_| Failure::ManualRequired)?;
    let phase: String = row.try_get(0).map_err(|_| Failure::ManualRequired)?;
    let existing: Option<String> = row.try_get(1).map_err(|_| Failure::ManualRequired)?;
    let revision: i64 = row.try_get(2).map_err(|_| Failure::ManualRequired)?;
    if phase == "identity_pending" && existing.is_none() {
        let changed = sqlx::query("UPDATE offline_recovery_binding SET phase='identity_ready',identity_digest=?,revision=revision+1 WHERE singleton=1 AND phase='identity_pending' AND revision=? AND identity_digest IS NULL AND root_digest=? AND admin_operation_id=?")
            .bind(&digest).bind(revision).bind(&intent.root_digest).bind(&intent.operation_id)
            .execute(&mut *tx).await.map_err(|_| Failure::Migration)?;
        if changed.rows_affected() != 1 {
            return Err(Failure::ManualRequired);
        }
    } else if phase != "identity_ready" || existing.as_deref() != Some(&digest) {
        return Err(Failure::ManualRequired);
    }
    let confirmed: i64 = sqlx::query_scalar("SELECT count(*) FROM offline_recovery_binding WHERE singleton=1 AND phase='identity_ready' AND root_digest=? AND admin_operation_id=? AND identity_digest=?")
        .bind(&intent.root_digest).bind(&intent.operation_id).bind(&digest)
        .fetch_one(&mut *tx).await.map_err(|_| Failure::Migration)?;
    if confirmed != 1 {
        return Err(Failure::ManualRequired);
    }
    tx.commit().await.map_err(|_| Failure::Migration)
}
