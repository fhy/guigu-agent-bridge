use std::path::Path;

use sqlx::{Row, SqlitePool};

use super::{
    Failure, evidence,
    identity_records::{IdentityEvidence, IdentityIntent},
    records,
    transition::{TransitionComplete, TransitionIntent},
    version2_records as graph,
};

pub(super) struct Files {
    root: graph::Root,
    schema: graph::Complete,
    identity: IdentityEvidence,
    transition: TransitionIntent,
    complete: TransitionComplete,
}

pub(super) fn files(database: &Path) -> Result<Files, Failure> {
    let root: graph::Root = graph::load(&records::anchor(database))?;
    if root.protocol != 2
        || root.operation.database != database
        || root.operation.database_identity != evidence::Identity::of(database)?
    {
        return Err(Failure::ManualRequired);
    }
    let directory = &root.operation.manifest_dir;
    for entry in std::fs::read_dir(directory).map_err(|_| Failure::ManualRequired)? {
        if entry
            .map_err(|_| Failure::ManualRequired)?
            .file_name()
            .to_string_lossy()
            .ends_with(".pending")
        {
            return Err(Failure::ManualRequired);
        }
    }
    let schema: graph::Complete = graph::load(&directory.join("schema-complete-v2.json"))?;
    let migration: graph::Intent = graph::load(&directory.join("migration-intent-v2.json"))?;
    let intent: IdentityIntent = graph::load(&directory.join("identity-intent-v1.json"))?;
    let identity: IdentityEvidence = graph::load(&directory.join("identity-evidence-v1.json"))?;
    let transition: TransitionIntent = graph::load(&directory.join("transition-intent-v1.json"))?;
    let complete: TransitionComplete = graph::load(&directory.join("transition-complete-v1.json"))?;
    intent.validate(&root, &records::encoded_digest(&schema)?)?;
    identity.validate(&intent, identity.captured_at)?;
    if schema.actual_schema != 25
        || migration.target_schema != 25
        || migration.root_digest != records::encoded_digest(&root)?
        || migration.migration_digest != evidence::migration_digest_through(25)
        || schema.intent_digest != records::encoded_digest(&migration)?
        || transition.protocol != 1
        || transition.operation_id != root.operation.operation_id
        || transition.identity_digest != records::encoded_digest(&identity)?
        || complete.intent_digest != records::encoded_digest(&transition)?
        || complete.plan_digest != transition.plan_digest
        || complete.classification_digest != transition.classification_digest
        || complete.native_operation_id != transition.native_operation_id
    {
        return Err(Failure::ManualRequired);
    }
    Ok(Files {
        root,
        schema,
        identity,
        transition,
        complete,
    })
}

pub(crate) async fn prepare(
    path: &Path,
    config: &crate::config::Config,
    pool: &SqlitePool,
    owner: &str,
) -> Result<crate::storage::recovery_eligibility::RecoveryScope, Failure> {
    let files = files(&config.bridge.database)?;
    let phase = verify_binding(&files, pool).await?;
    if files.root.operation.config != path
        || evidence::hash_file(path)? != files.root.operation.config_digest
    {
        return Err(Failure::ManualRequired);
    }
    let matrix = &config.transports.matrix;
    if phase != "active" && (!matrix.enabled || chrono::Utc::now() > files.identity.expires_at) {
        return Err(Failure::ManualRequired);
    }
    if phase != "active" {
        let listeners = super::configuration::listeners_at(path, &config.bridge.database)?;
        super::launcher::check(&listeners, true, "administrative-activation").await?;
    }
    let request = crate::offline_identity::CollectorRequest {
        homeserver: matrix.homeserver.clone(),
        user_id: matrix.user_id.clone(),
        device_id: matrix.device_id.clone(),
        access_token: matrix.access_token.expose().to_owned(),
        store: matrix.crypto_store_path.clone().ok_or(Failure::Preflight)?,
        passphrase: None,
    };
    if !matrix.device_trusted {
        return Err(Failure::Preflight);
    }
    if phase == "baseline_ready"
        && crate::offline_identity::store_generation(&request.store)
            .map_err(|_| Failure::Preflight)?
            != files.identity.store_after
    {
        return Err(Failure::ManualRequired);
    }
    let observation = crate::offline_identity::observe_server(&request)
        .await
        .map_err(|_| Failure::Preflight)?;
    files
        .identity
        .after
        .require_continuity(&observation, &files.identity.local)
        .map_err(|_| Failure::ManualRequired)?;
    if phase != "active" {
        tokio::time::timeout(
            std::time::Duration::from_secs(60),
            activate(config, pool, owner, &phase, &files),
        )
        .await
        .map_err(|_| Failure::Preflight)??;
    }
    let context =
        crate::storage::recovery_eligibility::administrative::context(pool, matrix.enabled)
            .await
            .map_err(|_| Failure::ManualRequired)?;
    Ok(crate::storage::recovery_eligibility::RecoveryScope::new(
        context,
        crate::storage::recovery_eligibility::EffectDomain::Local,
        None,
    ))
}

async fn verify_binding(files: &Files, pool: &SqlitePool) -> Result<String, Failure> {
    let row = sqlx::query("SELECT db_generation,root_digest,admin_operation_id,schema_complete_digest,identity_digest,plan_digest,classification_digest,transition_intent_digest,transition_complete_digest,native_operation_id,canonical_db_digest,schema_intent_digest,phase FROM offline_recovery_binding WHERE singleton=1 AND protocol=2")
        .fetch_one(pool).await.map_err(|_| Failure::ManualRequired)?;
    let expected = [
        files.root.db_generation.clone(),
        records::encoded_digest(&files.root)?,
        files.root.operation.operation_id.clone(),
        records::encoded_digest(&files.schema)?,
        records::encoded_digest(&files.identity)?,
        files.transition.plan_digest.clone(),
        files.transition.classification_digest.clone(),
        records::encoded_digest(&files.transition)?,
        records::encoded_digest(&files.complete)?,
        files.transition.native_operation_id.clone(),
        records::encoded_digest(&(
            &files.root.operation.database,
            &files.root.operation.database_identity,
        ))?,
        files.schema.intent_digest.clone(),
    ];
    for (index, expected) in expected.iter().enumerate() {
        if row
            .try_get::<&str, _>(index)
            .map_err(|_| Failure::ManualRequired)?
            != expected
        {
            return Err(Failure::ManualRequired);
        }
    }
    let phase: String = row.try_get(12).map_err(|_| Failure::ManualRequired)?;
    if !matches!(
        phase.as_str(),
        "baseline_ready" | "activation_prepared" | "active"
    ) {
        return Err(Failure::ManualRequired);
    }
    if phase == "active" {
        let count: i64 = sqlx::query_scalar("WITH RECURSIVE covered(token) AS (SELECT response_token FROM matrix_activation_intent WHERE state='committed' UNION SELECT batch.response_token FROM matrix_recovery_batches batch JOIN covered previous ON batch.previous_watermark=previous.token WHERE batch.state='terminal') SELECT count(*) FROM matrix_activation_intent i JOIN offline_recovery_binding b ON b.native_operation_id=i.native_operation_id JOIN matrix_recovery_control c ON c.cutover_operation_id=i.native_operation_id WHERE b.phase='active' AND i.state='committed' AND c.phase='active' AND c.watermark IN (SELECT token FROM covered) AND b.plan_digest=i.plan_digest AND b.identity_digest=i.identity_digest AND c.restart_hold_state IS NULL AND NOT EXISTS(SELECT 1 FROM matrix_recovery_batches WHERE state!='terminal')")
            .fetch_one(pool).await.map_err(|_| Failure::ManualRequired)?;
        if count != 1 {
            return Err(Failure::ManualRequired);
        }
    }
    Ok(phase)
}

pub(super) async fn verify_active(database: &Path) -> Result<(), Failure> {
    let files = files(database)?;
    let pool = crate::storage::connect(database)
        .await
        .map_err(|_| Failure::ManualRequired)?;
    let result = async {
        if verify_binding(&files, &pool).await? != "active" {
            return Err(Failure::ManualRequired);
        }
        crate::storage::recovery_eligibility::administrative::context(&pool, true)
            .await
            .map_err(|_| Failure::ManualRequired)?;
        Ok(())
    }
    .await;
    pool.close().await;
    result
}

async fn activate(
    config: &crate::config::Config,
    pool: &SqlitePool,
    owner: &str,
    phase: &str,
    files: &Files,
) -> Result<(), Failure> {
    let recovery = crate::storage::MatrixRecoveryStore::new(pool.clone());
    let now = chrono::Utc::now();
    let epoch = recovery
        .acquire_administrative_lease(
            owner,
            &now.to_rfc3339(),
            &(now + chrono::Duration::seconds(60)).to_rfc3339(),
        )
        .await
        .map_err(|_| Failure::ManualRequired)?;
    let token = if phase == "activation_prepared" {
        let client = crate::matrix::MatrixClient::restore(&config.transports.matrix)
            .await
            .map_err(|_| Failure::Preflight)?;
        client
            .prove_server_device_key()
            .await
            .map_err(|_| Failure::Preflight)?;
        recovery
            .resume_prepared_activation(owner, epoch)
            .await
            .map_err(|_| Failure::ManualRequired)?
    } else {
        baseline(config, &recovery, owner, epoch, files).await?
    };
    recovery
        .commit_activation_intent(
            &files.transition.native_operation_id,
            owner,
            epoch,
            &token,
            &chrono::Utc::now().to_rfc3339(),
        )
        .await
        .map_err(|_| Failure::ManualRequired)?;
    super::flow::fault("administrative-after-commit");
    Ok(())
}

async fn baseline(
    config: &crate::config::Config,
    recovery: &crate::storage::MatrixRecoveryStore,
    owner: &str,
    epoch: i64,
    files: &Files,
) -> Result<String, Failure> {
    let request = records::encoded_digest(&(
        owner,
        epoch,
        &files.transition.plan_digest,
        &files.transition.identity_digest,
    ))?;
    recovery
        .begin_activation_intent(
            owner,
            epoch,
            &uuid::Uuid::now_v7().to_string(),
            &request,
            &chrono::Utc::now().to_rfc3339(),
        )
        .await
        .map_err(|_| Failure::ManualRequired)?;
    let client = crate::matrix::MatrixClient::restore(&config.transports.matrix)
        .await
        .map_err(|_| Failure::Preflight)?;
    client
        .prove_server_device_key()
        .await
        .map_err(|_| Failure::Preflight)?;
    let token = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        client.baseline_sync_token(),
    )
    .await
    .map_err(|_| Failure::Preflight)?
    .map_err(|_| Failure::Preflight)?;
    recovery
        .prepare_activation_intent(
            owner,
            epoch,
            &token,
            &crate::storage::cutover_digest(token.as_bytes()),
            &chrono::Utc::now().to_rfc3339(),
        )
        .await
        .map_err(|_| Failure::ManualRequired)?;
    super::flow::fault("administrative-after-prepared");
    Ok(token)
}
