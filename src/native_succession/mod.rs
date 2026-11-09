//! Restricted native succession before ordinary runtime admission.
//! An ignored response may be completed atomically; uncertain work remains held.
mod history;
#[cfg(test)]
mod history_atomic_tests;
#[cfg(test)]
mod history_boot_tests;
#[cfg(test)]
mod history_fixture;
#[cfg(all(test, feature = "test-support"))]
mod history_runtime;
#[cfg(test)]
mod history_tests;
pub(crate) mod pre_accept;
pub(crate) mod process;
pub(crate) mod projections;
#[cfg(all(test, feature = "test-support"))]
mod restoration_copy_tests;
pub(crate) mod state;
use crate::{
    config::Config,
    matrix::{MatrixClient, succession_payload},
    runtime_admission::Guard,
};
use sqlx::SqlitePool;
use std::{path::Path, time::Duration};

pub(crate) async fn attempt(
    pool: &SqlitePool,
    config: &Config,
    path: &Path,
    _maintenance: &crate::maintenance_lock::MaintenanceLock,
) -> Result<bool, sqlx::Error> {
    if !config.transports.matrix.enabled {
        return Ok(false);
    }
    let mut connection = pool.acquire().await?;
    let Some(expected) = state::read(&mut connection).await? else {
        return Ok(false);
    };
    drop(connection);
    if let Some(delivery) = &expected.pre_accept {
        pre_accept::config_covers(delivery, config)?;
    }
    let guard = Guard::acquire(&config.bridge.database, path, "reconcile")
        .map_err(|_| state::refuse("admission-lock"))?;
    let mut proof = process::capture(&expected.owner, config, path)?;
    let payloads = if expected.hold.as_deref() == Some("refused") {
        let (rebuild, payloads) = tokio::time::timeout(Duration::from_secs(90), async {
            let client = MatrixClient::restore(&config.transports.matrix).await?;
            crate::matrix::rebuild_gap::fetch(&client, pool, config, &expected).await
        })
        .await
        .map_err(|_| state::refuse("rebuild-deadline"))?
        .map_err(|_| state::refuse("rebuild-coverage-or-effect"))?;
        proof.rebuild = Some(rebuild);
        payloads
    } else if let Some(batch) = &expected.batch {
        tokio::time::timeout(Duration::from_secs(90), async {
            let client = MatrixClient::restore(&config.transports.matrix).await?;
            succession_payload::fetch(&client, batch).await
        })
        .await
        .map_err(|_| state::refuse("payload-deadline"))?
        .map_err(|_| state::refuse("payload-unavailable-or-drift"))?
    } else {
        Vec::new()
    };
    proof.unrouted = payloads
        .iter()
        .filter_map(|(room, value)| {
            crate::matrix::rebuild_gap::unrouted_no_effect(config, value, room)
                .then(|| {
                    value
                        .get("event_id")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                })
                .flatten()
        })
        .collect();
    proof.revalidate(config, path)?;
    let outcome = crate::storage::recovery_eligibility::writer::succession::publish(
        pool,
        &expected,
        &proof,
        &payloads,
        || {
            guard
                .revalidate()
                .map_err(|_| state::refuse("admission-drift"))?;
            proof.revalidate(config, path)
        },
    )
    .await;
    // Commit errors never imply success. Reread using an independent connection.
    let confirmed = crate::storage::recovery_eligibility::writer::succession::confirmed(
        &config.bridge.database,
        &expected,
        &proof,
    )
    .await?;
    if confirmed {
        tracing::info!(phase="native-succession",owner=%expected.owner.token,batch=?expected.batch.as_ref().map(|b|&b.id),historical_joined=false,outcome="committed","current exclusion permits safe successor");
        Ok(true)
    } else {
        outcome?;
        Err(state::refuse("publication-not-confirmed"))
    }
}
