//! Explicit recover-runtime mode for one archive-authenticated native stop delta.
use super::{
    process::Exclusion,
    state::{self, State},
};
use crate::storage::recovery_eligibility::writer::succession::{
    self,
    stop_metadata::{Image, Witness, evidence_digest},
};
use sqlx::{Connection, SqliteConnection};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    sync::Arc,
};

pub(crate) fn parse(args: &[OsString]) -> Result<(PathBuf, PathBuf), sqlx::Error> {
    if args.len() != 5 || args[0] != "recover-runtime" {
        return Err(state::refuse("stop-metadata-arguments"));
    }
    let mut config = None;
    let mut archive = None;
    for pair in args[1..].chunks_exact(2) {
        let slot = match pair[0].to_str() {
            Some("--config") => &mut config,
            Some("--stop-metadata-archive") => &mut archive,
            _ => return Err(state::refuse("stop-metadata-arguments")),
        };
        if pair[1].is_empty() || slot.replace(PathBuf::from(&pair[1])).is_some() {
            return Err(state::refuse("stop-metadata-arguments"));
        }
    }
    Ok((
        config.ok_or_else(|| state::refuse("stop-metadata-arguments"))?,
        archive.ok_or_else(|| state::refuse("stop-metadata-arguments"))?,
    ))
}

async fn archived(path: &Path) -> Result<(State, Image, String), sqlx::Error> {
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(path)
        .read_only(true)
        .create_if_missing(false);
    let mut c = SqliteConnection::connect_with(&options).await?;
    let mut tx = c.begin().await?;
    let state = state::read(&mut tx)
        .await?
        .ok_or_else(|| state::refuse("archive-owner"))?;
    let image = Image::read(&mut tx, &state.owner.token).await?;
    let evidence = evidence_digest(&mut tx, &state).await?;
    tx.rollback().await?;
    c.close().await?;
    Ok((state, image, evidence))
}

pub(crate) async fn witness(
    c: &mut SqliteConnection,
    archive: &Path,
    expected: &State,
) -> Result<Witness, sqlx::Error> {
    let (old, before, evidence) = archived(archive).await?;
    if &old != expected || evidence_digest(c, expected).await? != evidence {
        return Err(state::refuse("archive-context-or-evidence"));
    }
    let witness = Witness {
        before,
        current: Image::read(c, &expected.owner.token).await?,
        evidence_digest: evidence,
    };
    witness.validate_delta(expected)?;
    Ok(witness)
}

pub(crate) async fn retry(
    pool: &sqlx::SqlitePool,
    database: &Path,
    archive: &Path,
) -> Result<bool, sqlx::Error> {
    let (source, before, evidence) = archived(archive).await?;
    let record: Option<String> = sqlx::query_scalar("SELECT child_snapshot FROM runtime_owner_evidence WHERE instance_token=? AND is_current=1 AND state='observing'")
        .bind(&source.owner.token).fetch_optional(pool).await?;
    let Some(record) = record else {
        return Ok(false);
    };
    #[derive(serde::Deserialize)]
    struct Record {
        kind: String,
        source: State,
        exclusion: Exclusion,
        historical_joined: bool,
    }
    let record: Record =
        serde_json::from_str(&record).map_err(|_| state::refuse("stop-retry-record"))?;
    if record.kind != "safe-succession-v1"
        || record.historical_joined
        || record.source != source
        || record
            .exclusion
            .stop_metadata
            .as_ref()
            .is_none_or(|w| w.before != before || w.evidence_digest != evidence)
    {
        return Err(state::refuse("stop-retry-binding"));
    }
    succession::confirmed(database, &source, &record.exclusion).await
}

pub(crate) async fn recover(
    config: &crate::config::Config,
    path: &Path,
    archive: &Path,
) -> Result<(), sqlx::Error> {
    crate::offline_production_migration::check_runtime_discovery(&config.bridge.database)
        .map_err(|_| state::refuse("discovery"))?;
    let maintenance = Arc::new(
        crate::offline_production_migration::acquire_runtime_lock(&config.bridge.database)
            .map_err(|_| state::refuse("maintenance-lock"))?,
    );
    let pool = crate::storage::connect_for_runtime(&config.bridge.database, maintenance.clone())
        .await
        .map_err(|_| state::refuse("database-open"))?;
    let result = async {
        // Retry is read-only; the ordinary attempt owns admission during publication.
        let guard =
            crate::runtime_admission::Guard::acquire(&config.bridge.database, path, "reconcile")
                .map_err(|_| state::refuse("admission-lock"))?;
        if retry(&pool, &config.bridge.database, archive).await? {
            return Ok(());
        }
        drop(guard);
        if !super::attempt_with_archive(&pool, config, path, &maintenance, Some(archive)).await? {
            return Err(state::refuse("stop-owner-missing"));
        }
        Ok(())
    }
    .await;
    pool.close().await;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn explicit_archive_mode_rejects_ambiguous_arguments() {
        let args = |a: &[&str]| a.iter().map(OsString::from).collect::<Vec<_>>();
        assert_eq!(
            parse(&args(&[
                "recover-runtime",
                "--config",
                "config",
                "--stop-metadata-archive",
                "before.db"
            ]))
            .unwrap(),
            ("config".into(), "before.db".into())
        );
        for a in [
            vec!["recover-runtime", "--config", "config"],
            vec![
                "recover-runtime",
                "--config",
                "config",
                "--config",
                "second",
            ],
            vec![
                "recover-runtime",
                "--config",
                "",
                "--stop-metadata-archive",
                "before.db",
            ],
            vec![
                "recover-runtime",
                "--database",
                "db",
                "--stop-metadata-archive",
                "before.db",
            ],
        ] {
            assert!(parse(&args(&a)).is_err());
        }
    }
}
