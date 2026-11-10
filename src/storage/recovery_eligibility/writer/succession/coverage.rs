//! Scoped coverage CAS. The authenticated control view is ALWAYS rolled back.
use super::*;
use crate::{config::Config, matrix::coverage::Coverage};

fn id() -> super::super::RowIdentity {
    super::super::RowIdentity {
        table: "matrix_recovery_coverage".into(),
        key: "[\"integer:31\"]".into(),
    }
}
fn scope(state: &State) -> RecoveryScope {
    RecoveryScope::new(
        EligibilityContext::NonAdmin {
            generation: state.native_generation.clone(),
            revision: state.revision,
            matrix_enabled: true,
            matrix_operation: Some(state.operation.clone()),
        },
        EffectDomain::Local,
        None,
    )
}
pub(crate) async fn read(c: &mut SqliteConnection) -> Result<Option<Coverage>, sqlx::Error> {
    let row: Option<(i64, String, String)> = sqlx::query_as(
        "SELECT revision,owner_token,document FROM matrix_recovery_coverage WHERE coverage_id=1",
    )
    .fetch_optional(c)
    .await?;
    row.map(|(rev, owner, doc)| {
        if doc.len() > crate::matrix::coverage::MAX_BYTES {
            return Err(state::refuse("coverage-row-bytes"));
        }
        let value: Coverage =
            serde_json::from_str(&doc).map_err(|_| state::refuse("coverage-row-format"))?;
        if value.revision != rev || value.state.owner.token != owner || value.encoded()? != doc {
            return Err(state::refuse("coverage-row-identity"));
        }
        value.validate()?;
        Ok(value)
    })
    .transpose()
}
async fn authenticate(
    c: &mut SqliteConnection,
    state: &State,
    s: &RecoveryScope,
) -> Result<(), sqlx::Error> {
    sqlx::query("SAVEPOINT coverage_auth")
        .execute(&mut *c)
        .await?;
    let result = async {
        lineage::reconstruct_control(c, state).await?;
        super::super::super::authority::validate_identity(c, &s.context()).await?;
        super::super::super::native_context::validate(c, &s.context()).await?;
        super::super::super::bootstrap::readback::inventory(c, &s.context()).await
    }
    .await;
    // Await restoration even on validation failure; caller rolls back on any error.
    sqlx::query("ROLLBACK TO coverage_auth")
        .execute(&mut *c)
        .await?;
    sqlx::query("RELEASE coverage_auth").execute(c).await?;
    result
}

pub(crate) async fn save(
    pool: &SqlitePool,
    state: &State,
    old: Option<&Coverage>,
    next: &Coverage,
    config: &Config,
    revalidate: impl Fn() -> Result<(), sqlx::Error>,
) -> Result<(), sqlx::Error> {
    next.validate()?;
    progress_deadline(old, next)?;
    super::coverage_transition::transition(old, next)?;
    if &next.state != state || next.revision != old.map_or(0, |v| v.revision + 1) {
        return Err(state::refuse("coverage-revision"));
    }
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    let result=async {
        if state::read(&mut tx).await?.as_ref()!=Some(state) || read(&mut tx).await?.as_ref()!=old {return Err(state::refuse("coverage-cas"));}
        revalidate()?;
        let s=scope(state);
        authenticate(&mut tx,state,&s).await?;
        if let Some(old)=old {super::coverage_transition::reusable(&mut tx,old,next).await?;}
        validate_new_events(&mut tx,old,next,config).await?;
        let document=next.encoded()?;
        if let Some(old)=old {
            let mut mutation=lineage::prepare(&mut tx,&s,&id()).await?;
            let changed=sqlx::query("UPDATE matrix_recovery_coverage SET revision=?,owner_token=?,document=? WHERE coverage_id=1 AND revision=? AND document=?")
                .bind(next.revision).bind(&state.owner.token).bind(&document).bind(old.revision).bind(old.encoded()?).execute(&mut *tx).await?;
            if changed.rows_affected()!=1 {return Err(state::refuse("coverage-cas"));}
            if old.status=="published" {super::coverage_transition::reparent(&mut tx,&s,&mut mutation).await?;}
            lineage::complete(&mut tx,&s,mutation).await?;
        } else {
            sqlx::query("INSERT INTO matrix_recovery_coverage(coverage_id,revision,owner_token,document) VALUES(1,?,?,?)")
                .bind(next.revision).bind(&state.owner.token).bind(&document).execute(&mut *tx).await?;
            lineage::inserted(&mut tx,&s,&id()).await?;
        }
        authenticate(&mut tx,state,&s).await?;
        if state::read(&mut tx).await?.as_ref()!=Some(state) {return Err(state::refuse("coverage-hold-drift"));}
        revalidate()?;
        Ok(())
    }.await;
    if let Err(error) = result {
        tx.rollback().await?;
        return Err(error);
    }
    if let Err(error) = progress_deadline(old, next) {
        tx.rollback().await?;
        return Err(error);
    }
    let outcome = tx.commit().await;
    // A separate connection observes an unknown commit; never infer success from error.
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&config.bridge.database)
        .read_only(true)
        .create_if_missing(false);
    let mut check = SqliteConnection::connect_with(&options).await?;
    if read(&mut check).await?.as_ref() == Some(next) {
        return Ok(());
    }
    outcome?;
    Err(state::refuse("coverage-commit-unconfirmed"))
}
async fn validate_new_events(
    c: &mut SqliteConnection,
    old: Option<&Coverage>,
    next: &Coverage,
    config: &Config,
) -> Result<(), sqlx::Error> {
    if old.is_none_or(|v| v.target.is_none())
        && let Some(t) = &next.target
    {
        for room in &t.rooms {
            for (ordinal, v) in room.initial.iter().enumerate() {
                crate::matrix::intent_diagnostic::check(
                    c,
                    config,
                    &room.id,
                    v,
                    crate::matrix::intent_diagnostic::Position::target(ordinal, room.initial.len()),
                )
                .await?;
            }
        }
    }
    let offset = old.map_or(0, |v| v.pages.len());
    for page in next.pages.iter().skip(offset) {
        for (ordinal, v) in page.events.iter().enumerate() {
            crate::matrix::intent_diagnostic::check(
                c,
                config,
                &page.room,
                v,
                crate::matrix::intent_diagnostic::Position::page(ordinal, page.events.len()),
            )
            .await?;
        }
    }
    Ok(())
}

pub(crate) async fn finish(
    c: &mut SqliteConnection,
    s: &RecoveryScope,
    proof: &Exclusion,
) -> Result<(), sqlx::Error> {
    let Some(expected) = &proof.coverage else {
        return Ok(());
    };
    if read(c).await?.as_ref() != Some(expected) {
        return Err(state::refuse("coverage-final-cas"));
    }
    let mutation = lineage::prepare(c, s, &id()).await?;
    let mut next = expected.clone();
    next.revision += 1;
    next.status = "published".into();
    next.final_response = next.target.as_ref().map(|t| t.next.clone());
    next.response_digest = proof.rebuild.as_ref().map(|r| r.batch.digest.clone());
    next.target = None;
    next.pages.clear();
    next.reservation = None;
    sqlx::query("UPDATE matrix_recovery_coverage SET revision=?,document=? WHERE coverage_id=1 AND revision=?")
        .bind(next.revision).bind(next.encoded()?).bind(expected.revision).execute(&mut *c).await?;
    lineage::complete(c, s, mutation).await
}
pub(crate) async fn confirmed(
    c: &mut SqliteConnection,
    proof: &Exclusion,
) -> Result<bool, sqlx::Error> {
    let Some(expected) = &proof.coverage else {
        return Ok(true);
    };
    let Some(actual) = read(c).await? else {
        return Ok(false);
    };
    let mut next = expected.clone();
    next.revision += 1;
    next.status = "published".into();
    next.final_response = next.target.as_ref().map(|t| t.next.clone());
    next.response_digest = proof.rebuild.as_ref().map(|r| r.batch.digest.clone());
    next.target = None;
    next.pages.clear();
    next.reservation = None;
    Ok(actual == next)
}

pub(super) fn publication_deadline(proof: &Exclusion) -> Result<(), sqlx::Error> {
    if let Some(row) = &proof.coverage {
        let now = crate::matrix::coverage::Clock::read()?;
        if row.publications == 0
            || row.publications > 3
            || now.boot != row.clock.boot
            || now.utc_ms < row.clock.utc_ms
            || now.boot_ms < row.clock.boot_ms
            || now.utc_ms > row.deadline_ms
            || row
                .elapsed_ms
                .checked_add((now.utc_ms - row.clock.utc_ms).max(now.boot_ms - row.clock.boot_ms))
                .is_none_or(|v| v > 1_800_000)
            || (now.utc_ms - row.clock.utc_ms).max(now.boot_ms - row.clock.boot_ms) > 30_000
        {
            return Err(state::refuse("coverage-publication-deadline"));
        }
    }
    Ok(())
}

/// Existing archive-only recovery also accepts its original schema25 input.
/// No fallback from a durable partial operation is permitted.
pub(crate) async fn legacy_allowed(c: &mut SqliteConnection) -> Result<(), sqlx::Error> {
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='matrix_recovery_coverage')").fetch_one(&mut *c).await?;
    if exists && read(c).await?.is_some_and(|row| row.status != "published") {
        return Err(state::refuse("coverage-active-archive-conflict"));
    }
    Ok(())
}

fn progress_deadline(old: Option<&Coverage>, next: &Coverage) -> Result<(), sqlx::Error> {
    if let Some(old) = old {
        let mut diagnostic = old.clone();
        diagnostic.revision = next.revision;
        diagnostic.refusal = next.refusal.clone();
        diagnostic.status = next.status.clone();
        if &diagnostic == next && next.refusal.is_some() {
            return Ok(());
        }
    }
    let now = crate::matrix::coverage::Clock::read()?;
    let mut checked = next.clone();
    checked.tick(now.clone())?;
    if next.status == "ready" && next.publications > 0 {
        if now.boot != next.clock.boot
            || (now.utc_ms - next.clock.utc_ms).max(now.boot_ms - next.clock.boot_ms) > 30_000
        {
            return Err(state::refuse("coverage-publication-deadline"));
        }
    } else {
        next.segment_remaining(&now)?;
    }
    Ok(())
}
