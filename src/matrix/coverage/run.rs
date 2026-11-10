use super::*;
use crate::{
    config::Config,
    matrix::{MatrixClient, rebuild_gap::Rebuild, response_identity},
    native_succession::{process::Exclusion, state::Batch},
    storage::recovery_eligibility::writer::succession::coverage as writer,
};
use sqlx::SqlitePool;
type Recovered = (Rebuild, Vec<(String, Value)>, Coverage);

pub(crate) async fn recover(
    pool: &SqlitePool,
    config: &Config,
    state: &State,
    proof: &Exclusion,
    revalidate: &(impl Fn() -> Result<()> + Sync),
) -> Result<Recovered> {
    let result = recover_inner(pool, config, state, proof, revalidate).await;
    if let Err(error) = &result {
        let _ = failure(pool, config, state, proof, revalidate, error).await;
    }
    result
}

async fn recover_inner(
    pool: &SqlitePool,
    config: &Config,
    state: &State,
    proof: &Exclusion,
    revalidate: &(impl Fn() -> Result<()> + Sync),
) -> Result<Recovered> {
    let mut c = pool.acquire().await?;
    let old = writer::read(&mut c).await?;
    drop(c);
    let mut row = resume(old.as_ref(), state, proof.pairing(config)?)?;
    writer::save(pool, state, old.as_ref(), &row, config, revalidate).await?;
    if row.status != "ready" {
        let remaining = row.segment_remaining(&Clock::read()?)?;
        let client =
            tokio::time::timeout(remaining, MatrixClient::restore(&config.transports.matrix))
                .await
                .map_err(|_| refuse("coverage-restore-timeout"))?
                .map_err(|_| refuse("coverage-restore"))?;
        if row.target.is_none() {
            mutate(pool, config, state, &mut row, revalidate, |r| {
                r.charge_sync(Clock::read()?)
            })
            .await?;
            let remaining = row.segment_remaining(&Clock::read()?)?;
            let target = tokio::time::timeout(remaining, sdk::target(&client, pool, state))
                .await
                .map_err(|_| refuse("coverage-target-timeout"))??;
            mutate(pool, config, state, &mut row, revalidate, |r| {
                r.tick(Clock::read()?)?;
                r.freeze(target)
            })
            .await?;
        }
        collect(pool, config, state, &mut row, &client, revalidate).await?;
        mutate(pool, config, state, &mut row, revalidate, |r| {
            r.charge_publish(Clock::read()?)
        })
        .await?;
    }
    reconstruction(state, row)
}
fn resume(old: Option<&Coverage>, state: &State, pairing: String) -> Result<Coverage> {
    let row = match old {
        None => Coverage::new(state, pairing.clone(), Clock::read()?)?,
        Some(v) if v.status == "published" => {
            let mut next = Coverage::new(state, pairing.clone(), Clock::read()?)?;
            next.revision = v.revision + 1;
            next
        }
        Some(v) => {
            if &v.state != state || v.pairing != pairing {
                return Err(refuse("coverage-binding-drift"));
            }
            let mut next = v.clone();
            if v.status == "ready" {
                // Persist the new publication reservation directly: completed
                // collection keeps its counters and may have an expired segment.
                // The scoped writer checks this reservation's publication deadline.
                next.charge_publish(Clock::read()?)?;
            } else {
                next.segment(Clock::read()?)?;
            }
            next.revision += 1;
            next
        }
    };
    Ok(row)
}
async fn collect(
    pool: &SqlitePool,
    config: &Config,
    state: &State,
    row: &mut Coverage,
    client: &MatrixClient,
    revalidate: &(impl Fn() -> Result<()> + Sync),
) -> Result<()> {
    while row.status == "collecting" {
        let now = Clock::read()?;
        let mut checked = row.clone();
        checked.tick(now.clone())?;
        if row.segment_pages >= 12
            || row.segment_events > 1100
            || row.segment_attempts >= 15
            || row.segment_bytes >= 4 * 1024 * 1024
            || row
                .segment_remaining(&now)
                .map_or(true, |t| t.as_secs() < 6)
        {
            mutate(pool, config, state, row, revalidate, |r| {
                r.segment(Clock::read()?)
            })
            .await?;
        }
        mutate(pool, config, state, row, revalidate, |r| {
            r.charge_page(Clock::read()?)
        })
        .await?;
        let (room, cursor) = row
            .reservation
            .clone()
            .ok_or_else(|| refuse("coverage-reservation"))?;
        let page = sdk::page(client, &room, &cursor).await?;
        let bytes = serde_json::to_vec(&page)
            .map_err(|_| refuse("coverage-encoding"))?
            .len();
        if bytes > 4 * 1024 * 1024 {
            return Err(refuse("coverage-page-bytes"));
        }
        if row
            .segment_bytes
            .checked_add(bytes)
            .is_none_or(|n| n > 4 * 1024 * 1024)
        {
            // This response is not committed. Keep its consumed attempt, charge a
            // new segment, and reserve before refetching this exact frontier.
            mutate(pool, config, state, row, revalidate, |r| {
                r.segment(Clock::read()?)
            })
            .await?;
            continue;
        }
        mutate(pool, config, state, row, revalidate, |r| {
            r.tick(Clock::read()?)?;
            r.segment_remaining(&r.clock)?;
            r.accept(page)
        })
        .await?;
    }
    Ok(())
}
fn reconstruction(state: &State, row: Coverage) -> Result<Recovered> {
    let payloads = row.payloads()?;
    let target = row
        .target
        .as_ref()
        .ok_or_else(|| refuse("coverage-target"))?;
    let epoch = state
        .leases
        .iter()
        .find(|(c, o, _, _, _)| c == "MatrixSync" && o == &state.owner.token)
        .ok_or_else(|| refuse("coverage-lease"))?
        .2;
    let mut expected = payloads
        .iter()
        .map(|(_, v)| v["event_id"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    expected.sort();
    let batch = Batch {
        id: row.batch_id.clone(),
        request: Some(state.watermark.clone()),
        previous: state.watermark.clone(),
        response: target.next.clone(),
        digest: response_identity::digest(Some(&state.watermark), &target.next, payloads.clone())
            .map_err(|_| refuse("coverage-digest"))?,
        owner: state.owner.token.clone(),
        epoch,
        expected,
        covered: vec![],
    };
    let rebuild = Rebuild {
        batch,
        anchors: target
            .rooms
            .iter()
            .filter_map(|r| r.anchor.clone())
            .collect(),
        unrouted: vec![],
    };
    Ok((rebuild, payloads, row))
}
async fn mutate(
    pool: &SqlitePool,
    config: &Config,
    state: &State,
    row: &mut Coverage,
    revalidate: &(impl Fn() -> Result<()> + Sync),
    change: impl FnOnce(&mut Coverage) -> Result<()>,
) -> Result<()> {
    let mut next = row.clone();
    change(&mut next)?;
    next.revision += 1;
    writer::save(pool, state, Some(row), &next, config, revalidate).await?;
    *row = next;
    Ok(())
}

// Persist the first static failure only when the same exclusion/binding still holds.
async fn failure(
    pool: &SqlitePool,
    config: &Config,
    state: &State,
    proof: &Exclusion,
    revalidate: &(impl Fn() -> Result<()> + Sync),
    error: &sqlx::Error,
) -> Result<()> {
    let mut c = pool.acquire().await?;
    let Some(row) = writer::read(&mut c).await? else {
        return Ok(());
    };
    drop(c);
    if row.state != *state || row.pairing != proof.pairing(config)? || row.status == "published" {
        return Ok(());
    }
    let reason = match error {
        sqlx::Error::Protocol(s) if s == "native-succession/coverage-page-timeout" => {
            "page-timeout"
        }
        sqlx::Error::Protocol(s) if s == "native-succession/coverage-page-transport" => {
            "page-transport"
        }
        sqlx::Error::Protocol(s) if s == "native-succession/coverage-target-timeout" => {
            "target-timeout"
        }
        sqlx::Error::Protocol(s) if s == "native-succession/coverage-restore-timeout" => {
            "restore-timeout"
        }
        sqlx::Error::Protocol(s) if s.contains("budget") || s.contains("lifetime") => {
            "finite-budget"
        }
        _ => "coverage-validation",
    };
    let mut next = row.clone();
    next.refusal.get_or_insert_with(|| reason.into());
    next.revision += 1;
    if matches!(reason, "finite-budget" | "coverage-validation") {
        next.status = "refused".into();
    }
    writer::save(pool, state, Some(&row), &next, config, revalidate).await
}
