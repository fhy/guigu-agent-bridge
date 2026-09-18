use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};

use super::{A2aError, A2aStore};

#[derive(Debug, Clone)]
pub struct CleanupPolicy {
    pub terminal_ttl: Duration,
    pub byte_ceiling: i64,
    pub low_watermark: i64,
    pub batch: u32,
    pub interval: Duration,
}

pub struct CleanupHandle {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    owner: tokio::task::JoinHandle<Result<(), A2aError>>,
}

pub async fn start_cleanup(
    store: A2aStore,
    runtime: String,
    policy: CleanupPolicy,
) -> Result<CleanupHandle, A2aError> {
    while store.retained_terminal_bytes().await? > policy.low_watermark {
        if clean_batch(&store, &runtime, &policy, true).await? == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    if store.retained_terminal_bytes().await? > policy.byte_ceiling {
        return Err(A2aError::RecoveryNeeded);
    }
    let (stop, mut stopped) = tokio::sync::oneshot::channel();
    let owner = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(policy.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = &mut stopped => return Ok(()),
                _ = ticker.tick() => {
                    let pressure = store.retained_terminal_bytes().await? > policy.byte_ceiling;
                    clean_batch(&store, &runtime, &policy, pressure).await?;
                }
            }
        }
    });
    Ok(CleanupHandle {
        stop: Some(stop),
        owner,
    })
}

async fn clean_batch(
    store: &A2aStore,
    runtime: &str,
    policy: &CleanupPolicy,
    pressure: bool,
) -> Result<usize, A2aError> {
    let ttl = ChronoDuration::from_std(policy.terminal_ttl)
        .map_err(|_| A2aError::Config("cleanup TTL out of range"))?;
    let now = Utc::now();
    let before = if pressure { now } else { now - ttl };
    let claims = store
        .claim_cleanup(runtime, before, policy.batch, now)
        .await?;
    for claim in &claims {
        store.clean_claim(runtime, claim, Utc::now()).await?;
    }
    Ok(claims.len())
}

impl CleanupHandle {
    pub async fn shutdown(mut self) -> Result<(), A2aError> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.owner.await.map_err(|_| A2aError::RecoveryNeeded)?
    }
}
