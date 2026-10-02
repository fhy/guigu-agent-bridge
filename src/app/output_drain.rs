use chrono::{SecondsFormat, Utc};
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

use crate::{
    acp::output::DurableOutputSpool,
    matrix::{DurableMatrixSender, DurableSendError},
    storage::OutputSpoolStore,
};

#[derive(Clone)]
pub(crate) struct OutputSetDrain {
    store: OutputSpoolStore,
    spool: DurableOutputSpool,
    sender: Arc<dyn DurableMatrixSender>,
}

impl OutputSetDrain {
    pub(crate) fn new(
        store: OutputSpoolStore,
        spool: DurableOutputSpool,
        sender: Arc<dyn DurableMatrixSender>,
    ) -> Self {
        Self {
            store,
            spool,
            sender,
        }
    }

    pub(crate) async fn drain_once(&self) -> Result<usize, ()> {
        let now = Utc::now();
        let stale = now
            - chrono::Duration::from_std(self.spool.policy().stale_claim_after).map_err(|_| ())?;
        let now = now.to_rfc3339_opts(SecondsFormat::Nanos, true);
        let stale = stale.to_rfc3339_opts(SecondsFormat::Nanos, true);
        let owner = uuid::Uuid::now_v7().to_string();
        let Some(mut set) = self
            .store
            .claim_next(&owner, &now, &stale)
            .await
            .map_err(|_| ())?
        else {
            return Ok(0);
        };
        loop {
            let Some(segment) = self.store.next_segment(&set).await.map_err(|_| ())? else {
                return Err(());
            };
            let bytes = self.spool.read_delivery(&segment).map_err(|_| ())?;
            let body = std::str::from_utf8(&bytes).map_err(|_| ())?;
            let txn = segment.part.stable_txn_id.as_str();
            match self
                .sender
                .send_durable(
                    &set.room_id,
                    set.thread_root.as_deref(),
                    set.reply_event_id.as_deref(),
                    body,
                    txn,
                )
                .await
            {
                Ok(()) => {
                    if !self
                        .store
                        .acknowledge(&set, segment.part.index, &now)
                        .await
                        .map_err(|_| ())?
                    {
                        return Err(());
                    }
                    set.acknowledged_cursor = segment.part.index;
                    if set.acknowledged_cursor + 1 == set.segment_count {
                        let segments = self
                            .store
                            .begin_cleanup(&set.set_id, &now)
                            .await
                            .map_err(|_| ())?;
                        self.spool
                            .cleanup_files(&set.set_id, &segments)
                            .map_err(|_| ())?;
                        self.store
                            .finish_cleanup(&set.set_id, &now)
                            .await
                            .map_err(|_| ())?;
                        return Ok(1);
                    }
                }
                Err(error) => {
                    let class = matches!(error, DurableSendError::Permanent).then_some("permanent");
                    self.store
                        .record_failure(
                            &set,
                            class,
                            self.spool.policy().delivery_attempt_limit,
                            &now,
                        )
                        .await
                        .map_err(|_| ())?;
                    return Ok(0);
                }
            }
        }
    }

    pub(crate) fn start(self, interval: Duration) -> OutputSetDrainHandle {
        let (shutdown, mut receiver) = watch::channel(false);
        let join = tokio::spawn(async move {
            loop {
                let _ = self.drain_once().await;
                tokio::select! { biased; _=receiver.changed()=>break, _=tokio::time::sleep(interval)=>{} }
            }
        });
        OutputSetDrainHandle { shutdown, join }
    }
}

pub(crate) struct OutputSetDrainHandle {
    shutdown: watch::Sender<bool>,
    join: tokio::task::JoinHandle<()>,
}
impl OutputSetDrainHandle {
    pub(crate) async fn shutdown(self) -> Result<(), tokio::task::JoinError> {
        let _ = self.shutdown.send(true);
        self.join.await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        acp::output_policy::DurableOutputPolicy,
        matrix::{DurableSendError, outbound::DurableSendFuture},
        storage::OutputSetInput,
    };
    use sqlx::SqlitePool;
    use std::{collections::VecDeque, sync::Mutex};

    struct Temp(std::path::PathBuf);
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    #[derive(Default)]
    struct Sender {
        outcomes: Mutex<VecDeque<Result<(), DurableSendError>>>,
        calls: Mutex<Vec<(String, String)>>,
    }
    impl DurableMatrixSender for Sender {
        fn send_durable<'a>(
            &'a self,
            _: &'a str,
            _: Option<&'a str>,
            _: Option<&'a str>,
            body: &'a str,
            txn: &'a str,
        ) -> DurableSendFuture<'a> {
            Box::pin(async move {
                self.calls.lock().unwrap().push((body.into(), txn.into()));
                self.outcomes.lock().unwrap().pop_front().unwrap_or(Ok(()))
            })
        }
    }

    async fn fixture(
        outcomes: Vec<Result<(), DurableSendError>>,
    ) -> (Temp, OutputSetDrain, Arc<Sender>, SqlitePool) {
        let root = std::env::temp_dir().join(format!("t051-drain-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&root).unwrap();
        let temp = Temp(root.clone());
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql("CREATE TABLE tasks(task_id TEXT PRIMARY KEY) STRICT; INSERT INTO tasks VALUES('task');").execute(&pool).await.unwrap();
        sqlx::raw_sql(include_str!("../../migrations/0012_durable_acp_output.sql"))
            .execute(&pool)
            .await
            .unwrap();
        let mut policy = DurableOutputPolicy::derive(&root.join("state.db"), 64).unwrap();
        policy.spool_segment_bytes = 8;
        policy.matrix_segment_bytes = 4;
        let store = OutputSpoolStore::new(pool.clone(), policy.process_quota_bytes).unwrap();
        store
            .create_set(&OutputSetInput {
                set_id: "set",
                task_id: "task",
                attempt: 1,
                turn: 1,
                runtime_fence: "f",
                room_id: "!r:x",
                thread_root: Some("$t"),
                reply_event_id: Some("$e"),
                now: "n",
            })
            .await
            .unwrap();
        let spool = DurableOutputSpool::new(policy, store.clone());
        spool.publish("set", "o", b"abcdefghij", "n").await.unwrap();
        sqlx::query("UPDATE acp_output_sets SET state='sealed' WHERE set_id='set'")
            .execute(&pool)
            .await
            .unwrap();
        let sender = Arc::new(Sender {
            outcomes: Mutex::new(outcomes.into()),
            calls: Mutex::new(Vec::new()),
        });
        let drain = OutputSetDrain::new(store, spool, sender.clone());
        (temp, drain, sender, pool)
    }

    #[tokio::test]
    async fn complete_set_is_ordered_stable_and_cleaned() {
        let (_t, drain, sender, pool) = fixture(vec![]).await;
        assert_eq!(drain.drain_once().await, Ok(1));
        {
            let calls = sender.calls.lock().unwrap();
            assert_eq!(
                calls.iter().map(|v| v.0.as_str()).collect::<Vec<_>>(),
                vec!["abcd", "efgh", "ij"]
            );
            assert!(calls.windows(2).all(|v| v[0].1 != v[1].1));
        }
        let state: String =
            sqlx::query_scalar("SELECT state FROM acp_output_sets WHERE set_id='set'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, "cleaned");
    }

    #[tokio::test]
    async fn unknown_commit_reuses_transaction_and_permanent_stops() {
        let (_t, drain, sender, pool) = fixture(vec![
            Err(DurableSendError::UnknownCommit),
            Ok(()),
            Ok(()),
            Ok(()),
        ])
        .await;
        assert_eq!(drain.drain_once().await, Ok(0));
        assert_eq!(drain.drain_once().await, Ok(1));
        {
            let calls = sender.calls.lock().unwrap();
            assert_eq!(calls[0].1, calls[1].1);
        }
        let state: String =
            sqlx::query_scalar("SELECT state FROM acp_output_sets WHERE set_id='set'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, "cleaned");
        let (_t, drain, sender, pool) = fixture(vec![Err(DurableSendError::Permanent)]).await;
        assert_eq!(drain.drain_once().await, Ok(0));
        assert_eq!(sender.calls.lock().unwrap().len(), 1);
        let state: String =
            sqlx::query_scalar("SELECT state FROM acp_output_sets WHERE set_id='set'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, "partial");
    }
}
