use std::os::unix::fs::OpenOptionsExt;
use std::{
    collections::VecDeque,
    fs,
    sync::{Arc, Mutex},
};

use sqlx::{Row, SqlitePool};

use super::{
    output::{DurableOutputSpool, OutputSpoolError},
    output_files,
    output_policy::DurableOutputPolicy,
    output_stream::StreamingPublisher,
};
use crate::{
    app::output_drain::OutputSetDrain,
    matrix::{DurableMatrixSender, DurableSendError, outbound::DurableSendFuture},
    storage::{OutputSetInput, OutputSpoolStore, OutputStoreError},
};

const CHILD: &str = "T051_PROCESS_ROW";

struct Temp(std::path::PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("t051-process-{}", uuid::Uuid::now_v7()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

async fn fixture(limit: u64) -> (Temp, DurableOutputSpool, OutputSpoolStore, SqlitePool) {
    let root = Temp::new();
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlx::raw_sql(
        "CREATE TABLE tasks(task_id TEXT PRIMARY KEY) STRICT; INSERT INTO tasks VALUES('task');",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::raw_sql(include_str!("../../migrations/0015_durable_acp_output.sql"))
        .execute(&pool)
        .await
        .unwrap();
    let mut policy = DurableOutputPolicy::derive(&root.0.join("state.db"), limit).unwrap();
    policy.spool_segment_bytes = 4;
    policy.matrix_segment_bytes = 2;
    policy.process_quota_bytes = limit;
    let store = OutputSpoolStore::new(pool.clone(), limit).unwrap();
    let spool = DurableOutputSpool::new(policy, store.clone());
    (root, spool, store, pool)
}

async fn create(store: &OutputSpoolStore, id: &str, turn: i64) {
    store
        .create_set(&OutputSetInput {
            set_id: id,
            task_id: "task",
            attempt: 1,
            turn,
            runtime_fence: "f",
            room_id: "!room:example.org",
            thread_root: None,
            reply_event_id: None,
            now: "n",
        })
        .await
        .unwrap();
}

fn quota(row: &sqlx::sqlite::SqliteRow) -> (i64, i64) {
    (row.get("reserved_bytes"), row.get("landed_bytes"))
}

async fn quota_row(pool: &SqlitePool) -> sqlx::sqlite::SqliteRow {
    sqlx::query("SELECT reserved_bytes,landed_bytes FROM acp_output_quota WHERE singleton=1")
        .fetch_one(pool)
        .await
        .unwrap()
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

async fn seal(
    spool: &DurableOutputSpool,
    store: &OutputSpoolStore,
    pool: &SqlitePool,
    id: &str,
    turn: i64,
    bytes: &[u8],
) {
    create(store, id, turn).await;
    spool.publish(id, "owner", bytes, "n").await.unwrap();
    sqlx::query("UPDATE acp_output_sets SET state='sealed' WHERE set_id=? AND state='prepared'")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn process_fixture_child() {
    let Ok(row) = std::env::var(CHILD) else {
        return;
    };
    let (_root, spool, store, pool) = fixture(32).await;
    match row.as_str() {
        "chunks" => {
            create(&store, "set", 1).await;
            let mut writer = StreamingPublisher::new(spool.clone(), "set", "o", "n").unwrap();
            for value in ["a", "bc", "def", "gh"] {
                writer.append(value).await.unwrap();
            }
            writer.finish().await.unwrap();
            let mut actual = Vec::new();
            for index in 0..2 {
                let segment = store.segment("set", index).await.unwrap().unwrap();
                actual.extend(
                    output_files::read_segment(&spool.policy().spool_root, &segment).unwrap(),
                );
            }
            assert_eq!(actual, b"abcdefgh");
            assert_eq!(quota(&quota_row(&pool).await), (0, 8));
        }
        "limit" => {
            create(&store, "set", 1).await;
            let mut writer = StreamingPublisher::new(spool.clone(), "set", "o", "n").unwrap();
            writer.append(&"x".repeat(32)).await.unwrap();
            assert!(matches!(
                writer.append("x").await,
                Err(OutputSpoolError::Store(OutputStoreError::Quota))
            ));
            assert_eq!(
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM acp_output_delivery_segments")
                    .fetch_one(&pool)
                    .await
                    .unwrap(),
                0
            );
        }
        "quota" => {
            create(&store, "one", 1).await;
            create(&store, "two", 2).await;
            let (one, two) = tokio::join!(
                store.reserve_segment("one", 0, 0, 32, "h", "segment-0.bin", "o", "n"),
                store.reserve_segment("two", 0, 0, 32, "h", "segment-0.bin", "o", "n")
            );
            assert_eq!(usize::from(one.is_ok()) + usize::from(two.is_ok()), 1);
            assert!(
                matches!(one, Err(OutputStoreError::Quota))
                    || matches!(two, Err(OutputStoreError::Quota))
            );
            assert_eq!(quota(&quota_row(&pool).await), (32, 0));
            assert!(!spool.policy().spool_root.exists());
        }
        "publication" => {
            for (turn, stage) in (1_i64..=4).zip(0..4) {
                let id = format!("set-{stage}");
                create(&store, &id, turn).await;
                store
                    .reserve_segment(
                        &id,
                        0,
                        0,
                        4,
                        &output_files::hash(b"data"),
                        "segment-0.bin",
                        "dead-owner",
                        "n",
                    )
                    .await
                    .unwrap();
                spool.prepare_root().unwrap();
                let dir = output_files::prepare_set(&spool.policy().spool_root, &id).unwrap();
                if stage == 1 {
                    use std::io::Write as _;
                    let mut temp = fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .mode(0o600)
                        .open(dir.join(".segment-0.bin.tmp"))
                        .unwrap();
                    temp.write_all(b"data").unwrap();
                    temp.sync_all().unwrap();
                } else if stage >= 2 {
                    output_files::publish_file(&dir, "segment-0.bin", b"data").unwrap();
                }
                if stage == 3 {
                    store.adopt_reserved(&id, 0, "new-owner").await.unwrap();
                    store.land_segment(&id, 0, "new-owner", "n").await.unwrap();
                }
                spool.publish(&id, "new-owner", b"data", "n").await.unwrap();
            }
            assert_eq!(quota(&quota_row(&pool).await), (0, 16));
            let manifest = spool
                .policy()
                .spool_root
                .join("sets/set-3/manifest-v1.json");
            let bytes = fs::read(&manifest).unwrap();
            output_files::publish_file(manifest.parent().unwrap(), "manifest-v1.json", &bytes)
                .unwrap();
            assert!(
                output_files::publish_file(
                    manifest.parent().unwrap(),
                    "manifest-v1.json",
                    b"conflict"
                )
                .is_err()
            );
        }
        "artifacts" => {
            seal(&spool, &store, &pool, "set", 1, b"data").await;
            let claimed = store.claim_next("d", "n", "old").await.unwrap().unwrap();
            let pending = store.next_segment(&claimed).await.unwrap().unwrap();
            let set_dir = spool.policy().spool_root.join("sets/set");
            let extra = set_dir.join("extra");
            fs::write(&extra, b"hostile").unwrap();
            assert!(matches!(
                spool.read_delivery(&pending),
                Err(OutputSpoolError::Identity)
            ));
            fs::remove_file(extra).unwrap();
            let external = _root.0.join("external-sentinel");
            fs::write(&external, b"sentinel").unwrap();
            let segment = set_dir.join("segment-0.bin");
            fs::remove_file(&segment).unwrap();
            std::os::unix::fs::symlink(&external, segment).unwrap();
            assert!(spool.read_delivery(&pending).is_err());
            assert_eq!(fs::read(external).unwrap(), b"sentinel");
        }
        "ordered" => delivery_case(spool, store, pool, vec![], "cleaned", 4).await,
        "unknown" => {
            delivery_case(
                spool,
                store,
                pool,
                vec![Err(DurableSendError::UnknownCommit)],
                "cleaned",
                5,
            )
            .await
        }
        "partial" => {
            delivery_case(
                spool,
                store,
                pool,
                vec![Err(DurableSendError::Permanent)],
                "partial",
                1,
            )
            .await;
            let (_root, spool, store, pool) = fixture(32).await;
            seal(&spool, &store, &pool, "set", 1, b"abcdefgh").await;
            let sender = Arc::new(Sender {
                outcomes: Mutex::new(vec![Err(DurableSendError::Retryable); 8].into()),
                calls: Mutex::new(Vec::new()),
            });
            let drain = OutputSetDrain::new(store, spool, sender.clone());
            for _ in 0..8 {
                assert_eq!(drain.drain_once().await, Ok(0));
            }
            let failure: String =
                sqlx::query_scalar("SELECT failure_class FROM acp_output_sets WHERE set_id='set'")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(failure, "attempts_exhausted");
            assert_eq!(sender.calls.lock().unwrap().len(), 8);
        }
        "cleanup" => {
            seal(&spool, &store, &pool, "set", 1, b"data").await;
            sqlx::query("UPDATE acp_output_sets SET state='delivered' WHERE set_id='set'")
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(spool.recover_cleanup("later").await.unwrap(), 1);
            assert_eq!(spool.recover_cleanup("again").await.unwrap(), 0);
            assert_eq!(quota(&quota_row(&pool).await), (0, 0));
        }
        "cancel" => {
            create(&store, "set", 1).await;
            let mut writer = StreamingPublisher::new(spool.clone(), "set", "o", "n").unwrap();
            writer.append("data").await.unwrap();
            drop(writer);
            let segments = store.abandon("set", "later").await.unwrap();
            spool.cleanup_files("set", &segments).unwrap();
            store.finish_abandon("set", "later").await.unwrap();
            assert_eq!(quota(&quota_row(&pool).await), (0, 0));
        }
        "migration" => {
            store.verify_quota().await.unwrap();
            assert_eq!(quota(&quota_row(&pool).await), (0, 0));
        }
        "redaction" => {
            create(&store, "set", 1).await;
            let hostile = "../secret-token";
            assert!(matches!(
                store
                    .reserve_segment("set", 0, 0, 33, hostile, hostile, "o", "n")
                    .await,
                Err(OutputStoreError::Quota)
            ));
            assert!(!spool.policy().spool_root.join(hostile).exists());
        }
        _ => panic!("unknown row"),
    }
}

async fn delivery_case(
    spool: DurableOutputSpool,
    store: OutputSpoolStore,
    pool: SqlitePool,
    outcomes: Vec<Result<(), DurableSendError>>,
    expected: &str,
    calls: usize,
) {
    seal(&spool, &store, &pool, "set", 1, b"abcdefgh").await;
    let sender = Arc::new(Sender {
        outcomes: Mutex::new(outcomes.into()),
        calls: Mutex::new(Vec::new()),
    });
    let drain = OutputSetDrain::new(store, spool, sender.clone());
    assert_eq!(
        drain.drain_once().await,
        Ok(if expected == "cleaned" && calls == 4 {
            1
        } else {
            0
        })
    );
    if expected == "cleaned" && calls == 5 {
        assert_eq!(drain.drain_once().await, Ok(1));
    }
    let state: String = sqlx::query_scalar("SELECT state FROM acp_output_sets WHERE set_id='set'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, expected);
    let observed = sender.calls.lock().unwrap();
    assert_eq!(observed.len(), calls);
    if calls == 4 {
        assert_eq!(
            observed
                .iter()
                .map(|call| call.0.as_str())
                .collect::<String>(),
            "abcdefgh"
        );
        assert!(observed.windows(2).all(|pair| pair[0].1 != pair[1].1));
    }
    if calls == 5 {
        assert_eq!(observed[0].1, observed[1].1);
    }
}
