//! Offline real-ingress examination of an explicitly saved private target.
use super::{coverage::Target, ingress_decision, intent_diagnostic};
use crate::{
    app::{
        FileConfigSource, MatrixIngress, PersistingBus, ReloadController, ReplyRegistry,
        TerminalReplyConsumer,
    },
    bus::{
        AdmissionContext, BusError, Cancellation, Clock, EndpointRegistry, EventConsumer,
        MpscEventSink,
    },
    matrix::{
        CommandLedger, DurableMatrixAdmission, MatrixAdmissionFuture, MatrixSender, ReplyContext,
        ReplyFuture,
    },
    models::{AgentTask, EventId, TaskEvent, TaskEventPayload, TaskStatus},
    storage::{ReliabilityStore, SqliteRepository},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Default)]
struct Sender(Mutex<Vec<&'static str>>);
impl MatrixSender for Sender {
    fn send_reply<'a>(&'a self, _: &'a ReplyContext, body: &'a str) -> ReplyFuture<'a> {
        Box::pin(async move {
            self.0
                .lock()
                .unwrap()
                .push(if body == super::reply::PERMISSION_DENIED_REPLY {
                    "permission"
                } else {
                    "reply"
                });
            Ok(())
        })
    }
}
struct Admission {
    inner: PersistingBus,
    lookups: AtomicUsize,
    intents: AtomicUsize,
    winners: Mutex<Vec<AgentTask>>,
}
impl DurableMatrixAdmission for Admission {
    fn winner<'a>(
        &'a self,
        id: &'a str,
    ) -> MatrixAdmissionFuture<'a, Result<Option<AgentTask>, BusError>> {
        Box::pin(async move {
            self.lookups.fetch_add(1, Ordering::SeqCst);
            let result = self.inner.winner(id).await?;
            if let Some(task) = &result {
                self.winners.lock().unwrap().push(task.clone());
            }
            Ok(result)
        })
    }
    fn admit<'a>(
        &'a self,
        _: AgentTask,
        _: AdmissionContext,
    ) -> MatrixAdmissionFuture<'a, Result<AgentTask, BusError>> {
        Box::pin(async move {
            self.intents.fetch_add(1, Ordering::SeqCst);
            Err(BusError::TaskChannelClosed)
        })
    }
}

#[tokio::test]
#[ignore = "explicit saved target and disposable durable copy; no network"]
async fn saved_target_all_events_through_recording_ingress() {
    let root = PathBuf::from(std::env::var_os("R112_OBSERVATION_ROOT").expect("private copy root"));
    let config = crate::config::load(root.join("config.toml")).unwrap();
    let target: Target =
        serde_json::from_slice(&std::fs::read(root.join("NEW-target.json")).unwrap()).unwrap();
    let mut results = vec![];
    for (ri, room) in target.rooms.iter().enumerate() {
        for (ei, raw) in room.initial.iter().enumerate() {
            let db = root.join(format!("ingress-{ri}-{ei}.db"));
            assert!(!db.exists());
            std::fs::copy(root.join("pair/state/state.db"), &db).unwrap();
            let pool = crate::storage::connect(&db).await.unwrap();
            let mut c = pool.acquire().await.unwrap();
            let predicate = ingress_decision::no_effect(&mut c, &config, &room.id, raw).await;
            let predicate = predicate
                .as_ref()
                .err()
                .map(intent_diagnostic::predicate)
                .unwrap_or("no-effect");
            drop(c);
            let event = super::event::decode_event(
                &raw.to_string(),
                &room.id,
                &config.transports.matrix.user_id,
            );
            let mut result = json!({"room_ordinal":ri,"event_ordinal":ei,"predicate":predicate,
                "event_sha256":format!("{:x}",Sha256::digest(raw["event_id"].as_str().unwrap_or("").as_bytes())),
                "payload_sha256":format!("{:x}",Sha256::digest(raw.to_string().as_bytes()))});
            match event {
                Ok(Some(event)) => {
                    let counts = ingress(&root, config.clone(), pool.clone(), event).await;
                    result["ingress"] = counts;
                }
                Ok(None) => {
                    result["ingress"] =
                        json!({"decoder":"ignored-or-nonordinary", "submitted":false});
                }
                Err(_) => {
                    result["ingress"] = json!({"decoder":"malformed", "submitted":false});
                }
            }
            results.push(result);
            pool.close().await;
        }
    }
    std::fs::write(
        root.join("target-classification.json"),
        serde_json::to_vec_pretty(&results).unwrap(),
    )
    .unwrap();
    println!(
        "new_target_events={} network=0 business_sends=0 tasks_executed=0",
        results.len()
    );
}

async fn ingress(
    root: &std::path::Path,
    config: crate::config::Config,
    pool: sqlx::SqlitePool,
    event: super::InboundMatrixEvent,
) -> serde_json::Value {
    let repository = SqliteRepository::new(pool.clone());
    let registry = Arc::new(EndpointRegistry::from_config(&config));
    let (sink, mut events) = MpscEventSink::new(8);
    let (bus, mut tasks) = PersistingBus::new(
        registry.clone(),
        8,
        Arc::new(sink),
        repository.clone(),
        Clock::system(),
    );
    let admission = Arc::new(Admission {
        inner: bus.with_reliability(
            ReliabilityStore::new(pool.clone()),
            "fixture-no-runtime".into(),
        ),
        lookups: AtomicUsize::new(0),
        intents: AtomicUsize::new(0),
        winners: Mutex::new(vec![]),
    });
    let reload = Arc::new(
        ReloadController::new(
            config,
            Arc::new(FileConfigSource::new(root.join("config.toml"))),
        )
        .unwrap(),
    );
    let sender = Arc::new(Sender::default());
    let replies = Arc::new(ReplyRegistry::default());
    let terminal = TerminalReplyConsumer::new(sender.clone(), replies.clone());
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    let handle = MatrixIngress::new(
        rx,
        Arc::new(repository),
        registry,
        admission.clone(),
        sender.clone(),
        reload,
        Cancellation::new(),
        Arc::new(CommandLedger::new(16)),
        replies,
        16,
    )
    .with_queue_control_store(Arc::new(ReliabilityStore::new(pool)))
    .start();
    tx.send(event).await.unwrap();
    drop(tx);
    tokio::time::timeout(Duration::from_secs(5), async {
        while handle.alive().load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    handle.shutdown().await;
    let (direct, winners) = terminal_intents(&sender, &admission, &terminal).await;
    assert!(tasks.try_recv().is_err());
    assert!(events.try_recv().is_err());
    json!({"winner_lookups":admission.lookups.load(Ordering::SeqCst), "winner_hits":winners.len(),
        "admission_intents":admission.intents.load(Ordering::SeqCst),"direct_reply_intents":direct,
        "terminal_reply_intents":sender.0.lock().unwrap().len()-direct,"executed":0,"sent":0})
}

async fn terminal_intents(
    sender: &Sender,
    admission: &Admission,
    terminal: &TerminalReplyConsumer,
) -> (usize, Vec<AgentTask>) {
    let direct = sender.0.lock().unwrap().len();
    let winners = admission.winners.lock().unwrap().clone();
    for task in &winners {
        terminal
            .consume(&TaskEvent {
                id: EventId::generate(),
                task_id: task.task_id,
                seq: 999999,
                status: TaskStatus::Completed,
                timestamp: Clock::system().now(),
                payload: TaskEventPayload::Completed {
                    output: "fixture completion only".into(),
                },
            })
            .await
            .unwrap();
    }
    (direct, winners)
}
