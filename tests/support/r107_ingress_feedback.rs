use super::*;
use std::time::Duration;

struct NoticeSender(tokio::sync::mpsc::Sender<(ReplyContext, String)>);

impl MatrixSender for NoticeSender {
    fn send_reply<'reply>(
        &'reply self,
        context: &'reply ReplyContext,
        body: &'reply str,
    ) -> ReplyFuture<'reply> {
        Box::pin(async move {
            self.0
                .send((context.clone(), body.into()))
                .await
                .map_err(|_| guigu_agent_bridge::matrix::ReplyError)
        })
    }
}

fn incoming(identity: &str, sender: &str, room: &str) -> InboundMatrixEvent {
    InboundMatrixEvent {
        event_id: identity.into(),
        room_id: room.into(),
        thread_root: None,
        sender: sender.into(),
        body: "private request must not be echoed".into(),
    }
}

#[tokio::test]
async fn unrouted_notice_is_allowlisted_room_scoped_deduplicated_and_never_admitted() {
    let directory = TempDir::new();
    let pool = connect(directory.0.join("state.db")).await.unwrap();
    migrate(&pool).await.unwrap();
    let repository = Arc::new(SqliteRepository::new(pool.clone()));
    let mut initial = config("");
    initial
        .transports
        .matrix
        .allowed_users
        .push("@alice:example".into());
    initial.transports.matrix.monitor_room = "!room:example".into();
    let registry = Arc::new(EndpointRegistry::from_config(&initial));
    let (sink, mut events) = guigu_agent_bridge::bus::MpscEventSink::new(4);
    let (bus, mut tasks) = PersistingBus::new(
        registry.clone(),
        4,
        Arc::new(sink),
        repository.as_ref().clone(),
        Clock::default(),
    );
    let bus = bus.with_reliability(ReliabilityStore::new(pool.clone()), "r107-feedback".into());
    let reload = Arc::new(
        ReloadController::new(
            initial.clone(),
            Arc::new(MutableSource(Mutex::new(initial))),
        )
        .unwrap(),
    );
    let (notice_tx, mut notices) = tokio::sync::mpsc::channel(8);
    let (incoming_tx, incoming_rx) = tokio::sync::mpsc::channel(8);
    let ingress = MatrixIngress::new(
        incoming_rx,
        repository,
        registry,
        Arc::new(bus),
        Arc::new(NoticeSender(notice_tx)),
        reload,
        Cancellation::new(),
        Arc::new(CommandLedger::new(8)),
        Arc::new(ReplyRegistry::default()),
        8,
    )
    .start();
    for event in [
        incoming("$first", "@alice:example", "!room:example"),
        incoming("$first", "@alice:example", "!room:example"),
        incoming("$unauthorized", "@mallory:example", "!room:example"),
        incoming("$other-room", "@alice:example", "!unrelated:example"),
        incoming("$last", "@alice:example", "!room:example"),
    ] {
        incoming_tx.send(event).await.unwrap();
    }
    for identity in ["$first", "$last"] {
        let (context, body) = tokio::time::timeout(Duration::from_secs(30), notices.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(context.event_id, identity);
        assert_eq!(
            body,
            "No enabled Agent route is available for this request."
        );
    }
    ingress.shutdown().await;
    assert!(notices.try_recv().is_err());
    assert!(tasks.try_recv().is_err());
    assert!(events.try_recv().is_err());
    pool.close().await;
}
