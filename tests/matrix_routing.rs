use std::sync::{Arc, Mutex};

use guigu_agent_bridge::{
    bus::{MemoryBus, derive_endpoint_id},
    config::load_from_str,
    matrix::{
        EventDedup, InboundMatrixEvent, MatrixSender, PermissionPolicy, ReplyContext, ReplyError,
        ReplyFuture, RouteError, RoutePolicy, derive_matrix_user_id, route_event,
        send_permission_denied, send_task_terminal_reply, send_terminal_reply,
        with_matrix_participant,
    },
    models::{Conversation, ConversationId, TaskEventPayload},
};

fn config() -> guigu_agent_bridge::config::Config {
    load_from_str(
        r#"
[agents.worker]
transport = "acp"
command = "worker"
workspace = "/tmp"
enabled = true

[agents.other]
transport = "acp"
command = "other"
workspace = "/tmp"
enabled = true

[agents.matrix_only]
transport = "matrix"
enabled = true
"#,
    )
    .unwrap()
}

fn event(id: &str, room: &str, sender: &str, body: &str) -> InboundMatrixEvent {
    InboundMatrixEvent {
        event_id: id.to_owned(),
        room_id: room.to_owned(),
        thread_root: None,
        sender: sender.to_owned(),
        body: body.to_owned(),
    }
}

#[tokio::test]
async fn explicit_target_routes_once_through_the_real_bus_and_strips_prefix() {
    let (bus, mut receivers) = MemoryBus::from_config(&config(), 4);
    let policy = RoutePolicy::new()
        .alias("worker", "worker")
        .bind_room("!room:x", "other");
    let permissions = PermissionPolicy::new().allow_user("@alice:x");
    let mut dedup = EventDedup::new(4).unwrap();
    let incoming = event("$one:x", "!room:x", "@alice:x", "@worker: do it");

    let task = route_event(
        &incoming,
        ConversationId::generate(),
        &policy,
        &permissions,
        bus.registry(),
        &bus,
        &mut dedup,
    )
    .await
    .unwrap();

    assert_eq!(task.task_id, task.root_task_id);
    assert_eq!(task.to_agent, derive_endpoint_id("worker"));
    assert_eq!(task.from_agent, derive_matrix_user_id("@alice:x").unwrap());
    assert_eq!(task.text, "do it");
    assert_eq!(receivers.tasks.recv().await.unwrap(), task);
    assert_eq!(receivers.events.recv().await.unwrap().task_id, task.task_id);

    assert_eq!(
        route_event(
            &incoming,
            task.conversation_id,
            &policy,
            &permissions,
            bus.registry(),
            &bus,
            &mut dedup,
        )
        .await
        .unwrap_err(),
        RouteError::Duplicate
    );
    assert!(receivers.tasks.try_recv().is_err());

    assert_eq!(
        route_event(
            &event("$unknown:x", "!room:x", "@alice:x", "@missing: no"),
            task.conversation_id,
            &policy,
            &permissions,
            bus.registry(),
            &bus,
            &mut dedup,
        )
        .await
        .unwrap_err(),
        RouteError::NoRoute
    );
    assert!(receivers.tasks.try_recv().is_err());
}

#[tokio::test]
async fn default_deny_ambiguous_and_unaddressable_routes_never_submit() {
    let (bus, mut receivers) = MemoryBus::from_config(&config(), 4);
    let mut dedup = EventDedup::new(4).unwrap();
    let denied = event("$deny:x", "!bound:x", "@mallory:x", "hello");
    let policy = RoutePolicy::new().bind_room("!bound:x", "worker");
    assert_eq!(
        route_event(
            &denied,
            ConversationId::generate(),
            &policy,
            &PermissionPolicy::new(),
            bus.registry(),
            &bus,
            &mut dedup,
        )
        .await
        .unwrap_err(),
        RouteError::Forbidden
    );
    assert!(receivers.tasks.try_recv().is_err());

    let ambiguous = RoutePolicy::new().direct_room("!dm:x", ["worker", "other"]);
    assert_eq!(
        route_event(
            &event("$amb:x", "!dm:x", "@alice:x", "hello"),
            ConversationId::generate(),
            &ambiguous,
            &PermissionPolicy::new().allow_user("@alice:x"),
            bus.registry(),
            &bus,
            &mut dedup,
        )
        .await
        .unwrap_err(),
        RouteError::Ambiguous
    );

    let unavailable = RoutePolicy::new().bind_room("!matrix:x", "matrix_only");
    assert_eq!(
        route_event(
            &event("$matrix:x", "!matrix:x", "@alice:x", "hello"),
            ConversationId::generate(),
            &unavailable,
            &PermissionPolicy::new().allow_user("@alice:x"),
            bus.registry(),
            &bus,
            &mut dedup,
        )
        .await
        .unwrap_err(),
        RouteError::Target
    );
    assert!(receivers.tasks.try_recv().is_err());
}

#[tokio::test]
async fn failed_bus_submission_does_not_poison_event_dedup() {
    let (bus, mut receivers) = MemoryBus::from_config(&config(), 1);
    let policy = RoutePolicy::new().bind_room("!room:x", "worker");
    let permissions = PermissionPolicy::new().allow_user("@alice:x");
    let mut dedup = EventDedup::new(4).unwrap();
    let first = event("$first:x", "!room:x", "@alice:x", "first");
    let retry = event("$retry:x", "!room:x", "@alice:x", "retry");
    route_event(
        &first,
        ConversationId::generate(),
        &policy,
        &permissions,
        bus.registry(),
        &bus,
        &mut dedup,
    )
    .await
    .unwrap();
    assert_eq!(
        route_event(
            &retry,
            ConversationId::generate(),
            &policy,
            &permissions,
            bus.registry(),
            &bus,
            &mut dedup,
        )
        .await
        .unwrap_err(),
        RouteError::Bus
    );
    assert!(!dedup.contains("!room:x", "$retry:x"));

    receivers.tasks.recv().await.unwrap();
    receivers.events.recv().await.unwrap();
    route_event(
        &retry,
        ConversationId::generate(),
        &policy,
        &permissions,
        bus.registry(),
        &bus,
        &mut dedup,
    )
    .await
    .unwrap();
    assert!(dedup.contains("!room:x", "$retry:x"));
}

#[tokio::test]
async fn closed_task_channel_leaves_event_unmarked_and_retryable() {
    let (bus, receivers) = MemoryBus::from_config(&config(), 1);
    let policy = RoutePolicy::new().bind_room("!room:x", "worker");
    let permissions = PermissionPolicy::new().allow_user("@alice:x");
    let mut dedup = EventDedup::new(4).unwrap();
    let incoming = event("$retry:x", "!room:x", "@alice:x", "retry");
    drop(receivers.tasks);

    for _ in 0..2 {
        assert_eq!(
            route_event(
                &incoming,
                ConversationId::generate(),
                &policy,
                &permissions,
                bus.registry(),
                &bus,
                &mut dedup,
            )
            .await
            .unwrap_err(),
            RouteError::Bus
        );
        assert!(!dedup.contains("!room:x", "$retry:x"));
    }
}

#[tokio::test]
async fn closed_event_sink_marks_enqueued_event_and_prevents_duplicate() {
    let (bus, mut receivers) = MemoryBus::from_config(&config(), 1);
    let policy = RoutePolicy::new().bind_room("!room:x", "worker");
    let permissions = PermissionPolicy::new().allow_user("@alice:x");
    let mut dedup = EventDedup::new(4).unwrap();
    let incoming = event("$accepted:x", "!room:x", "@alice:x", "run once");
    drop(receivers.events);

    assert_eq!(
        route_event(
            &incoming,
            ConversationId::generate(),
            &policy,
            &permissions,
            bus.registry(),
            &bus,
            &mut dedup,
        )
        .await
        .unwrap_err(),
        RouteError::Bus
    );
    assert!(dedup.contains("!room:x", "$accepted:x"));
    let queued = receivers.tasks.recv().await.unwrap();

    assert_eq!(
        route_event(
            &incoming,
            queued.conversation_id,
            &policy,
            &permissions,
            bus.registry(),
            &bus,
            &mut dedup,
        )
        .await
        .unwrap_err(),
        RouteError::Duplicate
    );
    assert!(receivers.tasks.try_recv().is_err());
}

#[test]
fn identity_participants_and_bounded_dedup_are_stable() {
    let alice = derive_matrix_user_id("@alice:x").unwrap();
    assert_eq!(alice, derive_matrix_user_id("@alice:x").unwrap());
    assert_ne!(alice, derive_endpoint_id("@alice:x"));
    assert!(derive_matrix_user_id("alice").is_err());

    let conversation = Conversation {
        id: ConversationId::generate(),
        participants: Vec::new(),
        external_ref: None,
    };
    let once = with_matrix_participant(&conversation, "@alice:x").unwrap();
    let twice = with_matrix_participant(&once, "@alice:x").unwrap();
    assert!(conversation.participants.is_empty());
    assert_eq!(twice.participants, vec![alice]);

    let mut dedup = EventDedup::new(2).unwrap();
    assert!(dedup.mark("!r:x", "$1:x"));
    assert!(dedup.mark("!r:x", "$2:x"));
    assert!(!dedup.mark("!r:x", "$2:x"));
    assert!(dedup.mark("!r:x", "$3:x"));
    assert!(!dedup.contains("!r:x", "$1:x"));
    assert!(EventDedup::new(0).is_err());
}

#[derive(Default)]
struct RecordingSender {
    sent: Mutex<Vec<(ReplyContext, String)>>,
}

impl MatrixSender for RecordingSender {
    fn send_reply<'a>(&'a self, context: &'a ReplyContext, body: &'a str) -> ReplyFuture<'a> {
        Box::pin(async move {
            self.sent
                .lock()
                .unwrap()
                .push((context.clone(), body.to_owned()));
            Ok(())
        })
    }
}

#[tokio::test]
async fn terminal_reply_preserves_room_thread_and_original_event() {
    let sender = Arc::new(RecordingSender::default());
    let context = ReplyContext {
        room_id: "!room:x".into(),
        thread_root: Some("$root:x".into()),
        event_id: "$incoming:x".into(),
    };
    send_terminal_reply(sender.as_ref(), &context, "done")
        .await
        .unwrap();
    send_permission_denied(sender.as_ref(), &context)
        .await
        .unwrap();
    assert!(
        send_task_terminal_reply(
            sender.as_ref(),
            &context,
            &TaskEventPayload::Completed {
                output: "terminal".into(),
            },
        )
        .await
        .unwrap()
    );
    assert!(
        !send_task_terminal_reply(sender.as_ref(), &context, &TaskEventPayload::Queued)
            .await
            .unwrap()
    );
    assert_eq!(
        sender.sent.lock().unwrap().as_slice(),
        &[
            (context.clone(), "done".to_owned()),
            (context.clone(), "This request is not permitted.".to_owned()),
            (context, "terminal".to_owned()),
        ]
    );
    assert_eq!(ReplyError.to_string(), "matrix reply failed");
}
