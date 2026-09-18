//! Event fan-out: one logical consumer of the event stream, many subscribers.
//!
//! T004 emits events into a single bounded channel. T009 (persistence) and T012
//! (Matrix projection) both need every event, so this module owns that one
//! receiving end ([`EventBroadcaster`]) and forwards each event, in arrival
//! order, to every registered [`EventConsumer`].
//!
//! ```text
//! Bus::submit ─┐
//!              ├─> Arc<dyn EventSink> ─> mpsc ─> EventBroadcaster ─┬─> T009 persistence
//! Worker ──────┘                                                   └─> T012 Matrix projection
//! ```
//!
//! The worker and the bus share one sink, and exactly one component owns the
//! receiving end, so downstream consumers never see two streams.
//!
//! # Why not `tokio::sync::broadcast`
//!
//! `broadcast` drops messages for lagging subscribers. The event stream is the
//! record of task state changes, so a dropped event is a lost state transition —
//! unacceptable (T004 handoff; [`crate::bus::event`]). Fan-out is therefore
//! lossless by construction: the broadcaster awaits each consumer in turn.
//!
//! # Ordering and `seq` (Q1 = A, accepted)
//!
//! **Arrival order is not `seq` order.** `Bus::submit` enqueues the task before
//! writing its `Queued` event, so on a multi-thread runtime the worker can write
//! `Dispatched(seq = 2)` while the submitter is still between those two steps;
//! the stream then contains `seq = 2` before `seq = 1` for one task. The bus
//! cannot avoid this without changing frozen T004 semantics.
//!
//! This broadcaster is faithful rather than clever: it preserves arrival order
//! exactly and never reorders or buffers per task. `seq` — not arrival order — is
//! the authoritative per-task ordering key
//! ([`TaskEvent::seq`](crate::models::TaskEvent::seq)), so consumers must sort or
//! merge per `task_id` by `seq` before interpreting a task's history. See
//! [`crate::bus::worker`] for the same contract stated from the producer side.
//!
//! # Backpressure
//!
//! Consumers are awaited sequentially, so a slow consumer slows the whole
//! broadcast loop; the bounded event buffer then fills and the producers (the
//! bus's `submit`, the worker's transitions) receive
//! [`BusError::EventBufferFull`](crate::bus::BusError::EventBufferFull). That is
//! explicit backpressure, consistent with T004's “never block, report instead”
//! rule: no event is dropped and no per-consumer queue is invented. Per-consumer
//! buffering is a T009/T016 optimisation, not part of v1.
//!
//! # Error isolation
//!
//! A consumer returning [`ConsumerError`] is logged and skipped; the broadcast
//! continues with the next consumer and the next event, and
//! [`EventBroadcaster::run`] returns `()` rather than a `Result`. This is
//! ARCHITECTURE §8: Matrix is a projection of state, so a failed Matrix send must
//! never roll back or interrupt a task's state machine.
//!
//! # Lifecycle
//!
//! - Consumers are registered at construction time. Runtime registration would
//!   need a lock and would race the ordering contract, so v1 does not offer it.
//! - `run()` terminates when the event channel closes, i.e. once **both** the bus
//!   and the worker have dropped every clone of the shared sink. If the worker
//!   fail-stops while a [`Bus`](crate::bus::Bus) handle is still alive, `run()`
//!   simply keeps waiting — that is a suspended broadcast, not a deadlock (there
//!   is no cycle). Whoever assembles the runtime must drop the last bus handle
//!   (and any other sink clone) for the loop to converge; tests do so explicitly
//!   instead of relying on a sleep or a timeout.
//! - A consumer's [`EventConsumer::consume`] must not wait for the broadcast loop
//!   to make progress — a consumer holds no broadcaster handle, so this is
//!   structural — and must not hold a lock across `.await`.
//!
//! # Boundaries
//!
//! No persistence (T009), no Matrix projection (T012), no retry, no per-consumer
//! queue, no deduplication, no reordering, and no cancellation (T006).

use std::sync::Arc;

use thiserror::Error;
use tokio::sync::mpsc;

use crate::bus::BusFuture;
use crate::models::TaskEvent;

/// A downstream subscriber of the task event stream.
///
/// Implemented by T009 (durable event store), T012 (Matrix projection) and test
/// doubles.
///
/// # Implementation contract
///
/// - Object-safe: implementations are held as `Arc<dyn EventConsumer>`, so
///   `consume` takes `&self` and returns a [`BusFuture`].
/// - `Send + Sync`, because the broadcaster may run on another thread.
/// - **Return `Err(Failed)` rather than panic.** A panic aborts the broadcast
///   task and every later event is lost; an `Err` is isolated and logged.
/// - **Never hold a lock across `.await`** and never block: the broadcaster
///   awaits consumers inline, so one blocking consumer stalls the whole stream
///   and ultimately backpressures event writes.
/// - `reason` strings must not contain credentials.
pub trait EventConsumer: Send + Sync {
    /// Consume one event.
    ///
    /// # Errors
    ///
    /// Return [`ConsumerError::Failed`] if this subscription could not process
    /// the event. The error is logged and isolated: the broadcaster continues
    /// with the remaining consumers and the next event, and never rolls back the
    /// task state that produced the event.
    fn consume<'a>(&'a self, event: &'a TaskEvent) -> BusFuture<'a, Result<(), ConsumerError>>;
}

/// Why one consumer could not process an event.
///
/// Deliberately coarse: the broadcaster's reaction is the same for every
/// consumer failure (log, continue), so inventing a taxonomy with no consumer
/// would only freeze a contract nobody reads. The variant set is
/// `#[non_exhaustive]` so a future distinction can be added without breaking
/// downstream matches.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ConsumerError {
    /// The subscription failed to process the event.
    #[error("event consumer failed: {reason}")]
    Failed {
        /// Subscription-supplied description. Must not contain credentials.
        reason: String,
    },
}

/// Forwards every event from one stream to every registered consumer, in order.
///
/// Exactly one component owns the event channel's receiving end — this one. See
/// the module docs for ordering, backpressure, isolation and shutdown contracts.
pub struct EventBroadcaster {
    events: mpsc::Receiver<TaskEvent>,
    consumers: Vec<Arc<dyn EventConsumer>>,
}

impl EventBroadcaster {
    /// Build a broadcaster over the sink's receiving end.
    ///
    /// `consumers` fixes both the subscriber set and the delivery order: each
    /// event is delivered to consumers in slice order, and every consumer sees
    /// the events in arrival order.
    pub fn new(events: mpsc::Receiver<TaskEvent>, consumers: Vec<Arc<dyn EventConsumer>>) -> Self {
        Self { events, consumers }
    }

    /// The number of registered consumers.
    pub fn consumer_count(&self) -> usize {
        self.consumers.len()
    }

    /// Forward events until the event channel closes.
    ///
    /// Returns `()`: fan-out failures never change or interrupt the state machine
    /// (ARCHITECTURE §8). Each consumer error is logged and isolated.
    pub async fn run(mut self) {
        loop {
            let Some(event) = self.events.recv().await else {
                return;
            };
            for consumer in &self.consumers {
                if let Err(err) = consumer.consume(&event).await {
                    tracing::warn!(
                        task_id = %event.task_id,
                        seq = event.seq,
                        error = %err,
                        "event consumer failed; broadcast continues"
                    );
                }
            }
        }
    }

    /// Run the broadcaster on the current runtime, for owners that want a handle.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime context (mirrors
    /// [`tokio::spawn`]).
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(self.run())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::MpscEventSink;
    use crate::bus::event::EventSink;
    use crate::models::{EventId, TaskEventPayload, TaskId, TaskStatus};
    use std::sync::Mutex;

    fn event(seq: u64) -> TaskEvent {
        TaskEvent {
            id: EventId::generate(),
            task_id: TaskId::generate(),
            seq,
            status: TaskStatus::Queued,
            timestamp: "2026-09-15T12:00:00Z".parse().unwrap(),
            payload: TaskEventPayload::Queued,
        }
    }

    #[derive(Default)]
    struct Recorder {
        seen: Mutex<Vec<u64>>,
    }

    impl Recorder {
        fn seen(&self) -> Vec<u64> {
            self.seen.lock().expect("not poisoned").clone()
        }
    }

    impl EventConsumer for Recorder {
        fn consume<'a>(&'a self, event: &'a TaskEvent) -> BusFuture<'a, Result<(), ConsumerError>> {
            Box::pin(async move {
                self.seen.lock().expect("not poisoned").push(event.seq);
                Ok(())
            })
        }
    }

    struct AlwaysFails;

    impl EventConsumer for AlwaysFails {
        fn consume<'a>(
            &'a self,
            _event: &'a TaskEvent,
        ) -> BusFuture<'a, Result<(), ConsumerError>> {
            Box::pin(async {
                Err(ConsumerError::Failed {
                    reason: "subscription is down".into(),
                })
            })
        }
    }

    #[test]
    fn consumer_trait_is_object_safe_and_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EventBroadcaster>();
        assert_send_sync::<Arc<dyn EventConsumer>>();
        assert_send_sync::<ConsumerError>();

        let consumers: Vec<Arc<dyn EventConsumer>> = vec![Arc::new(Recorder::default())];
        let (_sink, events_rx) = MpscEventSink::new(4);
        assert_eq!(
            EventBroadcaster::new(events_rx, consumers).consumer_count(),
            1
        );
    }

    #[tokio::test]
    async fn consumer_error_display_is_fixed_and_carries_the_reason() {
        let rendered = ConsumerError::Failed {
            reason: "boom".into(),
        }
        .to_string();
        assert_eq!(rendered, "event consumer failed: boom");
        for leaked in ["command", "args", "token", "password", "user_id"] {
            assert!(!rendered.contains(leaked));
        }
    }

    #[tokio::test]
    async fn a_failing_consumer_is_isolated_and_the_broadcast_continues() {
        let first = Arc::new(Recorder::default());
        let third = Arc::new(Recorder::default());
        let consumers: Vec<Arc<dyn EventConsumer>> = vec![
            first.clone(),
            Arc::new(AlwaysFails),
            Arc::new(AlwaysFails),
            third.clone(),
        ];
        let (sink, events_rx) = MpscEventSink::new(4);
        let broadcaster = EventBroadcaster::new(events_rx, consumers).spawn();

        for seq in 1..=3 {
            sink.emit(event(seq)).await.expect("emit");
        }
        drop(sink);

        broadcaster.await.expect("broadcaster task");
        assert_eq!(first.seen(), [1, 2, 3]);
        assert_eq!(third.seen(), [1, 2, 3]);
    }
}
