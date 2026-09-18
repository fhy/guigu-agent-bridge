//! Event write boundary for the Agent Bus.
//!
//! Every task state change produces an immutable [`TaskEvent`] (ARCHITECTURE.md
//! §7). T004 writes the first event of a task — `Queued`, `seq = 1`; later events
//! (`seq > 1`) belong to the T005 worker. A substitute [`EventSink`] is installed
//! through [`crate::bus::MemoryBus::with_event_sink`] — that is how T009 will add
//! its persistent sink — and T012 projects events to Matrix.
//!
//! # Consumer model
//!
//! v1 has a single logical consumer (the T005 worker), which fans out to
//! persistence (T009) and Matrix (T012) itself. A multi-subscriber
//! `tokio::sync::broadcast` sink is deliberately not used: broadcast drops events
//! for slow subscribers, which is unacceptable for a record of state changes.

use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;

use crate::bus::{BusError, BusFuture};
use crate::models::TaskEvent;

/// A source of "now" in UTC.
///
/// Injectable so tests can pin event timestamps exactly (the same reasoning as
/// T003's injectable environment): asserting against a wall-clock window is
/// fragile, asserting equality is not. Production wiring uses [`Clock::system`].
#[derive(Clone)]
pub struct Clock(Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>);

impl Clock {
    /// A clock backed by any `Fn` returning UTC.
    pub fn new(f: impl Fn() -> DateTime<Utc> + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }

    /// The system clock (`Utc::now`). The production default.
    pub fn system() -> Self {
        Self(Arc::new(Utc::now))
    }

    /// A clock that always returns `at`, for deterministic tests.
    pub fn fixed(at: DateTime<Utc>) -> Self {
        Self(Arc::new(move || at))
    }

    /// The current instant according to this clock.
    pub fn now(&self) -> DateTime<Utc> {
        (self.0)()
    }
}

impl Default for Clock {
    fn default() -> Self {
        Self::system()
    }
}

impl fmt::Debug for Clock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The closure is not printable; keep this opaque but labelled.
        f.write_str("Clock(..)")
    }
}

/// Writes immutable task events.
///
/// Object-safe (`Arc<dyn EventSink>`) and injectable: pass an implementation to
/// [`crate::bus::MemoryBus::with_event_sink`] to replace the v1 sink — e.g. a
/// durable implementation in T009, or a recording/failing test double — without
/// changing [`crate::bus::MemoryBus`] or its callers. [`MpscEventSink`] stays the
/// default installed by `MemoryBus::new`/`with_clock`/`from_config`.
pub trait EventSink: Send + Sync {
    /// Write one event.
    ///
    /// # Errors
    ///
    /// - [`BusError::EventBufferFull`] — the bounded buffer is full; the event was
    ///   not written and the caller must not assume it was.
    /// - [`BusError::EventSinkClosed`] — the consumer is gone after the task was
    ///   enqueued (the bus is shutting down).
    ///
    /// Implementations must not block indefinitely: v1 uses `try_send` so that a
    /// stalled consumer turns into an explicit error instead of a hung submitter.
    fn emit<'a>(&'a self, event: TaskEvent) -> BusFuture<'a, Result<(), BusError>>;
}

/// The v1 [`EventSink`]: a bounded, single-consumer `tokio::sync::mpsc` channel.
pub struct MpscEventSink {
    tx: mpsc::Sender<TaskEvent>,
}

impl MpscEventSink {
    /// Create a sink and the receiving end to hand to the consumer (T005).
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is `0` (mirrors `tokio::sync::mpsc::channel`).
    pub fn new(capacity: usize) -> (Self, mpsc::Receiver<TaskEvent>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Self { tx }, rx)
    }
}

impl EventSink for MpscEventSink {
    fn emit<'a>(&'a self, event: TaskEvent) -> BusFuture<'a, Result<(), BusError>> {
        Box::pin(async move {
            self.tx.try_send(event).map_err(|err| match err {
                mpsc::error::TrySendError::Full(_) => BusError::EventBufferFull,
                mpsc::error::TrySendError::Closed(_) => BusError::EventSinkClosed,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{EventId, TaskEventPayload, TaskId, TaskStatus};

    fn sample_event(seq: u64) -> TaskEvent {
        TaskEvent {
            id: EventId::generate(),
            task_id: TaskId::generate(),
            seq,
            status: TaskStatus::Queued,
            timestamp: "2026-09-15T12:00:00Z".parse().unwrap(),
            payload: TaskEventPayload::Queued,
        }
    }

    #[test]
    fn clock_is_injectable_and_defaults_to_system_time() {
        let fixed: DateTime<Utc> = "2026-09-15T12:00:00Z".parse().unwrap();
        assert_eq!(Clock::fixed(fixed).now(), fixed);

        let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let ticked = {
            let counter = Arc::clone(&counter);
            Clock::new(move || {
                let n = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                fixed + chrono::Duration::seconds(n as i64)
            })
        };
        assert_ne!(ticked.now(), ticked.now(), "a Clock may advance");

        // The default/system clock is near wall-clock now, not a fixed instant.
        let delta = (Utc::now() - Clock::default().now()).num_seconds().abs();
        assert!(delta < 60, "system clock should be close to Utc::now()");
    }

    #[tokio::test]
    async fn mpsc_sink_emits_events_in_order_through_dyn() {
        // Proves the trait is object-safe and usable exactly as producers will
        // use it: through `Arc<dyn EventSink>`.
        let (sink, mut rx) = MpscEventSink::new(4);
        let sink: Arc<dyn EventSink> = Arc::new(sink);

        let first = sample_event(1);
        let second = sample_event(2);
        sink.emit(first.clone()).await.expect("emit first");
        sink.emit(second.clone()).await.expect("emit second");

        assert_eq!(rx.recv().await, Some(first));
        assert_eq!(rx.recv().await, Some(second));
        assert_eq!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn mpsc_sink_reports_full_without_blocking() {
        let (sink, mut rx) = MpscEventSink::new(1);
        sink.emit(sample_event(1)).await.expect("first fits");

        let err = sink
            .emit(sample_event(2))
            .await
            .expect_err("buffer is full");
        assert_eq!(err, BusError::EventBufferFull);

        // The rejected event is not silently dropped into the buffer.
        assert!(rx.try_recv().is_ok());
        assert_eq!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn mpsc_sink_reports_closed_when_consumer_is_gone() {
        let (sink, rx) = MpscEventSink::new(1);
        drop(rx);
        let err = sink.emit(sample_event(1)).await.expect_err("closed");
        assert_eq!(err, BusError::EventSinkClosed);
    }
}
