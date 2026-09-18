//! In-memory [`Bus`] implementation over bounded `tokio::sync::mpsc` channels.
//!
//! v1 keeps the queue in memory (ARCHITECTURE.md §6) while SQLite stays the
//! system of record. Both channels are bounded and written with `try_send`, so a
//! stalled consumer produces an explicit [`BusError`] instead of an unbounded
//! queue or a blocked submitter.
//!
//! The event write goes through an [`EventSink`], not a hardcoded channel:
//! [`MemoryBus::with_event_sink`] installs any implementation (a durable sink in
//! T009, or a recording/failing test double), while the convenience constructors
//! keep installing the default bounded [`MpscEventSink`].

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::bus::event::{Clock, EventSink, MpscEventSink};
use crate::bus::registry::EndpointRegistry;
use crate::bus::{Bus, BusError, BusFuture};
use crate::config::Config;
use crate::models::{AgentTask, EventId, TaskEvent, TaskEventPayload, TaskStatus};

/// Default bounded capacity of the task queue and the event buffer.
///
/// A task is a coarse-grained unit of work, so 1024 in-flight tasks is far above
/// normal load. The value is not configurable in v1 (wiring it into `Config` is
/// a public-contract change, deferred to T016/T017).
pub const DEFAULT_QUEUE_CAPACITY: usize = 1024;

/// Receiving ends of the bus channels.
///
/// Handed out exactly once at construction: the consumer (T005 in production,
/// tests today) owns them, and the bus keeps no clone. Dropping them closes the
/// channels in the consumer->producer direction, which makes `submit` report
/// [`BusError::TaskChannelClosed`].
#[derive(Debug)]
pub struct BusReceivers {
    /// Queued tasks, in submission order.
    pub tasks: mpsc::Receiver<AgentTask>,
    /// Task events written by the bus (T004 writes `Queued`/`seq = 1`).
    pub events: mpsc::Receiver<TaskEvent>,
}

/// The in-memory [`Bus`]: a bounded task channel plus an [`EventSink`].
///
/// `Clone` is cheap and shares everything (`mpsc::Sender` clone, `Arc` handles),
/// so several producers can submit concurrently. There is no interior
/// mutability, no lock, and no background task: closing happens when the last
/// handle is dropped (see [`crate::bus`] for the drain-on-close semantics).
///
/// The sink defaults to [`MpscEventSink`] but is not fixed: any [`EventSink`] can
/// be injected through [`MemoryBus::with_event_sink`].
#[derive(Clone)]
pub struct MemoryBus {
    registry: Arc<EndpointRegistry>,
    tasks: mpsc::Sender<AgentTask>,
    events: Arc<dyn EventSink>,
    clock: Clock,
}

impl MemoryBus {
    /// Build a bus over `registry` with the system clock.
    ///
    /// `capacity` bounds both the task queue and the event buffer.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is `0` (mirrors `tokio::sync::mpsc::channel`).
    pub fn new(registry: Arc<EndpointRegistry>, capacity: usize) -> (Self, BusReceivers) {
        Self::with_clock(registry, capacity, Clock::system())
    }

    /// Build a bus with an explicit [`Clock`], for deterministic timestamps.
    ///
    /// Installs the default bounded [`MpscEventSink`]; use
    /// [`MemoryBus::with_event_sink`] to substitute another sink.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is `0`.
    pub fn with_clock(
        registry: Arc<EndpointRegistry>,
        capacity: usize,
        clock: Clock,
    ) -> (Self, BusReceivers) {
        let (sink, events_rx) = MpscEventSink::new(capacity);
        let (bus, tasks_rx) = Self::with_event_sink(registry, capacity, clock, Arc::new(sink));
        (
            bus,
            BusReceivers {
                tasks: tasks_rx,
                events: events_rx,
            },
        )
    }

    /// Build a bus that writes events through a caller-supplied [`EventSink`].
    ///
    /// This is the injection point for replacing the v1 sink: a durable sink
    /// (T009), a recording/failing test double, or any other implementation can
    /// be installed without changing [`MemoryBus`] or the [`Bus::submit`] path.
    /// The injected sink owns its own consuming end, so only the task receiver is
    /// returned; the convenience constructors ([`MemoryBus::new`],
    /// [`MemoryBus::with_clock`], [`MemoryBus::from_config`]) keep handing out
    /// both receivers as a [`BusReceivers`].
    ///
    /// `capacity` bounds the task queue only — the sink defines its own buffering
    /// and failure behaviour.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is `0` (mirrors `tokio::sync::mpsc::channel`).
    pub fn with_event_sink(
        registry: Arc<EndpointRegistry>,
        capacity: usize,
        clock: Clock,
        events: Arc<dyn EventSink>,
    ) -> (Self, mpsc::Receiver<AgentTask>) {
        let (tasks_tx, tasks_rx) = mpsc::channel(capacity);
        let bus = Self {
            registry,
            tasks: tasks_tx,
            events,
            clock,
        };
        (bus, tasks_rx)
    }

    /// Build a bus and its registry from a validated [`Config`].
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is `0`.
    pub fn from_config(config: &Config, capacity: usize) -> (Self, BusReceivers) {
        Self::new(Arc::new(EndpointRegistry::from_config(config)), capacity)
    }

    /// The registry this bus validates targets against.
    pub fn registry(&self) -> &EndpointRegistry {
        &self.registry
    }
}

impl Bus for MemoryBus {
    fn submit<'a>(&'a self, task: AgentTask) -> BusFuture<'a, Result<(), BusError>> {
        Box::pin(async move {
            // 1. Validate the target (unknown -> disabled -> unaddressable).
            self.registry.validate_target(task.to_agent)?;

            // 2. Enqueue the whole task, unchanged (priority included; ordering
            //    by priority is the T005 consumer's responsibility).
            let task_id = task.task_id;
            self.tasks.try_send(task).map_err(|err| match err {
                mpsc::error::TrySendError::Full(_) => BusError::QueueFull,
                mpsc::error::TrySendError::Closed(_) => BusError::TaskChannelClosed,
            })?;

            // 3. Emit the immutable Queued event (seq = 1). A failure here happens
            //    after the task is queued: that partial-commit window is part of
            //    the documented contract (see `Bus::submit`).
            let event = TaskEvent {
                id: EventId::generate(),
                task_id,
                seq: 1,
                status: TaskStatus::Queued,
                timestamp: self.clock.now(),
                payload: TaskEventPayload::Queued,
            };
            self.events.emit(event).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::derive_endpoint_id;
    use crate::models::EndpointId;
    use std::collections::BTreeMap;

    fn load(toml: &str) -> Config {
        let mut env = BTreeMap::new();
        env.insert("HOME".to_string(), "/home/tester".to_string());
        crate::config::load_from_str_with_env(toml, &env).expect("test config must be valid")
    }

    const ONE_ACP: &str = r#"
[agents.worker]
transport = "acp"
command = "worker-acp"
workspace = "/tmp"
enabled = true
"#;

    #[test]
    fn default_queue_capacity_is_frozen() {
        assert_eq!(DEFAULT_QUEUE_CAPACITY, 1024);
    }

    #[test]
    fn bus_handles_are_send_sync_and_cheaply_cloneable() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MemoryBus>();
        assert_send_sync::<Clock>();
        assert_send_sync::<BusReceivers>();

        let (bus, _receivers) = MemoryBus::from_config(&load(ONE_ACP), 4);
        let clone = bus.clone();
        assert_eq!(clone.registry().len(), bus.registry().len());
    }

    #[test]
    fn registry_is_built_from_config() {
        let (bus, _receivers) = MemoryBus::from_config(&load(ONE_ACP), 4);
        assert!(bus.registry().get_by_agent_id("worker").is_some());
        assert_eq!(
            bus.registry().resolve_agent_id("worker"),
            Some(derive_endpoint_id("worker"))
        );
        assert_eq!(bus.registry().get(EndpointId::generate()), None);
    }
}
