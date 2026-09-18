//! Agent-side implementations of the frozen dispatch contract.
//!
//! This module owns code that *acts as* an external agent from the bridge's point
//! of view — today only the mock agent ([`mock`]). It sits beside
//! [`crate::bus`] (which owns submission, consumption and the event stream) and
//! is deliberately not part of it: a dispatcher is a *peer* of the worker, not a
//! consumer of the bus.
//!
//! # Mock only — do not wire this to a real target
//!
//! [`MockAgent`] is **production code so that consumers outside the crate can
//! assemble it** (T018's end-to-end acceptance, and the integration tests of
//! T014/T010/T011/T016), but it is a **test double, reference implementation and
//! acceptance placeholder** — never a production adapter:
//!
//! - it performs **no real I/O**: no subprocess, no network, no filesystem, no
//!   clock, no `tokio::time` wait, no environment or credential read;
//! - it never writes events, never touches the worker's state machine, and never
//!   records `seq` — it only answers the two dispatch stages;
//! - nothing in the crate wires it into a running path: `src/main.rs` and
//!   [`crate::run`] do not mention it, and T007 adds no fallback that could reach
//!   it implicitly. It can only be assembled by an explicit call.
//!
//! # Assembly (v1): which transport does a mock impersonate?
//!
//! [`TransportType`](crate::models::TransportType) is a frozen domain model with
//! exactly three variants (`Acp`/`Matrix`/`Http`) and no mock-specific value, and
//! its deserialization rejects unknown values. [`MockAgent`] therefore **does not
//! know about transports at all** — it implements
//! [`TaskDispatcher`](crate::bus::TaskDispatcher) and never looks at
//! `request.target.transport()`. The assembly point decides the key:
//!
//! - **Recommended v1 key:
//!   [`TransportType::Acp`](crate::models::TransportType::Acp)** — it is the only
//!   transport whose endpoints are addressable today (the registry derives an
//!   [`EndpointAddress`](crate::models::EndpointAddress) only for `acp`;
//!   `matrix`/`http` are registered but rejected with
//!   [`BusError::AddressUnavailable`](crate::bus::BusError::AddressUnavailable)
//!   before any dispatcher runs). Keying the mock on `acp` lets an end-to-end test
//!   use a realistic configuration and still reach the mock.
//! - **[`DispatcherRegistry::with`](crate::bus::DispatcherRegistry::with) replaces
//!   per transport** (frozen T005 semantics). Registering the mock under `acp`
//!   therefore *replaces* that worker's real `acp` dispatcher — an explicit
//!   choice, not an accident — while any other transport's dispatcher is
//!   unaffected and keeps working. T010/T014 must revisit the key once `matrix`
//!   and `http` become addressable.
//!
//! No public constant pins that choice: T010/T014 should be free to change it, and
//! a constant would freeze a v1 limitation into the API.
//!
//! # Sharing one observation table
//!
//! [`MockAgent`] is **not `Clone`** on purpose: a clone would either split the
//! recorded calls or need a second layer of sharing, and both make "why did my
//! assertion not see the call" a silent failure. Hold one `Arc<MockAgent>` and
//! clone *that*:
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use guigu_agent_bridge::agents::MockAgent;
//! # use guigu_agent_bridge::bus::{DispatcherRegistry, TaskDispatcher};
//! # use guigu_agent_bridge::models::TransportType;
//! let mock = Arc::new(MockAgent::completing("ok"));
//! let dispatcher: Arc<dyn TaskDispatcher> = mock.clone();
//! let registry = DispatcherRegistry::new().with(TransportType::Acp, dispatcher);
//! // ... assemble the worker with `registry`, then assert on `mock.calls()`.
//! assert_eq!(registry.len(), 1);
//! ```

pub mod mock;

pub use mock::{
    DEFAULT_OUTPUT, DeliverBehavior, ExecuteBehavior, MockAgent, MockCall, MockReply, MockRule,
    MockStage, ReplySource,
};
