//! A2A v0.3 polling adapter for explicitly configured trusted-LAN peers.

pub(crate) mod assembly;
mod cleanup;
mod client;
mod content;
mod dispatcher;
mod error;
mod identity;
mod ingress;
mod mapping;
mod policy;
pub mod server;
mod store;
pub mod wire;

pub use cleanup::{CleanupHandle, CleanupPolicy, start_cleanup};
pub use client::{A2aClient, ClientPeer, TrustPolicy};
pub use dispatcher::{A2aDispatcher, A2aDispatcherRouter};
pub use error::A2aError;
pub use identity::derive_peer_endpoint_id;
pub use ingress::A2aIngress;
pub use mapping::{internal_status, outbound_outcome};
pub use policy::{ListenPolicy, validate_listener, validate_peer_url};
pub use server::{A2aServer, A2aServerConfig, A2aServerHandle, PeerAccess};
pub use store::{A2aStore, CleanupClaim, ExchangeRecord, InboundReservation};
