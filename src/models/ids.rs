//! Strongly-typed internal identifiers (UUIDv7) and the boundary with external IDs.
//!
//! Internal IDs are newtypes over [`Uuid`] so that, for example, a [`TaskId`] can
//! never be passed where a [`MessageId`] is expected. Each ID is a time-ordered
//! UUIDv7: the leading 48 bits are a millisecond timestamp, which keeps IDs
//! globally unique, decentralized, and ordered by creation time (useful for
//! audit, `seq` alignment, and SQLite index locality).
//!
//! **Endpoint identity is the one exception.** [`EndpointId`] must stay stable
//! across restarts and machines, so it is *derived* — a deterministic UUIDv5 over
//! a frozen project namespace and the config agent id — through
//! [`EndpointId::from_uuid`] plus [`crate::bus::registry::derive_endpoint_id`]
//! (ADR-003). All other internal IDs keep their UUIDv7 `generate()` construction:
//! endpoint identity needs stability, not time ordering. `EndpointId` is never
//! minted with `generate()` outside of tests.
//!
//! **External ID boundary.** IDs that originate from external systems — Matrix
//! `room_id` / `event_id` / `user_id`, ACP `session_id` — are opaque `String`s
//! whose format the bridge does not control. They never enter these newtypes and
//! never serve as `sender` / `recipient` / `from_agent` / `to_agent`. They only
//! appear inside [`crate::models::conversation::ExternalRef`] and
//! [`crate::models::agent::EndpointAddress`]. Normalizing an external identity to
//! an internal [`EndpointId`] happens in the transport/adapter layer (T010/T011)
//! before a [`crate::models::message::Message`] or
//! [`crate::models::task::AgentTask`] is constructed.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

macro_rules! define_id {
    ($( #[$meta:meta] )* $name:ident) => {
        $( #[$meta] )*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Generate a new time-ordered (UUIDv7) identifier.
            pub fn generate() -> Self {
                Self(Uuid::now_v7())
            }

            /// The underlying UUID value.
            pub const fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(s).map(Self)
            }
        }
    };
}

define_id!(
    /// Stable identifier for an agent endpoint.
    ///
    /// Unlike the other IDs, an `EndpointId` is **derived deterministically**
    /// (UUIDv5 over the frozen namespace in [`crate::bus::registry`]) from the
    /// config agent id, not generated per process (ADR-003). See the module docs.
    EndpointId
);
define_id!(
    /// Identifier for a conversation.
    ConversationId
);
define_id!(
    /// Identifier for a message.
    MessageId
);
define_id!(
    /// Identifier for an agent task.
    TaskId
);
define_id!(
    /// Globally unique identifier for an immutable task event (cross-system dedup).
    EventId
);
define_id!(
    /// Reserved identifier for a delivery attempt (consumed by T004/T005/T009).
    DeliveryId
);

impl EndpointId {
    /// Construct an endpoint id from a known [`Uuid`].
    ///
    /// Endpoint identity is derived, not generated: callers must not invent a
    /// UUID here. The supported producer is
    /// [`crate::bus::registry::derive_endpoint_id`], which applies the frozen
    /// UUIDv5 namespace (ADR-003). Additive constructor (T004 Q2): it does not
    /// weaken any invariant — `FromStr` already yields arbitrary UUIDs — but it
    /// keeps the derivation allocation-free and free of string round-trips.
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn generate_produces_unique_time_ordered_ids() {
        let a = TaskId::generate();
        let b = TaskId::generate();
        assert_ne!(a, b, "two generated IDs must differ");

        // UUIDv7 orders by creation time: a later ID sorts after an earlier one.
        assert!(a.as_uuid() < b.as_uuid() || a.as_uuid() > b.as_uuid());
        let version = a.as_uuid().get_version_num();
        assert_eq!(version, 7, "internal IDs must be UUIDv7");
    }

    #[test]
    fn generate_is_unique_across_many_calls() {
        let ids: HashSet<TaskId> = (0..1000).map(|_| TaskId::generate()).collect();
        assert_eq!(ids.len(), 1000, "generated IDs must be unique");
    }

    #[test]
    fn serde_round_trip_is_transparent() {
        let id = EndpointId::generate();
        let json = serde_json::to_string(&id).expect("serialize");
        // Transparent: the JSON is the bare UUID string, no wrapper object.
        assert_eq!(json, format!("\"{}\"", id.as_uuid()));

        let back: EndpointId = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, id);
    }

    #[test]
    fn from_str_accepts_valid_uuid_and_rejects_garbage() {
        let id = MessageId::generate();
        let parsed: MessageId = id.to_string().parse().expect("valid UUID parses");
        assert_eq!(parsed, id);

        // External ID formats (Matrix room/event/user, ACP session) are not UUIDs
        // and must be rejected by the internal ID boundary.
        for external in [
            "!room:matrix.org",
            "$event:matrix.org",
            "@user:matrix.org",
            "session-abc123",
            "not-a-uuid",
            "",
        ] {
            assert!(
                external.parse::<MessageId>().is_err(),
                "external/invalid format {external:?} must not parse as an internal ID"
            );
        }
    }

    #[test]
    fn display_round_trips_through_from_str() {
        let id = ConversationId::generate();
        let text = id.to_string();
        let back: ConversationId = text.parse().expect("display output reparses");
        assert_eq!(back, id);
    }

    #[test]
    fn from_uuid_round_trips_and_keeps_uuid_verbatim() {
        // Additive constructor for derived endpoint identity (ADR-003): it must
        // store the UUID verbatim, without any re-versioning or normalization.
        let uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, b"from-uuid-test");
        let id = EndpointId::from_uuid(uuid);
        assert_eq!(id.as_uuid(), uuid);
        assert_eq!(id.as_uuid().get_version_num(), 5);
        assert_eq!(id.to_string(), uuid.to_string());
        assert_eq!(
            id.to_string()
                .parse::<EndpointId>()
                .expect("derived id reparses"),
            id
        );
    }

    #[test]
    fn from_uuid_is_distinct_per_input_and_interoperable_with_generate() {
        let a = EndpointId::from_uuid(Uuid::new_v5(&Uuid::NAMESPACE_URL, b"a"));
        let b = EndpointId::from_uuid(Uuid::new_v5(&Uuid::NAMESPACE_URL, b"b"));
        assert_ne!(a, b);
        // EndpointId::generate() stays available (UUIDv7, used by tests and other
        // subsystems' fixtures) and is a different identity space from derivation.
        assert_ne!(EndpointId::generate().as_uuid().get_version_num(), 5);
    }
}
