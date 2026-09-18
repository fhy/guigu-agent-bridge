//! Conversation model: a persistent context shared by users and agents.
//!
//! A [`Conversation`] is modeled separately from tasks and messages but can be
//! linked to an external room / thread / session via [`ExternalRef`].

use serde::{Deserialize, Serialize};

use crate::models::agent::TransportType;
use crate::models::ids::{ConversationId, EndpointId};

/// A link from a [`Conversation`] to an external room / thread / session.
///
/// `external_id` and `thread_ref` are **opaque external identifiers** whose format
/// is owned by the referenced transport (e.g. a Matrix `room_id` and a thread root
/// `event_id`, or an ACP `session_id`). They are never parsed into internal ID
/// types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalRef {
    /// The transport this external reference belongs to.
    pub transport: TransportType,
    /// Opaque external identifier (e.g. Matrix `room_id`, ACP `session_id`).
    pub external_id: String,
    /// Optional opaque thread reference (e.g. Matrix thread root `event_id`).
    pub thread_ref: Option<String>,
}

/// A persistent context shared by users and agents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Conversation {
    /// Conversation identifier.
    pub id: ConversationId,
    /// Stable endpoint IDs of the participants.
    pub participants: Vec<EndpointId>,
    /// Optional link to an external room / thread / session.
    pub external_ref: Option<ExternalRef>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_conversation() -> Conversation {
        Conversation {
            id: ConversationId::generate(),
            participants: vec![EndpointId::generate(), EndpointId::generate()],
            external_ref: Some(ExternalRef {
                transport: TransportType::Matrix,
                external_id: "!room:matrix.org".into(),
                thread_ref: Some("$root-event:matrix.org".into()),
            }),
        }
    }

    #[test]
    fn conversation_serde_round_trip() {
        let conversation = sample_conversation();
        let json = serde_json::to_string(&conversation).expect("serialize");
        let back: Conversation = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, conversation);
    }

    #[test]
    fn conversation_without_external_ref_round_trips() {
        let mut conversation = sample_conversation();
        conversation.external_ref = None;
        let json = serde_json::to_string(&conversation).expect("serialize");
        let back: Conversation = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, conversation);
        assert!(back.external_ref.is_none());
    }

    #[test]
    fn external_ref_holds_opaque_external_ids() {
        // Matrix room_id / event_id are opaque strings; the model stores them
        // verbatim and never coerces them into internal ID types.
        let external_ref = ExternalRef {
            transport: TransportType::Matrix,
            external_id: "!abc123:matrix.org".into(),
            thread_ref: Some("$root456:matrix.org".into()),
        };
        let json = serde_json::to_string(&external_ref).expect("serialize");
        let back: ExternalRef = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, external_ref);
        assert_eq!(back.external_id, "!abc123:matrix.org");
        assert_eq!(back.thread_ref.as_deref(), Some("$root456:matrix.org"));
    }

    #[test]
    fn participants_are_stable_endpoint_ids() {
        // Participants reference resolved stable endpoint IDs, never raw external
        // or natural-language identifiers.
        let endpoint = EndpointId::generate();
        let conversation = Conversation {
            id: ConversationId::generate(),
            participants: vec![endpoint, endpoint],
            external_ref: None,
        };
        assert!(conversation.participants.iter().all(|p| *p == endpoint));
    }
}
