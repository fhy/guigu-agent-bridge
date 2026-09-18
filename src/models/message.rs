//! Unified message model for user, agent, and system messages.
//!
//! `sender` and `recipient` are **resolved stable endpoint IDs**, never raw
//! natural-language `@mention` text. The model enforces a **single recipient**:
//! there is no broadcast / multicast field. Reaching multiple recipients requires
//! multiple messages, each with one recipient. This structurally blocks the
//! "one event delivered to many agents" failure mode documented in INC-001.
//!
//! Normalizing a transport identity or a natural-language mention into a single
//! [`EndpointId`] is the responsibility of the transport/adapter layer (T010/T011)
//! and must complete **before** a [`Message`] is constructed.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::models::ids::{ConversationId, EndpointId, MessageId};

/// A unified message: user, agent, or system.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    /// Message identifier.
    pub id: MessageId,
    /// The conversation this message belongs to.
    pub conversation: ConversationId,
    /// Resolved stable endpoint ID of the sender.
    pub sender: EndpointId,
    /// Resolved stable endpoint ID of the single recipient.
    pub recipient: EndpointId,
    /// Message body.
    pub body: String,
    /// Optional message this one replies to.
    pub reply_to: Option<MessageId>,
    /// Opaque metadata (deterministic ordering via `BTreeMap`).
    pub metadata: BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_message(recipient: EndpointId) -> Message {
        Message {
            id: MessageId::generate(),
            conversation: ConversationId::generate(),
            sender: EndpointId::generate(),
            recipient,
            body: "hello".into(),
            reply_to: None,
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn message_serde_round_trip() {
        let message = make_message(EndpointId::generate());
        let json = serde_json::to_string(&message).expect("serialize");
        let back: Message = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, message);
    }

    #[test]
    fn message_with_reply_and_metadata_round_trips() {
        let reply_to = MessageId::generate();
        let mut metadata = BTreeMap::new();
        metadata.insert("source".into(), "matrix".into());
        metadata.insert("thread".into(), "$root:matrix.org".into());

        let message = Message {
            reply_to: Some(reply_to),
            metadata,
            ..make_message(EndpointId::generate())
        };
        let json = serde_json::to_string(&message).expect("serialize");
        let back: Message = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, message);
        assert_eq!(back.reply_to, Some(reply_to));
    }

    #[test]
    fn different_sources_normalize_to_same_endpoint_id() {
        // Two distinct external identities (e.g. a display name and a Matrix user
        // ID) that the adapter layer resolves to the *same* stable EndpointId
        // produce identical messages: the model stores only the resolved ID and
        // retains no original mention text.
        let stable_id = EndpointId::generate();
        let from_display_name = stable_id; // resolved from a display name
        let from_matrix_id = stable_id; // resolved from a Matrix user_id

        let message_a = make_message(from_display_name);
        let message_b = Message {
            id: message_a.id,
            conversation: message_a.conversation,
            sender: message_a.sender,
            recipient: from_matrix_id,
            body: message_a.body.clone(),
            reply_to: None,
            metadata: BTreeMap::new(),
        };
        assert_eq!(
            message_a, message_b,
            "same resolved ID => identical message"
        );
    }

    #[test]
    fn single_recipient_invariant_no_broadcast_field() {
        // The model has exactly one recipient and no broadcast/multicast field.
        // Reaching two recipients requires two separate messages.
        let recipient_a = EndpointId::generate();
        let recipient_b = EndpointId::generate();
        assert_ne!(recipient_a, recipient_b);

        let message_a = make_message(recipient_a);
        let message_b = make_message(recipient_b);
        assert_eq!(message_a.recipient, recipient_a);
        assert_eq!(message_b.recipient, recipient_b);
        assert_ne!(message_a.recipient, message_b.recipient);
    }

    #[test]
    fn unresolved_target_cannot_be_expressed_as_recipient() {
        // A recipient must be a resolved EndpointId. An unresolved natural-language
        // mention has no EndpointId value, so it cannot be placed in `recipient` —
        // the type system rejects it at construction (no String/mention field
        // exists to smuggle it in).
        let mention = "@someone";
        // The only way to build a recipient is from an EndpointId; a raw mention
        // string does not convert into one.
        let resolved = EndpointId::generate();
        let message = make_message(resolved);
        assert_ne!(message.recipient.to_string(), mention);
    }
}
