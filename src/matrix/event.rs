//! Bridge-owned Matrix event representation.

/// A validated inbound plain-text Matrix event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundMatrixEvent {
    /// Opaque Matrix event ID.
    pub event_id: String,
    /// Opaque Matrix room ID.
    pub room_id: String,
    /// Matrix thread root event ID, when this is an `m.thread` reply.
    pub thread_root: Option<String>,
    /// Opaque Matrix sender user ID. T011 resolves it to an endpoint.
    pub sender: String,
    /// Plain-text message body.
    pub body: String,
}

const MAX_ID_BYTES: usize = 1024;
const MAX_BODY_BYTES: usize = 64 * 1024;

pub(crate) fn decode_event(
    raw: &str,
    room_id: &str,
    own_user_id: &str,
) -> Result<Option<InboundMatrixEvent>, super::MatrixError> {
    if raw.len() > MAX_BODY_BYTES + 4 * MAX_ID_BYTES {
        return Err(super::MatrixError::Protocol {
            detail: "event is too large",
        });
    }
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|_| super::MatrixError::Protocol {
            detail: "malformed event JSON",
        })?;
    if value.get("state_key").is_some()
        || value
            .get("unsigned")
            .and_then(|unsigned| unsigned.get("redacted_because"))
            .is_some()
    {
        return Ok(None);
    }
    if value.get("type").and_then(|v| v.as_str()) != Some("m.room.message") {
        return Ok(None);
    }
    let content =
        value
            .get("content")
            .and_then(|v| v.as_object())
            .ok_or(super::MatrixError::Protocol {
                detail: "message content is missing",
            })?;
    if content.get("msgtype").and_then(|v| v.as_str()) != Some("m.text") {
        return Ok(None);
    }
    let event_id = required_bounded(&value, "event_id")?;
    let sender = required_bounded(&value, "sender")?;
    if sender == own_user_id {
        return Ok(None);
    }
    if room_id.is_empty() || room_id.len() > MAX_ID_BYTES {
        return Err(super::MatrixError::Protocol {
            detail: "invalid room ID",
        });
    }
    let body =
        content
            .get("body")
            .and_then(|v| v.as_str())
            .ok_or(super::MatrixError::Protocol {
                detail: "message body is missing",
            })?;
    if body.len() > MAX_BODY_BYTES {
        return Err(super::MatrixError::Protocol {
            detail: "message body is too large",
        });
    }
    let thread_root = match content.get("m.relates_to") {
        None => None,
        Some(relation) if relation.get("rel_type").and_then(|v| v.as_str()) == Some("m.thread") => {
            Some(required_bounded(relation, "event_id")?)
        }
        Some(_) => None,
    };
    Ok(Some(InboundMatrixEvent {
        event_id,
        room_id: room_id.to_owned(),
        thread_root,
        sender,
        body: body.to_owned(),
    }))
}

fn required_bounded(value: &serde_json::Value, field: &str) -> Result<String, super::MatrixError> {
    let text = value
        .get(field)
        .and_then(|v| v.as_str())
        .ok_or(super::MatrixError::Protocol {
            detail: "required event ID is missing",
        })?;
    if text.is_empty() || text.len() > MAX_ID_BYTES {
        return Err(super::MatrixError::Protocol {
            detail: "event identifier is invalid",
        });
    }
    Ok(text.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_room_and_thread_messages_map_without_sdk_types() {
        let room = decode_event(
            r#"{"type":"m.room.message","event_id":"$event:x","sender":"@alice:x","content":{"msgtype":"m.text","body":"hello"}}"#,
            "!room:x",
            "@bridge:x",
        )
        .unwrap()
        .unwrap();
        assert_eq!(room.thread_root, None);
        let thread = decode_event(
            r#"{"type":"m.room.message","event_id":"$reply:x","sender":"@alice:x","content":{"msgtype":"m.text","body":"hello","m.relates_to":{"rel_type":"m.thread","event_id":"$root:x","m.in_reply_to":{"event_id":"$other:x"}}}}"#,
            "!room:x",
            "@bridge:x",
        )
        .unwrap()
        .unwrap();
        assert_eq!(thread.thread_root.as_deref(), Some("$root:x"));
    }

    #[test]
    fn irrelevant_and_own_events_are_filtered() {
        for raw in [
            r#"{"type":"m.room.member","event_id":"$e:x","sender":"@alice:x","content":{}}"#,
            r#"{"type":"m.room.message","state_key":"topic","event_id":"$state:x","sender":"@alice:x","content":{"msgtype":"m.text","body":"state"}}"#,
            r#"{"type":"m.room.message","event_id":"$redacted:x","sender":"@alice:x","content":{},"unsigned":{"redacted_because":{"type":"m.room.redaction","event_id":"$redaction:x","sender":"@moderator:x","content":{}}}}"#,
            r#"{"type":"m.room.message","event_id":"$e:x","sender":"@bridge:x","content":{"msgtype":"m.text","body":"echo"}}"#,
            r#"{"type":"m.room.message","event_id":"$e:x","sender":"@alice:x","content":{"msgtype":"m.image","body":"image"}}"#,
        ] {
            assert_eq!(decode_event(raw, "!room:x", "@bridge:x").unwrap(), None);
        }
    }
}
