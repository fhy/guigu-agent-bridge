//! Static first-refusal diagnostics; never log arbitrary error or event fields.
use crate::config::Config;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::SqliteConnection;

pub(crate) struct Position {
    stage: &'static str,
    ordinal: usize,
    count: usize,
}
impl Position {
    pub fn target(ordinal: usize, count: usize) -> Self {
        Self {
            stage: "target",
            ordinal,
            count,
        }
    }
    pub fn page(ordinal: usize, count: usize) -> Self {
        Self {
            stage: "page",
            ordinal,
            count,
        }
    }
    pub fn publication(ordinal: usize, count: usize) -> Self {
        Self {
            stage: "final",
            ordinal,
            count,
        }
    }
}

pub(crate) fn predicate(error: &sqlx::Error) -> &'static str {
    let sqlx::Error::Protocol(reason) = error else {
        return "storage-error";
    };
    match reason.strip_prefix("native-succession/") {
        Some("event-id") => "event-id",
        Some("durable-event-intent") => "durable-event-intent",
        Some("event-malformed") => "event-malformed",
        Some("event-unknown") => "event-unknown",
        Some("admin-intent") => "admin-intent",
        Some("route-config") => "route-config",
        Some("route-admission-intent") => "route-admission-intent",
        Some("permission-reply-intent") => "permission-reply-intent",
        Some("unavailable-reply-intent") => "unavailable-reply-intent",
        Some("route-target-invalid") => "route-target-invalid",
        Some("route-ambiguous") => "route-ambiguous",
        Some("route-identity-invalid") => "route-identity-invalid",
        _ => "route-unexpected",
    }
}

fn correlation(domain: &str, value: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(domain.as_bytes());
    digest.update([0]);
    digest.update(value);
    format!("{:x}", digest.finalize())
}

fn report(error: &sqlx::Error, room: &str, value: &Value, position: Position) {
    let id = value.get("event_id").and_then(Value::as_str).unwrap_or("");
    tracing::warn!(
        stage = position.stage,
        predicate = predicate(error),
        ordinal = position.ordinal.min(super::coverage::MAX_EVENTS),
        count = position.count.min(super::coverage::MAX_EVENTS),
        event_digest = %correlation("event", id.as_bytes()),
        room_digest = %correlation("room", room.as_bytes()),
        payload_digest = %correlation("payload", value.to_string().as_bytes()),
        "coverage intent refused"
    );
}

/// Uses the caller's transaction. The original static error survives rollback.
pub(crate) async fn check(
    c: &mut SqliteConnection,
    config: &Config,
    room: &str,
    value: &Value,
    position: Position,
) -> Result<(), sqlx::Error> {
    let result = super::ingress_decision::no_effect(c, config, room, value).await;
    if let Err(error) = &result {
        report(error, room, value, position);
    }
    result
}

#[cfg(test)]
pub(super) fn report_for_test(error: &sqlx::Error, room: &str, value: &Value, position: Position) {
    report(error, room, value, position);
}
