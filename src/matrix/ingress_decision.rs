//! Shared side-effect decisions. Recovery never calls the mutating ingress adapter.
use super::{InboundMatrixEvent, PermissionPolicy, RouteError, RoutePolicy};
use crate::{bus::EndpointRegistry, config::Config, models::EndpointId};
use serde_json::Value;
use sqlx::SqliteConnection;

pub(crate) struct Selection {
    pub target: EndpointId,
    pub sender: EndpointId,
    pub text: String,
}

pub(crate) fn is_admin(event: &InboundMatrixEvent) -> bool {
    event
        .body
        .split_ascii_whitespace()
        .next()
        .is_some_and(|t| t.starts_with('/'))
}

pub(crate) fn select(
    event: &InboundMatrixEvent,
    policy: &RoutePolicy,
    permissions: &PermissionPolicy,
    registry: &EndpointRegistry,
) -> Result<Selection, RouteError> {
    let (agent, text) = super::router::select_target(event, policy)?;
    let target = registry.resolve_agent_id(agent).ok_or(RouteError::Target)?;
    registry
        .validate_target(target)
        .map_err(|_| RouteError::Target)?;
    let sender = super::derive_matrix_user_id(&event.sender).map_err(|_| RouteError::Identity)?;
    if !permissions.permits(&event.sender, target) {
        return Err(RouteError::Forbidden);
    }
    Ok(Selection {
        target,
        sender,
        text,
    })
}

pub(crate) fn unavailable_reply(
    event: &InboundMatrixEvent,
    route: &RoutePolicy,
    permissions: &PermissionPolicy,
    monitor: Option<&str>,
) -> bool {
    permissions.permits_unrouted(&event.sender)
        && (route.knows_room(&event.room_id) || monitor == Some(event.room_id.as_str()))
}

/// All durable reads deliberately use this connection, including at final commit.
pub(crate) async fn no_effect(
    c: &mut SqliteConnection,
    config: &Config,
    room: &str,
    value: &Value,
) -> Result<(), sqlx::Error> {
    let refuse = crate::native_succession::state::refuse;
    let id = value
        .get("event_id")
        .and_then(Value::as_str)
        .ok_or_else(|| refuse("event-id"))?;
    let intent: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM transport_receipts WHERE external_event_id=? OR reply_event_id=?)+(SELECT count(*) FROM projection_outbox WHERE reply_event_id=?)")
        .bind(id).bind(id).bind(id).fetch_one(&mut *c).await?;
    if intent != 0 {
        return Err(refuse("durable-event-intent"));
    }
    if super::succession_payload::ignored(value, room, &config.transports.matrix.user_id)
        .map_err(|_| refuse("event-malformed"))?
    {
        return Ok(());
    }
    let event =
        super::event::decode_event(&value.to_string(), room, &config.transports.matrix.user_id)
            .map_err(|_| refuse("event-malformed"))?
            .ok_or_else(|| refuse("event-unknown"))?;
    if is_admin(&event) {
        return Err(refuse("admin-intent"));
    }
    let registry = EndpointRegistry::from_config(config);
    let hot =
        crate::app::HotConfig::build(config, &registry).map_err(|_| refuse("route-config"))?;
    match select(&event, &hot.route, &hot.permissions, &registry) {
        Err(RouteError::NoRoute)
            if !unavailable_reply(
                &event,
                &hot.route,
                &hot.permissions,
                hot.monitor_room.as_deref(),
            ) =>
        {
            Ok(())
        }
        Ok(_) => Err(refuse("route-admission-intent")),
        Err(RouteError::Forbidden) => Err(refuse("permission-reply-intent")),
        Err(RouteError::NoRoute) => Err(refuse("unavailable-reply-intent")),
        Err(RouteError::Target) => Err(refuse("route-target-invalid")),
        Err(RouteError::Ambiguous) => Err(refuse("route-ambiguous")),
        Err(RouteError::Identity) => Err(refuse("route-identity-invalid")),
        Err(_) => Err(refuse("route-unexpected")),
    }
}
