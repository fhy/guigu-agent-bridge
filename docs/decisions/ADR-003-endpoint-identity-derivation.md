# ADR-003: Endpoint Identity Derivation (Deterministic UUIDv5)

## Status

Accepted (T004 design gate).

## Context

`AgentTask.from_agent` / `AgentTask.to_agent` and `Message.sender` / `Message.recipient`
carry a resolved `EndpointId`, never a natural-language name or an external ID
(INC-001). The runtime source of an endpoint is `Config::agents`, keyed by an
agent-id string (`^[A-Za-z0-9_-]+$`, validated in T003). A stable mapping from
that agent-id to an `EndpointId` is therefore a cross-module contract consumed by:

- T004 — builds the endpoint registry from config.
- T008/T009 — persist endpoints and tasks keyed by `EndpointId` and must rebuild
  idempotently after restart.
- T010/T011 — resolve an external Matrix identity (possibly several display names)
  to the same single `EndpointId`.
- T014 — build the ACP adapter's runtime `AgentEndpoint` from config.

The identity must be stable across restarts and across machines, and must not
require a database read before the bridge can construct a valid registry (a DB
read would invert T004's dependency on T009).

## Decision

Derive `EndpointId` **deterministically** from the agent id using UUIDv5 over a
frozen project namespace:

```rust
fn endpoint_namespace() -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_URL, b"guigu-agent-bridge/agent/v1")
}

pub fn derive_endpoint_id(agent_id: &str) -> EndpointId {
    EndpointId::from_uuid(Uuid::new_v5(&endpoint_namespace(), agent_id.as_bytes()))
}
```

- The namespace string `"guigu-agent-bridge/agent/v1"` is **frozen once released**;
  it must never change, and must have a single source of truth (re-exported from
  `crate::bus::registry`), never duplicated.
- Renaming an agent id changes its `EndpointId` (equivalent to a new identity).
  This is intentional and must be documented for operators.
- `EndpointId` is constructed via a new additive constructor
  `EndpointId::from_uuid(Uuid) -> EndpointId` (T004 Q2). Task/message/event/delivery/
  conversation IDs keep their UUIDv7 `generate()` construction; only `EndpointId`
  is derived this way because endpoint identity needs cross-restart stability,
  not time-ordering.

Rejected alternative: a fresh UUIDv7 per startup persisted to the database. That
would make identity exist only in the DB, require T004 to depend on T009 to build
a stable registry, and leave persisted tasks with dangling `to_agent` ids if the
DB were lost or migrations had not run.

## Consequences

- Positive: the registry is a pure function of config and rebuilds idempotently
  across restarts; T008/T009 can key endpoint rows by a deterministic id; T010/T011
  resolve external identities to the same `EndpointId` without a DB lookup.
- Cost: `uuid` gains the `v5` feature, which pulls in `sha1` transitively. This is
  a namespaced-ID hash (RFC 4122), not a security primitive; no cryptographic
  guarantee is being relied on.
- The `EndpointId` and `ids` module doc comments must be updated to note that
  endpoint IDs use deterministic UUIDv5 derivation while the other internal IDs
  remain UUIDv7.
- T008/T009 must reuse the same `derive_endpoint_id` (via the shared constant) and
  never mint a different id for the same agent id.
