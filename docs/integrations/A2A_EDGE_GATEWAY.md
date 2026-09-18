# A2A Relay / Edge Gateway Integration Contract

## Status

- Kind: external-project candidate, not a `guigu-agent-bridge` task
- Suggested project name: `a2a-edge-gateway`
- Architecture: central Relay/Hub with outbound Bridge connections; Nginx is the public security edge, not the message broker
- Start condition: deployments need transport-neutral cross-network communication beyond T020, especially when Bridges cannot accept inbound connections
- Current alternative: T020 A2A-over-Matrix Gateway, with Redis Streams optional

## Topology

```text
Bridge A -- outbound persistent connection --\
Bridge B -- outbound persistent connection ---- A2A Relay/Hub <-- Nginx security edge
Bridge C -- outbound persistent connection --/
```

Every Bridge initiates its connection. A Bridge therefore needs outbound reachability but no public address, inbound port, NAT traversal, or automatic port forwarding. Direct trusted-LAN A2A from T019 and A2A-over-Matrix from T020 remain valid alternatives.

The Relay/Hub is a delivery service, not the authoritative task store. A successful relay acknowledgement means only that the Relay accepted or delivered an envelope; it never means that the target Bridge accepted, started, or completed the task.

## Ownership Boundary

The external gateway project owns relay transport and public deployment:

- Authenticated Bridge registration, stable endpoint routing, capability advertisement, and connection replacement rules.
- Outbound WebSocket or HTTP streaming connections, heartbeat, reconnect backoff, bounded offline delivery, delivery acknowledgement, expiry, and dead-letter handling.
- Per-endpoint ordering guarantees, duplicate suppression, envelope version negotiation, backpressure, and connection/session limits.
- Relay-side durable queues where enabled, including retention, quota, restart recovery, and explicit loss semantics.
- Prevention of ambiguous routing, fan-out by accident, endpoint takeover, stale-connection delivery, and cross-tenant disclosure.

- Nginx configuration, packaging, version pinning, upgrade compatibility, and safe reload.
- Public HTTPS/TLS, optional mTLS, certificate/key rotation, and cipher/protocol policy.
- `auth_request` integration with OAuth/OIDC/API-key validation services.
- Connection/request rate limits, body limits, timeouts, IP/network policy, WAF integration, and access logs.
- Container/systemd/Kubernetes deployment, monitoring, incident response, rollout, and rollback.
- Adversarial gateway tests and independent security review.

`guigu-agent-bridge` owns the Relay client adapter and application semantics:

- An outbound-only Relay client with explicit endpoint identity, reconnect, resume cursor, acknowledgement, and clean shutdown behavior.
- Translation between the versioned relay envelope and Bridge-owned A2A/task models without leaking Relay SDK types into core models.
- Endpoint- and capability-level authorization.
- Task identity, idempotency, replay protection, state-machine validation, deadlines, execution leases, and artifact policy.
- Audit correlation IDs and secret/payload redaction.
- Rejection of envelopes whose authenticated transport identity, claimed sender, recipient, capability, tenant, or replay context disagree.

Nginx authentication success is not AgentTask authorization.

## Required Relay Contract

- Each connection authenticates one configured Bridge endpoint; reconnecting cannot silently create a second active writer for that endpoint.
- Every envelope carries a protocol version, message ID, sender and recipient endpoint IDs, correlation/idempotency key, expiry/deadline, content type, bounded payload or artifact reference, and integrity context.
- Routing resolves to zero or one endpoint unless an explicitly authorized multicast operation is introduced later. Unknown or ambiguous recipients fail closed.
- Relay receipt, target delivery, target task acceptance, execution, and completion are separate acknowledgements/events.
- Offline retention is bounded by time and bytes. Expired tasks are never delivered as new work after reconnect.
- Resume cursors and acknowledgements survive reconnect without converting at-least-once delivery into duplicate execution; Bridge remains responsible for task-level idempotency.
- The protocol defines heartbeat timeout, half-open connection cleanup, endpoint takeover policy, backpressure, cancellation races, and graceful shutdown.

## Required Nginx Boundary

- The gateway strips client-supplied forwarding, identity, tenant, and authorization-result headers before adding normalized authenticated claims.
- Relay accepts normalized proxy claims only from the configured loopback/Unix-socket boundary and validates required claim fields and correlation IDs.
- Public traffic cannot connect directly to the Relay application port or any Bridge A2A listener.
- Health/readiness endpoints disclose no Agent inventory, credentials, task content, or private topology.
- Request and artifact limits, timeouts, cancellation behavior, streaming behavior, and error mappings are versioned and tested across Nginx, Relay, and Bridge.

## Implementation Preference

Prefer standard Nginx directives plus `auth_request`. A native Nginx module is not the default because it adds memory-safety, ABI, packaging, and upgrade risk. `njs`, Lua, or a native module requires a documented missing capability, version pinning, minimized scope, applicable fuzzing, and independent review.

## Bridge Acceptance Before Relay Launch

- Relay client integration is a future Bridge task created when the external project starts; it is not silently added to T019 or T020.
- The outbound Relay client, envelope mapping, reconnect/resume, bounded buffering, and shutdown paths are implemented and tested in the Bridge project.
- Spoofed endpoint/tenant claims, ambiguous recipients, stale sessions, replay, and connection replacement races are rejected.
- Task authorization is tested independently from Relay/Nginx authentication.
- Duplicate delivery, offline expiry, reconnect, resource exhaustion, cross-tenant, artifact, cancellation, and state-regression tests cover the real Relay-to-Bridge contract.
- The gateway project records the exact compatible Bridge version and integration-contract version.
