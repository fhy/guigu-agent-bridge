# ADR-005: Keep A2A at the Bridge Interoperability Boundary

- Status: accepted
- Date: 2026-09-17
- Decision owners: user and project architecture

## Context

The internal Agent Bus needs deterministic routing, durable task state, retries, cancellation, cycle prevention, execution leases, and recovery. A2A provides interoperability between independently deployed Agent systems, while ACP provides the process/session protocol used to drive concrete Agent runtimes.

`guigu` is a Rust Agent runtime. It already exposes an embeddable `Agent` API, a custom remote protocol, a multi-session server, ACP over stdio, and ACP over SSE/HTTP. It does not own cross-Agent task DAGs, Bridge authorization, delivery acknowledgement, or the authoritative task event store.

## Decision

1. `AgentTask` and Agent Bus remain the internal scheduling protocol and source of runtime task semantics.
2. T019 implements a minimal A2A Server and Client for explicitly trusted private-LAN communication. The listener is disabled by default, requires explicit configuration, serves a bounded capability document, and rejects wildcard/public binds under the T019 profile.
3. T020 implements a project-defined A2A-over-third-party gateway for cross-LAN communication. Matrix is required first; Redis Streams is an optional backend for deployments that already operate it. This profile preserves A2A semantics but is not represented as a standard A2A wire transport. Third-party transport state is not the task fact source.
4. Generic cross-network relay and public A2A HTTPS are outside this repository. If deployments need a transport-neutral path for Bridges without inbound reachability, create a separate `a2a-edge-gateway` Relay/Hub governed by `docs/integrations/A2A_EDGE_GATEWAY.md`. Bridges connect outbound; the Relay owns connection routing and bounded offline delivery, while Nginx owns the public security edge. Bridge owns its Relay client plus task authorization, idempotency, replay, and state semantics.
5. A2A wire and SDK types do not enter the internal Bus, Storage, or core model APIs. The adapter maps external identities, tasks, messages, artifacts, states, and cancellation to Bridge-owned types and persists the mapping.
6. `guigu` continues to connect as an ACP execution backend. It does not add native A2A support now.
7. Native A2A in `guigu` may be reconsidered only if guigu must be deployed as a standalone A2A endpoint without Bridge. That would be an optional protocol adapter, not a replacement for its core `Agent` API or ACP support.

## Consequences

- Bridge owns federation security, authorization, idempotency, task recovery, cycle limits, and execution leases in one place.
- guigu remains a focused runtime and does not duplicate orchestration or protocol-security code.
- Other Agent runtimes can participate through ACP or future adapters without implementing A2A themselves.
- A simple inbound A2A path is available inside a trusted LAN without exposing a new public service. This trust assumption is explicit and is not a security guarantee against malicious local peers.
- T020 avoids direct A2A exposure for remote communication, but Matrix membership or Redis access is not treated as sufficient authorization and does not provide task execution idempotency by itself.
- Redis remains optional so that it does not become infrastructure required by all deployments.
- Public or generic Relay deployment is not a Bridge delivery milestone. Direct Bridge exposure remains prohibited; remote Bridges connect outbound to the external Relay or use T020 Matrix transport.
- Nginx authentication is not treated as task authorization. Client-supplied identity headers are discarded, and Bridge trusts normalized claims only from the configured proxy boundary.
- T019 must resolve the additive internal transport/capability designation (`TransportType::A2a` or a versioned extensible alternative) through a public-contract design gate; it must not leak A2A wire models into the domain layer.
