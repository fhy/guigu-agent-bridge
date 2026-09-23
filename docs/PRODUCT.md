# Product Overview

`guigu-agent-bridge` is a durable routing and control plane between Matrix users,
external Agent Client Protocol processes, and trusted A2A peers. SQLite is the task
and delivery fact source; in-memory queues provide bounded execution without becoming
a second source of truth.

## Capabilities

- Authenticate and route Matrix messages to configured agents.
- Persist tasks, events, deliveries, sessions, workflow envelopes, and busy queues.
- Run one fenced ACP executor per agent workspace with bounded continuation,
  cancellation, pause/resume, recovery, and graceful child-process shutdown.
- Observe status and traces through Matrix admin commands and loopback health endpoints.
- Interoperate with trusted private A2A HTTP peers and an independently configured
  A2A-over-Matrix gateway.

## Security And Reliability Boundary

All transports are disabled by default. Matrix identities, rooms, routes, A2A peers,
and targets require explicit configuration. Credentials come from environment
references and are redacted from debug output. A2A is intended for loopback or trusted
private networks; generic public relay deployment is outside v0.1.0.

The bridge does not provide models, prompts, Agent tools, a public Internet gateway,
or parallel ACP executors for one agent. Matrix/A2A transport acknowledgement is not a
task outcome. SQLite task events and fenced runtime leases remain authoritative.

## Compatibility

The package requires Rust 1.94 to build. ACP compatibility is negotiated at runtime
and backend identity is recorded; deployments should pin tested backend versions.
SQLite migrations are forward-only and applied at startup. See `RELEASE.md` before
replacing an existing bridge.
