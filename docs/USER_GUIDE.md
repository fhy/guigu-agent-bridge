# User Guide

## Install And Configure

Install the versioned binary with Cargo, place `config.example.toml` in a protected
location, and supply secrets through environment variables. Enable only the transports
and agents needed by the deployment. The process accepts the configuration path as its
first argument.

Before production startup, create the database parent directory, verify the service
account can write the database and session root, and verify every ACP command and
workspace is executable and accessible.

## Matrix Use

An allowed Matrix user sends ordinary text in a routed room or direct room. The bridge
deduplicates event IDs, persists accepted work, and replies in the originating context.
When an agent is busy, bounded work is queued rather than starting another executor.

Authorized administrators can use:

```text
/status <task-id>
/trace <task-id>
/cancel <task-id>
/pause <task-id>
/resume <task-id>
/retry <task-id>
```

Admin commands require both an allowed user and, when configured, an allowed admin
room. A pause reports `paused` only after ACP cancellation, process reap, lease release,
and a matching durable proof. Ambiguous work becomes `recovery_needed`.

## Operations

Use `GET /health` for diagnostics and `GET /ready` for traffic eligibility. The health
listener must remain loopback-only. A 503 readiness response, recovery backlog, expired
lease, repeated child exit, or queue-full response requires investigation before
routing more work.

Stop the process with SIGINT or SIGTERM and allow the configured shutdown timeout for
queue drain and ACP child reap. Deployment and rollback procedures are in `RELEASE.md`.
