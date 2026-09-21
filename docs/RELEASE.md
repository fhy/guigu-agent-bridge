# v0.1.0 Release Checklist

This document prepares replacement of `opencode-chat-bridge` by
`guigu-agent-bridge`. It does not authorize a push, tag, database migration, or
production service change.

## Candidate

- Build with Rust 1.88 from an exact reviewed commit.
- Verify `Cargo.toml` reports version `0.1.0` and `rust-version = "1.88"`.
- Run `cargo fmt --check`, `cargo check`, `cargo clippy -- -D warnings`, and
  `cargo test` before publishing the exact candidate.
- Keep credentials outside the repository. Resolve `{env:...}` values through the
  service environment or a secret manager.

## Pre-deployment

1. Record the old service unit, executable, arguments, environment, Matrix account,
   room bindings, database path, session root, health endpoint, and current revision.
2. Back up the SQLite database, including any WAL/SHM state by using SQLite's online
   backup mechanism or by stopping the old writer before copying all database files.
3. Copy `config.example.toml` to a protected deployment location and set explicit
   paths. Keep Matrix, gateway, and A2A disabled until their allowlists and peer
   identities are populated.
4. Validate ACP commands, workspaces, additional directories, and executable
   permissions under the service account. Pin compatible ACP backend versions.
5. Ensure `health_bind` remains loopback-only. Put authentication and TLS at a
   separate trusted edge if remote observation is required.

## Migration And Cutover

1. Stop `opencode-chat-bridge` and confirm no process still consumes the Matrix
   account. Never run both bridges against the same account or rooms.
2. Preserve the old executable, configuration, environment, and database backup.
3. Start `guigu-agent-bridge` with the reviewed candidate and protected config.
   Embedded migrations `0001` through `0010` run transactionally at startup.
4. Treat a migration checksum error, startup recovery backlog, or `/ready` HTTP 503
   as a failed cutover. Do not edit an applied migration or force readiness.
5. Wait for `GET /health` to respond and `GET /ready` to return HTTP 200 with
   `ready=true` before enabling external traffic.
6. Send one authorized Matrix request and verify its reply, task status, and trace.
   Verify an unauthorized sender is rejected. Exercise `/pause` only on disposable
   work during the rollout window.

## Monitoring

- Poll `/health` for owner state and `/ready` for traffic eligibility.
- Alert on recovery backlog, recovery-needed leases, expired leases, failed Matrix
  sync, ACP child exits, outbox backlog, queue-full replies, and repeated restarts.
- Confirm Matrix deduplication and gateway idempotency prevent duplicate task rows.
- Confirm A2A listeners remain private/loopback unless an explicitly reviewed private
  bind and peer policy is deployed. Public exposure is outside v0.1.0.
- Retain structured logs and the pre-cutover database backup through the observation
  window; do not log access tokens, task bodies, or ACP command arguments.

## Rollback

1. Stop `guigu-agent-bridge` and confirm all ACP children have exited.
2. Preserve the failed candidate database and logs for diagnosis.
3. Restore the pre-cutover database as a complete SQLite snapshot. Schema downgrade
   is not supported; do not point the old bridge at a database migrated by v0.1.0.
4. Restore the old executable, configuration, and environment, then start only
   `opencode-chat-bridge`.
5. Verify its Matrix sync cursor, health, routing, and a disposable request before
   reopening traffic.

Rollback loses work accepted after the backup. Operators must reconcile those task
IDs from the preserved candidate database before replaying requests.
