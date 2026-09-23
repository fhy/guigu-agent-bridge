# Configuration Reference

Start from `config.example.toml`. Unknown or invalid values fail startup. Secret values
use `{env:VARIABLE}` references; do not store access tokens in the file.

## Bridge

- `database`: SQLite fact-store path. Parent directory must already exist.
- `session_root`: root for isolated ACP session workspaces.
- `max_task_depth`, `max_task_hops`: cycle and fan-out bounds.
- `default_timeout_seconds`: task execution deadline when no tighter deadline exists.
- `queue_capacity`, `event_capacity`: bounded internal channels.
- `shutdown_timeout_seconds`: graceful owner and child-process drain bound.
- `health_bind`: optional loopback health/readiness listener.

## Runtime

- `max_turns`, `max_wall_seconds`, `max_inactivity_seconds`: continuation limits.
- `max_no_progress`, `max_output_bytes`: runaway/no-progress limits.
- `lease_ttl_seconds`: fenced executor lease lifetime; renewal and recovery use the
  same persisted lease identity.

## Matrix

Set `enabled = true`, `homeserver`, `user_id`, `access_token`, and a dedicated
`crypto_store_path`. The store directory must be private (`0700` on Unix), must not
be a symlink, and must be backed up and restored as one SQLite generation. The bridge
always restores the stable `GUIGU_BRIDGE` device; set `device_trusted = true` only
after that device is verified out of band. Missing trust, rejected sessions, unsafe
permissions, corrupt stores, and undecryptable events fail closed. `allowed_users`
controls ordinary ingress. `admin_users` and optional `admin_rooms` control slash
commands. Routes map aliases and rooms to configured agent IDs. Empty allowlists do
not grant access.

## Agents

Each `[agents.<id>]` selects a transport, command, arguments, enabled state, and ACP
workspace. Commands run as the bridge service account. Use absolute executable paths
in production and grant only the required workspace directories.

## Gateway And A2A

The Matrix gateway has a separate room, peer identity, endpoint identities, sender
allowlist, payload bound, and deadline. It does not inherit ordinary Matrix trust.

Direct A2A is disabled by default. Non-loopback peers require HTTPS, an expected peer
identity, bearer token, and target allowlist. Prefer a private CA. The
`danger_accept_invalid_certs` switch removes peer authentication and must remain false
unless a separately reviewed temporary exception exists.

Configuration reload applies only documented hot fields. Changes to identity,
database, listeners, or process topology require a controlled restart.
