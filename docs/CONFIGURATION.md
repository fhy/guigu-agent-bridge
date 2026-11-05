# Configuration Reference

## Owner-evidence freshness

`bridge.owner_evidence_max_age_seconds` is optional (default 30) and must be
between 1 and 300 seconds inclusive. Existing configurations remain valid.
The window controls freshness, not containment: no configuration value enables
modeled positive owner qualification or supplies a trusted execution domain.

For a standard single-turn ACP agent without the bridge's structured `taskResult`
extension, opt into `[runtime] legacy_acp_results = true`. This treats successful
`end_turn` as terminal and returns streamed output; refusal and other stop reasons
remain failures. The default is false (strict structured results). Do not combine
it with `allow_nonterminal_end_turn`, which deliberately continues rather than
completing such turns.

For ordinary Codex ACP, set this option explicitly before the first task; a
connected Matrix listener does not prove that the Agent can complete a turn.
Verify an encrypted request and its successful Agent reply, then repeat after a
same-device/store restart. Do not reset the store to work around admission errors.

## Retaining an unresolved dispatch

`hold-unresolved-admission --database DB --config CONFIG --task TASK --owner OWNER
--revision REVISION` is an offline, exact-scope recovery operation for a stopped,
non-administrative deployment. It changes only a matching `dispatching` admission
to `recovery_needed`, preserving the task and its queued/dispatched events. It
records an exact owner/revision-bound administrative hold receipt atomically;
an unrelated `recovery_needed` row does not gain this no-replay exemption. It
does not assert failure, nonacceptance or absence of effects and never replays it.
It refuses delivery, execution lease, continuation, workspace, queue or session
binding evidence for that task, a live durable runtime, stale revision or owner,
and administrative bindings. Preserve a consistent backup before using it.
Other unresolved execution/delivery evidence still blocks startup normally.

## Diagnosing chat and restart failures

Default INFO includes the first fatal category, task/delivery correlation where
available, secondary finalization failure and required-worker health degradation.
Do not treat an active PID or HTTP readiness alone as proof of a working Agent.
For one bounded reproduction, use `RUST_LOG=info,guigu_agent_bridge=debug` with the
existing launcher; inspect that instance's journal with
`journalctl --user -u UNIT --since 'YYYY-MM-DD HH:MM:SS' --no-pager`.
Return to the ordinary filter after capturing the incident. Do not enable global
TRACE, print environment files or publish raw logs containing private identifiers.

| Stage | Safe diagnostic distinction |
|---|---|
| initial-baseline / baseline-publication | No checkpoint versus durable active checkpoint; limited-room count; history executed=0; response acceptance is not a committed baseline. |
| incremental-sync | Actual checkpoint gap refuses without advancement; first transport retry warns, repeats count at DEBUG, bounded recovery reports its count. |
| acp-delivery / acp-session | Effective legacy/strict completion settings, delivery attempt and new/resume choice; session identifiers, prompts and remote payloads are not logged here. |
| acp-execution | Missing structured result in strict mode is distinct from transport, schema and agent refusal; explicit legacy mode is not an automatic fallback. |
| lease-acquisition / lease-reassignment | Pre-state/fence, authenticated released-parent reassignment and committed versus attempted transaction. |
| task-worker | Delivery and secondary-finalization categories remain distinct; a required owner exit degrades health without automatic respawn. |
| admission-recovery / hold-* | Exact state/revision/owner-state refusal, offline input/exclusion/classification/transaction stages and no-replay hold result. |

Correlate the first failure before interpreting a later channel-closed or startup
refusal. Reproduce a historically lost error only on a controlled equivalent;
never claim diagnostics reconstructed an error that was not preserved. An
unknown task stays unknown until a supported exact-scope disposition succeeds.

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

Set `enabled = true`, `homeserver`, `user_id`, the access token's exact non-empty
`device_id`, `access_token`, and a dedicated `crypto_store_path`. Startup verifies the
authenticated user and device with Matrix before opening the store. The store directory
and its ancestors must be private and must not be symlinks; on Unix the directory is
`0700` and SQLite, WAL, SHM, and identity-manifest files are `0600`. The bridge binds
the store to the normalized homeserver, user, and device and rejects an absent, corrupt,
or mismatched binding instead of adopting it.

Set `device_trusted = true` only as local operator permission to use this transport.
It does not mean Matrix owner or cross-signing verification. Clients may show an
unverified-device warning; this is accepted while owner verification remains a separate
operational concern. Authentication, identity, key upload, E2EE initialization, unsafe
permissions, and corrupt stores fail readiness closed. An encrypted event missing its
room key is dropped without plaintext fallback, task creation, or reply. `allowed_users`
controls ordinary ingress. `admin_users` and optional `admin_rooms` control slash
commands. Routes map aliases and rooms to configured agent IDs. Empty allowlists do not
grant access.

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
