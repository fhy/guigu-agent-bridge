# Environment and Matrix Testing

Tasks declare the environment before execution. A real homeserver, persistent
storage or a service named Observer does not by itself mean production.

| Environment | Default boundary |
|---|---|
| local/development | Synthetic or dedicated test state; reversible scoped SDK/CLI/scripts |
| isolated test/staging | Assigned identities, rooms, services and data; bounded requests and cleanup |
| production rehearsal | Fixed runbook, provenance and rollback evidence |
| production | Explicit mutation authorization, verified tooling and post-operation checks |

Record data class, assigned accounts/rooms/state roots, reset/restart permission,
database inspection policy, message/retry/time budgets, cleanup or retention,
and production exclusions. Missing classification needs clarification, not an
assumption that all test work is production. The task specification is the
continuing authorization; internal setup capability gaps are repair work, not
a reason to request the same test authorization again.

## Matrix coverage

Use dedicated identities with separate devices, stores and assigned state; see
[Staging Matrix Account Guide](STAGING_ACCOUNT_GUIDE.md). Inside the recorded
budget a test may create/reuse a private test room, invite assigned participants,
send messages, restart its own application with retained test state and verify
reception, replies, deduplication and recovery. Normal trusted staging can use
direct owned subprocess handles and finally/RAII cleanup. It need not introduce
systemd handshakes, retained-FD execution or persistent takeover proof protocols.
Optional production systemd support is unaffected.

Credentials remain secret. A restricted test process may consume assigned
environment files or inherited descriptors without echoing secrets. Do not log
tokens, passwords, crypto contents, message bodies or unrelated room history.
Retain redacted categories/counts and only necessary non-secret references.
Ambiguous issuance/send is not permission to retry; preserve durable budget facts.

Label evidence accurately: mock mapping tests are not real Matrix E2E, an
unencrypted-room routing result is not E2EE proof, and successful code checks
do not establish live compatibility. Report expected refusals separately from
success, process shutdown separately from retained file/remote residue.
Do not access historical protected tuples to prepare a fresh independent test.

## Optional client-assisted checks

When automation cannot cover Element UI, cross-signing, notifications or device
verification, request a bounded action in an assigned test room. Do not read
unrelated history, invite unrelated users or repeat a message without permission.
A client/gateway used only to exercise the product is optional infrastructure
unless the product specification owns it; a missing client can mark that cell
NOT_TESTED without blocking native-interface acceptance. Escalation to production
is separate authority, never inferred from successful staging coverage.
