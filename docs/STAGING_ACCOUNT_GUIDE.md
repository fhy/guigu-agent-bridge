# Staging Matrix Account Guide

Staging identities must not share production tokens, devices, rooms, databases,
crypto stores, session directories or ACP credentials. Roles are selected by
the task, not inferred from service names.

## Account roles

| Role | Intended use | Example identity |
|---|---|---|
| primary | Normal sync, routing, restart and readiness | `@bridge-staging:example.test` |
| failure | Authentication, timeout and recovery fixtures | `@bridge-staging-failure:example.test` |
| reviewer | Multi-user permissions and review routing | `@bridge-staging-reviewer:example.test` |
| observer | Test-only monitoring and alerts | `@bridge-staging-observer:example.test` |

These are examples, not actual account assignments. Private task records select
the homeserver, participating identities, room/state roots and budgets. A role
named observer is not permission to access an existing production Observer.

## Isolation and secrets

Use separate devices/stores and assigned test state for each participant. Store
secrets in a restricted local provider or mode0600 environment file. A fixed
test process may consume assigned material privately; do not print, copy into
handoffs or commit passwords, tokens, signing keys or crypto-store contents.
Record only necessary non-secret references, role, categories and counts.

Preserve existing device keys. A fresh local store does not by itself authorize
overwriting a remote device's crypto keys. Session/device setup must follow the
task's contract. Historical unknown-retained tuples are not reusable test inputs.

## Bounded lifecycle

The Project Controller records the environment, assigned accounts/room, message
and timeout budgets, retry policy, cleanup/retention and production exclusions.
Reversible setup within that continuing authorization does not require a new
approval for each action. Account deletion, unrelated history/membership changes
and production authentication changes are not implied by staging testing.

Prefer automated account-to-account tests. Private test rooms may include only
the assigned participants. Record whether the room is encrypted: basic routing
in an unencrypted room does not establish E2EE acceptance. A failed or ambiguous
send closes the attempt; diagnose before any explicitly permitted new attempt,
and never silently reset issuance/send budgets or retry an ambiguous operation.

Report owned-process cleanup separately from file/session/room history residue.
Do not delete unrelated accounts or rooms to claim zero residue. User-assisted
Element/device-verification checks are optional when automation cannot cover
that client property; ask for a bounded action in the assigned test room only.
See [Environment and Matrix Testing](ENVIRONMENT_AND_MATRIX_TESTING.md).
