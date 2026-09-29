# Staging Matrix Account Guide

Staging accounts are isolated test identities for development and recovery
rehearsals. They must never share a production access token, device, room,
database, crypto store, session directory, or ACP credential.

## Account Roles

Create roles only when a test requires them:

| Role | Intended use |
|---|---|
| `primary` | Normal sync, E2EE, restart, readiness, and smoke tests |
| `failure` | Authentication, timeout, retry, and recovery injection |
| `reviewer` | Multi-user permissions and review routing |
| `observer` | Observer-only monitoring and alert tests |

The standard account set is:

| Role | Standard user ID |
|---|---|
| `primary` | `@bridge-staging:example.test` |
| `failure` | `@bridge-staging-failure:example.test` |
| `reviewer` | `@bridge-staging-reviewer:example.test` |
| `observer` | `@bridge-staging-observer:example.test` |

Deployments replace `example.test` with the approved staging homeserver. The
identifiers are documentation examples; the private registry holds actual IDs.

Every task must select roles explicitly. A bounded scenario may use multiple assigned
roles to exchange messages automatically; Agents must not select unassigned accounts
or infer a production identity.

## Isolation And Credentials

Each role has its own Matrix user, device, room membership, access token, crypto
store, SQLite database, session root, and logs. Store credentials in a local
secret manager or a mode `0600` environment file. Record only the user ID, role,
non-secret storage references, and lifecycle status in documentation.

Never document passwords, access tokens, signing keys, crypto-store contents,
database contents, or production room identifiers.

## Lifecycle

The Coordinator assigns the role and test room in the task specification. The
Operator creates or disables the account using the homeserver administration
interface. The Developer uses only the assigned role and its isolated paths. A
reset may stop the staging service and recreate staging state, but must reject
production paths. The Reviewer verifies account, room, database, and crypto-store
separation before accepting evidence.

## Multi-Account Scenarios

Prefer automated account-to-account testing over using an Operator as the message
transport. A scenario may create private test rooms, invite its assigned identities and
send a bounded sequence through sender, bridge, observer and reviewer roles. The task
records maximum accounts, rooms, messages, duration and retry count before execution.

The default retry count is zero. A failed or ambiguous message stops the scenario unless
the specification explicitly permits another uniquely marked attempt. Tests verify
only redacted event counts, lifecycle states and delivery outcomes; they do not record
message bodies or Matrix identifiers.

## User-Assisted Fallback

If automated identities cannot cover Element behavior, device verification,
cross-signing, notifications or another real-client property, the Operator may invite
the user into a named test room. The user is asked for a bounded action such as sending
one marked message or confirming a displayed result. This does not make the test room
production.

Do not inspect unrelated room history, invite unrelated real users, alter unrelated
room permissions or automatically repeat a user-assisted message. See
[Environment And Matrix Testing](ENVIRONMENT_AND_MATRIX_TESTING.md) for the environment
classification and escalation rules.
