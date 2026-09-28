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

Every task must select a role explicitly. Agents must not choose an account from
an account pool or infer a production identity.

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
