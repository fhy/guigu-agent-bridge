# Environment And Matrix Testing

Every operational or integration task declares its environment before work starts.
Service names, persistent storage, a real homeserver, or the word `Observer` do not by
themselves make an environment production.

## Environment Classes

| Class | Data and identities | Default operations |
|---|---|---|
| `local` | Synthetic data and local processes | Create, reset, inspect, restart and delete freely inside the task root |
| `development` | Dedicated development accounts and test-only state | Debug, inspect, restart, reset and send bounded test traffic |
| `test` | Automated accounts and disposable fixtures | Create and destroy accounts, rooms and state within the declared test scope |
| `staging` | Production-like topology without production data | Scripted deployment, recovery and end-to-end testing with retained evidence |
| `production` | Real business identities or durable user state | Read-only by default; mutations require explicit, current authorization |

Unknown environments fail closed only until the Coordinator asks for and records the
classification. Unknown must not silently become a permanent production classification.

Every task records:

```text
environment_class
data_class: synthetic | test-only | real-user
assigned_accounts and rooms
state_reset: allowed | prohibited
service_restart: allowed | approval-required
database_inspection: full | redacted | prohibited
message_budget and timeout
cleanup_or_retention
explicit production exclusions
```

## Automated Matrix Testing

Development, test and staging tasks should use multiple dedicated Matrix identities
before asking a user to reproduce a problem. Typical roles are sender, bridge,
observer and reviewer. Each identity has its own token, device, crypto store, database,
session root and logs.

Within the recorded message, room and time budgets, an automated test may:

- create private test rooms and invite assigned test identities;
- have test identities send messages to one another;
- exercise encrypted ingress, routing, Agent execution, replies and observation;
- stop, restart and reset only the declared test services and state roots;
- verify deduplication, recovery and bounded repeat behavior;
- retain redacted state categories and counts for review.

Tests must not use unlimited retries. They do not print or commit tokens, passwords,
room keys, crypto contents, message bodies, room/event identifiers or databases.

## User-Assisted Testing

When automated identities cannot reproduce a real-client property, the Operator may
invite the user into a clearly named test room and request a bounded action. Examples
include Element UI behavior, device verification, cross-signing, notifications and a
real-client encrypted send.

The request states the room, action, maximum message count and stopping condition.
The test may correlate only the requested messages. It must not read unrelated history,
contact other real users, change unrelated membership or permissions, or retry a send
without user agreement.

The escalation order is:

```text
local mock
-> automated multi-account Matrix E2E
-> user-assisted real-client test
-> explicitly authorized production validation
```

Moving between these levels is a test operation, not a reason to create a new feature
or release task, unless the required product behavior or public contract changes.
