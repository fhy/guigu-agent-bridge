# Release 0.2.4

This patch release adds the reviewed, standalone offline delivery reconciliation
command required to unblock stale-runtime recovery workflows.

- `reconcile-delivery` accepts explicitly named delivery/task/attempt tuples and
  validates terminal task facts and closed execution work in one immediate SQLite
  transaction. It is not part of normal startup.
- Eligible existing dispositions are acknowledged and transitioned to terminal
  atomically; missing, conflicting, malformed, nonterminal, duplicate, unknown, or
  fenced state fails closed. No terminal event or inferred outcome is created.
- `recover-runtime` remains a separate fenced offline command. Neither command may
  be run against production data without a separately reviewed and authorized
  operations task.
- Output is fixed and redacted; credentials, message bodies, database contents,
  provider responses, and crypto-store data are never emitted.

This release does not authorize production reconciliation, Observer operations,
database or crypto-store access, Matrix traffic, or publication by itself. Follow the
release checklist and the applicable operations authorization.
