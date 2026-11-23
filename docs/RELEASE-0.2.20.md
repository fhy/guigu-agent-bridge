# v0.2.20 Local Candidate Notes

This local candidate contains the reviewed R050 terminal acknowledged cleanup
implementation. It is not published, installed, deployed, or authorized for
Observer operation.

## Included capability

- Adds private `historical-terminal-cleanup` dry-run/apply/verify paths.
- Selects exactly one acknowledged and terminal durable record automatically.
- Uses audit-before-state, fenced lease/continuation CAS, atomic rollback,
  repeat verification, and no-replay side-effect protection.
- Keeps runtime-owner and startup qualification fail-closed; no escaped-child
  or containment proof is created.

## Validation and limits

Disposable SQLite fixtures cover apply/reopen/verify/repeat, injected failures
after audit and CAS, rollback, and no-replay behavior. Release readiness uses
locked local gates and exact package/binary provenance. A bounded full-suite
cancel remains incomplete evidence if it occurs.

No Observer database, service, credentials, installation, publication,
deployment, or restart is part of this candidate task.

## Rollback provenance

The rollback target is the previously installed `v0.2.19` artifact. Database
downgrade is unsupported; any operational rollback requires a separately
authorized consistent database/configuration generation and matching old
executable.
