# v0.2.22 Offline Migration Release Candidate Notes

This local candidate carries the reviewed R071 offline production migration
capability, including the shared pre-open lock for selected pre-acceptance
recovery. It is a readiness artifact only: it is not published, installed,
deployed, or authorized for Observer operation.

## Included capability

- Preserves the reviewed offline migration CLI and its fail-closed lock,
  backup, intent, resume, verification and provenance contracts.
- Includes the R071 F1 repair for `recover-selected-preacceptance` before
  SQLite open, with durable no-mutation refusal evidence.
- Retains migration 0019/0020 and all existing owner, replay and abandonment
  safety behavior unchanged.

## Validation and limits

Readiness uses locked local gates, package dry-run, exact binary identity,
package migration/file-list checks, and synthetic offline fixtures. No
Observer database, service, credentials, installation, publication,
production migration, companion, or Matrix operation is part of this task.

## Rollback provenance

The rollback reference is the published and installed `v0.2.21` artifact.
Database downgrade is unsupported; any operational rollback requires a
separately authorized consistent database/configuration generation and a
matching prior executable.
