# v0.2.21 Schema-Ready Candidate Notes

This local candidate carries the reviewed R063 historical unknown-abandonment
implementation and migration 0020. It is a readiness artifact only: it is not
published, installed, deployed, or authorized for Observer operation.

## Included capability

- Preserves the reviewed `historical_unknown_abandoned` transition.
- Includes `0020_historical_unknown_abandonment_evidence.sql` and its five
  audit evidence columns.
- Retains fail-closed owner qualification and no-replay behavior.

## Validation and limits

Readiness uses locked local gates, package dry-run, exact binary identity, and
package migration/file-list checks. No Observer database, service, credentials,
installation, publication, deployment, restart, or Matrix operation is part of
this candidate task.

## Rollback provenance

The rollback reference is the published and installed `v0.2.20` artifact.
Database downgrade is unsupported; any operational rollback requires a
separately authorized consistent database/configuration generation and matching
old executable.
