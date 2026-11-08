# v0.2.26

This patch includes the encrypted reply, shutdown, same-generation recovery and
offline migration refusal fixes developed after v0.2.25. Publication, installation
and live service acceptance are separate steps; this document describes the source
and package, not a claim that a deployment has already been upgraded.

## Changes

- Preserve ACP failure feedback when a child exits before initialization, and
  retain the first failure while cleaning up owned work.
- Bound metrics reads and keep MatrixSync heartbeat/component leases progressing
  through slow storage and long response processing.
- Drain started sync responses during normal shutdown. Same-generation recovery
  uses current process/resource exclusion and fenced native succession without
  inventing historical joins or reaping, skipping uncertain input, or repeating
  unknown network effects.
- Preserve offline owner-binding publication outcomes and refuse unresolved
  commit state according to the existing recovery contract.
- Authenticate isolated migration publication targets and SQLite sidecars before
  discovery reads. Hostile sidecars receive the fixed refusal diagnostic; lock
  contention and unresolved maintenance remain fail-closed. Preserve stale
  admission evidence and supported retained-WAL handling.
- Align package repository/homepage metadata with the existing public repository,
  https://github.com/fhy/guigu-agent-bridge.

## Compatibility and operations

Embedded migrations remain 0001–0025, byte-identical to v0.2.25. The original
registry matrix-sdk 0.13.0 and dependency lock versions are unchanged. This release
adds no schema or public protocol migration.

Keep matched device, database, session and crypto state together during an upgrade.
An older binary can still lack the recovery semantics required by current durable
state despite sharing a schema. Establish compatibility before rollback; do not
restore only the database or crypto store, clear uncertainty, or replay held work.

A package built outside a Git checkout intentionally lacks trusted checkout identity
for legacy fixture artifact admission. VCS metadata alone does not grant that
capability. Ordinary runtime and supported native recovery retain their contracts.

Validation combines fresh version/package/artifact checks with compatible evidence
for unchanged behavior. Historical failed runs and ignored diagnostics remain
recorded; source acceptance is not proof of a successful live encrypted upgrade.
