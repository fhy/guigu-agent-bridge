# v0.2.10 Candidate Notes

v0.2.10 is a local release candidate that adds the reviewed T043
`reconcile-orphaned-preacceptance --database PATH` operation. The command proves an
orphaned pre-acceptance lease in one immediate transaction and reuses the reviewed
T037 terminal-closure core. It preserves the captured owner, resource, fence and
expiry identity and never guesses or acquires a replacement owner.

The operation is not automatic and is not run at startup. It may be used only after
separate T036 current-window authorization against the published artifact. Fixed
redacted output, exact repeat recognition, rollback, fencing and post-commit planner
verification are covered by real migrated SQLite and process tests.

This document does not claim publication, installation, deployment or production
execution. Publication requires independent review and separate authorization.
