# v0.2.27 preparation candidate

This candidate integrates the private storage and runtime repairs since public
v0.2.26. It is not yet published or deployed. Earlier two-service encrypted-reply
acceptance applies to its recorded private artifact, not automatically to these bytes.

## Changes

- Keep a healthy runtime owner through ctime-only changes to its running executable.
  Capture the running object and content digest once, then revalidate content on
  metadata change. Object, content, modification time, ownership, permissions,
  extended attributes and process/domain changes still refuse. Validation is bounded;
  a timeout or unreadable/raced sample remains a visible failure. Path replacement
  does not replace the pinned running object. Offline immutable-file checks retain
  their stricter ctime contract.
- Maintain scoped classifications for Continue, abnormal completion, workflow and
  hold writes. Preserve transaction rollback and stale-generation/fence refusal.
- Supervise required runtime owners, including heartbeat and output drains, so
  their exit cannot leave readiness falsely healthy. A refused completion CAS is
  visible, keeps uncertain sending state, and never resends it.
- Reconcile eligible native owner histories, preserve genuine unknown effects, and
  validate current resources across boot changes without comparing starttime across
  boots. Shared PWD hints alone do not establish resource ownership. Supported
  stop-metadata recovery authenticates the original state; see
  [native stop recovery](NATIVE-STOP-RECOVERY.md).
- Persist bounded Matrix history coverage across interruptions, retain diagnostic
  refusal categories, maintain lease/fence authority during fill, and separate
  collection and final-publication budgets. Coverage completion is not permission
  to execute historical commands or replay uncertain effects.

## Upgrade and rollback

Embedded migrations are 0001–0026. Existing migrations 0001–0025 are unchanged;
0026 adds `matrix_recovery_coverage`. Schema25-to26 is supported through the existing
migration/runtime admission path. The original registry matrix-sdk0.13.0 and all
third-party lock versions are unchanged. No SDK fork is required.

Stop the old writer and preserve the complete matched device/database/session/crypto
pair before upgrade. Do not run two consumers against one pair. Applied schema26
has no downgrade migration; public0.2.26 must not be pointed at that database.
Rollback requires a separately reviewed compatible complete-pair plan and accounting
for work accepted since the backup. Do not restore only a database or crypto store,
clear refused progress, or replay unknown tasks/notifications to force readiness.

The accepted operational rollout established new baselines after private archival
and retirement of old generations. It did not import or recover all old history;
uncertain old notifications were not replayed. This release does not introduce a
successor operation or a general historical-task disposal policy.

## Validation scope

Focused production-path tests cover identity revalidation, classified writes,
required-owner failure visibility, bounded coverage and same-database lifecycle.
Release preparation binds clean source, package inventory and executable smoke
checks. Historical non-green aggregate and ignored tests remain recorded; no fresh
all-tests PASS is claimed. Actual OS reboot is NOT_RUN for lack of an assigned
independent reboot environment. Simulated boot checks and process restarts do not
substitute for it. This is a disclosed validation limitation, not a claim that
cross-boot safety checks were disabled.
