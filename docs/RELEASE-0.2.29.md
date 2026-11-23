# v0.2.29 public mainline release candidate

This local candidate integrates the R129-accepted public-mainline lifecycle
repairs onto the v0.2.28 base. It has not been published, installed or
validated against live encrypted traffic.

## Changes

- Durable output spool cleanup: `finish_cleanup` lifecycle now distinguishes
  three fatal boundaries (fence drift, commit failure, transaction-start
  failure) from the recoverable `CleanupDeferred` path. All three fatal paths
  return `Sql(error)` or `Conflict`; none become deferred.
- Cleanup deferred path: successful rollback preserves ACK state, quota and
  segments; exactly-once recovery is possible on a later drain cycle without
  resend or duplicate effects.
- Sanitized `finish-cleanup` warning is emitted on the deferred path with
  redacted set identity.
- ACP mock fixture pre-check added to `t049_a2a_runtime` and
  `t050_native_output` test files, matching the existing guard in
  `r092_runtime_route.rs`. No production code affected.

## Compatibility and deployment

The 26 embedded migrations, dependency versions and registry matrix-sdk
0.13.0 are identical to v0.2.28. Existing schema26 stores require no new
migration. No schema downgrade migration exists.

Before an authorized deployment, stop the writer and archive the complete
matched configuration/device/database/session/crypto generation plus
unresolved-request evidence. Verify the reviewed executable and pair identity.
Do not run two consumers on one pair, edit live SQL, import retired state or
resend old requests. Rollback to v0.2.28 requires a compatible reviewed
complete-pair plan and must not erase effects accepted after a snapshot.

## Validation limits

Accepted focused regressions cover: cleanup fatal-path classification (3/3),
deferred-path exactly-once recovery (6/6), controlled stop (12/12), release
shutdown (2/2), Matrix recovery runtime (3/3), A2A runtime (5/5), native
output (8/8), Matrix recovery startup (8/8), Matrix recovery crypto (1/1),
and all 13 original lib failures re-checked passing on the integrated tree.

Historical aggregate gate remains `901 passed / 13 failed / 12 ignored`,
exit 101, with `UNKNOWN` cause attribution. The 13 failures are now
individually re-checked and passing in isolation; this does not constitute a
fresh aggregate PASS. No resource-contention root cause is claimed.

Live Matrix E2EE acceptance, OS reboot, and publication remain NOT_RUN.
