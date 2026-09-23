# guigu-agent-bridge 0.2.1

## Highlights

- Matrix startup now requires an explicit non-empty device ID and verifies that the
  access token's authenticated user and device match the configured identity.
- The persistent crypto store is bound to the normalized homeserver, user ID, and
  device ID. A missing, corrupt, or mismatched identity manifest fails closed instead
  of silently adopting or rebuilding the store.
- Before Matrix becomes ready, the bridge initializes E2EE and confirms through an
  authenticated server key query that the configured device and its fingerprint are
  published. Stale, missing, or conflicting server evidence keeps readiness at 503.
- Matrix preflight now finishes before ACP, A2A, durable bus, gateway, outbox, and task
  work owners are assembled. Authentication, store, E2EE, or key-proof failure leaves
  only health diagnostics running.
- Readiness updates are generation-fenced so an obsolete runtime cannot publish a late
  ready state. A stale runtime owner is reported as `recovery-blocked`.

## Compatibility And Configuration

Version 0.2.1 requires Rust 1.94 and keeps the 0.2.0 business SQLite schema and
migrations unchanged. Existing 0.2.0 deployments must configure the access token's
exact Matrix `device_id`. Do not guess a device ID or substitute another device owned
by the same user.

The configured homeserver, `user_id`, `device_id`, access token, and
`crypto_store_path` must identify one existing Matrix login and one persistent store.
`device_trusted=true` remains a local operator permission only. It does not assert
Matrix owner or cross-signing verification. Client unverified-device warnings are
accepted in this release and do not by themselves block readiness.

The bridge does not create a replacement identity, self-verify a device, or repair an
identity mismatch by deleting state. Element, SAS/emoji/number/QR verification,
cross-signing, and recovery-key management are outside this release.

## Upgrade And Backup

1. Record the running 0.2.0 binary, configuration, environment, Matrix identity,
   service unit, database path, crypto-store path, and health endpoint without
   recording access tokens or key material.
2. Stop the existing writer, or use SQLite-aware online backup, before snapshotting
   the business database and any WAL/SHM files.
3. Back up the entire Matrix crypto-store directory as a separate consistent
   generation, including `identity-v1.json` and every SQLite/WAL/SHM file. Record the
   matching normalized homeserver, user ID, and device ID with the backup.
4. Add the exact non-empty `device_id` to the protected deployment configuration and
   confirm its token authenticates as the same user and device. Keep the store path
   unchanged and private.
5. Preserve directory permissions of `0700` and store/manifest file permissions of
   `0600` on Unix. Do not move the store through a symlinked or group/world-writable
   ancestor.

Do not run 0.2.0 and 0.2.1 concurrently against the same Matrix account, rooms,
business database, or crypto store. Do not delete or rebuild the crypto store as an
upgrade step.

## Deployment And Readiness

Start the exact reviewed 0.2.1 candidate with Matrix traffic still disabled at the
external routing boundary. `GET /health` may become available for diagnosis while
`GET /ready` remains HTTP 503. Enable traffic only after `/ready` returns HTTP 200 and
the response reports `ready=true` for the current generation.

Treat each of the following as a failed deployment: authenticated user/device
mismatch, missing or mismatched store identity, unsafe path permissions, store
corruption, E2EE initialization or key-upload failure, missing/stale/fingerprint-
mismatched server device evidence, `recovery-blocked`, startup recovery backlog, or a
required Matrix task exit. These failures must not be bypassed by forcing readiness.

An encrypted event with a missing room key is rejected locally without plaintext
fallback, task creation, or reply. It is an event-local failure and does not change
transport readiness by itself; repeated missing-key failures still require operator
investigation.

## Monitoring

- Poll `/health` for bounded identity/readiness diagnostics and `/ready` for traffic
  eligibility. Alert on persistent 503 responses and `recovery-blocked`.
- Alert on authentication/device mismatch, store binding/permission/corruption errors,
  E2EE or key-upload failure, server key-proof failure, Matrix sync failure, missing
  room keys, required task exit, and repeated restarts.
- Continue monitoring business recovery backlog, leases, ACP child exits, queue/outbox
  backlog, and duplicate-delivery indicators.
- Never log access tokens, device private keys, recovery keys, crypto-store contents,
  message bodies, or ACP command arguments. An unverified-device warning is expected
  and must not be reported as verified owner/cross-signing state.

## Restore And Rollback

If startup or verification fails, leave traffic disabled and preserve the candidate's
database, crypto store, and redacted logs for diagnosis. Do not delete the store,
change the configured device, replace the token, or create a new Matrix login as an
automatic recovery action.

To roll back, stop 0.2.1 and confirm its Matrix tasks and ACP children have exited.
Restore the 0.2.0 binary, protected configuration, business database, and complete
pre-upgrade crypto-store snapshots as matching generations. Never pair a crypto store
with a different homeserver, user, device, or replacement token. Start only one bridge,
verify its health and Matrix sync, and use a disposable authorized request before
reopening traffic.

Rollback loses work accepted after the backup point. Reconcile those task IDs from the
preserved candidate database before replay. Publishing, production migration, Matrix
identity changes, service replacement, and cutover require separate authorization.
