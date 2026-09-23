# guigu-agent-bridge 0.2.0

## Highlights

- Matrix rooms can receive encrypted events and send encrypted replies through
  matrix-sdk's persistent E2EE support.
- The bridge restores the configured `GUIGU_BRIDGE` device and crypto state across
  process restarts. It does not create or verify a Matrix device automatically.
- The package now requires Rust 1.94. The business SQLite schema and migrations are
  unchanged from 0.1.0.

## Matrix Configuration And Store

Before enabling Matrix, configure `crypto_store_path` to a dedicated directory that
is separate from the business database and ACP `session_root`. Set `device_trusted =
true` only after an operator has verified the existing device out of band. The access
token, device identity, configuration, and crypto store must all refer to the same
Matrix account and device.

On Unix, keep the store directory private (`0700`) and its SQLite databases and WAL/
SHM files private (`0600`). Startup rejects symlinked or group/world-writable
ancestors, unsafe existing store files, and corrupt SQLite files before opening the
SDK store. Missing room keys and rejected/kicked sessions fail closed; encrypted
events are not treated as plaintext and are not admitted to task routing.

Back up and restore the state, crypto, and event-cache SQLite files as one consistent
store generation, including WAL/SHM state. Use an SQLite-aware online backup or stop
the bridge before copying the complete store directory. Preserve the matching
configuration and account/device credentials in the deployment secret system. Never
commit, log, or share the crypto store, access token, recovery key, or encryption keys.

## Upgrade, Monitoring, And Rollback

The business database continues to use the existing forward-only migrations; this
release adds no business schema migration. Preserve a verified backup of the business
database and the complete Matrix crypto store before upgrading. Do not delete the
crypto store or point a different Matrix device/token at it. Startup must remain
unready on unsafe permissions, corruption, authentication rejection, or unresolved
recovery. Monitor `/health`, `/ready`, Matrix sync failures, repeated restarts, and
recovery-needed state before allowing traffic.

For rollback, stop the 0.2.0 process and confirm its ACP children are reaped. Preserve
the 0.2.0 database and crypto-store snapshot for diagnosis. Restore the prior binary,
configuration, business database, and any prior Matrix state as a matched set; do not
pair a crypto store with a different device or token. The 0.1.0 client does not support
E2EE, so encrypted rooms must remain disabled or unavailable while it is running.
Reconcile work accepted after the rollback point before replaying requests. Production
cutover and Matrix login/device operations require separate authorization.
