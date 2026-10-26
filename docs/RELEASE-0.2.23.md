# v0.2.23 — R076 inbound cutover distribution

This release candidate selects `guigu-matrix-sdk`, `guigu-matrix-sdk-base` and
`guigu-matrix-sdk-sqlite` at exact `0.13.0-guigu.1`. They preserve the reviewed
Matrix SDK 0.13.0 code plus the R076 hydration and isolated repeat-processing
patch. Rust aliases remain unchanged. No root Cargo patch is required.

The supported graph is Bridge's native SQLite configuration with SDK defaults
disabled, rustls, E2EE and bundled SQLite enabled. Browser/indexeddb, all-features
and arbitrary third-party provider compatibility are not certified.

The persistent SDK anchor remains A; active requests always use A; application
watermark B is durable and never an SDK request token. Real repeated batches are
fully processed and durably deduplicated, with bounded cancellable idle pacing.
Missing/default, limited/reset, provenance and failed hydration refuse.

Local registry rehearsal is not crates.io publication. Future release execution
must verify package-name ownership, publish the three dependencies in order
base → SQLite → SDK, verify downloaded checksums, then verify/publish Bridge.
No dependency publication or installation is authorized by readiness evidence.
The previous v0.2.22 rollback is main-program-only; do not roll back schema or
assume preserved companion binaries constitute an operational rollback.
