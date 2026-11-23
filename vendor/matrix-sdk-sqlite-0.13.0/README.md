## Guigu distribution boundary

This package is `guigu-matrix-sdk-sqlite = 0.13.0-guigu.1`, a renamed fork of
upstream `matrix-sdk-sqlite 0.13.0` with the reviewed R076 patch. Only the native
SQLite feature graph used by guigu-agent-bridge is certified. Browser/indexeddb,
all-features and arbitrary providers are not certified as drop-in compatible.
Rust library names are unchanged. See `R076-PROVENANCE.toml` and `hydration.patch`.
`Cargo.toml` is the effective standalone fork manifest. The checkout retains the
historical upstream `Cargo.toml.orig` but excludes it from package inputs; Cargo
generates the packaged `.orig` from the current fork manifest.
