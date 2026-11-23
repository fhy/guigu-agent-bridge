This crate implements the base to build a [Matrix](https://matrix.org/) client library.

## Guigu distribution boundary

This package is `guigu-matrix-sdk-base = 0.13.0-guigu.1`, a renamed fork of
upstream `matrix-sdk-base 0.13.0` with the reviewed R076 patch. Only the native
SQLite feature graph used by guigu-agent-bridge is certified. Browser/indexeddb,
all-features and arbitrary providers are not certified as drop-in compatible.
Rust library names are unchanged. See `R076-PROVENANCE.toml` and `hydration.patch`.
`Cargo.toml` is the effective standalone fork manifest. The checkout retains the
historical upstream `Cargo.toml.orig` but excludes it from package inputs; Cargo
generates the packaged `.orig` from the current fork manifest.

## Crate Feature Flags

The following crate feature flags are available:

* `encryption`: Enables end-to-end encryption support in the library.
* `qrcode`: Enables QRcode generation and reading code.
* `testing`: Provides facilities and functions for tests, in particular for integration testing store implementations. ATTENTION: do not ever use outside of tests, we do not provide any stability warantees on these, these are merely helpers. If you find you _need_ any function provided here outside of tests, please open a Github Issue and inform us about your use case for us to consider.
