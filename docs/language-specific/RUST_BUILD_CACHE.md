# Rust Build Cache and Validation Inputs

This guidance is for Cargo/Rust, not a mandatory profile for other languages.
Project policy is summarized in [Local Rust Build Cache](../DEVELOPMENT_BUILD_CACHE.md).

## Record the effective inputs

For each build owner/candidate, record:

```text
source_commit and relevant tree identities
Rust/Cargo versions and MSRV
profile, target triple, features, locked/offline flags
shared target directory and build lock
sccache wrapper and incremental settings
RUSTFLAGS and binary bindings used by process fixtures
filesystem free space/usage, lock acquire/release, cache retention
per-command exits and required binary hashes
```

Inspect the effective Cargo target using metadata; configuration, environment
and explicit CLI options can affect it. Use only the task's assigned shared
target. A separate target requires a recorded owner and reason; do not silently
create one for an idle worktree. Never commit host-specific Cargo settings.

## Validation

Use focused tests while repairing. The designated owner runs the stable-candidate
validation required by the task; reuse accepted unchanged-input evidence rather
than repeating the same gate for docs or refs. Standard Rust checks are:

```text
cargo fmt --check
cargo check --locked
cargo clippy --locked -- -D warnings
cargo test --locked
```

Record actual features, all-target coverage and serial fixture flags as required
by the task. Process fixtures must resolve the intended shared-target executable,
including Cargo's exact hyphenated `CARGO_BIN_EXE_*` binding where used. An assembled
coverage ledger is not a single successful monolithic gate. Release package checks
are separate and are not authorized by routine source integration.

## Performance and retention

A persistent shared target and sccache can avoid repeated dependency compilation.
Incremental compilation or an alternative linker is optional, not a correctness
requirement. Retain warm caches; only filesystem pressure (less than 30 GiB free
or at least 85% used) triggers cleanup under the build lock after builds exit.
There is no default 20 GiB stop threshold. Preserve active images and dependencies,
and never clean runtime state, credentials or retained evidence as build artifacts.
