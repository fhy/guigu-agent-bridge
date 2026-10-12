# Local Rust Build Cache

This guide preserves shared-cache and single-build-owner practices. The reusable
[Rust build guidance](language-specific/RUST_BUILD_CACHE.md) records the selected
toolchain, target and cache inputs. It does not change runtime services, MSRV,
published artifacts or the task's accepted verification requirements.

## Toolchain and evidence

`Cargo.toml` is authoritative for Rust/MSRV. Use the task's locked toolchain and
features for focused checks and stable-candidate validation. Record each command
and exit; do not turn a failed gate into PASS by relabelling later focused results.
Reuse accepted evidence only when the relevant source, dependencies, features,
toolchain and invocation inputs are unchanged. Documentation-only changes need
document checks, not a repeat full compilation or live E2E.

## Shared target and ownership

The Project Controller assigns one Rust build owner. Record the approved shared
target, build lock, effective Cargo configuration and sccache/incremental settings
before building. All active worktrees use that shared target unless an independent
target is explicitly assigned. Task parallelism does not authorize parallel full
gates or a new worktree-local target tree.

Local settings may use `CARGO_TARGET_DIR`, a Cargo configuration `target-dir`, and
`rustc-wrapper` for sccache. Do not commit machine-specific paths. Cargo metadata
reports the effective path; explicit CLI `--target-dir` must also be recorded.
Keep warm shared dependencies and candidate images across tasks. Incremental
compilation and sccache are performance choices, not substitutes for validation.

## Disk pressure and cleanup

There is no fixed 20 GiB target cap. Before a full gate, inspect the filesystem
containing the shared target. Available space below 30 GiB or usage at or above
85% triggers resource cleanup under the build lock, with no build running.
Prefer abandoned independent targets and expired temporary outputs; preserve
active candidate binaries and shared dependencies. Do not routinely run
`cargo clean` after a task, review or documentation change.

Never treat runtime databases, WAL/SHM, crypto stores, credentials, sessions,
logs or retained operation evidence as cache. Age alone does not prove a test
directory can be deleted. Fixture cleanup must identify resources created by
the test and respect task retention/exclusions; it is separate from build-cache
maintenance. This guide intentionally supplies no blanket recursive-delete script.

## Local settings and rollback

An optional `SCCACHE_CACHE_SIZE` in the user/CI environment can bound sccache's
own LRU cache; the wrapper configuration alone does not set this budget.
Inspect `sccache --show-stats` before changing it. This is separate from the
shared Cargo target's disk-pressure rule and does not require cache deletion.

To revert local performance settings, stop that checkout's builds and remove
only its local target/wrapper setting, preserving other configuration. Keep
target contents unless a separately scoped cleanup is necessary. No service,
Observer, credential or runtime operation is needed to change build settings.
