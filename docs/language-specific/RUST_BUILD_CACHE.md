# Rust Build Cache And Gate Profile

This template applies to Rust projects using Cargo. It is a language-specific
companion to the general release manifest, not a requirement for non-Rust
projects.

## Profile

Record the selected profile in the task handoff:

```text
build_language: Rust
build_profile: fast | balanced | constrained
rust_toolchain:
msrv:
target_directory:
sccache_cache_size:
incremental: enabled | disabled
linker:
test_fixture_ttl:
```

Use `fast` when disk space is plentiful, `balanced` for normal development, and
`constrained` for small disks or shared CI workers. Profiles may change build
speed and storage use, but never weaken release gates or MSRV requirements.

## Cargo Path Closure

Before an Agent runs a build, record the effective path from:

```bash
cargo metadata --no-deps --format-version 1 | jq -r .target_directory
```

`CARGO_TARGET_DIR` and explicit `--target-dir` override Cargo configuration.
They are allowed only when the resulting path is an approved cache root and is
recorded in the handoff. An unregistered project-local `target` or temporary
target is a build hygiene finding.

## Agent Build Concurrency

The Project Controller assigns one primary Rust build owner at a time for a shared
project. Agents must not run full check/clippy/test gates concurrently against the
same project, and an idle worktree must not create a second local `target/` directory.
All active worktrees use the approved shared target root recorded in the handoff;
independent target roots require an explicit reason, an owner, and a cleanup deadline.

Before a full gate, the handoff records:

```text
build_owner:
build_lock:
target_directory:
target_budget_gib:
```

If the target root exceeds its budget, stop new full builds, report the size and
owner, and clean only after the owner exits. A completed handoff must remove stale
temporary target roots or record their exact retention reason. Build concurrency is
separate from task parallelism: agents may analyze or review unrelated work while
only one agent owns the shared Rust gate.

## Optional Speed Settings

- A persistent per-project target directory avoids recompiling unchanged
  dependencies.
- `sccache` can reuse compiler outputs across target directories. Set its size
  with `SCCACHE_CACHE_SIZE` in user or CI environment configuration.
- Cargo incremental compilation is usually faster for repeated local edits but
  consumes more disk. Disable it when cache reuse or disk limits take priority.
- `mold` with `clang` can reduce Linux link time when both tools are available.

These settings are local performance choices. They must not be committed with
machine-specific paths or made a prerequisite for published artifacts.

## Rust Gates

Development may use `cargo check` and focused tests. Review and release use the
exact project gates:

```text
cargo fmt --check
cargo check --locked
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets
```

Release work additionally runs the package and publish dry-run checks required by
the release task. Caches and faster linkers never replace these gates.

## Cleanup

Keep project targets, sccache, and test fixtures under named roots with explicit
budgets or TTLs. Clean only a verified target root after stopping builds. Never
clean Cargo registries, databases, WAL/SHM files, crypto stores, credentials,
sessions, or logs as part of Rust cache maintenance.

For this project, the default shared target budget is 20 GiB. The Project Controller
must schedule cleanup when the budget is exceeded and must not solve repeated growth
by silently creating another worktree-local target.
