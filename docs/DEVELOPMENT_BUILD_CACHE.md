# Local Rust Build Cache

This guide configures disposable developer build outputs only. It does not change
Rust/MSRV requirements, CI, release gates, runtime services, or published artifacts.

## Toolchain and gates

- The project MSRV remains Rust 1.94.0 (`Cargo.toml` is authoritative).
- Fast local feedback: `cargo +1.94.0 check --locked` and focused tests.
- Review-ready code must still pass all four gates on the exact candidate:
  `cargo +1.94.0 fmt --check`, `cargo +1.94.0 check --locked`,
  `cargo +1.94.0 clippy --locked --all-targets --all-features -- -D warnings`,
  and `cargo +1.94.0 test --locked --all-targets`.
- Release work additionally follows the release specification, including locked
  package and publish dry-run checks. Caching never substitutes for those gates.

## Local cache layout

On this Linux developer host, sccache is installed in Cargo's user bin directory.
The user-level `~/.cargo/config.toml` selects it as `rustc-wrapper`. Each checkout
has an ignored, untracked `.cargo/config.toml` pointing `target-dir` at a separate
directory below `~/.cache/cargo-target/`. The repository and published package do
not contain host paths or these settings.

The per-checkout settings disable Cargo incremental compilation. Rust incremental
outputs contain session-local state that prevents sccache from reusing those Rust
invocations across target directories. Disabling it improves cross-target reuse;
editing a crate within the same target may do more work than with incremental
compilation. Cold-to-warm and cross-target measurements vary by machine and must
not be treated as build-time guarantees.

Suggested local budgets:

| Cache | Soft budget | Hard/eviction policy |
|---|---:|---|
| Each project target | 20 GiB | Review size before removing only that project's target directory |
| sccache | 10 GiB | sccache's own bounded LRU eviction; inspect stats before changing it |

Check usage without changing data:

```bash
du -sh "$HOME/.cache/cargo-target/guigu-agent-bridge" \
  "$HOME/.cache/cargo-target/guigu" "$HOME/.cache/sccache" 2>/dev/null
sccache --show-stats
df -h "$HOME"
```

## Safe cleanup

First stop builds that use the selected checkout. To rebuild only one project's
artifacts, verify the exact canonical path and then remove only that target:

```bash
target="$HOME/.cache/cargo-target/guigu-agent-bridge"
test "$(realpath -m -- "$target")" = "$HOME/.cache/cargo-target/guigu-agent-bridge"
printf 'Target to remove: %s\n' "$target"
rm -rf -- "$target"
```

Use the exact corresponding `.../guigu` path for the guigu project. Do not replace
the variable with a broad directory, glob, `~`, or `/`. To evict sccache entries,
use its own bounded cache control (`sccache --show-stats` and, when necessary,
`sccache --zero-stats` only resets counters; it does not delete cache data). Do not
delete Cargo registry/git downloads, databases, WAL/SHM files, crypto stores,
credentials, logs, sessions, or another worktree's files. Do not run `cargo clean`
against active projects.

## Rollback

The per-checkout `.cargo/config.toml` files are local and ignored. To restore Cargo's
default target location for one checkout, stop its builds and remove only that
checkout's local `.cargo/config.toml`; to disable the wrapper, remove only the
`[build] rustc-wrapper` entry from `~/.cargo/config.toml`, preserving any unrelated
settings. Keep target contents unless there is a separate, explicit cleanup need.
Uninstalling sccache is optional and is not needed to roll back: removing the wrapper
entry makes Cargo invoke rustc directly. No Observer/systemd action is needed.
