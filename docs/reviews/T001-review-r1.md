# T001 Review Round 1

- Verdict: `PASS`
- Tested commit: `f20c3d1c0dca0e22c97dffc123551fbfb6f95297`
- Base commit: `be00814dcfcf8a9e5b2a914e0b30fb7eb1316b43`
- Specification: `docs/tasks/T001-project-scaffolding.md`
- Handoff: `docs/handoffs/T001-handoff.md`
- Environment: Linux (WSL), `rustc 1.98.0 (88d9e12ae 2026-08-18)`, `cargo 1.98.0 (797e8a9bc 2026-08-05)`, no `rust-toolchain.toml`
- Checks run and results:

```text
cargo fmt --check            PASS (exit 0)
cargo check                  PASS (exit 0)
cargo clippy -- -D warnings  PASS (exit 0)
cargo test                   PASS (4 tests: 2 lib unit + 2 integration)
```

## Verification method

Reviewed the diff of `f20c3d1` against its parent `be00814` (not HEAD). Confirmed the
working tree differs from `f20c3d1` only by Coordinator-side docs
(`docs/PROJECT_STATUS.md`, `docs/TASK_BOARD.md`, `docs/handoffs/T001-handoff.md`,
`docs/tasks/T001-project-scaffolding.md`), so the source under test is byte-identical
to the reviewed commit. Re-ran all four gates directly, then performed an independent
smoke test:

```text
$ ./target/debug/guigu-agent-bridge
2026-09-15T06:43:42.038242Z  INFO guigu_agent_bridge: starting
        ... (SIGINT delivered) ...
2026-09-15T06:43:45.036068Z  INFO guigu_agent_bridge: shutdown signal received, exiting gracefully
exit code: 0
```

## Scope and acceptance criteria

| Criterion | Result |
|-----------|--------|
| Crate structure: lib + thin bin | PASS — `src/lib.rs` exposes `run`/`run_with_shutdown`/`init_tracing`; `src/main.rs` only forwards `run()` |
| Tokio feature set minimal (`rt-multi-thread` + `macros` + `signal`) | PASS — `Cargo.toml` matches; no `full`/unused features |
| `tracing` initialization idempotent, `RUST_LOG`-controllable | PASS — `try_init` + `EnvFilter::try_from_default_env` with `info` fallback |
| Top-level error boundary (`main`/`lib` propagate errors) | PASS — `Error::Shutdown(#[source] io::Error)`; `main` returns `Result<(), Error>` |
| Ctrl-C graceful shutdown | PASS — verified exit code 0 with structured shutdown log |
| No serde/Matrix/ACP/SQLite deps | PASS — grep of `Cargo.lock` found none |
| No Bus/Storage/Matrix/ACP/Observer module stubs | PASS — `src/` contains only `lib.rs`, `error.rs`, `main.rs`; no subdirectories |
| Tests exercise real call paths, not just string assertions | PASS — `tests/shutdown.rs` drives `run_with_shutdown` (public API) for both success and error propagation |
| Dependency rationale documented | PASS — `docs/tasks/T001-analysis.md` §2 and handoff record version/MSRV/rationale |

## Findings

No blocking or change-requesting findings.

### Non-blocking observations

1. **`run()` log target deviates from analysis (informational).**
   `docs/tasks/T001-analysis.md` §3/§5 show `info!(target: "guigu_agent_bridge", ...)`,
   while `src/lib.rs:28` and `src/lib.rs:41` use `info!(...)` without an explicit
   `target`. The emitted record still carries target `guigu_agent_bridge` (crate
   default), so behavior is equivalent. No action required; noted for
   spec/code consistency only.

2. **No automated test for the real `run()` → `ctrl_c()` path (accepted residual risk).**
   The graceful-exit path is only covered by the manual smoke test; automated tests
   inject the signal through the `run_with_shutdown` seam (`tests/shutdown.rs`). This
   is the documented design decision (analysis §6) to avoid an unstable, platform-dependent
   real-signal test, and is consistent with the spec's "real call path" requirement at the
   seam level. No action required.

## Residual risk

- The real OS signal delivery (`tokio::signal::ctrl_c()`) is verified manually only;
  a regression in that specific call site would not be caught by CI. Accepted for T001;
  future tasks that spawn background tasks should add a shutdown-propagation test.
