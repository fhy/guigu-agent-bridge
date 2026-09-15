# T001 Handoff

- Status: `review_ready`
- Branch: main
- Worktree: /home/fhy/guigu-agent-bridge (single worktree)
- Base commit: be00814dcfcf8a9e5b2a914e0b30fb7eb1316b43
- Latest commit: f20c3d1c0dca0e22c97dffc123551fbfb6f95297
- Specification: docs/tasks/T001-project-scaffolding.md

## Changed files

- `Cargo.toml` — dependencies and features
- `Cargo.lock` — regenerated (36 packages locked to Rust 1.85-compatible versions)
- `src/main.rs` — thin async binary forwarding to lib
- `src/lib.rs` (new) — `init_tracing`, `run`, `run_with_shutdown`
- `src/error.rs` (new) — minimal extensible top-level `Error` enum
- `tests/shutdown.rs` (new) — integration tests for shutdown/error propagation
- `docs/tasks/T001-analysis.md` (new) — implementation analysis and rationale

## Tests run and results

- `cargo fmt --check` — PASS
- `cargo check` — PASS
- `cargo clippy -- -D warnings` — PASS
- `cargo test` — PASS (4 tests: 2 lib unit + 2 integration)
- Smoke test (`cargo build` then run + SIGINT): structured `INFO` log on start, `shutdown signal received, exiting gracefully` on SIGINT, exit code 0 — PASS

## Dependency rationale (acceptance: 依赖选型与理由)

- `tokio` `rt-multi-thread` + `macros` + `signal`: multi-thread runtime (long-running daemon hosting future Bus workers/adapters), `macros` for `#[tokio::main]`/`#[tokio::test]`, `signal` for `ctrl_c()`.
- `tracing` + `tracing-subscriber` (feature `env-filter`): structured logging via `RUST_LOG`, default `info`, `try_init` idempotent. No `json` feature (deferred to T016).
- `thiserror` 2: extensible top-level error enum, compile-time only, MSRV 1.71 ≤ 1.85 floor.
- Excluded per spec: no serde/Matrix/ACP/SQLite dependencies; no Bus/Storage/Matrix/ACP/Observer module stubs.

## Known risks or unchecked items

- Real Ctrl-C signal path is not exercised by an automated test (signal source is injected through the `run_with_shutdown` seam); the graceful-exit path was verified manually via the smoke test above.
- `docs/TASK_BOARD.md`, `docs/PROJECT_STATUS.md`, and `docs/tasks/T001-project-scaffolding.md` carry uncommitted Coordinator-side state edits; they were left untouched by this task and are not part of this commit.

## Next action

Coordinator verifies scope and exact commit `f20c3d1c0dca0e22c97dffc123551fbfb6f95297`, then dispatches Reviewer.
