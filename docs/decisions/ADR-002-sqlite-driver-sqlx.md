# ADR-002: SQLite Driver Selection (sqlx)

## Status

Accepted.

## Context

Stage 3 (T008/T009) introduces SQLite as the event store and source of truth for agents, conversations, messages, tasks, task events, deliveries, and sessions. Two Rust SQLite drivers were considered:

- `sqlx`: async-first, `SqlitePool` for safe concurrent access, embedded migrations via `sqlx::migrate!`, and an optional compile-time-checked query API.
- `rusqlite`: synchronous and mature, bundles SQLite, but each call must be moved off the Tokio runtime via `spawn_blocking` or a dedicated thread.

The Bridge runs on Tokio, and the Repository layer is called from async Bus, Matrix, and ACP paths. Blocking the async runtime on synchronous SQLite calls would add `spawn_blocking` discipline across every query path and complicate cancellation, timeout, and backpressure semantics.

## Decision

Use `sqlx` with the SQLite backend:

- Async queries over a pooled `SqlitePool`.
- Schema and migrations managed with embedded `sqlx::migrate!` migrations, applied at startup.
- Repository SQL written with the runtime-checked query API so statements stay inspectable and do not require a live database at build time; use compile-time macros only where they add clear value.
- SQLite features enabled for the Tokio runtime; use the bundled or system sqlite linkage as the environment dictates.
- Keep the `Repository` trait as the public boundary so the driver remains an implementation detail behind `Storage`.

Rationale: async-native access fits the Tokio architecture; `SqlitePool` gives safe concurrent reads and serialized writes; embedded migrations align with the restart-recovery requirement; the trait boundary preserves the ability to change drivers later.

## Consequences

- Positive: non-blocking database access on the async runtime, built-in migration support, connection pooling, and a clean driver boundary.
- Cost: additional dependencies and compile time; SQLite single-writer concurrency must still be configured explicitly (`busy_timeout`, `journal_mode=WAL`, `foreign_keys=ON`) rather than assumed.
- T008/T009 must define the Repository trait and migration layout before coding; migration ordering is immutable once released.
