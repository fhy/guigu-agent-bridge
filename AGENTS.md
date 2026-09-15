# Agent Entry Protocol

This file is the short startup contract for every agent working in this repository. Read it before acting on a new, resumed, or continued task.

## Startup

1. Confirm the repository path and read `docs/TASK_BOARD.md`.
2. Read `docs/PROJECT_STATUS.md` when it exists, then the relevant sections of `docs/COLLABORATION.md` and `docs/GOVERNANCE.md`.
3. Identify the Current task and read its specification, latest handoff, and latest review.
4. Inspect `git status --short --branch`, the current branch, worktrees, and target files before editing.
5. Do not use `git log` alone to infer current task status.

## Role Boundaries

- **Coordinator**: decomposes work, prioritizes, is the sole maintainer of task state, explicitly dispatches implementation and review, resolves process questions, and escalates product or high-risk decisions. It does not implement production code.
- **Architect-Developer**: performs implementation analysis before coding, implements scoped code and tests, and fixes ordinary defects.
- **Reviewer**: independently reviews and verifies changes, investigates complex defects, and writes review reports. It does not modify production code.

Matrix delivery or an `@mention` does not prove task acceptance. Developer acknowledges an assignment before work starts. On `review_ready`, Developer writes a handoff and notifies Coordinator; Coordinator alone dispatches Reviewer with Task ID, exact commit, and handoff path. Reviewer accepts only a review request containing those fields and reports its verdict to Coordinator.

If chat, task specification, and task board disagree, stop and report the conflict to Coordinator. Do not silently choose one source or alter task state outside the Coordinator role.

## Before Coding

Architect-Developer must first document the implementation analysis: call paths, target and excluded files, shared interfaces, dependencies, lock ordering, awaits, task/channel lifecycle, cancellation and timeout races, recovery, idempotency, failure behavior, and test strategy.

If facts are unclear, specifications conflict, an interface is incomplete, behavior has multiple valid choices, or the design may deadlock, race, leak resources, be untestable, or fail to recover, stop and report to Coordinator. Coordinator escalates unresolved product, API, security, compatibility, concurrency, or scope decisions to the user.

## Gates and Git

Before commit, run:

```text
cargo fmt --check
cargo check
cargo clippy -- -D warnings
cargo test
```

Never use blanket staging (`git add .`, `git add -A`, `git add -u`) or `--no-verify`. Stage explicit files only. Do not commit credentials, local databases, sessions, logs, generated outputs, or external absolute paths.

## Handoff

Every handoff identifies Task ID, status, branch, worktree, base commit, latest commit, changed files, tests, known risks, and next action. A review must name the exact tested commit and return `PASS`, `CHANGES_REQUESTED`, or `BLOCKED`.

## Stop Conditions

Stop instead of guessing when a required dependency, environment, credential, external service, or design fact is unavailable. Record the blocker and its impact; do not report an unexecuted check as passing.
