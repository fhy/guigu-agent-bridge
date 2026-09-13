# Governance Baseline

This document records the project-governance practices adopted from the `guigu`, `pcdn-codebase`, and `opencode-chat-bridge` projects. It is intentionally lighter than the source projects and is the baseline for this repository.

## Sources and Scope

- From `guigu`: short task-board index, separate task specifications and review reports, role boundaries, iterative review, and Rust DoD gates.
- From `pcdn-codebase`: task registration, dependency/conflict assessment, branch/worktree handoff, explicit file staging, blocker escalation, and evidence-based verification.
- From `opencode-chat-bridge`: incident-driven debugging of thread routing, allowlists, ACP permissions, session paths, process recovery, and observable tool events.

The source projects also demonstrate two additional practices adopted here: a short agent-facing entry point that routes readers to current documents, and evidence-based release/recovery records. Large historical documents are references, not startup instructions.

## Task Board

`docs/TASK_BOARD.md` is a short restart index, not a complete project history or runtime queue. Keep `Current`, `Queue`, `Blocked`, and `Recent` near the top. Detailed specifications live under `docs/tasks/`; completed history belongs in `docs/HISTORY.md`.

An Agent restarting work must read the board first, then the Current task specification and the latest handoff/review. `git log` is historical evidence and must not be used as the source of current task status.

## Task Registration

Every non-trivial task records its ID, owner, status, goal, scope, excluded files, dependencies, shared interfaces, risks, and acceptance criteria. Cross-branch or cross-Agent work additionally records branch, worktree, base commit, and handoff information.

Before implementation, check the current branch, worktrees, `git status`, target files, shared interfaces, unfinished dependencies, and possible conflicts. Tasks touching the same file, public interface, migration, or global configuration run serially unless explicitly coordinated.

## Implementation Analysis

Architect-Developer must analyze before coding. The analysis covers existing call paths, concurrency and lock ordering, awaits while holding locks, task/channel lifecycle, cancellation and timeout races, resource cleanup, restart recovery, idempotency, testability, and failure behavior.

Unclear facts, contradictory specifications, incomplete interfaces, multiple behavior choices, deadlock risks, races, leaks, or untestable designs must be reported before implementation. The Agent must not use “implement first and see what tests say” as a substitute for design analysis.

## Blockers and Decisions

A blocker records the task, concrete question, confirmed facts, options and consequences, recommendation, and impact. Coordinator resolves it from existing architecture, specifications, decisions, and reviews. Questions involving product intent, public API, security, compatibility, concurrency semantics, or scope are escalated to the user.

Durable architectural choices go in `docs/decisions/ADR-*.md`; incident-specific findings go in `docs/incidents/` and must link to a fix task and regression test.

## Review and Verification

Review reports are immutable per round (`docs/reviews/<id>-review-rN.md`) and identify the tested commit and environment. A report must include findings with file/location, impact, and required action, not only PASS/FAIL.

Every implementation must pass:

```text
cargo fmt --check
cargo check
cargo clippy -- -D warnings
cargo test
```

Bridge-specific tests must exercise real call paths, not only helper strings: task state transitions, cancellation, timeout, retry, loop detection, persistence recovery, ACP mock transport, and Matrix event mapping.

## Git and Artifact Discipline

Never use blanket staging (`git add .`, `git add -A`, or `git add -u`) or `--no-verify`. Stage explicit files after checking the diff. Keep code, task specifications, reviews, and generated runtime data in their designated areas. Credentials, local databases, sessions, logs, and temporary outputs must not enter Git.

## Incident Learning

Operational failures are treated as design input. Record symptom, impact, timeline, root cause, fix task, regression coverage, and prevention. Particularly important checks for this project are thread-to-session mapping, Agent allowlists, ACP option negotiation, session/workspace consistency, process exit propagation, task event delivery, and loop prevention.

When a defect escapes helper-level tests, add a regression at the real entry point. Configuration, adapter, and routing tests must prove that production code actually invokes the tested path; string-only or unreachable tests are insufficient.

## Documentation Navigation

Keep a small `TASK_BOARD.md` and, when the repository grows, add a `docs/INDEX.md` that maps document categories without duplicating their contents. Agent startup should read the entry point, current task index, and only the relevant specification/review. Move completed task detail to `HISTORY.md` rather than expanding the startup path.

For releases or operational changes, preserve evidence of the tested commit, changed files, impact, monitoring, and rollback. Do not treat a green feature branch as proof that the merged tree or runtime entrypoints are complete; verify the resulting integration target.

## Proportional Process

Use the lightest process that preserves correctness:

- Small task: task specification, implementation, review, and basic gates.
- Cross-module task: conflict assessment and handoff.
- Concurrent task: dedicated branch/worktree and explicit shared-interface ownership.
- Release or incident task: verification report, rollback notes, and history entry.

The five long-term roles (Governance, Design, Execution, Review, Verification/Diagnostic) are an expansion path. The current project uses Coordinator, Architect-Developer, and Reviewer.
