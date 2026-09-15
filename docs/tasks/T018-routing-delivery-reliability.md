# T018: Exclusive Routing and Delivery Reliability Acceptance

## Metadata

- Status: `queued`
- Phase: seven, reliability acceptance
- Owner: unassigned
- Dependencies: T005, T009, T011, T014, T016
- Incident: `docs/incidents/INC-001-shared-room-routing.md`
- Design gate: implementation analysis must map every scenario below to the real Transport, Core, Bus, Storage, and Adapter entry points.

## Goal

Prove that one inbound event produces at most one intended Agent execution and that delivery acknowledgement, retries, restarts, and concurrent state updates cannot duplicate execution or regress authoritative task state.

## Scope

- Build an integration harness with at least two configured Agent endpoints sharing one simulated Matrix room.
- Exercise explicit endpoint ID, display-name compatibility syntax, configured default recipient, unknown target, ambiguous target, and no target.
- Verify non-target instances do not create conversations/sessions, enqueue tasks, or start ACP processes.
- Exercise duplicate Matrix events, duplicate Bus submission, delivery timeout before acknowledgement, acknowledgement followed by process exit, retry, cancellation, and Bridge restart.
- Exercise concurrent and stale task-state updates; only valid expected-state/version transitions may commit.
- Correlate transport event ID, conversation ID, task ID, delivery attempt, target endpoint, process/session ID, and final status in diagnostic events.
- Exclude user-interface styling and backend-specific prompt behavior.

## Required Invariants

- A transport event maps to zero or one recipient unless an explicit structured multicast operation is introduced later.
- Message receipt, queue insertion, Agent acknowledgement, running, and completion are distinct facts.
- Retrying an unacknowledged attempt is idempotent; a late acknowledgement cannot resurrect a cancelled or completed task.
- Restart recovery does not execute a completed delivery again and does not lose an acknowledged running task without a recovery outcome.
- Logs and observer events contain routing identifiers and reasons but exclude credentials and prompt bodies by default.

## Acceptance Criteria

- [ ] Real entry-point tests cover all routing and failure scenarios in Scope.
- [ ] Exactly-one-target assertions include session/process start counters, not only returned helper values.
- [ ] Persistence recovery and stale-writer tests prove no duplicate execution or state regression.
- [ ] Unknown and ambiguous targets fail explicitly; they never fall back to broadcast.
- [ ] All Rust gates pass and an immutable review report identifies the exact tested commit.
