# INC-001: Shared Matrix Room Routed One Event to Multiple Agents

## Status

Resolved in the reference bridge; prevention requirements accepted for this project.

## Impact

A user message addressed to `bridge-coordinator` was consumed by Coordinator, Developer, and Reviewer bridge instances in the same encrypted Matrix room. Multiple ACP sessions were created for one event. Developer implemented T001 before reliable assignment acknowledgement, Reviewer received an unrelated coordination request, and shared task documents temporarily disagreed about task state.

## Confirmed Facts

- All three service logs contained the same Matrix `event_id`.
- Each instance allowed the sender and shared the same generic trigger.
- The connector tested whether text matched its own name but did not first reject text explicitly addressed to a configured peer.
- Natural-language `bridge-coordinator:` text was not a structured recipient.
- All development Agents shared one repository worktree, increasing the impact of duplicate dispatch.
- Reviewer respected its role and refused Coordinator work; role profiles were not swapped.

## Root Cause

Authentication (`allowedUsers`) was incorrectly serving as routing policy. The system had no exclusive target-resolution step before session creation, and message delivery was treated too closely to task acceptance. Shared mutable task documents had no versioned state transition guard.

## Reference Fix

`opencode-chat-bridge` commit `b8b6011` added configured peer names, one default handler for unaddressed group messages, exclusive leading-target resolution, and diagnostic ignore logs. A real Matrix event addressed to Observer was ignored by Developer and Reviewer and started only the Observer ACP backend.

## Requirements Adopted Here

- Resolve transport identity and natural-language compatibility syntax to one stable `AgentEndpoint` before Bridge Core dispatch.
- Never create a session or start an Agent in a non-target adapter instance.
- Separate delivery, acknowledgement, execution, and completion states.
- Use event and delivery-attempt identities for deduplication and retries.
- Guard state transitions with expected prior state/version so stale writers cannot regress state.
- Record event ID, selected target, handler, delivery attempt, and ignore/reject reason without logging secrets or prompt bodies by default.
- Test targeted, default, unknown, ambiguous, duplicate, retry, restart, and concurrent-update paths at the real connector/adapter entry points.
- During development, use a dedicated branch/worktree for production-code tasks; governance/state writes remain serialized by Coordinator.

## Follow-up

T002 incorporates stable recipient and identity semantics. T004/T005 must define delivery acknowledgement and authoritative transitions. T009 must persist deduplication and conditional updates. T010/T011 must implement transport-to-endpoint resolution and exclusive routing. T012 must expose routing evidence. T016/T018 must verify isolation and the complete failure matrix.
