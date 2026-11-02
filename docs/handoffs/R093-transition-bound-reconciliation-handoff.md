# R093 transition-bound reconciliation handoff

Task `R093`, status `assigned`; owner `bridge-developer`; controller
`bridge-coordinator`. This new-session delivery contains exactly the two-file
design package requested by the frozen specification. Base source is
`a4850812e0f7867ff471d7169e633c2b1894bad4`; governance worktree is
`/home/fhy/guigu-agent-bridge-governance`, branch `main`; source worktree is
`/home/fhy/guigu-agent-bridge`. R070 remains same OP
`r070-20261102-observer-recovery-01`, `identity_ready`, hold retained.

## Delivered files and limits

Analysis: `docs/analysis/R093-transition-bound-reconciliation.md`.
Handoff: this file. No source, SDK, dependency, build, test, inventory,
retained DB/store, credential, grant, service/process, installation, publication,
external Matrix/ACP action, or task-state file was changed. Only document
whitespace/link/fence/diff checks are in scope.

The analysis is one coherent S1–S8 protocol. It defines successor/grant
authentication and pinned I/O; native expected-state derivation; exact marker,
record, digest and schema routing; complete inventory/classification/parent
mapping; action predicates, FKs and affected-row counts; one-lock transaction,
close/final-CAS/resume order; and removal of the contradictory R3 text.

## R3 residual mapping

| R3 residual | Analysis target | Evidence in package |
|---|---|---|
| DQ1 protected grant, pinned I/O, binding and pre-equality routing | S1 | Original and successor authority are separated; complete grant fields, no-follow traversal, same-handle hashing, no-replace publication and pre-equality refusal are explicit. |
| DQ2 marker/schema/bootstrap/files and phase routing | S2–S3 | Complete marker/audit DDL, NULL/CHECK/FK rules, no-schema and intent-only routes, committed/finalized routes and all three legacy verifier branches are explicit. |
| DQ2 canonical delta and unchanged proof | S4–S5 | Full typed pre/post presence, every-cell comparison, parent graph, fixed native values and independent expected graph are required. |
| DQ3 owner/chain/seven/claims actions | S4 and S6 | 126 owner rows plus D2, two independent D1 decisions, three anchors, conditional claim receipts/deletes, exact predicates, FKs and one-row counts are mapped. |
| DQ3 lock/SQL/finalization/resume | S7 | One lock, one first transaction, explicit closes, atomic binding/marker final CAS, cancellation/crash and idempotent resume are specified. |
| Contradictory prose | S8 and whole analysis | Superseded alternatives are listed and absent from the active protocol. |

## Checks and next action

Document-only checks performed for this delivery are `git diff --check` and
Markdown fence/link sanity inspection. No tests or builds were run, as required.
The next action is Controller whole-package acceptance and, only after that,
one finite independent design review. A design pass does not authorize
implementation, grant publication, R070 disposition or activation.

The exact final commit and SHA-256 values for both files are reported in the
delivery message because the commit hash must not be embedded in its own bytes.
