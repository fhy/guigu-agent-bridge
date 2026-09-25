# Release 0.2.5

This patch release adds the reviewed offline recovery path for durable work proven
to have failed before acceptance.

- `recover-preacceptance` requires explicit delivery/task/attempt tuples and verifies
  the persisted typed `Dispatched` event and closed pre-acceptance predicates. It
  atomically records a failed terminal event, closes the admission and disposition,
  acknowledges the delivery, and releases only the exactly fenced recovery-needed
  lease.
- Missing, malformed, mismatched, nonterminal, ambiguous, or fenced state fails
  closed. The command does not infer a successful post-acceptance outcome.
- `reconcile-delivery` and `recover-runtime` remain separate offline tools with their
  existing predicates. None of these commands is part of normal startup.
- CLI output is fixed and redacted; identifiers, database contents, credentials,
  provider responses, and crypto-store data are not emitted.

This release candidate does not itself authorize production recovery, Observer
operations, database or crypto-store access, Matrix traffic, or publication. Use the
applicable separately reviewed and authorized operations procedure.
