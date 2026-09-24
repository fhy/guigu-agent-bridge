# Release 0.2.3

This patch release fixes ACP authentication negotiation and closes durable delivery
state when initialization or authentication fails before acceptance.

- A fully decoded authentication list may contain unsupported alternatives; exactly
  one supported API-key method is selected. Malformed entries, missing keys, and zero
  or multiple supported API-key methods remain fail-closed.
- API keys remain child-process-only and are never sent in JSON-RPC, logs, events,
  database state, or release evidence.
- The production ACP router forwards pre-acceptance finalization to the leased
  dispatcher. Terminal event, delivery acknowledgement, terminal disposition, and
  lease/continuation release remain one fenced transaction; duplicate consumption and
  fencing failures map to recovery-needed without retry.
- This release does not change the database schema, public dispatcher interface,
  Matrix identity/store, or deployment configuration.

No publication, production cutover, Matrix traffic, or store replacement is implied
by this document. Follow the release checklist and obtain separate publication and
deployment authorization.
