# Release 0.2.2

This patch release hardens ACP API-key authentication and runtime shutdown.

- ACP accepts only the default or `agent` `api-key` method. The 0.2.2 client
  historically rejected mixed advertisements; 0.2.3 selects one supported
  API-key alternative from a fully decoded mixed list while retaining fail-closed
  malformed and ambiguous behavior.
- Authentication remains child-process scoped; API keys are never sent over
  JSON-RPC or persisted by the bridge.
- Graceful shutdown fences the runtime owner as stopped even after an earlier
  shutdown error, and terminal failures acknowledge delivery atomically.
- Upgrade with the existing homeserver/user/device-bound crypto store and a
  separate business database backup. Keep `/health` available and require
  `/ready` before enabling work owners.
- Roll back by stopping the candidate, preserving evidence, and restoring the
  matching prior binary, configuration, database, and crypto-store generations.

No publication, production cutover, Matrix device change, or store replacement
is implied by this document.
