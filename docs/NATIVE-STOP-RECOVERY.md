# Native runtime stop metadata recovery

Normal startup performs native succession using current process/resource exclusion,
classified inventory and atomic lease fencing. The legacy `recover-runtime --database
... --owner-token ... --process-fingerprint ...` entry now refuses classified databases
before writing. Its legacy unclassified eligibility rules are unchanged.

A previous legacy invocation may have stopped one native runtime without maintaining
its classification. For that exact case, preserve a coherent current database/session/
crypto generation and the complete **pre-operation** database archive. With the service
stopped, use the same assigned service environment and configuration:

```sh
guigu-agent-bridge recover-runtime \
  --config /path/to/service-config.toml \
  --stop-metadata-archive /path/to/pre-operation/state.db
```

This mode reads the archive as evidence; it does not restore it. The selected native
owner, generation, context and owner evidence must match. The archived complete typed
runtime row must match the saved classification. Every current column except state
and heartbeat must be identical, with an active/stopping-to-stopped transition and a
valid later timestamp. Missing/incorrect archives or other drift are refused.

Current process and resource exclusion is still mandatory. All remaining inventory,
unknown-effect and lease predicates apply. Classification maintenance, lease fencing
and native succession commit together. Before/current metadata are retained in the
existing succession evidence; historical joins are not invented. Existing uncertain
notifications remain isolated, with no replay.

Success prints `recover-runtime result=native-stop-confirmed` after an independent
readback. An immediate exact retry confirms the committed record without another
publication. A failed response does not authorize a blind retry with different inputs;
retain its evidence and check the same archive/current generation. Once normal startup
advances that generation, this narrowly bound recovery is no longer a general retry or
repair command. It neither logs in nor resets crypto and cannot repair arbitrary rows,
failed task outcomes or an ambiguous owner set. No new schema migration is involved.
