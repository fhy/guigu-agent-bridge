# Direct staging acceptance runner

This is the simplified T050-N runner, not a namespace/ownership proof framework.
Production startup and optional normal systemd deployment are unchanged. The old
standalone proof crate and fixtures are removed. Python uses only the standard library.

Run focused lifecycle tests with a current shared-target bridge binary:

```sh
T050_N_BINARY=/path/to/current/guigu-agent-bridge python3 -m unittest discover -s staging/t050_n -v
```

Run actual native A2A and conditional Matrix acceptance:

```sh
python3 staging/t050_n/run.py --binary /path/to/current/guigu-agent-bridge --agent /path/to/native-acp-agent
```

The runner creates one private temporary directory under the user's `.cache` (not
world-writable `/tmp`, which the product's crypto-store guard rejects), uses free loopback ports, retains
Popen handles immediately, and stops/reaps its own process group in finally paths.
The same private DB is reused for restart. Child output is suppressed; fixed result
categories/counts are written to private `result.json`. That directory is deliberately
retained as test evidence, not reported as zero residue. No old namespace is queried.

Native ACP uses the selected installed real agent (for example, Qwen or Codex ACP)
and a text-only prompt. For Qwen, safe/plan mode and Advisor-disabled arguments are
supplied automatically. Native agent per-session recording is left at its normal
default; it may leave its own test-session evidence outside the runner directory.
It is not a fake backend. This needs the agent's ordinary existing model authentication.
Card, send, get, output, replay, conflict, cancel and restart are separate assertions.
Active cancellation waits for an observed `working` task; submission alone is not
labelled active execution. Queued cancellation and writer contention have separate
real root HTTP/SQLite regression fixtures.

Matrix privately consumes the four assigned staging EnvironmentFiles. A password
permits one login for a fresh independent device. Without it, an existing staging
session can initialize a fresh store only if real whoami/key-query facts show a
non-T046 device with no published keys. Existing keys are never replaced. Existing
sender tokens can be used for the other three roles. Login failures are not retried.
No password reset, canonical
device key replacement, original store copy or old supplier invocation is used.
The real private, non-federating room is intentionally unencrypted for this acceptance
scope. All four joined memberships are checked; three senders exercise the application,
then it restarts with the same DB/store and receives a new message. Remote room/session
residue is reported, never hidden by deleting unrelated resources. After process exit,
participants leave only the newly created room; room history and issued sessions/devices
remain honest remote residue.

If configured native providers are unavailable, `--matrix-transport-only` exercises
real Matrix membership, three real senders, expected application timeout replies,
same-state restart/dedup and a new post-restart reply. This is reported as a transport
and expected-timeout cell, **not successful native/Matrix E2E**. Diagnostic helpers
only emit fixed categories/counts/public endpoint metadata, never native response bodies.

After the candidate is stable and exact-clean, the assigned build owner can invoke
`bash staging/t050_n/gate.sh` once with `T050_BUILD_LOCK` and `CARGO_TARGET_DIR` set
to the approved global lock/shared target, and `RUSTC` set to the Rust 1.94 rustup
shim. It records six independent exits, versions and non-secret image/lock hashes.
It does not launch any live flow; per-stage logs are private. The former standalone
crate is not part of this graph. The full gate includes `test-support` so the new
HTTP/streaming regression fixture is exercised, using hyphenated Cargo binary bindings.

Do not run against production or touch T046/R23. A missing real credential is a precise
external input, not a successful Matrix E2E. Fixture PASS, native PASS, Matrix PASS,
recovery, refusal and cleanup are separate results. Do not blindly repeat a failed run.
