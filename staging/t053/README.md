# T053 encrypted staging runner

This runner is intentionally private and consumes the four assigned account env
files only through an inherited environment/worker process. It creates fresh
private DB/config/crypto-store roots under a run-owned cache directory, starts
owned application children directly, and does not inspect protected tuples. The
SDK helper is built from this candidate and authenticates each assigned device by
`whoami`; no password login, key upload, device replacement, or store handover is
performed. The helper accepts only bounded JSON commands and emits redacted facts.

`run_encrypted.py` is the bounded final flow: creates one invite-only encrypted
room, checks `m.room.encryption` and four memberships, sends three account inputs,
receives decrypted replies, restarts the same application/SDK state, replays the
same transactions, and requires a fresh encrypted post-restart reply. The long
output row now stops only after a durable set has a non-zero acknowledged cursor
and remaining segments, records the stable transaction IDs, restarts the same
state, and proves the remaining encrypted parts drain without transaction drift.
Observer `/status` and `/trace` replies are also collected through the encrypted
SDK path and asserted separately from `/ready` health/readiness. Durable 300 KiB
ACP coverage is driven by the real `acp-mock-backend` through ordinary application
wiring; it is never claimed from a plaintext or synthetic Matrix response.
