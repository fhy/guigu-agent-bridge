# ADR-001: ACP Transport Framing Compatibility

## Status

Accepted for implementation planning.

## Context

The current upstream `opencode-chat-bridge` ACP client writes one JSON-RPC object per line and splits stdout on newline. It uses `@agentclientprotocol/sdk` 0.13.x. Newer ACP agents, including current `@agentclientprotocol/codex-acp`, may use `Content-Length` framed JSON-RPC messages.

JSON-RPC defines message contents but does not make newline-delimited transport interchangeable with `Content-Length` framing. A client hard-coded to JSONL can start the child process successfully while both sides wait forever for incompatible message boundaries. This presents as an initialization hang rather than a useful protocol error.

## Decision

The Rust ACP Adapter will separate JSON-RPC message handling from byte framing and provide at least two codecs:

- `JsonLinesCodec`: one UTF-8 JSON object followed by `\n`.
- `ContentLengthCodec`: ASCII headers terminated by `\r\n\r\n`, followed by the exact declared body length.

Backend configuration may explicitly select a codec. An `auto` mode may inspect the first valid inbound frame, but it must use a bounded initialization deadline and must not silently switch codecs after a session starts.

The Adapter must report framing and schema negotiation failures separately from process startup, authentication, and Agent execution failures. Logs may include sizes, request IDs, and method names, but not prompt contents, credentials, or authorization headers by default.

## Required Tests

- JSONL frames split across arbitrary reads.
- Multiple JSONL frames in one read.
- `Content-Length` headers and bodies split across arbitrary reads.
- Multiple `Content-Length` frames in one read.
- UTF-8 bodies where byte length differs from character count.
- Invalid, missing, negative, oversized, and conflicting content lengths.
- Unexpected bytes before a header and malformed JSON bodies.
- Initialization timeout when client and backend use different framing.
- Mock backends for both framing modes completing initialize, session creation, prompt streaming, permission requests, cancellation, and shutdown.
- Process exit and partial-frame EOF propagation without deadlock.

Fuzz or property tests should feed arbitrary chunk boundaries to both codecs. Frame and message size limits must be configurable and bounded before allocation.

## Consequences

ACP backend support is no longer coupled to one SDK generation. The transport layer becomes slightly more complex, but framing errors become testable and diagnosable, and Codex ACP can be supported without a permanent external framing proxy.

A local JSONL-to-`Content-Length` proxy may be used during bootstrap, but it is a temporary deployment workaround and not the final project architecture.

## Reference Finding

At the time of this decision, upstream `ominiverdi/opencode-chat-bridge` `main` still writes `JSON.stringify(msg) + "\n"`, parses with `buffer.split("\n")`, and depends on `@agentclientprotocol/sdk ^0.13.1`. Upgrading to that current upstream version alone does not provide `Content-Length` compatibility.
