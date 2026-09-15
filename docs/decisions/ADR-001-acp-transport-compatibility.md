# ADR-001: ACP Transport Compatibility

## Status

Accepted for implementation planning.

## Context

ACP agents communicate JSON-RPC over stdio, but transport details and supported message schemas can evolve independently from Bridge session logic. A compatibility claim must cover initialization, session creation, streaming prompts, permission requests, cancellation, and shutdown, not only successful child-process startup.

Current `@agentclientprotocol/codex-acp` uses newline-delimited JSON (`ndJsonStream`) on its external ACP stdio connection. Its internal connection to the Codex App Server uses separate framing; that internal protocol must not be confused with the ACP-facing transport. A real test with `opencode-chat-bridge` completed `initialize`, `session/new`, and a Codex prompt over JSONL.

## Decision

The Rust ACP Adapter will:

- Treat JSONL as the required ACP v1 stdio codec and verify it against OpenCode, Ferrum, Qwen/CodeBuddy where applicable, and a pinned current Codex ACP release.
- Separate JSON-RPC messages, ACP schema mapping, and byte framing into distinct modules.
- Use bounded initialization and request deadlines so protocol mismatches cannot hang indefinitely.
- Report process startup, framing, schema negotiation, authentication, and Agent execution failures separately.
- Allow an explicit alternative framing codec later when a supported backend demonstrably requires one; no heuristic switching occurs during an active session.
- Pin backend versions in deployment configuration and include the compatible backend version in `backendId`.

## Required Tests

- JSONL messages fragmented across arbitrary reads and multiple messages coalesced into one read.
- UTF-8 messages and configurable maximum frame sizes.
- Malformed JSON, oversized frames, partial-frame EOF, and child exit propagation.
- Initialization timeout and clean child-process termination.
- Mock backend coverage for initialize, session creation/resume, prompt streaming, permission requests, cancellation, and shutdown.
- An opt-in integration test or release check against a pinned current `@agentclientprotocol/codex-acp`.
- Regression tests for any alternative codec before that codec is declared supported.

## Consequences

The first implementation remains simple and compatible with current Codex ACP while preserving a clean extension point for future transports. Compatibility is evidence-based and versioned rather than inferred from package names or internal implementation details.
