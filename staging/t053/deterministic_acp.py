#!/usr/bin/env python3
"""Controllable real ACP process: deterministic text, never a model claim."""
import json
import sys


def emit(value):
    print(json.dumps(value), flush=True)


for line in sys.stdin:
    message = json.loads(line)
    method, params, identity = message.get("method"), message.get("params", {}), message.get("id")
    result = {}
    if method == "initialize":
        result = {"protocolVersion":1, "agentInfo":{"name":"t053-deterministic", "version":"1"}, "authMethods":[]}
    elif method in ("session/new", "session/resume", "session/load"):
        result = {"sessionId":params.get("sessionId", "t053-session")}
    elif method == "session/prompt":
        prompt = "".join(part.get("text", "") for part in params.get("prompt", []))
        body = "L"*307200 if "T053_LONG" in prompt else "T053_POST_RESTART"
        for offset in range(0, len(body), 4096):
            emit({"jsonrpc":"2.0", "method":"session/update", "params":{
                "sessionId":params["sessionId"], "update":{"sessionUpdate":"agent_message_chunk",
                    "content":{"type":"text", "text":body[offset:offset+4096]}}}})
        result = {"stopReason":"end_turn"}
    if identity is not None:
        emit({"jsonrpc":"2.0", "id":identity, "result":result})
