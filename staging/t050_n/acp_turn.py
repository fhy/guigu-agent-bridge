"""Bounded native ACP diagnostic, stages/counts only, never raw peer text."""
import argparse
import json
import os
import selectors
import tempfile
import time

from owned import Owned, subreaper

TEXT_CATEGORIES = ("authentication", "unauthorized", "invalid_api_key", "401", "403", "429",
                   "model_not_found", "not supported", "model", "rate limit", "billing",
                   "credentials", "sign in", "login", "error", "timeout", "permission")


def exchange(owner, selector, request_id, method, params, report, timeout=15):
    owner.child.stdin.write((json.dumps({"jsonrpc": "2.0", "id": request_id,
                             "method": method, "params": params}) + "\n").encode())
    owner.child.stdin.flush()
    pending = b""
    text_tail = ""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        ready = selector.select(timeout=0.2)
        if not ready:
            continue
        if ready[0][0].data == "stderr":
            chunk = os.read(owner.child.stderr.fileno(), 65536)
            if not chunk:
                selector.unregister(owner.child.stderr)
                continue
            for category in ("401", "403", "429", "model", "ECONNREFUSED", "timeout",
                             "API key", "retry", "authentication", "stream", "error"):
                if category.lower().encode() in chunk.lower() and category not in report["stderr_categories"]:
                    report["stderr_categories"].append(category)
            continue
        chunk = os.read(owner.child.stdout.fileno(), 65536)
        if not chunk:
            raise RuntimeError("peer-exit")
        pending += chunk
        if len(pending) > 1048576:
            raise RuntimeError("frame-size")
        lines = pending.split(b"\n")
        pending = lines.pop()
        for line in lines:
            message = json.loads(line)
            if message.get("method"):
                report["notifications"] += 1
                method = message.get("method")
                for known_method in ("session/update", "session/request_permission", "fs/read_text_file",
                                     "fs/write_text_file", "terminal/create"):
                    if method == known_method and method not in report["method_classes"]:
                        report["method_classes"].append(method)
                if "id" in message:
                    refusal = {"jsonrpc": "2.0", "id": message["id"],
                               "error": {"code": -32601, "message": "test tool unavailable"}}
                    owner.child.stdin.write((json.dumps(refusal) + "\n").encode())
                    owner.child.stdin.flush()
                else:
                    update = message.get("params", {}).get("update", {})
                    if update.get("sessionUpdate") == "agent_message_chunk":
                        content = update.get("content", {})
                        text = content.get("text", "")
                        report["text_bytes"] += len(text.encode())
                        observed = text_tail + text
                        report["marker_seen"] |= "T050_N_NATIVE_OK" in observed
                        for category in TEXT_CATEGORIES:
                            if category in observed.lower() and category not in report["text_categories"]:
                                report["text_categories"].append(category)
                        text_tail = observed[-64:]
                    for update_class in ("available_commands_update", "agent_thought_chunk",
                                         "agent_message_chunk", "tool_call", "tool_call_update"):
                        if update.get("sessionUpdate") == update_class and update_class not in report["update_classes"]:
                            report["update_classes"].append(update_class)
            elif message.get("id") == request_id:
                if "error" in message:
                    report["rpc_error_code"] = message["error"].get("code")
                    raise RuntimeError("rpc-error")
                return message["result"]
    raise RuntimeError("deadline")


def turn(agent, observe=False):
    subreaper()
    os.umask(0o077)
    report = {"stage": "initialize", "notifications": 0, "text_bytes": 0,
              "marker_seen": False, "prompts": 0, "stderr_categories": [],
              "method_classes": [], "update_classes": [], "text_categories": []}
    with tempfile.TemporaryDirectory(prefix="t050-n-native-turn-") as root:
        arguments = ["--acp", "--safe-mode", "--advisor", "off", "--approval-mode", "plan"] if os.path.basename(agent) == "qwen" else []
        auth_method = "openai" if arguments else "api-key"
        environment = dict(os.environ, INITIAL_AGENT_MODE="read-only", NO_BROWSER="1")
        with Owned([agent] + arguments, env=environment,
                   cwd=root, pipe=True, stderr_pipe=True) as owner:
            with selectors.DefaultSelector() as selector:
                selector.register(owner.child.stdout, selectors.EVENT_READ, "stdout")
                selector.register(owner.child.stderr, selectors.EVENT_READ, "stderr")
                try:
                    exchange(owner, selector, 1, "initialize", {"protocolVersion": 1,
                             "clientCapabilities": {},
                             "clientInfo": {"name": "t050-n-diagnostic", "version": "1"}}, report)
                    report["stage"] = "authenticate"
                    exchange(owner, selector, 2, "authenticate", {"methodId": auth_method}, report)
                    report["stage"] = "session"
                    session = exchange(owner, selector, 3, "session/new",
                                       {"cwd": root, "mcpServers": []}, report)
                    models = session.get("models", {})
                    known = {"gpt-6.1-sol", "gpt-6.1", "gpt-5.5", "gpt-5.6-sol",
                             "gpt-5.4", "qwen3-coder-plus", "coder-model"}
                    report["known_current_model"] = models.get("currentModelId") if models.get("currentModelId") in known else "other-or-missing"
                    report["known_available_models"] = [item.get("modelId") for item in models.get("availableModels", [])
                                                        if item.get("modelId") in known]
                    if observe:
                        report["category"] = "session-metadata"
                        report["cleanup"] = owner.stop()
                        return report
                    report["stage"] = "prompt"
                    report["prompts"] = 1
                    result = exchange(owner, selector, 4, "session/prompt", {
                        "sessionId": session["sessionId"], "prompt": [{"type": "text",
                        "text": "Reply exactly T050_N_NATIVE_OK. Do not use tools."}]}, report, 45)
                    report["category"] = "prompt-result"
                    report["stop_end_turn"] = result.get("stopReason") == "end_turn"
                    report["structured_result"] = "taskResult" in result
                except (RuntimeError, OSError, ValueError, KeyError) as error:
                    report["category"] = str(error) if str(error) in (
                        "peer-exit", "frame-size", "rpc-error", "deadline") else "protocol-error"
            report["cleanup"] = owner.stop()
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("agent")
    parser.add_argument("--observe-only", action="store_true")
    args = parser.parse_args()
    print(json.dumps(turn(args.agent, args.observe_only)))
