"""Owned native protocol restart probe; fixed classes only, no session output."""
import json
import os
from pathlib import Path
import selectors
import sys
import tempfile

from acp_turn import exchange
from owned import Owned, subreaper


def connection(agent, root, report, old_session=None):
    owner = Owned([agent], cwd=root, pipe=True, stderr_pipe=True)
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(owner.child.stdout, selectors.EVENT_READ, "stdout")
            selector.register(owner.child.stderr, selectors.EVENT_READ, "stderr")
            report["stage"] = "initialize"
            initialized = exchange(owner, selector, 1, "initialize", {
                "protocolVersion": 1, "clientCapabilities": {},
                "clientInfo": {"name": "t050-n-restart-diagnostic", "version": "1"}}, report)
            methods = [item["id"] for item in initialized.get("authMethods", [])
                       if item.get("id") in ("api-key", "openai")]
            if initialized.get("authMethods"):
                if len(methods) != 1:
                    raise RuntimeError("authentication-shape")
                report["stage"] = "authenticate"
                exchange(owner, selector, 2, "authenticate", {"methodId": methods[0]}, report)
            report["stage"] = "resume" if old_session else "new"
            method = "session/resume" if old_session else "session/new"
            params = {"cwd": str(root), "mcpServers": []}
            if old_session:
                params["sessionId"] = old_session
            response = exchange(owner, selector, 3, method, params, report)
            session = old_session or response["sessionId"]
            if old_session:
                report["resume"] = "PASS"
                return session
            report["stage"] = "prompt"
            report["prompts"] += 1
            response = exchange(owner, selector, 4, "session/prompt", {
                "sessionId": session, "prompt": [{"type": "text", "text":
                "Reply exactly T050_N_NATIVE_OK. Do not use tools."}]}, report, timeout=45)
            report["first_end_turn"] = response.get("stopReason") == "end_turn"
            return session
    finally:
        report.setdefault("cleanup", []).append(owner.stop())


def probe(agent):
    os.umask(0o077)
    subreaper()
    report = {"stage": "start", "notifications": 0, "text_bytes": 0,
              "marker_seen": False, "prompts": 0, "stderr_categories": [],
              "method_classes": [], "update_classes": [], "text_categories": []}
    with tempfile.TemporaryDirectory(prefix="t050-n-acp-restart-", dir=Path.home() / ".cache") as root:
        try:
            session = connection(agent, root, report)
            connection(agent, root, report, session)
            report["category"] = "resume-result"
        except (RuntimeError, OSError, ValueError, KeyError) as error:
            allowed = ("peer-exit", "frame-size", "rpc-error", "deadline", "authentication-shape")
            report["category"] = str(error) if str(error) in allowed else "protocol-error"
    return report


if __name__ == "__main__":
    print(json.dumps(probe(sys.argv[1])))
