"""Native protocol metadata only; no prompt, tokens or raw peer responses."""
import argparse
import json
import os
import selectors
import tempfile
import time

from owned import Owned, subreaper

KNOWN = {"api-key", "openai", "openai-api-key", "qwen-oauth", "qwen_oauth",
         "oauth-personal", "chat-gpt", "chat-gpt-device-code", "gateway"}


def probe(agent, arguments):
    subreaper()
    os.umask(0o077)
    report = {"stage": "initialize", "prompts": 0}
    with tempfile.TemporaryDirectory(prefix="t050-n-acp-probe-") as root:
        with Owned([agent] + arguments, cwd=root, pipe=True) as owner:
            call = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
                "protocolVersion": 1, "clientCapabilities": {},
                "clientInfo": {"name": "t050-n-probe", "version": "1"}}}
            owner.child.stdin.write((json.dumps(call) + "\n").encode())
            owner.child.stdin.flush()
            with selectors.DefaultSelector() as selector:
                selector.register(owner.child.stdout, selectors.EVENT_READ)
                deadline = time.monotonic() + 20
                pending = b""
                while time.monotonic() < deadline:
                    if not selector.select(timeout=0.2):
                        continue
                    chunk = os.read(owner.child.stdout.fileno(), 65536)
                    if not chunk:
                        report["category"] = "peer-exit"
                        break
                    pending += chunk
                    if len(pending) > 1048576:
                        report["category"] = "frame-size"
                        break
                    lines = pending.split(b"\n")
                    pending = lines.pop()
                    for line in lines:
                        response = json.loads(line)
                        if response.get("id") != 1:
                            continue
                        result = response.get("result", {})
                        methods = result.get("authMethods", [])
                        report["methods"] = [{"id": item["id"] if item.get("id") in KNOWN else "other",
                                              "type": item.get("type") if item.get("type") in (None, "agent", "terminal", "env") else "other"}
                                             for item in methods]
                        report["category"] = "initialize-result"
                        return report, owner.stop()
                report.setdefault("category", "initialize-timeout")
            return report, owner.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("agent")
    parser.add_argument("args", nargs="*")
    args = parser.parse_args()
    report, cleanup = probe(args.agent, args.args)
    print(json.dumps({"observation": report, "cleanup": cleanup}))
