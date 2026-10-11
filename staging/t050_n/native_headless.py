"""Isolate native agent/provider from ACP; never expose stdout/stderr contents."""
import json
import os
from pathlib import Path
import selectors
import tempfile
import time

from owned import Owned, subreaper


def main():
    subreaper()
    os.umask(0o077)
    report = {"stdout_bytes": 0, "stderr_bytes": 0, "marker_seen": False,
              "categories": [], "prompt_count": 1}
    with tempfile.TemporaryDirectory(prefix="t050-n-headless-") as root:
        argv = [str(Path.home() / ".local/bin/qwen"), "--safe-mode", "--advisor", "off",
                "--approval-mode", "plan", "-p", "Reply exactly T050_N_NATIVE_OK. Do not use tools."]
        with Owned(argv, cwd=root, pipe=True, stderr_pipe=True) as owner:
            owner.child.stdin.close()
            with selectors.DefaultSelector() as selector:
                selector.register(owner.child.stdout, selectors.EVENT_READ, "stdout")
                selector.register(owner.child.stderr, selectors.EVENT_READ, "stderr")
                deadline = time.monotonic() + 45
                while selector.get_map() and time.monotonic() < deadline:
                    for key, _ in selector.select(0.2):
                        chunk = os.read(key.fileobj.fileno(), 65536)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        report[key.data + "_bytes"] += len(chunk)
                        report["marker_seen"] |= b"T050_N_NATIVE_OK" in chunk
                        for word in ("authentication", "401", "403", "429", "model", "error",
                                     "timeout", "trust", "workspace", "recording", "api key"):
                            if word.encode() in chunk.lower() and word not in report["categories"]:
                                report["categories"].append(word)
            report["natural_exit"] = owner.exited()
            report["cleanup"] = owner.stop()
    print(json.dumps(report))


if __name__ == "__main__":
    main()
