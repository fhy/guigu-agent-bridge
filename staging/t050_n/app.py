"""Real application config, direct child startup and bounded HTTP readiness."""
import json
import os
from pathlib import Path
import secrets
import socket
import time
import urllib.error
import urllib.request

from materials import Refusal
from owned import Owned


def port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def http(url, token=None, body=None, timeout=5):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    data = None if body is None else json.dumps(body).encode()
    call = urllib.request.Request(url, data=data, headers=headers)
    try:
        with urllib.request.urlopen(call, timeout=timeout) as response:
            return response.status, response.read(1048576)
    except urllib.error.HTTPError as error:
        try:
            return error.code, error.read(1048576)
        finally:
            error.close()
    except OSError:
        raise Refusal("http-unavailable") from None


def closed(port_number):
    with socket.socket() as probe:
        probe.settimeout(0.5)
        return probe.connect_ex(("127.0.0.1", port_number)) != 0


class Application:
    def __init__(self, root, executable, agent, agent_args=None, crypto_store=None):
        self.root = Path(root)
        self.executable = executable
        self.agent = agent
        self.crypto_store = Path(crypto_store) if crypto_store else self.root / "crypto"
        self.agent_args = agent_args if agent_args is not None else (
            ["--acp", "--safe-mode", "--advisor", "off", "--approval-mode", "plan"]
            if Path(agent).name == "qwen" else [])
        self.health, self.a2a = port(), port()
        while self.a2a == self.health:
            self.a2a = port()
        self.token = secrets.token_urlsafe(32)
        self.env = dict(os.environ, T050_N_TOKEN=self.token, RUST_LOG="guigu_agent_bridge=warn")
        self.owner = None
        self.cleanups = []
        self.refusals = []
        self.config = self.root / "bridge.toml"
        self.database = self.root / "state.db"
        self.write_config()

    def write_config(self, matrix="enabled = false", timeout=120, task_timeout=None):
        text = f'''[bridge]
database = {json.dumps(str(self.database))}
session_root = {json.dumps(str(self.root / "sessions"))}
health_bind = "127.0.0.1:{self.health}"
default_timeout_seconds = {task_timeout or timeout}
shutdown_timeout_seconds = 3
[runtime]
legacy_acp_results = true
max_wall_seconds = {timeout}
max_inactivity_seconds = {timeout}
[transports.matrix]
{matrix}
[transports.a2a]
enabled = true
listen = "tcp:127.0.0.1:{self.a2a}"
allow_private_plaintext = true
exposed_endpoints = ["worker"]
[transports.a2a.peers.test]
url = "http://127.0.0.1:9/a2a/worker/rpc"
expected_peer_id = "t050-n-test"
token = "{{env:T050_N_TOKEN}}"
allowed_targets = ["worker"]
[agents.worker]
transport = "acp"
command = {json.dumps(self.agent)}
args = {json.dumps(self.agent_args)}
workspace = {json.dumps(str(self.root))}
enabled = true
'''
        self.config.write_text(text)
        self.config.chmod(0o600)

    def start(self, timeout=45):
        self.owner = Owned([self.executable, str(self.config)], env=self.env,
                           cwd=self.root, pipe=True)
        self.owner.child.stdin.close()
        os.set_blocking(self.owner.child.stdout.fileno(), False)
        deadline = time.monotonic() + timeout
        try:
            while time.monotonic() < deadline:
                self.drain()
                if self.owner.exited():
                    raise Refusal("application-exited-before-ready")
                try:
                    status, _ = http(f"http://127.0.0.1:{self.health}/ready")
                    if status == 200:
                        return
                except Refusal:
                    pass
                time.sleep(0.15)
            raise Refusal("readiness-timeout")
        except BaseException:
            self.stop()
            raise

    def drain(self):
        if self.owner is None:
            return
        try:
            raw = os.read(self.owner.child.stdout.fileno(), 65536)
        except BlockingIOError:
            return
        for category in ("matrix-auth-failed", "matrix-user-mismatch", "matrix-device-missing",
                         "matrix-device-mismatch", "matrix-unsafe-store-path", "matrix-unsafe-store-ancestor",
                         "matrix-store-corrupt", "matrix-store-binding-mismatch", "matrix-crypto-initialization-failed",
                         "matrix-device-key-upload-failed", "matrix-startup-timeout", "recovery-blocked"):
            if category.encode() in raw and category not in self.refusals:
                self.refusals.append(category)

    def stop(self):
        if self.owner is not None:
            self.drain()
            cleanup = self.owner.stop()
            cleanup["fixed_refusals"] = list(self.refusals)
            self.cleanups.append(cleanup)
            self.owner = None
            if not closed(self.health) or not closed(self.a2a):
                raise Refusal("listener-residue")

    def rpc(self, method, params, request_id="t050-n-query"):
        status, raw = http(f"http://127.0.0.1:{self.a2a}/a2a/worker/rpc",
                           self.token, {"jsonrpc": "2.0", "id": request_id,
                                        "method": method, "params": params})
        try:
            return status, json.loads(raw)
        except ValueError:
            raise Refusal("rpc-response-format") from None

    def terminal(self, task, timeout=130):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status, value = self.rpc("tasks/get", {"id": task})
            if status != 200 or "result" not in value:
                raise Refusal("task-get")
            result = value["result"]
            if result["status"] not in ("submitted", "working"):
                return result
            time.sleep(0.3)
        raise Refusal("task-timeout")
