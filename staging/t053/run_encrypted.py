"""Bounded actual SDK/application flow. Reports never contain account secrets."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import selectors
import signal
import sqlite3
import sys
import tempfile
import time
import urllib.parse
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "t050_n"))
from app import Application, http
from materials import classify, request, SERVER, USERS, ROLES, Refusal
from owned import Owned, subreaper


class Recipient:
    def __init__(self, executable, fields, identity, store):
        self.owner = Owned([executable], env=dict(os.environ, T053_TOKEN=fields["MATRIX_ACCESS_TOKEN"],
            T053_USER=identity["user_id"], T053_DEVICE=identity["device_id"], T053_STORE=str(store)), pipe=True)
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.owner.child.stdout, selectors.EVENT_READ)
        self.buffer = b""
        self.read()

    def read(self, deadline=50):
        limit = time.monotonic() + deadline
        while time.monotonic() < limit:
            if b"\n" in self.buffer:
                line, self.buffer = self.buffer.split(b"\n", 1)
                try:
                    value = json.loads(line)
                except ValueError:
                    raise Refusal("sdk-response-format") from None
                if value.get("failure"):
                    raise Refusal("sdk-" + value["failure"])
                return value
            if len(self.buffer) > 1048576:
                raise Refusal("sdk-response-bound")
            if self.selector.select(min(1, max(0, limit-time.monotonic()))):
                chunk = os.read(self.owner.child.stdout.fileno(), 65536)
                if not chunk:
                    raise Refusal("sdk-child-exit")
                self.buffer += chunk
        raise Refusal("sdk-child-timeout")

    def command(self, room, action="sync", **kwargs):
        data = json.dumps(dict(room=room, action=action, **kwargs)).encode()+b"\n"
        self.owner.child.stdin.write(data)
        self.owner.child.stdin.flush()
        return self.read()

    def close(self):
        self.selector.close()
        return self.owner.stop()


def database(path, query, arguments=()):
    with sqlite3.connect("file:"+str(path)+"?mode=ro", uri=True) as connection:
        return connection.execute(query, arguments).fetchall()


class Flow:
    def __init__(self, args, root, report):
        self.args, self.root, self.report = args, root, report
        self.recipients, self.room, self.tokens, self.app = [], None, [], None
        self.sent, self.received = {}, {}
        self.count = 0

    def api(self, path, role=0, body=None, method=None):
        self.report["helper_requests"] += 1
        return request(SERVER, "/_matrix/client/v3"+path, token=self.tokens[role], body=body,
                       method=method or ("GET" if body is None else "POST"))[1]

    def setup(self):
        summaries, accounts = classify(Path.home()/".config/guigu-agent-bridge")
        self.report["materials"] = summaries
        if any(not item or not item.get("MATRIX_ACCESS_TOKEN") for item in accounts):
            raise Refusal("session-material-missing")
        self.tokens = [item["MATRIX_ACCESS_TOKEN"] for item in accounts]
        identities = []
        for role, user in enumerate(USERS):
            identity = self.api("/account/whoami", role)
            device = identity.get("device_id")
            if identity.get("user_id") != user or identity.get("is_guest") or not device or device.startswith("T046"):
                raise Refusal("independent-session-identity")
            identities.append(identity)
            keys = self.api("/keys/query", role, {"device_keys":{user:[device]}})
            if keys.get("failures") or not isinstance(keys.get("device_keys", {}).get(user), dict):
                raise Refusal("key-query-incomplete")
            present = bool(keys["device_keys"][user].get(device))
            if role and present and not self.args.recipient_store_root:
                raise Refusal("recipient-store-required-for-existing-keys")
            self.report.setdefault("identities", []).append({"role":ROLES[role], "user":user,
                "device":device, "existing_keys":present})
        # Existing, explicitly run-owned T050-N primary store is retained in place.
        # No copy, key replacement or unknown tuple adoption occurs.
        self.app = Application(self.root, self.args.binary, self.args.agent,
            agent_args=[], crypto_store=self.args.primary_store)
        self.app.env["T053_MATRIX_TOKEN"] = self.tokens[0]
        self.app.env["T053_MATRIX_DEVICE"] = identities[0]["device_id"]
        for role in range(1, 4):
            store = (Path(self.args.recipient_store_root)/ (ROLES[role]+"-crypto")) if self.args.recipient_store_root else self.root/(ROLES[role]+"-crypto")
            if not store.exists():
                store.mkdir(mode=0o700)
            self.recipients.append(Recipient(self.args.sdk, accounts[role], identities[role], store))
        result = self.api("/createRoom", body={"visibility":"private", "preset":"private_chat",
            "creation_content":{"m.federate":False}, "name":"T053 encrypted "+self.root.name,
            "invite":list(USERS[1:]), "initial_state":[{"type":"m.room.encryption", "state_key":"",
                "content":{"algorithm":"m.megolm.v1.aes-sha2"}}]})
        self.room = result.get("room_id")
        if not self.room:
            raise Refusal("create-result")
        self.report["room"] = self.room
        encoded = urllib.parse.quote(self.room, safe="")
        for role in range(1, 4):
            self.api("/join/"+encoded, role, {})
        members = self.api("/rooms/"+encoded+"/joined_members")["joined"]
        encryption = self.api("/rooms/"+encoded+"/state/m.room.encryption/")
        if set(members) != set(USERS) or encryption.get("algorithm") != "m.megolm.v1.aes-sha2":
            raise Refusal("membership-encryption")
        self.report.update(memberships=4, encryption="m.megolm.v1.aes-sha2", room_create=1,
                           login_requests=0, issuances=0, primary_store="assigned-existing-in-place")
        for recipient in self.recipients:
            recipient.command(self.room)
        self.configure(self.args.agent)

    def configure(self, agent):
        self.app.agent, self.app.agent_args = agent, []
        matrix = f'''enabled = true
homeserver = {json.dumps(SERVER)}
user_id = {json.dumps(USERS[0])}
device_id = "{{env:T053_MATRIX_DEVICE}}"
access_token = "{{env:T053_MATRIX_TOKEN}}"
allowed_users = {json.dumps(list(USERS[1:]))}
admin_users = {json.dumps([USERS[3]])}
admin_rooms = {json.dumps([self.room])}
monitor_room = {json.dumps(self.room)}
device_trusted = true
crypto_store_path = {json.dumps(str(self.app.crypto_store))}
[transports.matrix.routes.rooms]
{json.dumps(self.room)} = "worker"'''
        self.app.write_config(matrix, timeout=180)

    def send(self, role, label, body=None, transaction=None):
        if self.count >= 96:
            raise Refusal("message-budget")
        self.count += 1
        transaction = transaction or uuid.uuid4().hex
        body = body or "Reply exactly "+label+". Do not use tools."
        result = self.recipients[role-1].command(self.room, "send", body=body, transaction=transaction)
        event = result["event"]
        raw = self.api("/rooms/"+urllib.parse.quote(self.room, safe="")+"/event/"+urllib.parse.quote(event, safe=""), role)
        if raw.get("type") != "m.room.encrypted":
            raise Refusal("plaintext-request")
        self.sent[label] = (role, event, transaction, body)
        return event

    def receive(self, event, label=None, timeout=185, expected=None):
        deadline = time.monotonic()+timeout
        parts = {}
        while time.monotonic() < deadline:
            self.app.drain()
            for recipient in self.recipients:
                for value in recipient.command(self.room)["events"]:
                    if not value["encrypted"]:
                        raise Refusal("plaintext-reply")
                    self.received[value["event"]] = value
                    if value.get("reply") == event:
                        parts[value["event"]] = value["body"]
            text = "".join(parts.values())
            if (expected is not None and text == expected) or (label is not None and label in text):
                for reply in parts:
                    raw = self.api("/rooms/"+urllib.parse.quote(self.room, safe="")+"/event/"+urllib.parse.quote(reply, safe=""))
                    if raw.get("type") != "m.room.encrypted":
                        raise Refusal("plaintext-server-reply")
                return {"parts":len(parts), "bytes":len(text.encode()), "sha256":hashlib.sha256(text.encode()).hexdigest()}
            time.sleep(0.1)
        raise Refusal("encrypted-receive-timeout")

    def exercise(self):
        self.setup()
        self.app.start(timeout=90)
        self.report["initial_ready"] = True
        # Readiness is the process/HTTP predicate; allow one bounded encrypted
        # sync cycle to import the room state and megolm session before the first
        # sender request. This is not an unbounded retry.
        time.sleep(8)
        for role in range(1, 4):
            label = "T053_REAL_MODEL_"+str(role)
            event = self.send(role, label)
            self.report.setdefault("model_replies", []).append(self.receive(event, label))
        self.report["model_prompts"] = 3
        before = database(self.app.database, "SELECT COUNT(*) FROM tasks")[0][0]
        self.app.stop()
        self.configure(self.args.deterministic)
        self.app.start(timeout=90)
        self.report["restart_cycles"] = 1
        for label, (role, event, transaction, body) in list(self.sent.items()):
            if self.send(role, label, body, transaction) != event:
                raise Refusal("replay-event-drift")
        time.sleep(3)
        if database(self.app.database, "SELECT COUNT(*) FROM tasks")[0][0] != before:
            raise Refusal("replay-task-duplicate")
        self.report["replay_dedup"] = True
        event = self.send(2, "T053_POST_RESTART", "T053_SHORT")
        self.report["post_restart"] = self.receive(event, "T053_POST_RESTART")
        event = self.send(1, "T053_LONG", "T053_LONG")
        self.report["long_output"] = self.receive(event, expected="L"*307200)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            rows = database(self.app.database,
                "SELECT total_bytes,segment_count,acknowledged_cursor,state FROM acp_output_sets ORDER BY created_at")
            if rows and rows[-1][3] == "cleaned":
                break
            time.sleep(0.25)
        self.report["durable_rows"] = database(self.app.database,
            "SELECT total_bytes,segment_count,acknowledged_cursor,state FROM acp_output_sets ORDER BY created_at")
        if not self.report["durable_rows"] or self.report["durable_rows"][-1][3] != "cleaned":
            raise Refusal("durable-drain-not-cleaned")
        # Observer commands use the same encrypted room and actual admin handler.
        self.send(3, "T053_STATUS", "/status")
        self.report["health_endpoint"] = "/ready"
        self.report["health"] = http(f"http://127.0.0.1:{self.app.health}/ready")[0]
        self.report["readiness"] = http(f"http://127.0.0.1:{self.app.health}/ready")[0]
        self.report["application_sends"] = self.count

    def cleanup(self):
        if self.app:
            self.app.stop()
            self.report["app_cleanup"] = self.app.cleanups
        self.report["sdk_cleanup"] = [recipient.close() for recipient in self.recipients]
        if self.room:
            outcomes = []
            for role in range(4):
                try:
                    self.api("/rooms/"+urllib.parse.quote(self.room, safe="")+"/leave", role, {})
                    outcomes.append({"role":ROLES[role], "left":True})
                except Refusal as error:
                    outcomes.append({"role":ROLES[role], "failure":str(error)})
            self.report["room_cleanup"] = outcomes
        self.report["residue"] = "private DB/config/crypto/evidence and room/device history retained"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--recipient-store-root")
    for name in ("binary", "sdk", "agent", "deterministic", "primary-store"):
        parser.add_argument("--"+name, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    subreaper()
    root = Path(tempfile.mkdtemp(prefix="t053-encrypted-", dir=Path.home()/".cache"))
    report = {"helper_requests":0, "root":str(root), "source":os.environ.get("T053_SOURCE", "unrecorded")}
    for name in ("binary", "sdk", "agent", "deterministic"):
        report[name+"_sha256"] = hashlib.sha256(Path(getattr(args,name)).read_bytes()).hexdigest()
    flow = Flow(args, root, report)
    code = 0
    try:
        flow.exercise()
    except Refusal as error:
        report["failure"] = str(error)
        code = 3
    except BaseException:
        report["failure"] = "private-runner-error"
        code = 4
    finally:
        try:
            flow.cleanup()
        except BaseException:
            report["cleanup_failure"] = "owned-cleanup-failed"
            code = 5
        (root/"result.json").write_text(json.dumps(report, indent=2))
    print(json.dumps({"root":str(root), "exit":code, "failure":report.get("failure")}))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
