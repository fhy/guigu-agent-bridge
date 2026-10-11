"""Independent real Matrix flow. Secrets are consumed only in this runner."""
import json
from pathlib import Path
import sqlite3
import time
import urllib.parse
import uuid

from materials import Refusal, ROLES, SERVER, USERS, request


class MatrixFlow:
    def __init__(self, accounts, report, owned_store=None):
        self.accounts = accounts
        self.owned_store = owned_store
        self.report = report
        self.tokens = []
        self.room = None
        self.room_issued = False
        self.device = "T050-N-" + uuid.uuid4().hex
        self.report.update({"requests": 0, "login_requests": 0,
                            "room_create_requests": 0, "send_requests": 0})

    def call(self, path, role=0, body=None, method=None):
        self.report["requests"] += 1
        token = self.tokens[role] if len(self.tokens) > role else None
        return request(SERVER, "/_matrix/client/v3" + path, token, body, method)[1]

    def authenticate(self):
        if len(self.accounts) != 4 or any(item is None for item in self.accounts):
            raise Refusal("matrix-material-unavailable")
        if all(item.get("MATRIX_ACCESS_TOKEN") for item in self.accounts):
            self.tokens = [item["MATRIX_ACCESS_TOKEN"] for item in self.accounts]
            primary = None
            for role, expected in enumerate(USERS):
                identity = self.call("/account/whoami", role)
                if identity.get("user_id") != expected or identity.get("is_guest", False):
                    raise Refusal("existing-account-identity")
                if role == 0:
                    primary = identity
            self.report["existing_sessions_verified"] = 4
            if not self.accounts[0].get("MATRIX_PASSWORD"):
                self.use_key_free_session(primary)
                return
            self.tokens = []
        if not self.accounts[0].get("MATRIX_PASSWORD"):
            raise Refusal("fresh-device-auth-material-unavailable")
        # No retry: one password POST for one new, independent staging device.
        self.report["login_requests"] += 1
        session = self.call("/login", body={"type": "m.login.password",
                            "identifier": {"type": "m.id.user", "user": USERS[0]},
                            "password": self.accounts[0]["MATRIX_PASSWORD"],
                            "device_id": self.device})
        if session.get("user_id") != USERS[0] or session.get("device_id") != self.device:
            raise Refusal("fresh-session-identity")
        self.report["sessions_issued"] = 1
        self.tokens.append(session["access_token"])
        for fields in self.accounts[1:]:
            if not fields.get("MATRIX_ACCESS_TOKEN"):
                raise Refusal("sender-session-unavailable")
            self.tokens.append(fields["MATRIX_ACCESS_TOKEN"])
        verified = []
        for role, expected in enumerate(USERS):
            identity = self.call("/account/whoami", role)
            if identity.get("user_id") != expected or identity.get("is_guest", False):
                raise Refusal("matrix-account-identity")
            if role == 0 and identity.get("device_id") != self.device:
                raise Refusal("matrix-device-identity")
            verified.append(ROLES[role])
        self.report["verified_roles"] = verified

    def use_key_free_session(self, identity):
        device = identity.get("device_id")
        if not device or device.startswith("T046"):
            raise Refusal("independent-device-material-unavailable")
        keys = self.call("/keys/query", body={"device_keys": {USERS[0]: [device]}})
        if keys.get("failures") or not isinstance(keys.get("device_keys", {}).get(USERS[0]), dict):
            raise Refusal("device-key-query-incomplete")
        present = bool(keys.get("device_keys", {}).get(USERS[0], {}).get(device))
        self.report["existing_primary_device_keys_present"] = present
        if present and self.owned_store is None:
            raise Refusal("fresh-store-would-replace-existing-device-keys")
        self.device = device
        self.report["app_identity"] = "existing-owned-store" if present else "existing-key-free-staging-device"
        self.report["verified_roles"] = list(ROLES)

    def setup(self, app, task_timeout=None):
        self.authenticate()
        self.report["room_create_requests"] += 1
        result = self.call("/createRoom", body={"visibility": "private",
                           "preset": "private_chat", "creation_content": {"m.federate": False},
                           "name": "T050-N private " + uuid.uuid4().hex,
                           "invite": list(USERS[1:])})
        self.room = result.get("room_id")
        if not self.room or not self.room.startswith("!"):
            raise Refusal("room-create-result")
        self.room_issued = True
        encoded = urllib.parse.quote(self.room, safe="")
        for role in range(1, 4):
            self.call("/join/" + encoded, role, {})
        members = self.call("/rooms/" + encoded + "/joined_members")["joined"]
        if set(members) != set(USERS):
            raise Refusal("room-membership")
        state = self.call("/rooms/" + encoded + "/state")
        by_type = {item.get("type"): item.get("content", {}) for item in state
                   if item.get("state_key", "") == ""}
        if (by_type.get("m.room.create", {}).get("m.federate") is not False or
                by_type.get("m.room.join_rules", {}).get("join_rule") != "invite"):
            raise Refusal("room-private-federation-state")
        if "m.room.encryption" in by_type:
            raise Refusal("encrypted-room-needs-sdk-senders")
        self.report.update({"room": self.room, "memberships": 4,
                            "room_encryption": "observed-not-enabled",
                            "private_nonfederating_state": "verified"})
        app.env.update(T050_N_MATRIX_TOKEN=self.tokens[0],
                       T050_N_MATRIX_USER=USERS[0], T050_N_MATRIX_DEVICE=self.device)
        matrix = f'''enabled = true
homeserver = {json.dumps(SERVER)}
user_id = "{{env:T050_N_MATRIX_USER}}"
device_id = "{{env:T050_N_MATRIX_DEVICE}}"
access_token = "{{env:T050_N_MATRIX_TOKEN}}"
allowed_users = {json.dumps(list(USERS[1:]))}
device_trusted = true
crypto_store_path = {json.dumps(str(app.crypto_store))}
[transports.matrix.routes.rooms]
{json.dumps(self.room)} = "worker"'''
        app.write_config(matrix, task_timeout=task_timeout)

    def sync(self, since=None):
        room_filter = {"room": {"rooms": [self.room], "timeline": {"limit": 30}},
                       "presence": {"not_types": ["*"]},
                       "account_data": {"not_types": ["*"]}}
        path = "/sync?timeout=1000&filter=" + urllib.parse.quote(json.dumps(room_filter), safe="")
        if since:
            path += "&since=" + urllib.parse.quote(since, safe="")
        return self.call(path, 1)

    def message(self, role, label):
        transaction = uuid.uuid4().hex
        encoded = urllib.parse.quote(self.room, safe="")
        self.report["send_requests"] += 1
        result = self.call("/rooms/" + encoded + "/send/m.room.message/" + transaction,
                           role, {"msgtype": "m.text", "body":
                                  "Reply exactly " + label + ". Do not use tools."}, "PUT")
        return result["event_id"], transaction

    def receive(self, since, labels, timeout=160):
        deadline = time.monotonic() + timeout
        found = set()
        while time.monotonic() < deadline:
            response = self.sync(since)
            since = response["next_batch"]
            events = response.get("rooms", {}).get("join", {}).get(self.room, {}).get("timeline", {}).get("events", [])
            for event in events:
                if event.get("sender") == USERS[0] and event.get("type") == "m.room.message":
                    body = event.get("content", {}).get("body", "")
                    found.update(label for label in labels if label in body)
            if found == set(labels):
                return since
        raise Refusal("matrix-receive-timeout")

    def exercise(self, app):
        self.setup(app)
        app.start()
        since = self.sync()["next_batch"]
        labels = ["T050_N_MATRIX_" + role.upper() for role in ROLES[1:]]
        events = [self.message(role, label) for role, label in enumerate(labels, 1)]
        since = self.receive(since, labels)
        self.report["send_receive"] = "PASS"
        self.report["sent_event_count"] = len(events)
        app.stop()
        app.start()
        # Same Matrix transaction returns the original event; reception must not
        # admit it twice across application restart with retained DB/crypto state.
        with sqlite3.connect(app.database) as db:
            before = db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        encoded = urllib.parse.quote(self.room, safe="")
        for role, (event, transaction) in enumerate(events, 1):
            self.report["send_requests"] += 1
            replay = self.call("/rooms/" + encoded + "/send/m.room.message/" + transaction,
                               role, {"msgtype": "m.text", "body":
                                      "Reply exactly " + labels[role - 1] + ". Do not use tools."}, "PUT")
            if replay["event_id"] != event:
                raise Refusal("matrix-replay-event")
        time.sleep(2)
        with sqlite3.connect(app.database) as db:
            after = db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        if after != before:
            raise Refusal("matrix-restart-dedup")
        self.report["restart_dedup"] = "PASS"
        label = "T050_N_MATRIX_RESTART"
        self.message(2, label)
        self.receive(since, [label])
        self.report["restart_receive"] = "PASS"
        self.report["e2e"] = "PASS"

    def receive_timeout_replies(self, since, expected, timeout=60):
        deadline = time.monotonic() + timeout
        found = set()
        while time.monotonic() < deadline:
            response = self.sync(since)
            since = response["next_batch"]
            events = response.get("rooms", {}).get("join", {}).get(self.room, {}).get("timeline", {}).get("events", [])
            for event in events:
                content = event.get("content", {})
                original = content.get("m.relates_to", {}).get("m.in_reply_to", {}).get("event_id")
                if event.get("sender") == USERS[0] and original in expected:
                    if content.get("body") != "The task timed out.":
                        raise Refusal("unexpected-timeout-reply")
                    found.add(original)
            if found == set(expected):
                return since
        raise Refusal("matrix-timeout-reply-missing")

    def exercise_transport(self, app):
        # This is an expected real timeout-response cell, not successful native E2E.
        self.setup(app, task_timeout=20)
        app.start()
        self.report["readiness"] = "PASS"
        since = self.sync()["next_batch"]
        events = []
        for role in range(1, 4):
            event, transaction = self.message(role, "T050_N_EXPECTED_TIMEOUT_" + str(role))
            events.append((event, transaction))
            since = self.receive_timeout_replies(since, [event])
        self.report["multi_account_timeout_replies"] = 3
        with sqlite3.connect(app.database) as db:
            before = db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        app.stop()
        app.start()
        self.report["restart_readiness"] = "PASS"
        time.sleep(2)
        with sqlite3.connect(app.database) as db:
            after = db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        if before != 3 or after != before:
            raise Refusal("matrix-restart-dedup")
        self.report["restart_dedup"] = "PASS"
        event, _ = self.message(2, "T050_N_RESTART_EXPECTED_TIMEOUT")
        self.receive_timeout_replies(since, [event])
        self.report["restart_timeout_reply"] = "PASS"
        self.report["transport_expected_timeout"] = "PASS"
        self.report["e2e"] = "NOT_PASS-native-provider-unavailable"

    def cleanup(self, safe=True):
        outcomes = []
        if self.room_issued and safe:
            encoded = urllib.parse.quote(self.room, safe="")
            for role in range(4):
                try:
                    self.call("/rooms/" + encoded + "/leave", role, {})
                    outcomes.append({"role": ROLES[role], "leave": "confirmed"})
                except Refusal as error:
                    outcomes.append({"role": ROLES[role], "leave": str(error)})
        self.report["remote_cleanup"] = outcomes
        # Matrix offers no client-side room deletion; retain honest history residue.
        self.report["remote_residue"] = {
            "new_room": bool(self.room),
            "new_session": "issued" if self.report.get("sessions_issued") else (
                "unknown" if self.report["login_requests"] else "none"),
            "policy": "leave own new-room memberships; retain room history/session/device",
            "remaining_memberships": "unknown" if not safe or any(item["leave"] != "confirmed" for item in outcomes) else 0}


if __name__ == "__main__":
    from materials import classify
    summaries, accounts = classify(Path.home() / ".config/guigu-agent-bridge")
    report = {"materials": summaries}
    flow = MatrixFlow(accounts, report)
    try:
        flow.authenticate()
        report["authentication"] = "PASS"
    except Refusal as error:
        report["failure"] = str(error)
    flow.cleanup()
    print(json.dumps(report))
