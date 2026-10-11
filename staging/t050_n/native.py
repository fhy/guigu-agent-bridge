"""Native A2A against the actual application and native ACP agent."""
import json
import sqlite3
import time

from app import http
from materials import Refusal


def send_params(message="native-message", text="Reply exactly T050_N_NATIVE_OK. Do not use tools."):
    return {"message": {"messageId": message, "contextId": "native-context",
                        "role": "user", "parts": [{"kind": "text", "text": text}]}}


def rows(database):
    with sqlite3.connect(database) as db:
        return db.execute("SELECT COUNT(*) FROM a2a_exchanges WHERE direction='inbound'").fetchone()[0]


def wait_working(app, task, report, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status, snapshot = app.rpc("tasks/get", {"id": task})
        if status != 200 or "result" not in snapshot:
            raise Refusal("cancel-state")
        state = snapshot["result"]["status"]
        if state == "working":
            report["cancel_observed_stage"] = "working"
            return
        if state != "submitted":
            raise Refusal("cancel-task-finished-before-request")
        time.sleep(0.1)
    raise Refusal("cancel-working-timeout")


def exercise(app, report):
    app.start()
    status, raw = http(f"http://127.0.0.1:{app.a2a}/.well-known/agent-card.json")
    if status != 200 or not json.loads(raw):
        raise Refusal("card")
    report["card"] = "PASS"
    params = send_params()
    status, value = app.rpc("message/send", params, "native-send")
    if status != 200 or "result" not in value:
        raise Refusal("native-send")
    task = value["result"]["id"]
    report["send"] = "accepted"
    result = app.terminal(task)
    report["terminal"] = result["status"]
    if result["status"] != "completed":
        raise Refusal("native-agent-not-completed")
    artifacts = result.get("artifacts", [])
    report["receive"] = "PASS" if "T050_N_NATIVE_OK" in json.dumps(artifacts) else "missing-output"
    if report["receive"] != "PASS":
        raise Refusal("native-output-missing")
    count = rows(app.database)
    status, replay = app.rpc("message/send", params, "native-send")
    if status != 200 or replay["result"]["id"] != task or rows(app.database) != count:
        raise Refusal("identical-replay")
    report["identical_replay"] = "PASS"
    conflict = send_params(text="Different payload")
    status, value = app.rpc("message/send", conflict, "native-send")
    if status != 409 or value.get("error", {}).get("code") != -32009:
        raise Refusal("conflict")
    report["conflict"] = "PASS"
    status, canceled = app.rpc("tasks/cancel", {"id": task})
    if status != 200 or canceled["result"]["id"] != task:
        raise Refusal("cancel-completed")
    report["cancel_completed"] = "PASS"
    app.stop()
    app.start()
    status, replay = app.rpc("message/send", params, "native-send")
    if status != 200 or replay["result"]["id"] != task or rows(app.database) != count:
        raise Refusal("restart-dedup")
    report["restart_dedup"] = "PASS"
    if app.terminal(task)["status"] != "completed":
        raise Refusal("restart-get")
    report["restart_get"] = "PASS"
    status, submitted = app.rpc("message/send", send_params(
        "native-cancel", "Write a long explanation of the number pi, without tools."),
        "native-cancel")
    if status != 200:
        raise Refusal("cancel-send")
    canceled_id = submitted["result"]["id"]
    wait_working(app, canceled_id, report)
    status, _ = app.rpc("tasks/cancel", {"id": canceled_id})
    if status != 200 or app.terminal(canceled_id)["status"] != "canceled":
        raise Refusal("cancel-active")
    report["cancel_active"] = "PASS"
