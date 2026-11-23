"""Focused private OS/localhost tests; never labelled real backend E2E."""
import contextlib
import io
import json
import os
import selectors
from pathlib import Path
import signal
import socket
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from app import Application, closed, port
from materials import Refusal, classify, read_material
from matrix_flow import MatrixFlow
from owned import Owned, subreaper
from acp_turn import exchange
from acp_restart import connection
from native import wait_working


class CancellationStageTests(unittest.TestCase):
    def test_stage_is_observed_not_inferred_from_submission(self):
        app = Mock()
        app.rpc.side_effect = [(200, {"result": {"status": "submitted"}}),
                               (200, {"result": {"status": "working"}})]
        report = {}
        with patch("native.time.sleep"):
            wait_working(app, "test-task", report)
        self.assertEqual(report, {"cancel_observed_stage": "working"})
        self.assertEqual(app.rpc.call_count, 2)

    def test_finished_task_does_not_claim_active_cancellation(self):
        app = Mock()
        app.rpc.return_value = (200, {"result": {"status": "completed"}})
        report = {}
        with self.assertRaisesRegex(Refusal, "finished-before-request"):
            wait_working(app, "test-task", report)
        self.assertEqual(report, {})

    def test_timeout_does_not_issue_cancel_or_claim_working(self):
        app = Mock()
        report = {}
        with self.assertRaisesRegex(Refusal, "working-timeout"):
            wait_working(app, "test-task", report, timeout=0)
        app.rpc.assert_not_called()
        self.assertEqual(report, {})


class LifecycleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        subreaper()

    def test_immediate_exit_reaped(self):
        with Owned([sys.executable, "-c", "raise SystemExit(7)"]) as owner:
            time.sleep(0.1)
        self.assertEqual(owner.result["exit"], 7)

    def test_timeout_and_descendant_reap(self):
        script = ("import os,signal,time; os.fork(); "
                  "signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(20)")
        owner = Owned([sys.executable, "-c", script])
        time.sleep(0.1)
        result = owner.stop(0.15)
        self.assertEqual(result["exit"], -signal.SIGKILL)
        self.assertEqual(result["descendants_reaped"], 1)
        self.assertEqual(owner.stop(), result)

    def test_exception_finally_cleanup(self):
        owner = None
        with self.assertRaisesRegex(ValueError, "test"):
            with Owned([sys.executable, "-c", "import time;time.sleep(20)"]) as owner:
                raise ValueError("test")
        self.assertIsNotNone(owner.result)

    def test_cancel_finally_cleanup(self):
        owner = None
        with self.assertRaises(KeyboardInterrupt):
            with Owned([sys.executable, "-c", "import time;time.sleep(20)"]) as owner:
                raise KeyboardInterrupt
        self.assertIsNotNone(owner.result)

    def test_os_sigterm_runs_nested_owned_finally(self):
        module = str(Path(__file__).parent)
        with tempfile.TemporaryDirectory(prefix="t050-n-os-cancel-") as root:
            result = Path(root) / "result.json"
            ready = Path(root) / "ready"
            code = f'''import sys,signal,time,json
from pathlib import Path
sys.path.insert(0,{module!r})
from owned import Owned,subreaper
subreaper()
def cancel(*_):
    raise KeyboardInterrupt
signal.signal(signal.SIGTERM,cancel)
owner=None
try:
    with Owned([sys.executable,"-c","import time;time.sleep(20)"]) as owner:
        Path({str(ready)!r}).touch()
        time.sleep(20)
except KeyboardInterrupt:
    Path({str(result)!r}).write_text(json.dumps(owner.result))
'''
            with Owned([sys.executable, "-c", code]) as owner:
                deadline = time.monotonic() + 3
                while not ready.exists() and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertTrue(ready.exists())
                owner.stop()
            self.assertEqual(owner.result["exit"], 0)
            self.assertEqual(json.loads(result.read_text())["cleanup"], "owned-group-reaped")

    def test_listener_closed_after_cleanup(self):
        number = port()
        script = (f"import socket,time; s=socket.socket(); "
                  f"s.bind(('127.0.0.1',{number})); s.listen();time.sleep(20)")
        with Owned([sys.executable, "-c", script]):
            deadline = time.monotonic() + 2
            while closed(number) and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertFalse(closed(number))
        self.assertTrue(closed(number))

    def test_refused_real_application(self):
        binary = os.environ.get("T050_N_BINARY")
        if not binary:
            self.skipTest("real binary not supplied")
        with tempfile.TemporaryDirectory(prefix="t050-n-refused-") as root:
            config = Path(root) / "bad.toml"
            config.write_text("[bridge]\ndefault_timeout_seconds=0\n")
            with Owned([binary, str(config)]) as owner:
                deadline = time.monotonic() + 5
                while not owner.exited() and time.monotonic() < deadline:
                    time.sleep(0.05)
                self.assertTrue(owner.exited())
            self.assertEqual(owner.result["exit"], 1)

    def test_readiness_failure_cleans_real_child(self):
        binary = os.environ.get("T050_N_BINARY")
        if not binary:
            self.skipTest("real binary not supplied")
        with tempfile.TemporaryDirectory(prefix="t050-n-readiness-") as root:
            app = Application(root, binary, "/nonexistent/private-test-agent")
            with self.assertRaises(Refusal):
                app.start(timeout=0.001)
            self.assertIsNone(app.owner)
            self.assertTrue(app.cleanups)


class DiagnosticTests(unittest.TestCase):
    def test_restart_probe_retains_cleanup_and_no_session_output(self):
        subreaper()
        script = ('import sys,json; '
                  '\nfor line in sys.stdin:\n'
                  ' req=json.loads(line); method=req["method"]; '
                  'result={"protocolVersion":1} if method=="initialize" else '
                  '({"sessionId":"private-fixture-session"} if method=="session/new" else '
                  '({"stopReason":"end_turn"} if method=="session/prompt" else {})); '
                  'print(json.dumps({"id":req["id"],"result":result}),flush=True)')
        report = {"prompts": 0}
        with tempfile.TemporaryDirectory(prefix="t050-n-probe-") as root:
            with patch("acp_restart.Owned", side_effect=lambda argv, **kw:
                       Owned([sys.executable, "-c", script], **kw)):
                session = connection("fixture", root, report)
                connection("fixture", root, report, session)
        self.assertEqual(report["resume"], "PASS")
        self.assertEqual(len(report["cleanup"]), 2)
        self.assertTrue(all(item["cleanup"] == "owned-group-reaped" for item in report["cleanup"]))
        self.assertNotIn("private-fixture-session", json.dumps(report))

    def test_rpc_error_is_fixed_class_and_never_peer_message(self):
        subreaper()
        script = ('import json; req=json.loads(input()); '
                  'print(json.dumps({"id":req["id"],"error":{"code":-32603,'
                  '"message":"session not found fixture-private-secret"}}),flush=True)')
        report = {}
        with Owned([sys.executable, "-c", script], pipe=True, stderr_pipe=True) as owner:
            with selectors.DefaultSelector() as selector:
                selector.register(owner.child.stdout, selectors.EVENT_READ, "stdout")
                selector.register(owner.child.stderr, selectors.EVENT_READ, "stderr")
                with self.assertRaisesRegex(RuntimeError, "rpc-error"):
                    exchange(owner, selector, 1, "session/resume", {}, report, timeout=3)
        self.assertEqual(report["rpc_error_code"], -32603)
        self.assertEqual(report["rpc_error_classes"], ["not found", "session"])
        self.assertNotIn("fixture-private-secret", json.dumps(report))

    def test_application_diagnostic_is_fixed_class(self):
        with tempfile.TemporaryDirectory(prefix="t050-n-diagnostic-") as root:
            app = Application(root, "unused", "unused")
            app.owner = Mock()
            with patch("app.os.read", return_value=b"session storage failed: fixture-private-secret"):
                app.drain()
            self.assertEqual(app.refusals, ["acp-session-storage"])
            self.assertNotIn("fixture-private-secret", json.dumps(app.refusals))

    def test_split_marker_is_seen_without_exposing_peer_text(self):
        subreaper()
        script = ('import sys,json; req=json.loads(input()); '
                  'chunks=["T050_N_","NATIVE_OK fixture-private-text"]; '
                  '[print(json.dumps({"method":"session/update","params":{"update":'
                  '{"sessionUpdate":"agent_message_chunk","content":{"text":text}}}}),flush=True) '
                  'for text in chunks]; '
                  'print(json.dumps({"id":req["id"],"result":{"stopReason":"end_turn"}}),flush=True)')
        report = {"notifications": 0, "text_bytes": 0, "marker_seen": False,
                  "stderr_categories": [], "text_categories": [],
                  "method_classes": [], "update_classes": []}
        with Owned([sys.executable, "-c", script], pipe=True, stderr_pipe=True) as owner:
            with selectors.DefaultSelector() as selector:
                selector.register(owner.child.stdout, selectors.EVENT_READ, "stdout")
                selector.register(owner.child.stderr, selectors.EVENT_READ, "stderr")
                exchange(owner, selector, 1, "session/prompt", {}, report, timeout=3)
        self.assertTrue(report["marker_seen"])
        self.assertNotIn("fixture-private-text", json.dumps(report))

    def test_actual_gate_stage_keeps_evidence_private_and_child_umask_normal(self):
        source = Path(__file__).with_name("gate.sh").read_text()
        stage = source[source.index("stage() {"):source.index("\nstage build")]
        with tempfile.TemporaryDirectory() as root:
            environment = dict(os.environ, gate_evidence=root)
            with Owned(["bash", "-c", "umask 077; gate_failed=0; " + stage +
                        "\nstage mode bash -c 'umask'"], env=environment) as owner:
                deadline = time.monotonic() + 5
                while not owner.exited() and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertTrue(owner.exited())
                self.assertEqual(owner.stop()["exit"], 0)
            self.assertEqual((Path(root) / "mode.log").read_text().strip(), "0022")
            self.assertEqual((Path(root) / "mode.log").stat().st_mode & 0o777, 0o600)


class MaterialTests(unittest.TestCase):
    def test_private_parse_and_no_echo(self):
        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "source"
            source.write_text('MATRIX_PASSWORD="synthetic-secret"\nMATRIX_ACCESS_TOKEN=test\n')
            source.chmod(0o600)
            capture = io.StringIO()
            with contextlib.redirect_stdout(capture):
                material = read_material(source)
            self.assertEqual(material["MATRIX_PASSWORD"], "synthetic-secret")
            self.assertEqual(capture.getvalue(), "")

    def test_permission_symlink_format_refusal(self):
        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "source"
            source.write_text("MATRIX_PASSWORD=a\nMATRIX_PASSWORD=b\n")
            source.chmod(0o600)
            with self.assertRaisesRegex(Refusal, "source-format"):
                read_material(source)
            source.chmod(0o644)
            with self.assertRaisesRegex(Refusal, "source-permission"):
                read_material(source)
            link = Path(root) / "link"
            link.symlink_to(source)
            with self.assertRaises(OSError):
                read_material(link)

    def test_missing_material_zero_http(self):
        with tempfile.TemporaryDirectory() as root:
            summaries, accounts = classify(root)
        self.assertTrue(all(item["category"] == "source-missing" for item in summaries))
        report = {}
        flow = MatrixFlow(accounts, report)
        with self.assertRaisesRegex(Refusal, "material-unavailable"):
            flow.authenticate()
        self.assertEqual(report["requests"], 0)

    def test_existing_token_not_used_for_fresh_store(self):
        report = {}
        flow = MatrixFlow([{"MATRIX_ACCESS_TOKEN": "synthetic"}] * 4, report)
        users = __import__("materials").USERS
        identities = [(200, {"user_id": user, "device_id": "old-device"}) for user in users]
        identities.append((200, {"device_keys": {users[0]: {"old-device": {"keys": {}}}}}))
        with patch("matrix_flow.request", side_effect=identities) as request:
            with self.assertRaisesRegex(Refusal, "fresh-store-would-replace"):
                flow.authenticate()
            self.assertEqual(request.call_count, 5)
            self.assertTrue(all("/account/whoami" in call.args[1]
                                for call in request.call_args_list[:4]))
        self.assertEqual(report["login_requests"], 0)

    def test_login_not_retried_on_failure(self):
        report = {}
        flow = MatrixFlow([{"MATRIX_PASSWORD": "synthetic"}] * 4, report)
        with patch("matrix_flow.request", side_effect=Refusal("http-403")) as request:
            with self.assertRaisesRegex(Refusal, "http-403"):
                flow.authenticate()
            self.assertEqual(request.call_count, 1)
        self.assertEqual(report["login_requests"], 1)

    def test_bad_quote_is_fixed_refusal(self):
        with tempfile.TemporaryDirectory() as root:
            source = Path(root) / "source"
            source.write_text('MATRIX_PASSWORD="fixture-secret\n')
            source.chmod(0o600)
            with self.assertRaisesRegex(Refusal, "^source-format$"):
                read_material(source)

    def test_existing_key_free_nonprotected_session(self):
        users = __import__("materials").USERS
        identities = [(200, {"user_id": user, "device_id": "fresh-test"}) for user in users]
        identities.append((200, {"device_keys": {users[0]: {}}}))
        report = {}
        flow = MatrixFlow([{"MATRIX_ACCESS_TOKEN": "synthetic"}] * 4, report)
        with patch("matrix_flow.request", side_effect=identities):
            flow.authenticate()
        self.assertEqual(report["app_identity"], "existing-key-free-staging-device")
        self.assertEqual(report["login_requests"], 0)

    def test_protected_device_not_queried_or_restored(self):
        users = __import__("materials").USERS
        identities = [(200, {"user_id": user, "device_id": "T046-PRIMARY"}) for user in users]
        report = {}
        flow = MatrixFlow([{"MATRIX_ACCESS_TOKEN": "synthetic"}] * 4, report)
        with patch("matrix_flow.request", side_effect=identities) as request:
            with self.assertRaisesRegex(Refusal, "independent-device"):
                flow.authenticate()
            self.assertEqual(request.call_count, 4)
        self.assertEqual(report["login_requests"], 0)

    def test_partial_remote_cleanup_retains_original_failure(self):
        report = {"failure": "matrix-receive-timeout"}
        flow = MatrixFlow([], report)
        flow.room = "!synthetic-fixture:example.test"
        flow.room_issued = True
        flow.tokens = ["fixture"] * 4
        with patch("matrix_flow.request", side_effect=[(200, {}), Refusal("http-403"), (200, {}), (200, {})]) as request:
            flow.cleanup()
        self.assertEqual(request.call_count, 4)
        self.assertEqual(report["failure"], "matrix-receive-timeout")
        self.assertEqual(report["remote_residue"]["remaining_memberships"], "unknown")


if __name__ == "__main__":
    unittest.main()
