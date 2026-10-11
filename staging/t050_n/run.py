"""Private staging runner: only fixed categories leave this process."""
import argparse
import json
import os
import signal
from pathlib import Path
import tempfile

from app import Application
from materials import Refusal, classify
from native import exercise
from matrix_flow import MatrixFlow
from owned import subreaper


def finish(app, stage):
    try:
        app.stop()
    except Exception:
        stage["cleanup_failure"] = "owned-cleanup-incomplete"
    stage["cleanup"] = list(app.cleanups)


def cancel(*_):
    raise KeyboardInterrupt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    parser.add_argument("--agent", required=True)
    parser.add_argument("--matrix-transport-only", action="store_true")
    parser.add_argument("--owned-crypto-store", help="reuse only this task's previously created SDK store")
    args = parser.parse_args()
    owned_store = None
    if args.owned_crypto_store:
        candidate = Path(args.owned_crypto_store)
        if (candidate.name != "crypto" or not candidate.parent.name.startswith("t050-n-direct-") or
                candidate.parent.parent != Path.home() / ".cache"):
            parser.error("SDK store must be this task's private cache tree")
        if not candidate.is_dir() or candidate.is_symlink():
            parser.error("owned SDK store is unavailable")
        owned_store = candidate
    os.umask(0o077)
    subreaper()
    signal.signal(signal.SIGTERM, cancel)
    # The product rejects world-writable crypto-store ancestors, including /tmp.
    root = Path(tempfile.mkdtemp(prefix="t050-n-direct-", dir=Path.home() / ".cache"))
    report = {"root": str(root), "backend": Path(args.agent).name,
              "backend_kind": "caller-supplied native executable", "native": {}, "matrix": {}}
    summaries, accounts = classify(Path.home() / ".config/guigu-agent-bridge")
    report["materials"] = summaries
    app = Application(root, args.binary, args.agent, crypto_store=owned_store)
    report["crypto_store"] = str(app.crypto_store)
    interrupted = False
    try:
        if args.matrix_transport_only:
            report["native"]["not_run"] = "configured-native-provider-unavailable"
        else:
            exercise(app, report["native"])
    except Refusal as error:
        report["native"]["failure"] = str(error)
    except KeyboardInterrupt:
        interrupted = True
        report["native"]["failure"] = "runner-cancelled"
    except Exception:
        report["native"]["failure"] = "runner-internal"
    finally:
        finish(app, report["native"])
    matrix = MatrixFlow(accounts, report["matrix"], owned_store=owned_store)
    try:
        if interrupted or report["native"].get("cleanup_failure"):
            raise Refusal("not-run-after-cancel")
        if args.matrix_transport_only:
            matrix.exercise_transport(app)
        else:
            matrix.exercise(app)
    except Refusal as error:
        report["matrix"]["failure"] = str(error)
    except KeyboardInterrupt:
        report["matrix"]["failure"] = "runner-cancelled"
    except Exception:
        report["matrix"]["failure"] = "runner-internal"
    finally:
        finish(app, report["matrix"])
        matrix.cleanup(safe=not report["matrix"].get("cleanup_failure"))
    evidence = root / "result.json"
    evidence.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))
    return 0 if (not report["native"].get("failure") and
                 not any(stage.get("cleanup_failure") for stage in (report["native"], report["matrix"])) and
                 report["matrix"].get("e2e") == "PASS") else 3


if __name__ == "__main__":
    raise SystemExit(main())
