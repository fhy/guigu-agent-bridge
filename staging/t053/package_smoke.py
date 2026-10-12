"""Short final-package encrypted smoke; secrets stay in the reviewed worker."""
import argparse
import hashlib
import os
from pathlib import Path
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "t050_n"))
sys.path.insert(0, str(Path(__file__).resolve()))
from app import Application
from materials import classify, Refusal
from owned import subreaper
from run_encrypted import Recipient, database, Flow


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    parser.add_argument("--sdk", required=True)
    parser.add_argument("--agent", required=True)
    parser.add_argument("--primary-store", required=True)
    parser.add_argument("--recipient-store-root", required=True)
    args = parser.parse_args()
    os.umask(0o077)
    subreaper()
    root = Path(tempfile.mkdtemp(prefix="r023-package-smoke-", dir=Path.home()/".cache"))
    class Args: pass
    flow_args = Args()
    for name in ("binary", "sdk", "agent", "primary_store", "recipient_store_root"):
        setattr(flow_args, name, getattr(args, name))
    flow_args.deterministic = args.agent
    flow = Flow(flow_args, root, {"source": "package-smoke", "helper_requests": 0})
    flow.report.update({"package_binary_sha256": hashlib.sha256(Path(args.binary).read_bytes()).hexdigest()})
    try:
        # Reuse the reviewed T053 lifecycle state machine so the final-artifact
        # check exercises the same restart, durable-output, encryption and
        # cleanup predicates as the accepted staging evidence.  Only the
        # executable is replaced by the package-built binary.
        flow.exercise()
        flow.report.update({"package_artifact_smoke": True, "result": "PASS"})
        print(flow.report)
    finally:
        flow.cleanup()


if __name__ == "__main__":
    main()
