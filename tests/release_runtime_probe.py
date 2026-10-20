import hashlib
import json
import pathlib
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request


def main():
    binary = pathlib.Path(sys.argv[1]).resolve(strict=True)
    result = {"binary": str(binary), "sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
    with tempfile.TemporaryDirectory(prefix="r044-runtime-") as directory:
        root = pathlib.Path(directory)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        config = root / "bridge.toml"
        config.write_text(
            "[bridge]\n"
            f"database={json.dumps(str(root / 'bridge.db'))}\n"
            f"session_root={json.dumps(str(root / 'sessions'))}\n"
            f'health_bind="127.0.0.1:{port}"\n'
        )
        with (root / "stdout").open("wb") as stdout, (root / "stderr").open("wb") as stderr:
            child = subprocess.Popen([str(binary), str(config)], stdout=stdout, stderr=stderr)
            try:
                deadline = time.monotonic() + 60
                ready = False
                while time.monotonic() < deadline and child.poll() is None:
                    try:
                        with urllib.request.urlopen(f"http://127.0.0.1:{port}/ready", timeout=1) as response:
                            ready = response.status == 200
                    except (OSError, urllib.error.URLError):
                        pass
                    if ready:
                        break
                    time.sleep(0.1)
                result["ready"] = ready
                if child.poll() is None:
                    child.terminate()
                result["shutdown_exit"] = child.wait(timeout=30)
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait()
        with sqlite3.connect(root / "bridge.db") as database:
            result["runtime_states"] = database.execute("SELECT state FROM runtime_instances").fetchall()
            result["owner_states"] = database.execute("SELECT state FROM runtime_owner_evidence WHERE is_current=1").fetchall()
        result["stderr"] = (root / "stderr").read_text()
        print(json.dumps(result, indent=2))
        return 0 if result["ready"] and result["shutdown_exit"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
