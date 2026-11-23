"""Private account consumption. Never return raw errors or secret metadata."""
import json
import os
from pathlib import Path
import shlex
import stat
import urllib.error
import urllib.request

ROLES = ("primary", "failure", "reviewer", "observer")
FILES = ("staging-matrix.env", "bridge-staging-failure.env",
         "bridge-staging-reviewer.env", "bridge-staging-observer.env")
USERS = ("@bridge-staging:matrix.0x8.xyz",
         "@bridge-staging-failure:matrix.0x8.xyz",
         "@bridge-staging-reviewer:matrix.0x8.xyz",
         "@bridge-staging-observer:matrix.0x8.xyz")
SERVER = "https://matrix.0x8.xyz"


class Refusal(Exception):
    """Only fixed public categories may be raised here."""


def read_material(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(fd)
        if (metadata.st_uid != os.getuid() or
                stat.S_IMODE(metadata.st_mode) != 0o600 or
                not stat.S_ISREG(metadata.st_mode) or metadata.st_size > 65536):
            raise Refusal("source-permission")
        with os.fdopen(fd, "r", closefd=False) as stream:
            source = stream.read(65537)
        result = {}
        for line in source.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, separator, raw = line.partition("=")
            if not separator or not key.replace("_", "").isalnum() or key in result:
                raise Refusal("source-format")
            try:
                parsed = shlex.split(raw, comments=False)
            except ValueError:
                raise Refusal("source-format") from None
            if len(parsed) > 1:
                raise Refusal("source-format")
            result[key] = parsed[0] if parsed else ""
        return result
    finally:
        os.close(fd)


def request(server, path, token=None, body=None, method=None, timeout=20):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    data = None if body is None else json.dumps(body).encode()
    call = urllib.request.Request(server + path, data=data, headers=headers,
                                  method=method)
    try:
        with urllib.request.urlopen(call, timeout=timeout) as response:
            raw = response.read(4 * 1024 * 1024 + 1)
            if len(raw) > 4 * 1024 * 1024:
                raise Refusal("response-size")
            return response.status, json.loads(raw)
    except urllib.error.HTTPError as error:
        code = error.code
        error.close()
        raise Refusal("http-" + str(code)) from None
    except (OSError, ValueError):
        raise Refusal("transport-or-format") from None


def classify(directory):
    summaries, accounts = [], []
    for role, filename, user in zip(ROLES, FILES, USERS):
        try:
            fields = read_material(Path(directory) / filename)
            accounts.append(fields)
            summaries.append({"role": role,
                              "password_available": bool(fields.get("MATRIX_PASSWORD")),
                              "session_available": bool(fields.get("MATRIX_ACCESS_TOKEN")),
                              "category": "material-classified"})
        except FileNotFoundError:
            accounts.append(None)
            summaries.append({"role": role, "category": "source-missing"})
        except (OSError, ValueError, Refusal):
            accounts.append(None)
            summaries.append({"role": role, "category": "source-invalid"})
    return summaries, accounts


if __name__ == "__main__":
    # Fixed allowlist, no recursive search or arbitrary source printing.
    summaries, _ = classify(Path.home() / ".config/guigu-agent-bridge")
    print(json.dumps({"classification": summaries, "http_requests": 0}))
