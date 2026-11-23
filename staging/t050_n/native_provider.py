"""Bounded read-only check of configured native model endpoints; no secret output."""
import json
import argparse
import os
from pathlib import Path
import re
import socket
import urllib.error
import urllib.parse
import urllib.request

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--direct", action="store_true", help="read-only endpoint check without environment proxy")
    args = parser.parse_args()
    settings = json.loads((Path.home() / ".qwen" / "settings.json").read_text())
    results = []
    for item in settings.get("modelProviders", {}).get("openai", []):
        model = item.get("id", "")
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9._/-]{0,90}", model):
            continue
        server = item.get("baseUrl", "")
        parsed = urllib.parse.urlsplit(server)
        row = {"configured_model": model, "requests": 0}
        if parsed.scheme not in ("http", "https") or parsed.username or parsed.password or parsed.port == 9191:
            row["category"] = "endpoint-not-in-scope"
            results.append(row)
            continue
        if parsed.hostname in ("127.0.0.1", "localhost", "::1"):
            row["loopback_port"] = parsed.port
        row["endpoint_host"] = parsed.hostname
        row["endpoint_port"] = parsed.port
        key_name = item.get("apiKeyEnvKey", "OPENAI_API_KEY")
        key = os.environ.get(key_name)
        row["requests"] = 1
        try:
            headers = {"Authorization": "Bearer " + key} if key else {}
            call = urllib.request.Request(server.rstrip("/") + "/models", headers=headers)
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({})) if args.direct else urllib.request.build_opener()
            with opener.open(call, timeout=3) as response:
                row["status"] = response.status
                data = json.loads(response.read(1048576))
                row.update(category="models-response", models_count=len(data.get("data", [])))
        except urllib.error.HTTPError as error:
            row["category"] = "http-" + str(error.code)
            error.close()
        except (TimeoutError, socket.timeout):
            row["category"] = "endpoint-timeout"
        except urllib.error.URLError as error:
            row["category"] = "endpoint-timeout" if isinstance(error.reason, TimeoutError) else "endpoint-unreachable"
        except ValueError:
            row["category"] = "models-response-not-json"
        except OSError:
            row["category"] = "endpoint-unreachable"
        results.append(row)
    print(json.dumps({"provider_checks": results, "mutations": 0,
                      "route": "direct" if args.direct else "environment-proxy"}))


if __name__ == "__main__":
    main()
