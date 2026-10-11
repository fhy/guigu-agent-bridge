"""Non-secret launcher metadata; do not echo shell source or assignments."""
import json
import os
import re
from pathlib import Path
import shlex


def metadata(path):
    source = Path(path).read_text()
    report = {"exec_present": False, "safe_flags": [], "provider_fields_present": []}
    for field in ("OPENAI_API_KEY", "OPENAI_BASE_URL", "OPENAI_MODEL", "CODEX_API_KEY"):
        if field in source:
            report["provider_fields_present"].append(field)
    for line in source.splitlines():
        if line.strip().startswith("exec "):
            words = shlex.split(line)
            report["exec_present"] = True
            for word in words[1:]:
                if word.startswith("/") and Path(word).name in ("qwen", "node", "cli.js"):
                    report["executable_basename"] = Path(word).name
                    report["executable_path"] = word
                if word in ("--model", "--acp", "--approval-mode", "--yolo", "--base-url"):
                    report["safe_flags"].append(word)
    return report


def auth_metadata():
    report = {"environment_api_key_available": bool(os.environ.get("OPENAI_API_KEY")),
              "environment_base_url_available": bool(os.environ.get("OPENAI_BASE_URL"))}
    path = Path.home() / ".qwen" / "settings.json"
    try:
        settings = json.loads(path.read_text())
        report["user_settings_present"] = True
        model = settings.get("model", {})
        report["model_name_present"] = isinstance(model, dict) and bool(model.get("name"))
        name = model.get("name") if isinstance(model, dict) else None
        if isinstance(name, str) and re.fullmatch(r"[A-Za-z][A-Za-z0-9._/-]{0,90}", name):
            report["model_name"] = name
        report["providers_present"] = bool(settings.get("modelProviders"))
        providers = settings.get("modelProviders", {})
        report["provider_families"] = [key for key in ("openai", "anthropic", "gemini", "qwen") if key in providers]
        public_models = []
        for item in providers.get("openai", []) if isinstance(providers, dict) else []:
            if not isinstance(item, dict):
                continue
            value = item.get("id")
            if isinstance(value, str) and re.fullmatch(r"[A-Za-z][A-Za-z0-9._/-]{0,90}", value):
                public_models.append(value)
        report["configured_model_ids"] = public_models
        security = settings.get("security", {})
        auth = security.get("auth", {}) if isinstance(security, dict) else {}
        report["selected_auth_present"] = bool(auth.get("selectedType"))
        report["native_key_configured"] = bool(auth.get("apiKey"))
    except (OSError, ValueError):
        report["user_settings_present"] = False
    return report


if __name__ == "__main__":
    print(json.dumps({"launcher": metadata(Path.home() / ".local/bin/qwen"),
                      "auth_metadata": auth_metadata()}))
