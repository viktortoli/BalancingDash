from __future__ import annotations

import json
from dataclasses import asdict

import requests

from alarms import Thresholds

THRESHOLDS_FILE = "thresholds.json"
STATE_FILE = "state.json"
_API = "https://api.github.com/gists"


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def load_json(token: str, gist_id: str, filename: str) -> dict | None:
    r = requests.get(f"{_API}/{gist_id}", headers=_headers(token), timeout=10)
    r.raise_for_status()
    files = r.json().get("files", {})
    if filename not in files:
        return None
    return json.loads(files[filename]["content"])


def save_json(token: str, gist_id: str, filename: str, obj: dict) -> None:
    payload = {"files": {filename: {"content": json.dumps(obj, indent=2)}}}
    r = requests.patch(f"{_API}/{gist_id}", headers=_headers(token), json=payload, timeout=10)
    r.raise_for_status()


def load(token: str, gist_id: str) -> dict | None:
    return load_json(token, gist_id, THRESHOLDS_FILE)


def save(token: str, gist_id: str, thresholds: Thresholds) -> None:
    save_json(token, gist_id, THRESHOLDS_FILE, asdict(thresholds))
