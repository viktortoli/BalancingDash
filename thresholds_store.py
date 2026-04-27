from __future__ import annotations

import json
from dataclasses import asdict

import requests

from alarms import Thresholds

GIST_FILE = "thresholds.json"
_API = "https://api.github.com/gists"


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def load(token: str, gist_id: str) -> dict | None:
    r = requests.get(f"{_API}/{gist_id}", headers=_headers(token), timeout=10)
    r.raise_for_status()
    files = r.json().get("files", {})
    if GIST_FILE not in files:
        return None
    return json.loads(files[GIST_FILE]["content"])


def save(token: str, gist_id: str, thresholds: Thresholds) -> None:
    payload = {"files": {GIST_FILE: {"content": json.dumps(asdict(thresholds), indent=2)}}}
    r = requests.patch(f"{_API}/{gist_id}", headers=_headers(token), json=payload, timeout=10)
    r.raise_for_status()
