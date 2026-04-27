"""Standalone alarm-check + mail job. Run on a schedule (cron / GitHub Actions)."""
from __future__ import annotations

import os
import sys

import pandas as pd

import mailer
import thresholds_store
from alarms import Thresholds, check_alarms
from transelectrica import _TZ, fetch_merged

DEFAULT_THRESHOLDS = dict(
    afrr_up_threshold_mwh=20.0,
    afrr_down_threshold_mwh=15.0,
    mfrr_up_threshold_mwh=30.0,
    mfrr_down_threshold_mwh=25.0,
    rate_of_change_mwh=20.0,
    afrr_spike_mwh=25.0,
    stale_data_minutes=20,
    mfrr_publication_lag_minutes=5,
)


def _env(name: str, required: bool = True) -> str:
    v = os.environ.get(name, "").strip()
    if required and not v:
        print(f"missing env: {name}", file=sys.stderr)
        sys.exit(1)
    return v


def main() -> int:
    token = _env("GITHUB_TOKEN")
    gist_id = _env("GIST_ID")

    overrides = thresholds_store.load(token, gist_id) or {}
    th = Thresholds(**{**DEFAULT_THRESHOLDS, **overrides})

    df = fetch_merged()
    alarms = check_alarms(df, thresholds=th)

    state = thresholds_store.load_json(token, gist_id, thresholds_store.STATE_FILE)
    if state is None:
        last = {}
        for a in alarms:
            if a.timestamp.isoformat() > last.get(a.code, ""):
                last[a.code] = a.timestamp.isoformat()
        thresholds_store.save_json(token, gist_id, thresholds_store.STATE_FILE, {"last_per_code": last})
        print(f"first run: initialized state with {len(last)} codes, no mail sent.")
        return 0

    last = state.get("last_per_code", {})
    fresh = [a for a in alarms if a.timestamp.isoformat() > last.get(a.code, "")]
    if not fresh:
        print(f"{len(alarms)} alarms, none fresh.")
        return 0

    mailer.send(
        fresh,
        host=_env("SMTP_HOST"),
        port=int(_env("SMTP_PORT", required=False) or 587),
        user=_env("SMTP_USER"),
        password=_env("SMTP_PASSWORD"),
        sender=_env("MAIL_FROM"),
        recipients=[r.strip() for r in _env("MAIL_TO").split(",") if r.strip()],
    )

    for a in fresh:
        last[a.code] = a.timestamp.isoformat()
    thresholds_store.save_json(token, gist_id, thresholds_store.STATE_FILE, {"last_per_code": last})
    print(f"sent {len(fresh)} alarm(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
