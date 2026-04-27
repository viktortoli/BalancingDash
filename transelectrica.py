from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import requests

_TZ = ZoneInfo("Europe/Bucharest")
_BASE = (
    "https://newmarkets.transelectrica.ro/usy-durom-publicreportg01/"
    "00121002500000000000000000000100/publicReport"
)

_ABE_COLS = {
    "aFRR_Up": "aFRR Up [MWh]",
    "aFRR_Down": "aFRR Down [MWh]",
    "mFRR_Up": "mFRR Up [MWh]",
    "mFRR_Down": "mFRR Down [MWh]",
}

_IMB_COLS = {
    "estimatedPriceNegativeImbalance": "Estimated price negative imbalance [Lei/MWh]",
    "estimatedPricePositiveImbalance": "Estimated price positive imbalance [Lei/MWh]",
    "imbalanceNettingImport": "Imbalance netting import [MWh]",
    "imbalanceNettingExport": "Imbalance netting export [MWh]",
    "estimatedUnintendedDeviationInArea": "Estimated unintended deviation IN area [MWh]",
    "estimatedUnintendedDeviationOutArea": "Estimated unintended deviation OUT area [MWh]",
}


def _to_utc_iso(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _default_window() -> tuple[datetime, datetime]:
    start_local = datetime.now(_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    return start_local, start_local + timedelta(days=1)


def _fetch(report_code: str, start: datetime, end: datetime, session: requests.Session) -> list[dict]:
    params = {
        "timeInterval.from": _to_utc_iso(start),
        "timeInterval.to": _to_utc_iso(end),
        "pageInfo.pageSize": 10000,
    }
    r = session.get(f"{_BASE}/{report_code}", params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("itemList", [])


def _to_df(items: list[dict], col_map: dict[str, str]) -> pd.DataFrame:
    if not items:
        return pd.DataFrame(columns=["Time interval", *col_map.values()]).set_index("Time interval")
    rows = []
    for it in items:
        row = {"Time interval": pd.to_datetime(it["timeInterval"]["from"], utc=True).tz_convert(_TZ)}
        for src, dst in col_map.items():
            row[dst] = it.get(src)
        rows.append(row)
    return pd.DataFrame(rows).set_index("Time interval").sort_index()


def fetch_activated_balancing_energy(
    start: datetime | None = None,
    end: datetime | None = None,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    s, e = (start, end) if start and end else _default_window()
    return _to_df(_fetch("activatedBalancingEnergyOverview", s, e, session or requests.Session()), _ABE_COLS)


def fetch_estimated_imbalance(
    start: datetime | None = None,
    end: datetime | None = None,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    s, e = (start, end) if start and end else _default_window()
    return _to_df(_fetch("estimatedImbalancePrices", s, e, session or requests.Session()), _IMB_COLS)


def fetch_merged(
    start: datetime | None = None,
    end: datetime | None = None,
) -> pd.DataFrame:
    s, e = (start, end) if start and end else _default_window()
    session = requests.Session()
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_abe = pool.submit(fetch_activated_balancing_energy, s, e, session)
        f_imb = pool.submit(fetch_estimated_imbalance, s, e, session)
        return f_abe.result().join(f_imb.result(), how="outer")


if __name__ == "__main__":
    print(fetch_merged())
