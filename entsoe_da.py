from __future__ import annotations

import time
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests

_TZ = ZoneInfo("Europe/Bucharest")
_ENTSOE_URL = "https://web-api.tp.entsoe.eu/api"
_BNR_URL = "https://www.bnr.ro/nbrfxrates.xml"
_RO_EIC = "10YRO-TEL------P"

DA_COL = "DA price [Lei/MWh]"

# Per-day cache: once a day's prices are fetched they are never re-fetched
# for the lifetime of the process. Failed attempts back off _RETRY_SECONDS
# so the 2s autorefresh loop does not hammer ENTSO-E.
_cache: dict[str, pd.DataFrame] = {}
_last_attempt: dict[str, float] = {}
_RETRY_SECONDS = 60.0


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _fetch_eur_ron(session: requests.Session) -> float:
    r = session.get(_BNR_URL, timeout=30)
    r.raise_for_status()
    for el in ET.fromstring(r.content).iter():
        if _local(el.tag) == "Rate" and el.get("currency") == "EUR":
            return float(el.text) / int(el.get("multiplier") or 1)
    raise ValueError("EUR rate not found in BNR feed")


_RES_MINUTES = {"PT15M": 15, "PT30M": 30, "PT60M": 60, "P1D": 1440}


def _parse_da_xml(content: bytes) -> pd.Series:
    root = ET.fromstring(content)
    if _local(root.tag) == "Acknowledgement_MarketDocument":
        reason = next((el.text for el in root.iter() if _local(el.tag) == "text"), "no data")
        raise ValueError(f"ENTSO-E: {reason}")

    # Keep the finest resolution when the same timestamp appears in
    # several TimeSeries (e.g. PT60M and PT15M published side by side).
    best: dict[pd.Timestamp, tuple[int, float]] = {}
    for period in (el for el in root.iter() if _local(el.tag) == "Period"):
        start = resolution = None
        points: list[tuple[int, float]] = []
        for el in period.iter():
            name = _local(el.tag)
            if name == "start":
                start = pd.to_datetime(el.text, utc=True)
            elif name == "resolution":
                resolution = _RES_MINUTES.get(el.text)
            elif name == "Point":
                pos = price = None
                for child in el:
                    if _local(child.tag) == "position":
                        pos = int(child.text)
                    elif _local(child.tag) == "price.amount":
                        price = float(child.text)
                if pos is not None and price is not None:
                    points.append((pos, price))
        if start is None or resolution is None or not points:
            continue
        # Curve type A03: repeated values are omitted, so carry the last
        # price forward across missing positions.
        points.sort()
        last_pos, last_price = points[0]
        expanded = []
        for pos, price in points:
            expanded.extend((p, last_price) for p in range(last_pos + 1, pos))
            expanded.append((pos, price))
            last_pos, last_price = pos, price
        for pos, price in expanded:
            ts = start + timedelta(minutes=resolution * (pos - 1))
            cur = best.get(ts)
            if cur is None or resolution < cur[0]:
                best[ts] = (resolution, price)

    if not best:
        raise ValueError("ENTSO-E: no price points in response")
    return pd.Series({ts: price for ts, (_, price) in best.items()}).sort_index()


def _fetch_day(day: date, api_key: str) -> pd.DataFrame:
    start = datetime(day.year, day.month, day.day, tzinfo=_TZ)
    nxt = day + timedelta(days=1)
    end = datetime(nxt.year, nxt.month, nxt.day, tzinfo=_TZ)

    session = requests.Session()
    params = {
        "securityToken": api_key,
        "documentType": "A44",
        "in_Domain": _RO_EIC,
        "out_Domain": _RO_EIC,
        "periodStart": start.astimezone(ZoneInfo("UTC")).strftime("%Y%m%d%H%M"),
        "periodEnd": end.astimezone(ZoneInfo("UTC")).strftime("%Y%m%d%H%M"),
    }
    r = session.get(_ENTSOE_URL, params=params, timeout=30)
    r.raise_for_status()
    eur = _parse_da_xml(r.content).tz_convert(_TZ)

    rate = _fetch_eur_ron(session)
    grid = pd.date_range(start, end, freq="15min", inclusive="left")
    lei = eur.reindex(grid, method="ffill").mul(rate).round(2)
    df = lei.to_frame(DA_COL)
    df.index.name = "Time interval"
    df.attrs["eur_ron"] = rate
    return df


def get_da_prices(day: date, api_key: str) -> pd.DataFrame | None:
    """DA prices for `day` in Lei/MWh at 15-min resolution.

    Fetches from ENTSO-E only if the day is not already cached; returns
    None while backing off after a failed attempt.
    """
    key = day.isoformat()
    if key in _cache:
        return _cache[key]
    now = time.monotonic()
    if now - _last_attempt.get(key, float("-inf")) < _RETRY_SECONDS:
        return None
    _last_attempt[key] = now
    df = _fetch_day(day, api_key)
    if not df[DA_COL].dropna().empty:
        _cache[key] = df
    return df


if __name__ == "__main__":
    import sys

    print(get_da_prices(datetime.now(_TZ).date(), sys.argv[1]))
