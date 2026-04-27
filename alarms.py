from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Literal

import pandas as pd

from transelectrica import _TZ

Severity = Literal["Warning", "Critical"]

AFRR_UP = "aFRR Up [MWh]"
AFRR_DOWN = "aFRR Down [MWh]"
MFRR_UP = "mFRR Up [MWh]"
MFRR_DOWN = "mFRR Down [MWh]"
INTERVAL = timedelta(minutes=15)


@dataclass(frozen=True)
class Alarm:
    timestamp: pd.Timestamp
    severity: Severity
    code: str
    message: str


@dataclass(frozen=True)
class Thresholds:
    afrr_up_threshold_mwh: float
    afrr_down_threshold_mwh: float
    mfrr_up_threshold_mwh: float
    mfrr_down_threshold_mwh: float
    rate_of_change_mwh: float
    afrr_spike_mwh: float
    stale_data_minutes: int
    mfrr_publication_lag_minutes: int


def _prep(df: pd.DataFrame) -> pd.DataFrame:
    cols = [AFRR_UP, AFRR_DOWN, MFRR_UP, MFRR_DOWN]
    out = df[cols].apply(pd.to_numeric, errors="coerce")
    # Skip rows where aFRR has not been published yet; missing mFRR means no activation.
    out = out.dropna(subset=[AFRR_UP, AFRR_DOWN]).copy()
    out[[MFRR_UP, MFRR_DOWN]] = out[[MFRR_UP, MFRR_DOWN]].fillna(0.0)
    out["total_up"] = out[AFRR_UP] + out[MFRR_UP]
    out["total_down"] = out[AFRR_DOWN] + out[MFRR_DOWN]
    return out


def _freshness_alarms(df: pd.DataFrame, th: Thresholds, now: pd.Timestamp) -> list[Alarm]:
    published = _prep(df) if not df.empty else df
    if published.empty:
        return [Alarm(now, "Critical", "no_data", "No data.")]

    alarms: list[Alarm] = []
    last_start = published.index[-1]
    last_end = last_start + INTERVAL

    age = now - last_end
    if age > timedelta(minutes=th.stale_data_minutes):
        alarms.append(Alarm(
            last_start, "Critical", "stale_data",
            f"Stale data: {int(age.total_seconds() // 60)} min late.",
        ))

    if len(published) >= 2:
        expected_next_end = last_end + INTERVAL
        lag_minutes = (now - expected_next_end).total_seconds() / 60.0
        if lag_minutes > th.mfrr_publication_lag_minutes:
            last_two = published.iloc[-2:]
            if ((last_two[MFRR_UP] > 0) | (last_two[MFRR_DOWN] > 0)).all():
                alarms.append(Alarm(
                    last_end, "Critical", "mfrr_publication_lag",
                    f"mFRR active, next quarter {int(lag_minutes)} min late.",
                ))
    return alarms


def _row_alarms(df: pd.DataFrame, th: Thresholds) -> list[Alarm]:
    if len(df) < 2:
        return []

    d = _prep(df)
    prev = d.shift(1)

    direction_up_to_down = (prev["total_up"] > prev["total_down"]) & (d["total_down"] > d["total_up"])
    direction_down_to_up = (prev["total_down"] > prev["total_up"]) & (d["total_up"] > d["total_down"])

    mfrr_up_down_div = (d[MFRR_UP] < prev[MFRR_UP]) & (d[AFRR_DOWN] > prev[AFRR_DOWN])
    mfrr_down_up_div = (d[MFRR_DOWN] < prev[MFRR_DOWN]) & (d[AFRR_UP] > prev[AFRR_UP])

    d_mfrr_up = d[MFRR_UP] - prev[MFRR_UP]
    d_mfrr_down = d[MFRR_DOWN] - prev[MFRR_DOWN]
    mfrr_up_jump = d_mfrr_up.abs() >= th.rate_of_change_mwh
    mfrr_down_jump = d_mfrr_down.abs() >= th.rate_of_change_mwh

    deficit_to_surplus = (prev[MFRR_UP] > th.mfrr_up_threshold_mwh) & (d[MFRR_DOWN] > th.mfrr_down_threshold_mwh)
    surplus_to_deficit = (prev[MFRR_DOWN] > th.mfrr_down_threshold_mwh) & (d[MFRR_UP] > th.mfrr_up_threshold_mwh)

    afrr_opp_down_spike = (
        (prev[AFRR_UP] > prev[AFRR_DOWN])
        & (prev[AFRR_UP] > th.afrr_up_threshold_mwh)
        & (d[AFRR_DOWN] > prev[AFRR_DOWN])
        & (d[AFRR_DOWN] > th.afrr_down_threshold_mwh)
    )
    afrr_opp_up_spike = (
        (prev[AFRR_DOWN] > prev[AFRR_UP])
        & (prev[AFRR_DOWN] > th.afrr_down_threshold_mwh)
        & (d[AFRR_UP] > prev[AFRR_UP])
        & (d[AFRR_UP] > th.afrr_up_threshold_mwh)
    )

    afrr_dom_up_to_down = (prev[AFRR_UP] > prev[AFRR_DOWN]) & (d[AFRR_DOWN] > d[AFRR_UP])
    afrr_dom_down_to_up = (prev[AFRR_DOWN] > prev[AFRR_UP]) & (d[AFRR_UP] > d[AFRR_DOWN])

    d_afrr_up = (d[AFRR_UP] - prev[AFRR_UP]).abs()
    d_afrr_down = (d[AFRR_DOWN] - prev[AFRR_DOWN]).abs()
    afrr_up_spike = d_afrr_up >= th.afrr_spike_mwh
    afrr_down_spike = d_afrr_down >= th.afrr_spike_mwh

    rules: list[tuple[pd.Series, Severity, str, callable]] = [
        (direction_up_to_down, "Critical", "system_dir_up_to_down",
         lambda t: "System Up→Down."),
        (direction_down_to_up, "Critical", "system_dir_down_to_up",
         lambda t: "System Down→Up."),
        (mfrr_up_down_div, "Warning", "mfrr_up_afrr_down_divergence",
         lambda t: "mFRR Up↓, aFRR Down↑."),
        (mfrr_down_up_div, "Warning", "mfrr_down_afrr_up_divergence",
         lambda t: "mFRR Down↓, aFRR Up↑."),
        (mfrr_up_jump, "Warning", "mfrr_up_rate",
         lambda t: f"mFRR Up {d_mfrr_up.loc[t]:+.1f} MWh."),
        (mfrr_down_jump, "Warning", "mfrr_down_rate",
         lambda t: f"mFRR Down {d_mfrr_down.loc[t]:+.1f} MWh."),
        (deficit_to_surplus, "Critical", "deficit_to_surplus",
         lambda t: "mFRR deficit→surplus."),
        (surplus_to_deficit, "Critical", "surplus_to_deficit",
         lambda t: "mFRR surplus→deficit."),
        (afrr_opp_down_spike, "Critical", "afrr_opposite_down_spike",
         lambda t: "aFRR Down spike vs Up dominance."),
        (afrr_opp_up_spike, "Critical", "afrr_opposite_up_spike",
         lambda t: "aFRR Up spike vs Down dominance."),
        (afrr_dom_up_to_down, "Warning", "afrr_dom_up_to_down",
         lambda t: "aFRR Up→Down."),
        (afrr_dom_down_to_up, "Warning", "afrr_dom_down_to_up",
         lambda t: "aFRR Down→Up."),
        (afrr_up_spike, "Critical", "afrr_up_spike",
         lambda t: f"aFRR Up spike {d_afrr_up.loc[t]:.1f} MWh."),
        (afrr_down_spike, "Critical", "afrr_down_spike",
         lambda t: f"aFRR Down spike {d_afrr_down.loc[t]:.1f} MWh."),
    ]

    alarms: list[Alarm] = []
    for mask, severity, code, fmt in rules:
        for ts in d.index[mask.fillna(False)]:
            alarms.append(Alarm(ts, severity, code, fmt(ts)))
    return alarms


def check_alarms(
    df: pd.DataFrame,
    thresholds: Thresholds,
    now: pd.Timestamp | None = None,
) -> list[Alarm]:
    ref = now or pd.Timestamp.now(tz=_TZ)
    return sorted(
        _freshness_alarms(df, thresholds, ref) + _row_alarms(df, thresholds),
        key=lambda a: (a.timestamp, a.severity),
    )
