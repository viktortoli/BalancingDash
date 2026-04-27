from __future__ import annotations

from datetime import timedelta

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from streamlit_autorefresh import st_autorefresh

import thresholds_store
from alarms import Thresholds, check_alarms
from transelectrica import _TZ, fetch_merged

POS = "#22c55e"
NEG = "#ef4444"
NEU = "#9ca3af"
INTERVAL = timedelta(minutes=15)

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


def _gist_creds() -> tuple[str, str] | None:
    if not hasattr(st, "secrets"):
        return None
    token = st.secrets.get("github_token")
    gid = st.secrets.get("gist_id")
    return (token, gid) if token and gid else None


@st.cache_data(ttl=60, show_spinner=False)
def _load_thresholds_cached(_creds_key: str) -> dict:
    creds = _gist_creds()
    if not creds:
        return {}
    try:
        return thresholds_store.load(*creds) or {}
    except Exception as e:
        st.sidebar.error(f"Gist load failed: {e}")
        return {}


def _load_thresholds() -> Thresholds:
    creds = _gist_creds()
    overrides = _load_thresholds_cached(creds[1] if creds else "local") if creds else {}
    return Thresholds(**{**DEFAULT_THRESHOLDS, **overrides})


def _is_admin() -> bool:
    expected = st.secrets.get("admin_password") if hasattr(st, "secrets") else None
    if not expected:
        return True  # no password configured -> local dev mode
    if st.session_state.get("is_admin"):
        return True
    pwd = st.sidebar.text_input("Admin password", type="password")
    if pwd and pwd == expected:
        st.session_state["is_admin"] = True
        return True
    return False


def _fmt_cell(val: float, prev: float) -> str:
    if pd.isna(val):
        return ""
    color = POS if val > 0 else (NEG if val < 0 else NEU)
    base = 0.0 if pd.isna(prev) else prev
    arrow = ""
    if val != base:
        arrow = f' <span style="color:{POS}">▲</span>' if val > base else f' <span style="color:{NEG}">▼</span>'
    return f'<span style="color:{color}">{val:,.2f}</span>{arrow}'.replace(",", " ")


def _render_table_html(df: pd.DataFrame) -> str:
    if df.empty:
        return "<i>No data.</i>"
    numeric = df.apply(pd.to_numeric, errors="coerce")
    formatted = pd.DataFrame(index=numeric.index)
    for col in numeric.columns:
        prev = numeric[col].shift(1)
        formatted[col] = [_fmt_cell(v, p) for v, p in zip(numeric[col], prev)]
    formatted.index = formatted.index.strftime("%H:%M")
    formatted.index.name = "Time"
    html = formatted.to_html(escape=False, classes="bal", border=0)
    return (
        '<style>'
        '.bal{border-collapse:collapse;font-size:0.82rem;width:max-content;}'
        '.bal th,.bal td{padding:4px 8px;border-bottom:1px solid #2a2a2a;text-align:right;}'
        '.bal td{white-space:nowrap;}'
        '.bal th{position:sticky;top:0;background:#0e1117;text-align:right;'
        'white-space:normal;max-width:90px;font-weight:600;line-height:1.15;vertical-align:bottom;}'
        '.bal th:first-child,.bal td:first-child{text-align:left;'
        'position:sticky;left:0;background:#0e1117;}'
        '</style>'
        f'<div id="bal-wrap" style="overflow:auto;max-height:650px;">{html}</div>'
    )


def _next_refresh_ms(df: pd.DataFrame) -> int:
    now = pd.Timestamp.now(tz=_TZ)
    last_completed_start = now.floor("15min") - INTERVAL
    if not df.empty and df.index[-1] >= last_completed_start:
        wait = (now.ceil("15min") - now).total_seconds() + 20
        return int(max(wait, 5) * 1000)
    return 5_000


@st.cache_data(ttl=5, show_spinner=False)
def _cached_fetch(cache_key: str) -> pd.DataFrame:
    return fetch_merged()


def _bucket_key() -> str:
    return pd.Timestamp.now(tz=_TZ).floor("5s").isoformat()


st.set_page_config(page_title="Transelectrica balancing", layout="wide")

DEFAULTS = _load_thresholds()
admin = _is_admin()

if admin:
    st.sidebar.header("Alarm thresholds")
    th = Thresholds(
        afrr_up_threshold_mwh=st.sidebar.number_input(
            "Threshold aFRR Up (MWh)", min_value=0.0, value=DEFAULTS.afrr_up_threshold_mwh, step=1.0),
        afrr_down_threshold_mwh=st.sidebar.number_input(
            "Threshold aFRR Down (MWh)", min_value=0.0, value=DEFAULTS.afrr_down_threshold_mwh, step=1.0),
        mfrr_up_threshold_mwh=st.sidebar.number_input(
            "Threshold mFRR Up (MWh)", min_value=0.0, value=DEFAULTS.mfrr_up_threshold_mwh, step=1.0),
        mfrr_down_threshold_mwh=st.sidebar.number_input(
            "Threshold mFRR Down (MWh)", min_value=0.0, value=DEFAULTS.mfrr_down_threshold_mwh, step=1.0),
        rate_of_change_mwh=st.sidebar.number_input(
            "Rate of Change Threshold (MWh)", min_value=0.0, value=DEFAULTS.rate_of_change_mwh, step=1.0),
        afrr_spike_mwh=st.sidebar.number_input(
            "aFRR Spike Threshold (MWh)", min_value=0.0, value=DEFAULTS.afrr_spike_mwh, step=1.0),
        stale_data_minutes=DEFAULTS.stale_data_minutes,
        mfrr_publication_lag_minutes=DEFAULTS.mfrr_publication_lag_minutes,
    )
    creds = _gist_creds()
    if creds and st.sidebar.button("Save as default", type="primary"):
        try:
            thresholds_store.save(*creds, th)
            _load_thresholds_cached.clear()
            st.sidebar.success("Saved.")
        except Exception as e:
            st.sidebar.error(f"Save failed: {e}")
    elif not creds:
        st.sidebar.caption("Gist not configured: changes are session-only.")
else:
    th = DEFAULTS

if st.sidebar.button("Refresh now"):
    _cached_fetch.clear()

df = _cached_fetch(_bucket_key())
alarms = check_alarms(df, thresholds=th)
refresh_ms = _next_refresh_ms(df)
st_autorefresh(interval=refresh_ms, key="poll")

left, right = st.columns([4, 1], gap="large")

with left:
    st.subheader("Balancing data, today (Europe/Bucharest)")
    last_ts = df.index[-1] if not df.empty else None
    st.caption(f"Rows: {len(df)} | Last interval: {last_ts} | Next poll in ~{refresh_ms // 1000}s")
    st.markdown(_render_table_html(df), unsafe_allow_html=True)
    components.html(
        """
        <script>
          const doc = window.parent.document;
          const wrap = doc.querySelector('#bal-wrap');
          if (wrap) {
            const rows = wrap.querySelectorAll('tbody tr');
            let last = null;
            rows.forEach(r => {
              const cells = r.querySelectorAll('td');
              for (const c of cells) {
                if (c.innerText.trim() !== '') { last = r; break; }
              }
            });
            if (last) {
              const wrapTop = wrap.getBoundingClientRect().top;
              const rowTop = last.getBoundingClientRect().top;
              wrap.scrollTop += (rowTop - wrapTop) - (wrap.clientHeight / 2);
            }
          }
        </script>
        """,
        height=0,
    )

with right:
    st.subheader("Alarms")
    crit = sum(1 for a in alarms if a.severity == "Critical")
    warn = sum(1 for a in alarms if a.severity == "Warning")
    st.caption(f"{crit} critical, {warn} warning")
    if not alarms:
        st.success("No alarms.")
    else:
        for a in sorted(alarms, key=lambda x: x.timestamp, reverse=True):
            body = f"**{a.timestamp.strftime('%H:%M')}** {a.message}"
            (st.error if a.severity == "Critical" else st.warning)(body)
