"""NL OV Delay Monitor - the live face of the pipeline.

Reads only Postgres (aggregates + latest state, written by the processor) and, for the ops
strip, Prometheus. Auto-refreshes every 10 s via a Streamlit fragment so the map and tiles update
without reloading the page. All timestamps are shown in Europe/Amsterdam (stored UTC, PRD N5).
"""

from __future__ import annotations

import html
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st
from psycopg_pool import ConnectionPool

from dashboard import queries
from dashboard.ops import pipeline_stats
from dashboard.theme import (
    ACCENT,
    BG,
    BORDER,
    BUCKETS,
    MUTED,
    PANEL,
    PANEL_2,
    TEXT,
    UNKNOWN_HEX,
    fmt_age,
    fmt_delay,
    hex_for,
    local,
    rgb_for,
    route_type_name,
    window_label,
)

st.set_page_config(
    page_title="NL OV Delay Monitor",
    page_icon="🚆",
    layout="wide",
    initial_sidebar_state="collapsed",
)

PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090")
CARTO_DARK = "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json"


# --------------------------------------------------------------------------- styling
def inject_styles() -> None:
    css = (Path(__file__).with_name("styles.css")).read_text(encoding="utf-8")
    root = ";".join(
        f"--ov-{name.lower().replace('_', '-')}:{value}"
        for name, value in {
            "BG": BG,
            "PANEL": PANEL,
            "PANEL_2": PANEL_2,
            "BORDER": BORDER,
            "TEXT": TEXT,
            "MUTED": MUTED,
            "ACCENT": ACCENT,
        }.items()
    )
    fonts = (
        '<link rel="preconnect" href="https://fonts.googleapis.com">'
        '<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700'
        '&family=JetBrains+Mono:wght@500;600&display=swap" rel="stylesheet">'
    )
    st.markdown(f"{fonts}<style>:root{{{root}}}{css}</style>", unsafe_allow_html=True)


inject_styles()


# --------------------------------------------------------------------------- data access
@st.cache_resource
def pool() -> ConnectionPool:
    dsn = (
        f"postgresql://{os.environ.get('POSTGRES_USER', 'ov')}:{os.environ.get('POSTGRES_PASSWORD', 'ov')}"
        f"@{os.environ.get('POSTGRES_HOST', 'localhost')}:{os.environ.get('POSTGRES_PORT', '5432')}"
        f"/{os.environ.get('POSTGRES_DB', 'ov')}"
    )
    return ConnectionPool(dsn, min_size=1, max_size=6, kwargs={"autocommit": True})


def q(fn, *args, **kwargs):
    with pool().connection() as conn:
        return fn(conn, *args, **kwargs)


@st.cache_data(ttl=5, show_spinner=False)
def load_live(operators: tuple[str, ...], route_types: tuple[int, ...], min_count: int) -> dict:
    ops = list(operators) or None
    types = list(route_types) or None
    with pool().connection() as conn:
        ws, we = queries.latest_window(conn)
        data = {
            "window": (ws, we),
            "fresh": queries.freshness(conn),
            "now": queries.right_now(conn, ops, types),
            "vehicles": queries.vehicles(conn, ops, types),
            "trend": queries.network_trend(conn, hours=3),
            "hist": queries.delay_histogram(conn, ops),
        }
        if ws is not None:
            data["lines"] = queries.worst_lines(conn, ws, ops, types, min_count=min_count)
            data["stations"] = queries.worst_stations(conn, ws, ops, min_count=min_count)
            data["operators"] = queries.operators_last_window(conn, ws)
        else:
            data["lines"] = data["stations"] = data["operators"] = pd.DataFrame()
    return data


@st.cache_data(ttl=30, show_spinner=False)
def load_operator_choices() -> pd.DataFrame:
    return q(queries.operator_choices)


@st.cache_data(ttl=10, show_spinner=False)
def load_ops() -> dict:
    return pipeline_stats(PROMETHEUS_URL)


# --------------------------------------------------------------------------- render helpers
def esc(value) -> str:
    return html.escape(
        "" if value is None or (isinstance(value, float) and pd.isna(value)) else str(value)
    )


def kpi(label: str, value: str, sub: str = "", accent: str = ACCENT) -> str:
    return (
        f'<div class="ov-kpi" style="--accent:{accent}"><div class="l">{esc(label)}</div>'
        f'<div class="v">{esc(value)}</div><div class="s">{esc(sub)}</div></div>'
    )


def chip(delay_s) -> str:
    return f'<span class="chip" style="background:{hex_for(delay_s)}">{fmt_delay(delay_s)}</span>'


def line_badge(row) -> str:
    color = row.get("route_color") or None
    bg = f"#{color}" if color and len(str(color)) == 6 else PANEL_2
    text = row.get("line") or row.get("route_id") or "?"
    return f'<span class="badge" style="background:{bg};color:#fff;text-shadow:0 0 2px rgba(0,0,0,.6)">{esc(text)}</span>'


def lines_table(df: pd.DataFrame) -> str:
    if df.empty:
        return '<div style="color:#8A94B3;padding:.6rem 0">No finalized window with enough observations yet.</div>'
    rows = []
    for _, r in df.iterrows():
        dest = r.get("long_name") or ""
        rows.append(
            f"<tr><td>{line_badge(r)}</td><td class='sub' title='{esc(dest)}'>"
            f"<b style='color:{TEXT}'>{esc(r.get('agency'))}</b> · {esc(route_type_name(r.get('route_type')))}"
            f"<br><span style='font-size:.75rem'>{esc(dest)}</span></td>"
            f"<td class='num'>{chip(r['mean_s'])}</td><td class='num'>{esc(fmt_delay(r['p90_s']))}</td>"
            f"<td class='num' style='color:{MUTED}'>{int(r['n'])}</td></tr>"
        )
    return (
        "<table class='ov'><thead><tr><th>Line</th><th>Operator · route</th><th style='text-align:right'>Mean</th>"
        "<th style='text-align:right'>P90</th><th style='text-align:right'>n</th></tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def stations_table(df: pd.DataFrame) -> str:
    if df.empty:
        return '<div style="color:#8A94B3;padding:.6rem 0">No finalized window with enough observations yet.</div>'
    rows = []
    for _, r in df.iterrows():
        rows.append(
            f"<tr><td><b>{esc(r['station'])}</b><br><span style='font-size:.75rem;color:{MUTED}'>{esc(r.get('operator'))}</span></td>"
            f"<td class='num'>{chip(r['mean_s'])}</td><td class='num'>{esc(fmt_delay(r['p90_s']))}</td>"
            f"<td class='num'>{esc(fmt_delay(r['max_s']))}</td><td class='num' style='color:{MUTED}'>{int(r['n'])}</td></tr>"
        )
    return (
        "<table class='ov'><thead><tr><th>Station · next stop</th><th style='text-align:right'>Mean</th>"
        "<th style='text-align:right'>P90</th><th style='text-align:right'>Max</th><th style='text-align:right'>n</th></tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def legend_html() -> str:
    items = "".join(
        f"<span><i style='background:{hex_}'></i>{label}</span>" for label, _, hex_, _ in BUCKETS
    )
    return f"<div class='legend'>{items}<span><i style='background:{UNKNOWN_HEX}'></i>no prediction yet</span></div>"


def build_map(
    vehicles: pd.DataFrame, stations: pd.DataFrame, show_stations: bool, point_size: float
) -> pdk.Deck:
    layers = []
    if show_stations and not stations.empty:
        s = stations.dropna(subset=["lat", "lon"]).copy()
        s["color"] = s["mean_s"].map(lambda d: [*rgb_for(d), 70])
        s["radius"] = 600 + s["mean_s"].clip(lower=0) * 2.5
        layers.append(
            pdk.Layer(
                "ScatterplotLayer",
                data=s,
                get_position=["lon", "lat"],
                get_fill_color="color",
                get_radius="radius",
                radius_min_pixels=10,
                radius_max_pixels=60,
                stroked=True,
                get_line_color=[255, 255, 255, 60],
                line_width_min_pixels=1,
                pickable=False,
            )
        )
    v = vehicles.copy()
    v["color"] = v["delay_s"].map(lambda d: [*rgb_for(None if pd.isna(d) else d), 235])
    v["delay_txt"] = v["delay_s"].map(lambda d: fmt_delay(None if pd.isna(d) else d))
    v["line_txt"] = v["line"].fillna("?")
    v["mode"] = v["route_type"].map(route_type_name)
    v["seen"] = v["observed_at"].map(lambda t: local(t, "%H:%M:%S"))
    v["agency"] = v["agency"].fillna(v["operator"])
    v["headsign"] = v["headsign"].fillna("")
    v["next_stop"] = v["next_stop"].fillna("")
    layers.append(
        pdk.Layer(
            "ScatterplotLayer",
            data=v[
                [
                    "lon",
                    "lat",
                    "color",
                    "delay_txt",
                    "line_txt",
                    "mode",
                    "agency",
                    "headsign",
                    "next_stop",
                    "seen",
                    "status",
                ]
            ],
            get_position=["lon", "lat"],
            get_fill_color="color",
            get_radius=point_size * 60,
            radius_min_pixels=point_size,
            radius_max_pixels=point_size * 3,
            stroked=True,
            get_line_color=[11, 16, 32, 200],
            line_width_min_pixels=0.6,
            pickable=True,
            auto_highlight=True,
        )
    )
    tooltip = {
        "html": (
            "<div style='font-family:Inter,sans-serif;font-size:12px;line-height:1.35'>"
            "<div style='font-size:14px;font-weight:700'>{agency} · {mode} {line_txt}</div>"
            "<div style='color:#c7cde0'>→ {headsign}</div>"
            "<div style='margin-top:4px'>Delay <b style='font-family:monospace;font-size:13px'>{delay_txt}</b></div>"
            "<div style='color:#c7cde0'>{status} {next_stop}</div>"
            "<div style='color:#8A94B3;margin-top:2px'>seen {seen}</div></div>"
        ),
        "style": {
            "backgroundColor": "#131A2E",
            "color": "#E6EAF2",
            "border": "1px solid #243055",
            "borderRadius": "8px",
        },
    }
    return pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(
            latitude=52.2, longitude=5.35, zoom=6.7, pitch=0, bearing=0
        ),
        map_provider="carto",
        map_style=CARTO_DARK,
        tooltip=tooltip,
    )


def trend_figure(trend: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if not trend.empty:
        x = [pd.Timestamp(t).tz_convert("Europe/Amsterdam") for t in trend["window_start"]]
        fig.add_bar(
            x=x,
            y=trend["n"],
            name="observations",
            marker_color="rgba(138,148,179,.28)",
            yaxis="y2",
            hovertemplate="%{y} observations<extra></extra>",
        )
        fig.add_scatter(
            x=x,
            y=trend["p90_s"] / 60,
            name="P90 delay",
            mode="lines",
            line={"color": "#F97316", "width": 2, "shape": "spline"},
            fill="tozeroy",
            fillcolor="rgba(249,115,22,.10)",
            hovertemplate="P90 %{y:.1f} min<extra></extra>",
        )
        fig.add_scatter(
            x=x,
            y=trend["mean_s"] / 60,
            name="mean delay",
            mode="lines+markers",
            line={"color": ACCENT, "width": 2.5, "shape": "spline"},
            marker={"size": 5},
            hovertemplate="mean %{y:.1f} min<extra></extra>",
        )
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin={"l": 8, "r": 8, "t": 8, "b": 8},
        height=260,
        legend={"orientation": "h", "y": 1.12, "x": 0, "font": {"size": 11}},
        xaxis={"showgrid": False, "tickformat": "%H:%M"},
        yaxis={
            "title": "minutes",
            "gridcolor": "rgba(36,48,85,.6)",
            "rangemode": "tozero",
            "zeroline": False,
        },
        yaxis2={
            "overlaying": "y",
            "side": "right",
            "showgrid": False,
            "visible": False,
            "rangemode": "tozero",
        },
        hovermode="x unified",
        font={"family": "Inter, sans-serif", "color": TEXT},
    )
    return fig


def operators_figure(ops: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if not ops.empty:
        d = ops.sort_values("mean_s", ascending=True).tail(14)
        fig.add_bar(
            x=d["mean_s"] / 60,
            y=d["agency"],
            orientation="h",
            marker_color=[hex_for(v) for v in d["mean_s"]],
            text=[fmt_delay(v) for v in d["mean_s"]],
            textposition="outside",
            textfont={"family": "JetBrains Mono, monospace", "size": 11},
            customdata=d[["n", "p90_s"]].to_numpy(),
            hovertemplate="%{y}<br>mean %{x:.1f} min · P90 %{customdata[1]:.0f}s · n=%{customdata[0]}<extra></extra>",
        )
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin={"l": 8, "r": 40, "t": 4, "b": 8},
        height=330,
        xaxis={"title": "mean delay (min)", "gridcolor": "rgba(36,48,85,.6)", "zeroline": False},
        yaxis={"tickfont": {"size": 11}},
        font={"family": "Inter, sans-serif", "color": TEXT},
        showlegend=False,
    )
    return fig


def histogram_figure(hist: pd.DataFrame) -> go.Figure:
    # width_bucket(delay, -120, 900, 17): bucket 0 = < -2 min, 1..17 = 60 s bins, 18 = > 15 min
    edges = [-120 + 60 * i for i in range(18)]
    labels = ["< -2"] + [f"{e // 60:+d}" for e in edges[:-1]] + ["> 15"]
    counts = dict(zip(hist["b"], hist["n"], strict=True)) if not hist.empty else {}
    ys = [counts.get(b, 0) for b in range(19)]
    centers = [-150] + [e + 30 for e in edges[:-1]] + [930]
    fig = go.Figure(
        go.Bar(
            x=labels,
            y=ys,
            marker_color=[hex_for(c) for c in centers],
            hovertemplate="%{y} trips at %{x} min<extra></extra>",
        )
    )
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin={"l": 8, "r": 8, "t": 4, "b": 8},
        height=330,
        xaxis={"title": "current delay (minutes)", "showgrid": False, "tickfont": {"size": 10}},
        yaxis={"title": "trips", "gridcolor": "rgba(36,48,85,.6)", "zeroline": False},
        bargap=0.15,
        font={"family": "Inter, sans-serif", "color": TEXT},
        showlegend=False,
    )
    return fig


def ops_strip(stats: dict) -> str:
    def f(v, fmt="{:.0f}", suffix=""):
        return "—" if v is None else fmt.format(v) + suffix

    cells = [
        ("msg/s in", f(stats["msg_in_per_s"], "{:.1f}")),
        ("msg/s processed", f(stats["msg_processed_per_s"], "{:.1f}")),
        ("consumer lag", f(stats["consumer_lag"])),
        ("watermark lag", f(stats["watermark_lag_s"], "{:.0f}", "s")),
        ("windows / 1h", f(stats["windows_1h"])),
        ("late dropped / 1h", f(stats["late_1h"])),
        ("DLQ / 1h", f(stats["dlq_1h"])),
        ("archive objects / 1h", f(stats["archive_objects_1h"])),
    ]
    return (
        "<div class='ops'>"
        + "".join(
            f"<div><div class='l'>{label}</div><div class='v'>{value}</div></div>"
            for label, value in cells
        )
        + "</div>"
    )


# --------------------------------------------------------------------------- sidebar
with st.sidebar:
    st.markdown("### Filters")
    choices = load_operator_choices()
    labels = {r.operator: f"{r.agency} ({r.vehicles})" for r in choices.itertuples()}
    operators = st.multiselect(
        "Operators", options=list(labels), format_func=labels.get, placeholder="All operators"
    )
    modes = st.multiselect(
        "Modality",
        options=[2, 3, 0, 1, 4],
        format_func=route_type_name,
        placeholder="All modalities",
    )
    min_count = st.slider("Min. observations per line/station", 3, 30, 5)
    st.markdown("### Map")
    show_stations = st.toggle("Station hotspots (last window)", value=True)
    point_size = st.slider("Vehicle size", 2.0, 6.0, 3.2, 0.2)
    st.markdown("### Refresh")
    refresh_s = st.select_slider("Auto-refresh", options=[5, 10, 20, 30, 60], value=10)
    st.caption(
        "Positions ≤ 30 s behind the feed · windows finalize 2 min after they close (allowed lateness)."
    )


# --------------------------------------------------------------------------- live view
@st.fragment(run_every=timedelta(seconds=refresh_s))
def live_view(
    operators: tuple[str, ...],
    modes: tuple[int, ...],
    min_count: int,
    show_stations: bool,
    point_size: float,
):
    data = load_live(operators, modes, min_count)
    ws, we = data["window"]
    fresh, now = data["fresh"], data["now"]
    vehicles = data["vehicles"]
    now_ams = datetime.now(tz=UTC)

    st.markdown(
        f"""
<div class="ov-header">
  <div class="ov-title">🚆 NL OV Delay Monitor <span class="ov-live"><span class="ov-dot"></span>LIVE</span></div>
  <div class="ov-meta">
    <span>positions <b>{esc(fmt_age(fresh["positions_age_s"]))}</b></span>
    <span>last window <b>{esc(window_label(ws, we))}</b></span>
    <span>Europe/Amsterdam <b>{esc(local(now_ams, "%a %d %b %H:%M:%S"))}</b></span>
  </div>
</div>""",
        unsafe_allow_html=True,
    )

    trips = int(now["trips"] or 0)
    mean_s, p90_s = now["mean_s"], now["p90_s"]
    on_time = float(now["on_time_share"] or 0) * 100
    severe = float(now["severe_share"] or 0) * 100
    worst = data["lines"].iloc[0] if not data["lines"].empty else None
    worst_label = (
        f"{worst.get('line') or worst.get('route_id')} · {worst.get('agency') or ''}"
        if worst is not None
        else "—"
    )
    st.markdown(
        '<div class="ov-kpis">'
        + kpi(
            "Vehicles live",
            f"{int(fresh['vehicles_live'] or 0):,}",
            f"{len(vehicles):,} shown on the map",
        )
        + kpi("Trips with prediction", f"{trips:,}", "delay known right now", "#60A5FA")
        + kpi("Mean delay", fmt_delay(mean_s), "across active trips", hex_for(mean_s))
        + kpi("P90 delay", fmt_delay(p90_s), "9 in 10 trips are better", hex_for(p90_s))
        + kpi(
            "On time",
            f"{on_time:.0f}%",
            f"{severe:.1f}% five minutes or worse",
            ACCENT if on_time >= 70 else "#F97316",
        )
        + kpi(
            "Worst line",
            fmt_delay(worst["mean_s"]) if worst is not None else "—",
            worst_label,
            hex_for(worst["mean_s"]) if worst is not None else UNKNOWN_HEX,
        )
        + "</div>",
        unsafe_allow_html=True,
    )

    left, right = st.columns([1.55, 1], gap="medium")
    with left:
        deck = build_map(vehicles, data["stations"], show_stations, point_size)
        try:
            st.pydeck_chart(deck, use_container_width=True, height=640)
        except TypeError:
            st.pydeck_chart(deck, use_container_width=True)
        st.markdown(legend_html(), unsafe_allow_html=True)
    with right:
        st.markdown(
            f"<div class='ov-panel'><h4>Worst lines · {esc(window_label(ws, we))}</h4>{lines_table(data['lines'])}</div>",
            unsafe_allow_html=True,
        )
        st.markdown("<div style='height:.6rem'></div>", unsafe_allow_html=True)
        st.markdown(
            f"<div class='ov-panel'><h4>Worst stations · {esc(window_label(ws, we))}</h4>{stations_table(data['stations'])}</div>",
            unsafe_allow_html=True,
        )

    st.markdown("<div style='height:.6rem'></div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='ov-panel'><h4>Network delay · last 3 hours, 5-minute tumbling windows</h4>",
        unsafe_allow_html=True,
    )
    st.plotly_chart(
        trend_figure(data["trend"]), use_container_width=True, config={"displayModeBar": False}
    )
    st.markdown("</div>", unsafe_allow_html=True)

    c1, c2 = st.columns(2, gap="medium")
    with c1:
        st.markdown(
            f"<div class='ov-panel'><h4>Operators · mean delay, {esc(window_label(ws, we))}</h4>",
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            operators_figure(data["operators"]),
            use_container_width=True,
            config={"displayModeBar": False},
        )
        st.markdown("</div>", unsafe_allow_html=True)
    with c2:
        st.markdown(
            "<div class='ov-panel'><h4>Delay distribution · active trips right now</h4>",
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            histogram_figure(data["hist"]),
            use_container_width=True,
            config={"displayModeBar": False},
        )
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown(ops_strip(load_ops()), unsafe_allow_html=True)
    st.caption(
        "OVapi GTFS-realtime → Redpanda → windowed processor → Postgres → this page. "
        "Aggregates are exact per 5-minute window (count, mean, P90, max); tables use the last finalized window."
    )


live_view(tuple(operators), tuple(modes), min_count, show_stations, point_size)
