"""Colours, delay buckets and formatting shared by the app and its tests (pure functions)."""

from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

AMS = ZoneInfo("Europe/Amsterdam")

BG = "#0B1020"
PANEL = "#131A2E"
PANEL_2 = "#1A2340"
BORDER = "#243055"
TEXT = "#E6EAF2"
MUTED = "#8A94B3"
ACCENT = "#2DD4BF"

# Delay severity buckets (seconds). "on time" is anything within a minute either way.
BUCKETS: tuple[tuple[str, float, str, tuple[int, int, int]], ...] = (
    # label, upper bound (exclusive), hex, rgb
    ("early", -60, "#60A5FA", (96, 165, 250)),
    ("on time", 60, "#2DD4BF", (45, 212, 191)),
    ("< 2 min", 120, "#FDE047", (253, 224, 71)),
    ("< 5 min", 300, "#F97316", (249, 115, 22)),
    ("5 min +", float("inf"), "#EF4444", (239, 68, 68)),
)
UNKNOWN_HEX = "#6B7280"
UNKNOWN_RGB = (107, 114, 128)

ROUTE_TYPES = {0: "Tram", 1: "Metro", 2: "Rail", 3: "Bus", 4: "Ferry", 7: "Funicular"}


def bucket(delay_s: float | None) -> str:
    if delay_s is None:
        return "unknown"
    for label, upper, _, _ in BUCKETS:
        if delay_s < upper:
            return label
    return BUCKETS[-1][0]


def hex_for(delay_s: float | None) -> str:
    if delay_s is None:
        return UNKNOWN_HEX
    for _, upper, hex_, _ in BUCKETS:
        if delay_s < upper:
            return hex_
    return BUCKETS[-1][2]


def rgb_for(delay_s: float | None) -> list[int]:
    if delay_s is None:
        return list(UNKNOWN_RGB)
    for _, upper, _, rgb in BUCKETS:
        if delay_s < upper:
            return list(rgb)
    return list(BUCKETS[-1][3])


def fmt_delay(delay_s: float | None, *, signed: bool = True) -> str:
    """Seconds -> '+2:35', '-0:40', '0:00'. Rounded to whole seconds."""
    if delay_s is None:
        return "—"
    total = round(abs(delay_s))
    minutes, seconds = divmod(total, 60)
    sign = "-" if delay_s < 0 else ("+" if signed and total > 0 else "")
    return f"{sign}{minutes}:{seconds:02d}"


def fmt_age(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    if seconds < 60:
        return f"{int(seconds)}s ago"
    if seconds < 3600:
        return f"{int(seconds // 60)}m {int(seconds % 60)}s ago"
    return f"{seconds / 3600:.1f}h ago"


def local(ts: datetime | None, fmt: str = "%H:%M") -> str:
    """UTC -> Europe/Amsterdam for display only (PRD N5)."""
    if ts is None:
        return "—"
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return ts.astimezone(AMS).strftime(fmt)


def window_label(start: datetime | None, end: datetime | None) -> str:
    if start is None or end is None:
        return "no window yet"
    return f"{local(start)}–{local(end)}"


def route_type_name(route_type: int | None) -> str:
    return ROUTE_TYPES.get(route_type, "Other") if route_type is not None else "Other"
