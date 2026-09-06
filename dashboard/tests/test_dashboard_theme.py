from datetime import UTC, datetime

from dashboard.theme import bucket, fmt_age, fmt_delay, hex_for, local, rgb_for, window_label


def test_buckets_follow_the_documented_thresholds():
    assert bucket(-90) == "early"
    assert bucket(-59) == "on time" and bucket(0) == "on time" and bucket(59) == "on time"
    assert bucket(60) == "< 2 min" and bucket(119) == "< 2 min"
    assert bucket(120) == "< 5 min" and bucket(299) == "< 5 min"
    assert bucket(300) == "5 min +" and bucket(9999) == "5 min +"
    assert bucket(None) == "unknown"


def test_colors_match_buckets():
    assert hex_for(0) == "#2DD4BF" and rgb_for(0) == [45, 212, 191]
    assert hex_for(600) == "#EF4444" and hex_for(None) == "#6B7280"


def test_fmt_delay():
    assert fmt_delay(0) == "0:00"
    assert fmt_delay(155) == "+2:35"
    assert fmt_delay(-40) == "-0:40"
    assert fmt_delay(3600) == "+60:00"
    assert fmt_delay(59.6) == "+1:00"
    assert fmt_delay(None) == "—"


def test_fmt_age():
    assert fmt_age(12) == "12s ago" and fmt_age(125) == "2m 5s ago" and fmt_age(7200) == "2.0h ago"


def test_local_time_is_amsterdam_and_windows_render_as_ranges():
    start = datetime(2026, 9, 6, 22, 0, tzinfo=UTC)  # CEST = UTC+2
    end = datetime(2026, 9, 6, 22, 5, tzinfo=UTC)
    assert local(start) == "00:00"
    assert window_label(start, end) == "00:00–00:05"
    assert window_label(None, None) == "no window yet"
