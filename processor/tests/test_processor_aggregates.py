"""The hand-computed window (week-2 exit criterion): arithmetic in comments, engine must match."""

from datetime import UTC, datetime, timedelta

import pytest

from processor.aggregates import percentile_nearest_rank
from processor.windows import TumblingWindower

T0 = datetime(2026, 9, 6, 8, 15, 0, tzinfo=UTC)  # 08:15 is a 5-minute boundary


def test_hand_computed_window_for_line_ns_ic_800():
    # Ten predictions for one line inside the 08:15-08:20 window, delays in seconds:
    #
    #   t=+10s  120     t=+40s   60     t=+70s  300     t=+100s  0      t=+130s  45
    #   t=+160s 600     t=+190s  90     t=+220s  15     t=+250s  180    t=+280s  30
    #
    # sorted:  0, 15, 30, 45, 60, 90, 120, 180, 300, 600
    # count  = 10
    # sum    = 0+15+30+45+60+90+120+180+300+600 = 1440   -> mean = 144.0
    # P90    = nearest rank ceil(0.9 * 10) = 9th value    -> 300
    # max    = 600, min = 0
    delays = [120, 60, 300, 0, 45, 600, 90, 15, 180, 30]
    w = TumblingWindower(window_s=300, allowed_lateness_s=120)
    for i, delay in enumerate(delays):
        w.add(T0 + timedelta(seconds=10 + 30 * i), "line", "NS:IC800", delay, operator="NS")
    # Close the window: any event at/after 08:22:00 (= end 08:20 + 2 min lateness) does it.
    w.add(T0 + timedelta(minutes=7), "line", "NS:IC800", 0, operator="NS")

    [agg] = w.pop_finalized()
    assert agg.key.window_start == T0
    assert agg.key.window_end == T0 + timedelta(minutes=5)
    assert (agg.key.dimension, agg.key.dimension_id, agg.operator) == ("line", "NS:IC800", "NS")
    assert agg.count == 10
    assert agg.mean == pytest.approx(144.0)
    assert agg.p90 == 300
    assert agg.max == 600
    assert agg.min == 0


@pytest.mark.parametrize(
    "values, p, expected",
    [
        ([1], 90, 1),
        ([1, 2], 90, 2),  # ceil(1.8) = 2 -> second value
        ([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 90, 9),
        ([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 50, 5),
        ([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], 90, 10),  # ceil(9.9) = 10
        ([5, 5, 5], 90, 5),
    ],
)
def test_percentile_nearest_rank(values, p, expected):
    assert percentile_nearest_rank(values, p) == expected


def test_percentile_of_empty_sample_is_an_error():
    with pytest.raises(ValueError):
        percentile_nearest_rank([], 90)


def test_negative_delays_early_vehicles_are_real_values_not_clamped():
    w = TumblingWindower(300, 0)
    for d in (-60, -30, 0, 30):
        w.add(T0, "station", "S1", d)
    w.advance_to(T0 + timedelta(minutes=6))
    [agg] = w.pop_finalized()
    assert agg.min == -60 and agg.mean == pytest.approx(-15.0)
