"""Window assignment, lateness, duplicates and finalization with synthetic event sequences."""

from datetime import UTC, datetime, timedelta

from processor.windows import TumblingWindower, window_start_for

T0 = datetime(2026, 9, 6, 12, 0, 0, tzinfo=UTC)  # a window boundary (12:00:00 is a multiple of 5m)


def at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


def test_window_start_is_epoch_aligned():
    assert window_start_for(at(0), 300) == T0
    assert window_start_for(at(299.999), 300) == T0
    assert window_start_for(at(300), 300) == at(300)  # boundary event -> the window starting there
    assert window_start_for(datetime(2026, 9, 6, 12, 3, 17, tzinfo=UTC), 300) == T0


def test_nothing_finalizes_before_watermark_passes_window_end():
    w = TumblingWindower(window_s=300, allowed_lateness_s=120)
    w.add(at(10), "line", "L1", 60)
    w.add(at(290), "line", "L1", 120)
    assert w.pop_finalized() == []  # watermark = 290 - 120 = 170 < 300
    w.add(at(419), "line", "L1", 0)  # watermark 299: still open
    assert w.pop_finalized() == []
    w.add(at(420), "line", "L1", 0)  # watermark 300 == window end -> finalize
    [agg] = w.pop_finalized()
    assert (agg.key.window_start, agg.key.window_end) == (T0, at(300))
    assert agg.count == 2 and agg.mean == 90 and agg.max == 120 and agg.min == 60
    assert w.open_windows() == 1  # the 12:05 window with the two zero delays


def test_out_of_order_within_lateness_is_accepted():
    w = TumblingWindower(300, 120)
    w.add(at(400), "line", "L1", 10)  # watermark 280
    assert w.add(at(250), "line", "L1", 50) is True  # earlier than max, but window still open
    w.add(at(421), "line", "L1", 0)
    [agg] = w.pop_finalized()
    assert agg.count == 1 and agg.mean == 50


def test_event_exactly_at_lateness_boundary_is_late_once_window_closed():
    w = TumblingWindower(300, 120)
    w.add(at(420), "line", "L1", 1)  # watermark = 300: window [0,300) is closed
    assert w.add(at(299), "line", "L1", 99) is False
    assert w.stats.late == 1
    assert w.pop_finalized() == []  # nothing was ever in the closed window


def test_late_events_are_counted_and_dropped_not_reopened():
    w = TumblingWindower(300, 120)
    w.add(at(10), "station", "S1", 30)
    w.add(at(430), "station", "S1", 30)
    [first] = w.pop_finalized()
    assert first.count == 1
    assert w.add(at(20), "station", "S1", 999) is False  # arrives after finalization
    assert w.stats.late == 1 and w.open_windows() == 1
    assert w.pop_finalized() == []  # no re-emission of the [0,300) window


def test_duplicates_within_open_window_are_ignored():
    w = TumblingWindower(300, 120)
    assert w.add(at(1), "line", "L1", 10, dedupe_key=("v1", at(1)))
    assert w.add(at(1), "line", "L1", 10, dedupe_key=("v1", at(1))) is False
    assert w.add(at(1), "line", "L1", 10, dedupe_key=("v2", at(1)))  # a different vehicle counts
    assert w.stats.duplicates == 1
    w.advance_to(at(500))
    [agg] = w.pop_finalized()
    assert agg.count == 2


def test_one_event_can_feed_two_dimensions_independently():
    w = TumblingWindower(300, 120)
    w.add(at(5), "line", "L1", 100)
    w.add(at(5), "station", "S9", 100)
    w.advance_to(at(1000))
    aggs = w.pop_finalized()
    assert {(a.key.dimension, a.key.dimension_id) for a in aggs} == {
        ("line", "L1"),
        ("station", "S9"),
    }


def test_idle_advance_closes_the_last_window():
    w = TumblingWindower(300, 120)
    w.add(at(100), "line", "L1", 5)
    assert w.pop_finalized() == []
    w.advance_to(at(419))
    assert w.pop_finalized() == []  # 419 - 120 = 299 < 300
    w.advance_to(at(420))
    assert len(w.pop_finalized()) == 1


def test_seeded_watermark_treats_redelivered_old_events_as_late():
    w = TumblingWindower(300, 120)
    w.seed_watermark(at(600))  # the sink already holds windows up to 12:10
    assert w.add(at(450), "line", "L1", 7) is False  # belongs to [300,600): already written
    assert w.add(at(650), "line", "L1", 7) is True
    assert w.stats.late == 1


def test_finalized_windows_come_out_in_time_order():
    w = TumblingWindower(300, allowed_lateness_s=1000)  # generous lateness keeps all three open
    for s in (700, 10, 350):
        w.add(at(s), "line", "L1", 1)
    w.advance_to(at(5000))
    starts = [a.key.window_start for a in w.pop_finalized()]
    assert starts == sorted(starts) == [T0, at(300), at(600)]


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def test_watermark_follows_the_slowest_active_partition():
    w = TumblingWindower(300, allowed_lateness_s=0, clock=FakeClock())
    w.add(at(1000), "line", "L1", 1, source=("p", 0))  # partition 0 is far ahead
    assert w.watermark == at(1000)
    w.add(at(100), "line", "L1", 1, source=("p", 1))  # partition 1 lags: window [0,300) still open
    assert w.watermark == at(1000)  # never moves backwards ...
    w2 = TumblingWindower(300, allowed_lateness_s=0, clock=FakeClock())
    w2.add(at(100), "line", "L1", 1, source=("p", 1))
    w2.add(at(1000), "line", "L1", 1, source=("p", 0))
    assert w2.watermark == at(100)  # ... but with both sources known, the minimum rules
    assert w2.add(at(50), "line", "L1", 1, source=("p", 1)) is True  # not late on the slow one
    assert w2.stats.late == 0


def test_idle_partition_stops_holding_the_watermark_back():
    clock = FakeClock()
    w = TumblingWindower(300, allowed_lateness_s=0, idle_source_s=60, clock=clock)
    w.add(at(100), "line", "L1", 1, source=("p", 1))
    w.add(at(1000), "line", "L1", 1, source=("p", 0))
    assert w.watermark == at(100) and w.active_sources() == 2
    clock.t = 61  # partition 1 said nothing for over a minute
    w.add(at(1001), "line", "L1", 1, source=("p", 0))
    assert w.watermark == at(1001) and w.active_sources() == 1
    clock.t = 62
    w.add(at(400), "line", "L1", 1, source=("p", 1))  # it speaks again, behind: counted late
    assert w.stats.late == 1
