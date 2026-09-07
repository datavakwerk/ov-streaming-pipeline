from producer.differ import SnapshotDiffer


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def make(clock, **kw):
    return SnapshotDiffer(key=lambda e: e[0], fingerprint=lambda e: e[1], clock=clock, **kw)


def test_first_snapshot_is_emitted_entirely():
    differ = make(Clock())
    out, stats = differ.diff([("a", 1), ("b", 2)])
    assert out == [("a", 1), ("b", 2)] and stats.emitted == 2


def test_unchanged_keys_are_suppressed_and_changed_ones_emitted():
    differ = make(Clock())
    differ.diff([("a", 1), ("b", 2)])
    out, stats = differ.diff([("a", 1), ("b", 3)])
    assert out == [("b", 3)]
    assert (stats.emitted, stats.unchanged) == (1, 1)


def test_heartbeat_reemits_stable_keys_after_max_age():
    clock = Clock()
    differ = make(clock, max_age_s=60)
    differ.diff([("a", 1)])
    clock.t += 30
    assert differ.diff([("a", 1)])[0] == []
    clock.t += 31
    out, stats = differ.diff([("a", 1)])
    assert out == [("a", 1)] and stats.heartbeats == 1


def test_heartbeat_tolerates_poll_scheduling_jitter():
    clock = Clock()
    differ = make(clock, max_age_s=60, jitter_s=5)
    differ.diff([("a", 1)])
    clock.t += 59.98  # a "60 s" poll interval as the monotonic clock actually sees it
    out, stats = differ.diff([("a", 1)])
    assert out == [("a", 1)] and stats.heartbeats == 1


def test_no_heartbeat_when_disabled():
    clock = Clock()
    differ = make(clock, max_age_s=None)
    differ.diff([("a", 1)])
    clock.t += 3600
    assert differ.diff([("a", 1)])[0] == []


def test_keys_missing_for_n_polls_are_evicted_then_treated_as_new():
    differ = make(Clock(), evict_after_polls=2)
    differ.diff([("a", 1), ("b", 1)])
    differ.diff([("a", 1)])
    _, stats = differ.diff([("a", 1)])
    assert stats.evicted == 1 and len(differ) == 1
    out, _ = differ.diff([("a", 1), ("b", 1)])
    assert out == [("b", 1)]
