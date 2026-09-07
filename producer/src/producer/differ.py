"""Turn full-snapshot polls into change events.

Both OVapi feeds are ``FULL_DATASET`` snapshots: every poll repeats every vehicle and every
trip. Publishing each snapshot verbatim would turn a 10 s poll of 7 000 trips into 700 msg/s
of mostly-unchanged data. The differ keeps the last fingerprint per key and emits an event only
when it changed — plus a periodic heartbeat (``max_age_s``) so every active key still shows up
in every window even when its delay is stable. That heartbeat is what keeps the window means
honest: without it a trip that sits at +300 s for ten minutes would be counted once.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Hashable, Iterable
from dataclasses import dataclass, field


@dataclass(slots=True)
class _Seen:
    fingerprint: Hashable
    emitted_at: float
    last_seen_poll: int


@dataclass(slots=True)
class DiffStats:
    emitted: int = 0
    unchanged: int = 0
    heartbeats: int = 0
    evicted: int = 0


@dataclass
class SnapshotDiffer[T]:
    """``diff(events)`` returns the subset worth publishing.

    ``key`` and ``fingerprint`` are callables on the event; ``max_age_s`` re-emits unchanged keys
    that were last published longer ago than that (``None`` disables heartbeats); keys absent
    for ``evict_after_polls`` consecutive polls are forgotten (vehicle went to the depot).
    """

    key: Callable[[T], Hashable]
    fingerprint: Callable[[T], Hashable]
    max_age_s: float | None = 60.0
    # A poll scheduled every 60 s lands at 59.98 s on the monotonic clock; without slack a 60 s
    # heartbeat would fire on every *other* poll. Any poll at or past (max_age_s - jitter_s) counts.
    jitter_s: float = 5.0
    evict_after_polls: int = 30
    clock: Callable[[], float] = time.monotonic
    _seen: dict[Hashable, _Seen] = field(default_factory=dict, init=False, repr=False)
    _poll: int = field(default=0, init=False)

    def __len__(self) -> int:
        return len(self._seen)

    def diff(self, events: Iterable[T]) -> tuple[list[T], DiffStats]:
        self._poll += 1
        now = self.clock()
        stats = DiffStats()
        out: list[T] = []
        for event in events:
            k = self.key(event)
            fp = self.fingerprint(event)
            seen = self._seen.get(k)
            if seen is None or seen.fingerprint != fp:
                self._seen[k] = _Seen(fp, now, self._poll)
                out.append(event)
                stats.emitted += 1
            elif (
                self.max_age_s is not None
                and now - seen.emitted_at >= self.max_age_s - self.jitter_s
            ):
                seen.emitted_at = now
                seen.last_seen_poll = self._poll
                out.append(event)
                stats.emitted += 1
                stats.heartbeats += 1
            else:
                seen.last_seen_poll = self._poll
                stats.unchanged += 1
        stale = [
            k
            for k, s in self._seen.items()
            if self._poll - s.last_seen_poll >= self.evict_after_polls
        ]
        for k in stale:
            del self._seen[k]
        stats.evicted = len(stale)
        return out, stats
