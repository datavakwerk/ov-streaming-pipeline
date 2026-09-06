"""Tumbling-window engine. Pure Python, zero Kafka imports, deterministic (COMMITS.md commit 8).

Semantics
---------
* Windows are ``window_s`` wide, aligned to the epoch: ``start = floor(t / window_s) * window_s``.
  An event exactly on a boundary belongs to the window that *starts* there ([start, end)).
* **Watermark** = min over *active sources* of (max event time seen on that source)
  - ``allowed_lateness_s``. A source is a Kafka partition: partitions are consumed at slightly
  different speeds (very different during catch-up or replay), so a single global max would
  declare a slower partition's perfectly good events "late". Taking the minimum is what Flink
  does with per-partition watermarks. A source that produced nothing for ``idle_source_s`` is
  excluded until it speaks again (Flink's idleness timeout), so one quiet partition cannot stall
  the whole pipeline. A window is finalized (emitted once, then forgotten) as soon as the
  watermark reaches its end.
* Events whose window has already been finalized are **late**: counted, dropped. This is the
  documented trade-off (PRD §5.2): bounded state and a single emission per window, in exchange
  for ignoring anything more than ``allowed_lateness_s`` behind the stream.
* Duplicates (same ``dedupe_key``) inside an open window are ignored; cross-finalization
  redelivery safety comes from the sink's idempotent upsert, not from here.
* ``advance_to(t)`` lets the caller move the watermark by wall-clock when the stream goes quiet
  (the overnight lull) so the last window still closes.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Hashable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from processor.aggregates import Accumulator, Aggregate, WindowKey

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def window_start_for(event_time: datetime, window_s: int) -> datetime:
    seconds = int((event_time - EPOCH).total_seconds())
    return EPOCH + timedelta(seconds=seconds - seconds % window_s)


@dataclass(slots=True)
class WindowStats:
    accepted: int = 0
    late: int = 0
    duplicates: int = 0
    finalized: int = 0


DEFAULT_SOURCE = "default"


@dataclass
class TumblingWindower:
    window_s: int = 300
    allowed_lateness_s: int = 120
    idle_source_s: float = 120.0
    watermark: datetime | None = None
    stats: WindowStats = field(default_factory=WindowStats)
    clock: Callable[[], float] = time.monotonic
    _open: dict[WindowKey, Accumulator] = field(default_factory=dict, init=False, repr=False)
    # source -> (max event time seen on it, when we last heard from it, in clock() seconds)
    _sources: dict[Hashable, tuple[datetime, float]] = field(
        default_factory=dict, init=False, repr=False
    )

    # ----------------------------------------------------------------- ingest
    def add(
        self,
        event_time: datetime,
        dimension: str,
        dimension_id: str,
        value: float,
        *,
        operator: str | None = None,
        dedupe_key: Hashable | None = None,
        source: Hashable = DEFAULT_SOURCE,
    ) -> bool:
        """Route one observation into its window. Returns False if late or duplicate."""
        start = window_start_for(event_time, self.window_s)
        end = start + timedelta(seconds=self.window_s)
        if self.watermark is not None and end <= self.watermark:
            self.stats.late += 1
            return False
        key = WindowKey(start, end, dimension, dimension_id)
        acc = self._open.get(key)
        if acc is None:
            acc = self._open[key] = Accumulator(operator=operator)
        if not acc.add(value, dedupe_key):
            self.stats.duplicates += 1
            return False
        self.stats.accepted += 1
        self._observe_source(source, event_time)
        return True

    # ------------------------------------------------------------- watermark
    def _observe_source(self, source: Hashable, event_time: datetime) -> None:
        now = self.clock()
        previous = self._sources.get(source)
        self._sources[source] = (
            event_time if previous is None or event_time > previous[0] else previous[0],
            now,
        )
        self._recompute_watermark(now)

    def _recompute_watermark(self, now: float) -> None:
        active = [t for t, seen in self._sources.values() if now - seen <= self.idle_source_s]
        if active:
            self._raise_watermark(min(active) - timedelta(seconds=self.allowed_lateness_s))

    def _raise_watermark(self, candidate: datetime) -> None:
        if self.watermark is None or candidate > self.watermark:
            self.watermark = candidate

    def active_sources(self) -> int:
        now = self.clock()
        return sum(1 for _, seen in self._sources.values() if now - seen <= self.idle_source_s)

    def advance_to(self, now: datetime) -> None:
        """Idle-time watermark: pretend an event at ``now`` arrived, without adding data."""
        self._raise_watermark(now - timedelta(seconds=self.allowed_lateness_s))

    def seed_watermark(self, watermark: datetime) -> None:
        """Start from a known point (e.g. the newest finalized window in the sink) so redelivered
        events for already-written windows are treated as late instead of recomputed partially."""
        self._raise_watermark(watermark)

    # -------------------------------------------------------------- finalize
    def pop_finalized(self) -> list[Aggregate]:
        if self.watermark is None:
            return []
        done = [k for k in self._open if k.window_end <= self.watermark]
        out = [self._open.pop(k).finalize(k) for k in sorted(done, key=lambda k: k.window_start)]
        self.stats.finalized += len(out)
        return out

    def open_windows(self) -> int:
        return len(self._open)

    def open_observations(self) -> int:
        return sum(len(a.values) for a in self._open.values())
