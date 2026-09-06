"""Tumbling-window engine. Pure Python, zero Kafka imports, deterministic (COMMITS.md commit 8).

Semantics
---------
* Windows are ``window_s`` wide, aligned to the epoch: ``start = floor(t / window_s) * window_s``.
  An event exactly on a boundary belongs to the window that *starts* there ([start, end)).
* **Watermark** = max event time seen - ``allowed_lateness_s``. A window is finalized (emitted
  once, then forgotten) as soon as the watermark reaches its end.
* Events whose window has already been finalized are **late**: counted, dropped. This is the
  documented trade-off (PRD §5.2): bounded state and a single emission per window, in exchange
  for ignoring anything more than ``allowed_lateness_s`` behind the stream.
* Duplicates (same ``dedupe_key``) inside an open window are ignored; cross-finalization
  redelivery safety comes from the sink's idempotent upsert, not from here.
* ``advance_to(t)`` lets the caller move the watermark by wall-clock when the stream goes quiet
  (the overnight lull) so the last window still closes.
"""

from __future__ import annotations

from collections.abc import Hashable
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


@dataclass
class TumblingWindower:
    window_s: int = 300
    allowed_lateness_s: int = 120
    watermark: datetime | None = None
    stats: WindowStats = field(default_factory=WindowStats)
    _open: dict[WindowKey, Accumulator] = field(default_factory=dict, init=False, repr=False)
    _max_event_time: datetime | None = field(default=None, init=False)

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
        if self._max_event_time is None or event_time > self._max_event_time:
            self._max_event_time = event_time
            self._raise_watermark(event_time - timedelta(seconds=self.allowed_lateness_s))
        return True

    # ------------------------------------------------------------- watermark
    def _raise_watermark(self, candidate: datetime) -> None:
        if self.watermark is None or candidate > self.watermark:
            self.watermark = candidate

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
