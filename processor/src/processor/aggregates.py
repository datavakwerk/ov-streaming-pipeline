"""Per-window accumulators: count, mean, P90, max, min of delay seconds.

P90 is computed exactly (nearest-rank on the sorted values). At our volumes — a few thousand
observations per window per key at most — that is trivially cheap and, unlike an approximation,
reproducible in a hand-computed test. At real scale (millions per window) this is the piece
you swap for a mergeable sketch such as t-digest or KLL; the interface stays the same.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True, slots=True)
class WindowKey:
    window_start: datetime
    window_end: datetime
    dimension: str  # "line" | "station"
    dimension_id: str


@dataclass(frozen=True, slots=True)
class Aggregate:
    key: WindowKey
    operator: str | None
    count: int
    mean: float
    p90: float
    max: float
    min: float


def percentile_nearest_rank(sorted_values: list[float], p: float) -> float:
    """Nearest-rank percentile: the smallest value with at least p% of the sample at or below it."""
    if not sorted_values:
        raise ValueError("percentile of an empty sample")
    rank = max(1, math.ceil(p / 100.0 * len(sorted_values)))
    return sorted_values[rank - 1]


@dataclass(slots=True)
class Accumulator:
    operator: str | None = None
    values: list[float] = field(default_factory=list)
    seen: set = field(default_factory=set)  # dedupe keys inside this open window

    def add(self, value: float, dedupe_key=None) -> bool:
        if dedupe_key is not None:
            if dedupe_key in self.seen:
                return False
            self.seen.add(dedupe_key)
        self.values.append(value)
        return True

    def finalize(self, key: WindowKey) -> Aggregate:
        values = sorted(self.values)
        return Aggregate(
            key=key,
            operator=self.operator,
            count=len(values),
            mean=sum(values) / len(values),
            p90=percentile_nearest_rank(values, 90),
            max=values[-1],
            min=values[0],
        )
