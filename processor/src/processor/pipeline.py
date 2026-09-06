"""The processing logic, independent of Kafka: decode -> route -> window -> sink.

Kept separate from ``consumer.py`` so the live consumer and ``ov-replay`` (reading the MinIO
archive) share exactly one code path — replay is not a second implementation.
"""

from __future__ import annotations

import logging
from collections.abc import Hashable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from common.events import ArrivalPrediction, Event, VehiclePosition
from common.serde import SchemaError, decode
from processor import metrics
from processor.aggregates import Aggregate
from processor.sink import PostgresSink
from processor.windows import TumblingWindower

log = logging.getLogger(__name__)


@dataclass
class Pipeline:
    windower: TumblingWindower
    sink: PostgresSink
    _positions: list[VehiclePosition] = field(default_factory=list, init=False)
    _predictions: list[ArrivalPrediction] = field(default_factory=list, init=False)
    _reported: dict[str, int] = field(
        default_factory=lambda: {"late": 0, "duplicates": 0}, init=False
    )

    # ------------------------------------------------------------------ ingest
    def decode(self, raw: bytes) -> Event:
        """Raises SchemaError for the caller to dead-letter."""
        return decode(raw)

    def ingest(self, event: Event, source: Hashable = "default") -> None:
        """``source`` identifies the Kafka partition for per-partition watermarks."""
        if isinstance(event, VehiclePosition):
            self._positions.append(event)
            return
        self._predictions.append(event)
        # One prediction feeds every dimension (PRD §5.2): line and station as required, plus
        # operator and the whole network so the dashboard gets exact (not re-aggregated) P90s.
        # Dedupe key: a trip cannot have two different observations at the same instant;
        # heartbeats are distinct by timestamp.
        dedupe = (event.trip_id, event.timestamp)
        for dimension, dim_id in (
            ("line", event.route_id),
            ("station", event.stop_id),
            ("operator", event.operator),
            ("network", "NL"),
        ):
            if not dim_id:
                continue
            accepted = self.windower.add(
                event.timestamp,
                dimension,
                dim_id,
                float(event.delay_seconds),
                operator=event.operator,
                dedupe_key=dedupe,
                source=source,
            )
            if accepted:
                metrics.OBSERVATIONS.labels(dimension).inc()

    # ------------------------------------------------------------------- flush
    def flush_state(self) -> None:
        """Write the 'right now' tables for everything ingested since the last flush."""
        if self._positions:
            self.sink.upsert_vehicles(self._positions)
            self._positions.clear()
        if self._predictions:
            self.sink.upsert_trip_delays(self._predictions)
            self._predictions.clear()

    def finalize(self, now: datetime | None = None, idle_s: float | None = None) -> list[Aggregate]:
        """Finalize windows the watermark has passed and upsert them. Returns what was written."""
        if now is not None and idle_s is not None:
            self.windower.advance_to(now)
        aggregates = self.windower.pop_finalized()
        if aggregates:
            self.sink.upsert_aggregates(aggregates)
            for agg in aggregates:
                metrics.WINDOWS_FINALIZED.labels(agg.key.dimension).inc()
            log.info(
                "finalized %d windows up to %s (watermark %s)",
                len(aggregates),
                max(a.key.window_end for a in aggregates).isoformat(timespec="seconds"),
                self.windower.watermark.isoformat(timespec="seconds"),
            )
        self._export_gauges()
        return aggregates

    def _export_gauges(self) -> None:
        stats = self.windower.stats  # the pure engine counts; we mirror deltas into Prometheus
        metrics.LATE.inc(stats.late - self._reported["late"])
        metrics.DUPLICATES.inc(stats.duplicates - self._reported["duplicates"])
        self._reported.update(late=stats.late, duplicates=stats.duplicates)
        metrics.OPEN_WINDOWS.set(self.windower.open_windows())
        metrics.ACTIVE_SOURCES.set(self.windower.active_sources())
        metrics.OPEN_OBSERVATIONS.set(self.windower.open_observations())
        if self.windower.watermark is not None:
            metrics.WATERMARK_LAG.set(
                (datetime.now(tz=UTC) - self.windower.watermark).total_seconds()
            )


__all__ = ["Pipeline", "SchemaError"]
