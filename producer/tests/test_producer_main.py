"""Producer poll loop against recorded fixtures with a fake feed client and fake publisher."""

from pathlib import Path

from producer.feed import FeedResponse
from producer.main import Config, Producer

FIXTURES = Path(__file__).parent / "fixtures" / "gtfsrt"


class FakeClient:
    def __init__(self, bodies):
        self.bodies = bodies

    def fetch(self, path):
        return FeedResponse(body=self.bodies[path], status=200, elapsed_s=0.01)

    def close(self):
        pass


class FakePublisher:
    def __init__(self):
        self.events, self.dead = [], []

    def publish(self, event):
        self.events.append(event)

    def publish_dead_letter(self, source_topic, key, raw, reason, **extra):
        self.dead.append((source_topic, key, reason))

    def flush(self, timeout_s=30):
        pass


def make(bodies, **overrides):
    defaults = dict(
        base_url="http://feed",
        bootstrap="none",
        operator_filter=frozenset(),
        positions_interval_s=10,
        trip_updates_interval_s=30,
        prediction_horizon_s=None,
        heartbeat_s=60,
        max_position_age_s=10**9,  # fixtures were recorded in the past; don't age-filter by default
        metrics_port=0,
    )
    cfg = Config(**{**defaults, **overrides})
    publisher = FakePublisher()
    return Producer(cfg, FakeClient(bodies), publisher), publisher


def test_parse_errors_go_to_dlq_once_not_every_poll():
    body = (FIXTURES / "vehicle_positions_mixed.pb").read_bytes()
    producer, publisher = make({"vehiclePositions.pb": body})
    positions = producer.feeds[0]
    producer.poll_once(positions)
    producer.poll_once(positions)
    assert len(publisher.events) == 1  # unchanged snapshot: published once
    assert len(publisher.dead) == 3  # three distinct malformed entities, once each
    assert {d[0] for d in publisher.dead} == {"vehicle_positions"}


def test_stale_positions_are_dropped():
    body = (FIXTURES / "vehicle_positions.pb").read_bytes()
    fresh, pub_fresh = make({"vehiclePositions.pb": body})
    fresh.poll_once(fresh.feeds[0])
    strict, pub_strict = make({"vehiclePositions.pb": body}, max_position_age_s=60)
    strict.poll_once(strict.feeds[0])
    assert len(pub_strict.events) < len(pub_fresh.events)
    assert len(pub_strict.events) > 100  # most of the recorded fleet reported within a minute


def test_garbage_feed_goes_to_dlq_whole():
    producer, publisher = make({"vehiclePositions.pb": (FIXTURES / "garbage.pb").read_bytes()})
    producer.poll_once(producer.feeds[0])
    assert publisher.events == []
    [(topic, key, reason)] = publisher.dead
    assert (topic, key) == ("vehicle_positions", "feed")
    assert reason.startswith("not a GTFS-RT FeedMessage")


def test_trip_updates_flow_and_heartbeat():
    body = (FIXTURES / "trip_updates.pb").read_bytes()
    producer, publisher = make({"tripUpdates.pb": body})
    feed = producer.feeds[1]
    producer.poll_once(feed)
    first = len(publisher.events)
    producer.poll_once(feed)
    assert len(publisher.events) == first  # identical snapshot, nothing new
    feed.differ.clock = lambda: 10**9  # far in the future -> heartbeat re-emits everything
    producer.poll_once(feed)
    assert len(publisher.events) == 2 * first


def test_rate_limit_backs_off_instead_of_hammering():
    from producer.feed import RateLimited

    class LimitedClient(FakeClient):
        def fetch(self, path):
            raise RateLimited(retry_after_s=120)

    producer, publisher = make({})
    producer.client = LimitedClient({})
    feed = producer.feeds[0]
    producer.poll_once(feed)
    assert feed.backoff_s == 120 and feed.next_due > 0
    assert publisher.events == []


def test_positions_from_the_future_are_dropped():
    from datetime import UTC, datetime, timedelta

    from common.events import VehiclePosition
    from producer.parse import ParseResult

    feed_time = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)
    ok = VehiclePosition("v-ok", "t", "r", "OP", 52.0, 4.9, feed_time - timedelta(seconds=30))
    ahead = VehiclePosition(
        "v-future", "t", "r", "OP", 52.0, 4.9, feed_time + timedelta(minutes=29)
    )
    producer, publisher = make({"vehiclePositions.pb": b"unused"}, max_position_age_s=600)
    producer._parse = lambda feed, body: ParseResult(feed_timestamp=feed_time, events=[ok, ahead])
    producer.poll_once(producer.feeds[0])
    assert [e.vehicle_id for e in publisher.events] == ["v-ok"]


def test_same_broken_entity_with_a_new_timestamp_is_not_dead_lettered_again():
    from datetime import UTC, datetime

    from producer.parse import ParseError, ParseResult

    producer, publisher = make({"vehiclePositions.pb": b"unused"})
    feed = producer.feeds[0]
    for poll in range(3):  # the feed re-sends the entity every poll with fresh bytes
        err = ParseError("2026-09-06:BAD:1:1", "vehicle without position", raw=bytes([poll]))
        producer._parse = lambda f, body, err=err: ParseResult(
            feed_timestamp=datetime(2026, 9, 6, 12, poll, tzinfo=UTC), errors=[err]
        )
        producer.poll_once(feed)
    assert len(publisher.dead) == 1
