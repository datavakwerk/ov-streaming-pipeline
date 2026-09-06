"""Publisher routing/serialization with a fake librdkafka producer (no broker in CI)."""

from datetime import UTC, datetime

from common.events import ArrivalPrediction, VehiclePosition
from common.serde import decode
from producer.publisher import KafkaPublisher, default_config

T = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)


class FakeRawProducer:
    def __init__(self, fail=False):
        self.messages = []
        self.fail = fail
        self.flushed = False

    def produce(self, topic, value, key, headers, on_delivery):
        self.messages.append((topic, key, value, dict(headers)))
        on_delivery(RuntimeError("boom") if self.fail else None, _Msg(topic, key))

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        self.flushed = True
        return 0


class _Msg:
    def __init__(self, topic, key):
        self._topic, self._key = topic, key

    def topic(self):
        return self._topic

    def key(self):
        return self._key


def test_routes_by_event_type_and_keys_by_entity():
    raw = FakeRawProducer()
    publisher = KafkaPublisher(raw)
    position = VehiclePosition("GVB:13:1", "t1", "r1", "GVB", 52.0, 4.9, T)
    prediction = ArrivalPrediction("t1", "r1", "GVB", "s1", 42, T)

    assert publisher.publish(position) == "vehicle_positions"
    assert publisher.publish(prediction) == "arrival_predictions"

    (topic_a, key_a, value_a, headers_a), (topic_b, key_b, value_b, _) = raw.messages
    assert (topic_a, key_a) == ("vehicle_positions", b"GVB:13:1")  # key = vehicle id
    assert (topic_b, key_b) == ("arrival_predictions", b"t1")  # key = trip id
    assert decode(value_a) == position and decode(value_b) == prediction
    assert headers_a["schema_version"] == b"1" and headers_a["event_type"] == b"vehicle_position"


def test_dead_letter_carries_original_bytes_and_error_headers():
    raw = FakeRawProducer()
    publisher = KafkaPublisher(raw)
    topic = publisher.publish_dead_letter(
        "vehicle_positions",
        key="2026-09-06:BAD:1:1",
        raw=b"\x01\x02",
        reason="vehicle without position",
        entity_id="2026-09-06:BAD:1:1",
    )
    assert topic == "dlq.vehicle_positions"
    (_, key, value, headers) = raw.messages[0]
    assert value == b"\x01\x02" and key == b"2026-09-06:BAD:1:1"
    assert headers["stage"] == b"parse"
    assert headers["source_topic"] == b"vehicle_positions"
    assert headers["error"] == b"vehicle without position"
    assert b"T" in headers["failed_at"]


def test_delivery_errors_are_counted_not_raised():
    publisher = KafkaPublisher(FakeRawProducer(fail=True))
    publisher.publish(VehiclePosition("v", "t", "r", "OP", 52.0, 4.9, T))
    assert publisher.delivery_errors == 1


def test_flush_delegates():
    raw = FakeRawProducer()
    KafkaPublisher(raw).flush()
    assert raw.flushed


def test_default_config_is_idempotent_and_acks_all():
    cfg = default_config("redpanda:9092")
    assert cfg["enable.idempotence"] is True and cfg["acks"] == "all"
