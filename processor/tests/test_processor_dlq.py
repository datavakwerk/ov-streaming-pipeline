from common.serde import SchemaError, decode
from processor.dlq import DeadLetterPublisher, dead_letter_headers


class FakeMsg:
    def __init__(self, value, key=b"k", headers=None):
        self._v, self._k, self._h = value, key, headers or [("schema_version", b"1")]

    def topic(self):
        return "arrival_predictions"

    def partition(self):
        return 3

    def offset(self):
        return 4242

    def value(self):
        return self._v

    def key(self):
        return self._k

    def headers(self):
        return self._h


class FakeRaw:
    def __init__(self):
        self.messages = []

    def produce(self, topic, value, key, headers, on_delivery=None):
        self.messages.append((topic, value, key, dict(headers)))

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        return 0


def test_malformed_bytes_become_a_dlq_message_with_error_metadata():
    garbage = b"\xff\xfe{not json"
    try:
        decode(garbage)
    except SchemaError as exc:
        reason = str(exc)
    raw = FakeRaw()
    topic = DeadLetterPublisher(raw).send(FakeMsg(garbage), reason)
    assert topic == "dlq.arrival_predictions"
    (_topic, value, key, headers) = raw.messages[0]
    assert value == garbage and key == b"k"  # original bytes, untouched
    assert headers["stage"] == b"decode"
    assert headers["source_topic"] == b"arrival_predictions"
    assert (headers["source_partition"], headers["source_offset"]) == (b"3", b"4242")
    assert headers["error"].startswith(b"not valid UTF-8 JSON")
    assert headers["schema_version"] == b"1"


def test_headers_survive_missing_originals():
    headers = dict(dead_letter_headers("vehicle_positions", 0, 1, "boom", None))
    assert headers["schema_version"] == b"?" and headers["event_type"] == b"?"
