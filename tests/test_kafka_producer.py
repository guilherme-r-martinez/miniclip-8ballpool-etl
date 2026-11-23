"""Purpose: verify generated events can be saved locally or sent to Kafka.

test_stream_events_dry_run_jsonl checks local JSONL output and event validity;
test_stream_events_to_mock_kafka_broker checks publishing and message keys without
requiring a live Kafka broker.
"""

import json

from src.producer import kafka_producer


def test_stream_events_dry_run_jsonl(tmp_path):
    """Check dry-run mode writes the requested number of valid events as JSONL."""
    out_file = tmp_path / "kafka_dry_run.jsonl"

    # A local output path exercises event generation without connecting to Kafka.
    count = kafka_producer.stream_events_to_kafka(
        num_players=5,
        max_events=20,
        seed=42,
        dry_run_jsonl=str(out_file),
    )

    assert count == 20
    lines = [json.loads(line) for line in out_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) == 20
    # Only supported event types should be emitted, with player initialization first.
    assert {ev["event-type"] for ev in lines}.issubset({"init", "match", "in-app-purchase"})
    assert lines[0]["event-type"] == "init"


def test_stream_events_to_mock_kafka_broker(monkeypatch):
    """Check generated events are sent to the configured topic with non-empty byte keys."""
    sent_records: list[tuple[str, bytes, dict]] = []

    # Capture producer calls so the test does not need a real Kafka broker.
    class FakeKafkaProducer:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.flushed = False
            self.closed = False

        def send(self, topic, key=None, value=None):
            sent_records.append((topic, key, value))

        def flush(self):
            self.flushed = True

        def close(self):
            self.closed = True

    monkeypatch.setattr(kafka_producer, "build_kafka_producer", lambda **kwargs: FakeKafkaProducer(**kwargs))

    count = kafka_producer.stream_events_to_kafka(
        bootstrap_servers="localhost:9092",
        topic="8ballpool-events",
        num_players=4,
        max_events=15,
        seed=7,
    )

    assert count == 15
    assert len(sent_records) == 15
    assert all(topic == "8ballpool-events" for topic, _, _ in sent_records)
    assert all(isinstance(key, bytes) and len(key) > 0 for _, key, _ in sent_records)