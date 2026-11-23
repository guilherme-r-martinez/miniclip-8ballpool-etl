"""Purpose: verify Kafka consumer input paths and their Silver/Gold outputs.

The tests cover direct event processing, JSONL input through the CLI, and Kafka
consumption through a mocked consumer without requiring a live broker.
"""

from __future__ import annotations

import json
import sys
from types import SimpleNamespace

from src.consumer import kafka_consumer
from src.producer.event_generator import EightBallPoolEventGenerator


def _write_rules_yaml(path) -> str:
    """Write a small DQ configuration for consumer tests and return its path."""
    rules_path = path / "dq_rules.yaml"
    rules_path.write_text(
        """
rules:
  - name: normalize_platform
    type: uppercase
    field: platform
  - name: map_country
    type: map_lookup
    source_field: country
    target_field: country_name
    default_value: UNKNOWN
    mapping:
      PT: Portugal
      ES: Spain
      GB: United Kingdom
""".strip(),
        encoding="utf-8",
    )
    return str(rules_path)


def test_consume_and_process_events_writes_silver_and_gold(tmp_path):
    """Check direct event processing writes all events to Silver and creates Gold outputs."""
    rules_file = _write_rules_yaml(tmp_path)
    silver_out = tmp_path / "silver" / "events.jsonl"
    gold_dir = tmp_path / "gold"

    # Use a fixed seed so this test always processes the same number of generated events.
    gen = EightBallPoolEventGenerator(seed=42)
    raw_events = list(gen.stream_events(num_players=5, max_events=25))

    count = kafka_consumer.consume_and_process_events(
        raw_events=raw_events,
        rules_config=rules_file,
        silver_out=str(silver_out),
        gold_dir=str(gold_dir),
    )

    assert count == 25
    assert silver_out.exists()

    silver_rows = [
        json.loads(line)
        for line in silver_out.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(silver_rows) == 25

    daily_users = json.loads((gold_dir / "daily_users.json").read_text(encoding="utf-8"))
    minute_kpis = json.loads((gold_dir / "minute_kpis.json").read_text(encoding="utf-8"))
    assert len(daily_users) > 0
    assert len(minute_kpis) > 0


def test_run_consumer_with_input_jsonl_cli(tmp_path, monkeypatch):
    """Check CLI JSONL mode honors the message limit and writes Silver and Gold files."""
    rules_file = _write_rules_yaml(tmp_path)
    input_jsonl = tmp_path / "bronze_events.jsonl"
    silver_out = tmp_path / "silver_cli.jsonl"
    gold_dir = tmp_path / "gold_cli"

    gen = EightBallPoolEventGenerator(seed=10)
    events = list(gen.stream_events(num_players=4, max_events=12))
    input_jsonl.write_text(
        "\n".join(json.dumps(ev) for ev in events) + "\n",
        encoding="utf-8",
    )

    # Supply CLI arguments without starting a separate process.
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "kafka_consumer",
            "--rules",
            rules_file,
            "--input-jsonl",
            str(input_jsonl),
            "--silver-out",
            str(silver_out),
            "--gold-dir",
            str(gold_dir),
            "--max-messages",
            "8",
        ],
    )

    kafka_consumer.main()

    silver_rows = [
        json.loads(line)
        for line in silver_out.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(silver_rows) == 8
    assert (gold_dir / "daily_users.json").exists()
    assert (gold_dir / "minute_kpis.json").exists()


def test_run_consumer_with_mocked_kafka_consumer(tmp_path, monkeypatch):
    """Check Kafka mode processes a message limit using a fake consumer, without a broker."""
    rules_file = _write_rules_yaml(tmp_path)
    silver_out = tmp_path / "silver_kafka.jsonl"
    gold_dir = tmp_path / "gold_kafka"

    gen = EightBallPoolEventGenerator(seed=99)
    sample_events = list(gen.stream_events(num_players=3, max_events=10))

    # Provide iterable messages with the same value attribute as Kafka's message objects.
    class FakeKafkaConsumer:
        def __init__(self, *topics, **kwargs):
            self.topics = topics
            self.kwargs = kwargs
            self.closed = False
            self._messages = [SimpleNamespace(value=ev) for ev in sample_events]

        def __iter__(self):
            return iter(self._messages)

        def close(self):
            self.closed = True

    # Replace the real consumer factory to keep this test independent of Kafka infrastructure.
    monkeypatch.setattr(
        kafka_consumer,
        "build_kafka_consumer",
        lambda **kwargs: FakeKafkaConsumer(kwargs["topic"], **kwargs),
    )

    count = kafka_consumer.run_consumer(
        bootstrap_servers="localhost:9092",
        topic="8ballpool-events",
        rules_config=rules_file,
        silver_out=str(silver_out),
        gold_dir=str(gold_dir),
        max_messages=6,
    )

    assert count == 6
    silver_rows = [
        json.loads(line)
        for line in silver_out.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(silver_rows) == 6