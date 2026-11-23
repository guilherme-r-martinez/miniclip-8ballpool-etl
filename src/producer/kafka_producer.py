from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from kafka import KafkaProducer

from src.producer.event_generator import EightBallPoolEventGenerator


def _extract_event_key(event: dict[str, Any]) -> bytes:
    """Partition key by primary user/player ID so related user events stay ordered."""
    key_val = (
        event.get("user-id")
        or event.get("user-a")
        or event.get("user-b")
        or event.get("event-type")
        or "unknown"
    )
    return str(key_val).encode("utf-8")


def build_kafka_producer(bootstrap_servers: str, client_id: str = "miniclip-8ballpool-producer") -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        client_id=client_id,
        key_serializer=lambda k: k if isinstance(k, bytes) else str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=3,
    )


def stream_events_to_kafka(
    bootstrap_servers: str = "localhost:9092",
    topic: str = "8ballpool-events",
    num_players: int = 15,
    max_events: int | None = 80,
    interval_sec: float = 0.0,
    seed: int = 42,
    dry_run_jsonl: str | None = None,
) -> int:
    generator = EightBallPoolEventGenerator(seed=seed)
    event_stream = generator.stream_events(num_players=num_players, max_events=max_events)

    dry_run_file = None
    if dry_run_jsonl:
        out_path = Path(dry_run_jsonl)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        dry_run_file = out_path.open("w", encoding="utf-8")

    producer: KafkaProducer | None = None
    if not dry_run_jsonl:
        producer = build_kafka_producer(bootstrap_servers=bootstrap_servers)

    sent_count = 0
    try:
        for event in event_stream:
            if dry_run_file is not None:
                dry_run_file.write(json.dumps(event, ensure_ascii=False) + "\n")
            elif producer is not None:
                # Sending to a non-existent topic triggers auto-creation when enabled on the broker
                producer.send(topic, key=_extract_event_key(event), value=event)

            sent_count += 1
            if interval_sec > 0:
                time.sleep(interval_sec)

        if producer is not None:
            producer.flush()
    finally:
        if producer is not None:
            producer.close()
        if dry_run_file is not None:
            dry_run_file.close()

    return sent_count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stream simulated 8 Ball Pool events (init, match, in-app-purchase) to a Kafka topic."
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap server(s)")
    parser.add_argument("--topic", default="8ballpool-events", help="Target Kafka topic (auto-created by broker)")
    parser.add_argument("--num-players", type=int, default=15, help="Number of simulated players")
    parser.add_argument(
        "--max-events",
        type=int,
        default=80,
        help="Total events to emit (use 0 for continuous streaming until Ctrl+C)",
    )
    parser.add_argument("--interval", type=float, default=0.0, help="Sleep delay in seconds between events")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for deterministic generation")
    parser.add_argument(
        "--dry-run-jsonl",
        default=None,
        help="Optional local JSONL path to write events instead of connecting to a Kafka broker",
    )
    args = parser.parse_args()

    limit = None if args.max_events == 0 else args.max_events
    count = stream_events_to_kafka(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        num_players=args.num_players,
        max_events=limit,
        interval_sec=args.interval,
        seed=args.seed,
        dry_run_jsonl=args.dry_run_jsonl,
    )
    target = args.dry_run_jsonl if args.dry_run_jsonl else f"topic '{args.topic}' ({args.bootstrap_servers})"
    print(f"Published {count} events to {target}.")


if __name__ == "__main__":
    main()