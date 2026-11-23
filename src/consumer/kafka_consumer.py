from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Iterable

import yaml
from kafka import KafkaConsumer

from src.dq.processor import process_stream
from src.dq.rules import RuleEngine
from src.gold.run_gold import compute_gold_from_silver


def load_rule_engine(config_path: str = "config/dq_rules.yaml") -> RuleEngine:
    path = Path(config_path)
    if not path.is_absolute() and not path.exists():
        repo_root = Path(__file__).resolve().parents[2]
        candidate = repo_root / path
        if candidate.exists():
            path = candidate

    with path.open("r", encoding="utf-8") as f:
        config_data = yaml.safe_load(f) or {}

    return RuleEngine.from_config(config_data)


def build_kafka_consumer(
    topic: str,
    bootstrap_servers: str = "localhost:9092",
    group_id: str = "miniclip-8ballpool-consumer",
    auto_offset_reset: str = "earliest",
    consumer_timeout_ms: int = 1000,
) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        consumer_timeout_ms=consumer_timeout_ms,
        enable_auto_commit=True,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )


def _read_existing_silver_events(silver_path: Path) -> list[dict[str, Any]]:
    if not silver_path.exists():
        return []
    return [
        json.loads(line)
        for line in silver_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _write_gold_outputs(gold_dir: Path, silver_events: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    gold = compute_gold_from_silver(silver_events)
    gold_dir.mkdir(parents=True, exist_ok=True)
    (gold_dir / "daily_users.json").write_text(json.dumps(gold["daily_users"], indent=2), encoding="utf-8")
    (gold_dir / "minute_kpis.json").write_text(json.dumps(gold["minute_kpis"], indent=2), encoding="utf-8")
    return gold


def consume_and_process_events(
    raw_events: Iterable[dict[str, Any]],
    rules_config: str = "config/dq_rules.yaml",
    silver_out: str = "data/silver/events.jsonl",
    gold_dir: str = "data/gold",
    update_gold_every: int = 1,
) -> int:
    engine = load_rule_engine(rules_config)
    silver_path = Path(silver_out)
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    gold_path = Path(gold_dir)

    all_silver_events = _read_existing_silver_events(silver_path)
    valid_raw = [ev for ev in raw_events if isinstance(ev, dict)]
    cleaned_batch = process_stream(valid_raw, engine)

    processed_count = 0
    with silver_path.open("a", encoding="utf-8") as silver_file:
        for cleaned in cleaned_batch:
            silver_file.write(json.dumps(cleaned, ensure_ascii=False) + "\n")
            all_silver_events.append(cleaned)
            processed_count += 1

            if update_gold_every > 0 and processed_count % update_gold_every == 0:
                _write_gold_outputs(gold_path, all_silver_events)

    if processed_count > 0:
        _write_gold_outputs(gold_path, all_silver_events)

    return processed_count


def run_consumer(
    bootstrap_servers: str = "localhost:9092",
    topic: str = "8ballpool-events",
    group_id: str = "miniclip-8ballpool-consumer",
    rules_config: str = "config/dq_rules.yaml",
    silver_out: str = "data/silver/events.jsonl",
    gold_dir: str = "data/gold",
    max_messages: int | None = None,
    poll_batches: int = 1,
    poll_sleep_sec: float = 0.5,
    input_jsonl: str | None = None,
) -> int:
    if input_jsonl:
        source_path = Path(input_jsonl)
        events = [
            json.loads(line)
            for line in source_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        if max_messages is not None:
            events = events[:max_messages]
        return consume_and_process_events(
            raw_events=events,
            rules_config=rules_config,
            silver_out=silver_out,
            gold_dir=gold_dir,
        )

    consumer = build_kafka_consumer(
        topic=topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
    )
    collected: list[dict[str, Any]] = []
    try:
        for batch_idx in range(poll_batches):
            for msg in consumer:
                if isinstance(msg.value, dict):
                    collected.append(msg.value)
                    if max_messages is not None and len(collected) >= max_messages:
                        break
            if max_messages is not None and len(collected) >= max_messages:
                break
            if batch_idx + 1 < poll_batches and poll_sleep_sec > 0:
                time.sleep(poll_sleep_sec)
    finally:
        consumer.close()

    return consume_and_process_events(
        raw_events=collected,
        rules_config=rules_config,
        silver_out=silver_out,
        gold_dir=gold_dir,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Consume 8 Ball Pool events from Kafka, apply DQ rules, write Silver, and update Gold."
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap server(s)")
    parser.add_argument("--topic", default="8ballpool-events", help="Kafka topic to consume from")
    parser.add_argument("--group-id", default="miniclip-8ballpool-consumer", help="Kafka consumer group ID")
    parser.add_argument("--rules", default="config/dq_rules.yaml", help="Path to DQ rules YAML")
    parser.add_argument("--silver-out", default="data/silver/events.jsonl", help="Silver JSONL output path")
    parser.add_argument("--gold-dir", default="data/gold", help="Gold output directory")
    parser.add_argument("--max-messages", type=int, default=None, help="Optional max messages to consume per run")
    parser.add_argument("--poll-batches", type=int, default=1, help="Number of consumer poll cycles to run")
    parser.add_argument(
        "--input-jsonl",
        default=None,
        help="Optional local JSONL path to simulate consuming without a live Kafka broker",
    )
    args = parser.parse_args()

    count = run_consumer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        rules_config=args.rules,
        silver_out=args.silver_out,
        gold_dir=args.gold_dir,
        max_messages=args.max_messages,
        poll_batches=args.poll_batches,
        input_jsonl=args.input_jsonl,
    )
    print(f"Consumed and processed {count} events into Silver ({args.silver_out}) and Gold ({args.gold_dir}).")


if __name__ == "__main__":
    main()