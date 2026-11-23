from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

import yaml

from src.dq.rules import RuleEngine
from src.producer.event_generator import EightBallPoolEventGenerator


def load_rule_engine(config_path: str | Path) -> RuleEngine:
    """Loads YAML Data Quality rules and builds a RuleEngine instance."""
    with Path(config_path).open("r", encoding="utf-8") as f:
        raw_cfg = yaml.safe_load(f) or {}
    return RuleEngine.from_config(raw_cfg)


def process_stream(events: Iterable[dict], engine: RuleEngine) -> list[dict]:
    """Applies DQ rules across an ordered stream of events."""
    # Keep the input order so downstream files preserve the original event sequence.
    return [engine.transform(event) for event in events]


def write_jsonl(records: list[dict], output_path: str | Path) -> Path:
    """Writes records as newline-delimited JSON (JSONL) for Bronze/Silver inspection."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Replace the prior output so each run produces a self-contained snapshot.
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    return path


def run_local_simulation(
    rules_config: str = "config/dq_rules.yaml",
    bronze_out: str = "data/bronze/events.jsonl",
    silver_out: str = "data/silver/events.jsonl",
    num_players: int = 15,
    max_events: int = 80,
    seed: int = 42,
) -> None:
    """Generates simulated events, writes Bronze, applies DQ rules, and writes Silver."""
    # A fixed seed makes local runs reproducible for debugging and comparisons.
    generator = EightBallPoolEventGenerator(seed=seed)
    raw_events = list(generator.stream_events(num_players=num_players, max_events=max_events))
    # Bronze retains the generated input; Silver contains the same events after DQ rules.
    write_jsonl(raw_events, bronze_out)

    engine = load_rule_engine(rules_config)
    clean_events = process_stream(raw_events, engine)
    write_jsonl(clean_events, silver_out)

    print(f"Wrote {len(raw_events)} raw events -> {bronze_out}")
    print(f"Wrote {len(clean_events)} DQ-cleaned events -> {silver_out}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run local Bronze -> DQ -> Silver simulation.")
    parser.add_argument("--rules", default="config/dq_rules.yaml", help="Path to DQ YAML config")
    parser.add_argument("--bronze", default="data/bronze/events.jsonl", help="Bronze output path")
    parser.add_argument("--silver", default="data/silver/events.jsonl", help="Silver output path")
    parser.add_argument("--players", type=int, default=15, help="Simulated player count")
    parser.add_argument("--events", type=int, default=80, help="Total events to generate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()

    run_local_simulation(
        rules_config=args.rules,
        bronze_out=args.bronze,
        silver_out=args.silver,
        num_players=args.players,
        max_events=args.events,
        seed=args.seed,
    )