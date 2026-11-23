"""Purpose: verify the end-to-end Bronze-to-Silver-to-Gold processing flow.

test_end_to_end_poc_silver_to_gold checks that generated events can be cleaned
with DQ rules, aggregated into Gold metrics, and produce non-empty expected results.
"""

from src.dq.processor import process_stream
from src.dq.rules import RuleEngine
from src.gold.run_gold import compute_gold_from_silver
from src.producer.event_generator import EightBallPoolEventGenerator


def test_end_to_end_poc_silver_to_gold():
    """Verify generated events flow through DQ processing into Gold aggregates."""
    # A fixed seed makes the sample event stream repeatable.
    gen = EightBallPoolEventGenerator(seed=42)
    raw_events = list(gen.stream_events(num_players=5, max_events=25))

    # Apply the same platform normalization and country mapping used in the pipeline.
    engine = RuleEngine.from_config(
        {
            "rules": [
                {"name": "plat", "type": "uppercase", "field": "platform"},
                {
                    "name": "country_map",
                    "type": "map_lookup",
                    "source_field": "country",
                    "target_field": "country_name",
                    "default_value": "UNKNOWN",
                    "mapping": {"PT": "Portugal", "ES": "Spain", "GB": "United Kingdom"},
                },
            ]
        }
    )

    silver_events = process_stream(raw_events, engine)
    gold = compute_gold_from_silver(silver_events)

    # Confirm both aggregate outputs contain data and daily rows expose their key metric.
    assert len(gold["daily_users"]) > 0
    assert len(gold["minute_kpis"]) > 0
    assert all("distinct_users" in row for row in gold["daily_users"])