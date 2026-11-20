from src.dq.rules import RuleEngine


def test_rule_engine_normalizes_platform_and_maps_country():
    engine = RuleEngine.from_config(
        {
            "rules": [
                {"name": "plat", "type": "uppercase", "field": "platform"},
                {
                    "name": "country",
                    "type": "map_lookup",
                    "source_field": "country_id",
                    "target_field": "country_name",
                    "default_value": "UNKNOWN",
                    "mapping": {"PT": "Portugal", "US": "United States"},
                },
            ]
        }
    )

    raw_event = {
        "event_type": "Match",
        "user_id": "u_101",
        "platform": "ios",
        "country_id": "PT",
    }

    clean_event = engine.transform(raw_event)

    assert clean_event["platform"] == "IOS"
    assert clean_event["country_name"] == "Portugal"
    assert raw_event["platform"] == "ios"  # original dict not mutated