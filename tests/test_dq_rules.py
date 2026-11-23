from src.dq.rules import RuleEngine


def test_init_then_match_and_purchase_dq_rules():
    engine = RuleEngine.from_config(
        {
            "rules": [
                {"name": "plat", "type": "uppercase", "field": "platform"},
                {
                    "name": "match_plat_a",
                    "type": "uppercase",
                    "field": "user-a-postmatch-info.platform",
                },
                {
                    "name": "country_map",
                    "type": "map_lookup",
                    "source_field": "country",
                    "target_field": "country_name",
                    "default_value": "UNKNOWN",
                    "mapping": {"PT": "Portugal", "ES": "Spain"},
                },
            ]
        }
    )

    init_clean = engine.transform(
        {
            "event-type": "init",
            "time": 1732000000,
            "user-id": "u_1",
            "country": "PT",
            "platform": "android",
        }
    )
    assert init_clean["platform"] == "ANDROID"
    assert init_clean["country_name"] == "Portugal"

    match_clean = engine.transform(
        {
            "event-type": "match",
            "time": 1732000060,
            "user-a": "u_1",
            "user-b": "u_2",
            "user-a-postmatch-info": {
                "coin-balance-after-match": 1200,
                "level-after-match": 5,
                "device": "Pixel 8",
                "platform": "android",
            },
            "winner": "u_1",
            "game-tier": 2,
            "duration": 140,
        }
    )
    assert match_clean["country_name"] == "Portugal"
    assert match_clean["user-a-postmatch-info"]["platform"] == "ANDROID"