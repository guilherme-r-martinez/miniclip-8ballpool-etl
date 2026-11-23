from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


def _get_by_path(data: dict[str, Any], path: str) -> Any:
    """Reads a nested value using dot notation (e.g. 'user-a-postmatch-info.platform')."""
    current: Any = data
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _set_by_path(data: dict[str, Any], path: str, value: Any) -> bool:
    """Sets a nested value if parent dicts exist; returns True if written."""
    parts = path.split(".")
    current: Any = data
    for part in parts[:-1]:
        if not isinstance(current, dict) or part not in current or not isinstance(current[part], dict):
            return False
        current = current[part]
    if isinstance(current, dict):
        current[parts[-1]] = value
        return True
    return False


class DQRule(ABC):
    """Base interface for extensible record-level Data Quality rules."""

    name: str

    @abstractmethod
    def apply(self, record: dict[str, Any]) -> dict[str, Any]:
        """Apply transformation or enrichment to a single event dictionary."""


@dataclass
class UppercaseRule(DQRule):
    """Uppercases a top-level or nested string field when present."""

    name: str
    field: str

    def apply(self, record: dict[str, Any]) -> dict[str, Any]:
        value = _get_by_path(record, self.field)
        if isinstance(value, str):
            _set_by_path(record, self.field, value.strip().upper())
        return record


@dataclass
class MapLookupRule(DQRule):
    """Maps a source field value (e.g. country code) to a target field."""

    name: str
    source_field: str
    target_field: str
    mapping: dict[str, str]
    default_value: str = "UNKNOWN"

    def apply(self, record: dict[str, Any]) -> dict[str, Any]:
        raw_value = _get_by_path(record, self.source_field)
        if raw_value is None:
            record[self.target_field] = self.default_value
            return record

        key = str(raw_value).strip()
        record[self.target_field] = self.mapping.get(key, self.default_value)
        return record


@dataclass
class UserCountryTracker:
    """Learns user-id -> country from 'init' events and enriches later events."""

    user_to_country: dict[str, str] = field(default_factory=dict)
    default_country: str = "UNKNOWN"

    def enrich_and_learn(self, event: dict[str, Any]) -> dict[str, Any]:
        enriched = dict(event)
        event_type = enriched.get("event-type")

        if event_type == "init":
            user_id = str(enriched.get("user-id", ""))
            country = enriched.get("country")
            if user_id and isinstance(country, str) and country.strip():
                self.user_to_country[user_id] = country.strip()
            enriched["country"] = country.strip() if isinstance(country, str) else self.default_country

        elif event_type == "in-app-purchase":
            user_id = str(enriched.get("user-id", ""))
            enriched["country"] = self.user_to_country.get(user_id, self.default_country)

        elif event_type == "match":
            user_a = str(enriched.get("user-a", ""))
            enriched["country"] = self.user_to_country.get(user_a, self.default_country)

        return enriched


class RuleEngine:
    """Runs session country enrichment followed by ordered DQRule transformations."""

    def __init__(
        self,
        rules: list[DQRule],
        country_tracker: UserCountryTracker | None = None,
    ) -> None:
        self.rules = rules
        self.country_tracker = country_tracker or UserCountryTracker()

    def transform(self, event: dict[str, Any]) -> dict[str, Any]:
        enriched = self.country_tracker.enrich_and_learn(event)
        for rule in self.rules:
            enriched = rule.apply(enriched)
        return enriched

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> RuleEngine:
        rule_builders = {
            "uppercase": lambda cfg: UppercaseRule(
                name=cfg["name"],
                field=cfg["field"],
            ),
            "map_lookup": lambda cfg: MapLookupRule(
                name=cfg["name"],
                source_field=cfg["source_field"],
                target_field=cfg["target_field"],
                mapping={str(k): str(v) for k, v in cfg.get("mapping", {}).items()},
                default_value=cfg.get("default_value", "UNKNOWN"),
            ),
        }

        built_rules: list[DQRule] = []
        for rule_cfg in config.get("rules", []):
            rule_type = rule_cfg.get("type")
            if rule_type not in rule_builders:
                raise ValueError(f"Unsupported DQ rule type: '{rule_type}'")
            built_rules.append(rule_builders[rule_type](rule_cfg))

        return cls(built_rules)