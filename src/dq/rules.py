from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


class DQRule(ABC):
    """Base interface for extensible record-level Data Quality rules."""

    name: str

    @abstractmethod
    def apply(self, record: dict[str, Any]) -> dict[str, Any]:
        """Apply transformation or validation to a single event dictionary."""


@dataclass
class UppercaseRule(DQRule):
    """Uppercases a string field when present and non-null."""

    name: str
    field: str

    def apply(self, record: dict[str, Any]) -> dict[str, Any]:
        value = record.get(self.field)
        if isinstance(value, str):
            record[self.field] = value.strip().upper()
        return record


@dataclass
class MapLookupRule(DQRule):
    """Maps a source identifier (e.g. country_id) to a target label (e.g. country_name)."""

    name: str
    source_field: str
    target_field: str
    mapping: dict[str, str]
    default_value: str = "UNKNOWN"

    def apply(self, record: dict[str, Any]) -> dict[str, Any]:
        raw_key = record.get(self.source_field)
        if raw_key is None:
            record[self.target_field] = self.default_value
            return record

        lookup_key = str(raw_key).strip()
        record[self.target_field] = self.mapping.get(lookup_key, self.default_value)
        return record


class RuleEngine:
    """Executes an ordered list of DQRule instances against incoming events."""

    def __init__(self, rules: list[DQRule]) -> None:
        self.rules = rules

    def transform(self, event: dict[str, Any]) -> dict[str, Any]:
        enriched = dict(event)
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