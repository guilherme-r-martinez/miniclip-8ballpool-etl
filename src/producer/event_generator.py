from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Iterator


@dataclass
class PlayerSession:
    """Tracks per-player state so 'init' is always emitted before other events."""

    user_id: str
    country: str
    platform: str
    device: str
    level: int = 1
    coins: int = 500
    initialized: bool = False


@dataclass
class EightBallPoolEventGenerator:
    """Generates schema-compliant 'init', 'match', and 'in-app-purchase' events."""

    seed: int | None = None
    countries: list[str] = field(
        default_factory=lambda: ["PT", "ES", "GB", "US", "BR", "FR", "DE"]
    )
    platforms: list[str] = field(
        default_factory=lambda: ["ios", "android", "web"]
    )
    devices_by_platform: dict[str, list[str]] = field(
        default_factory=lambda: {
            "ios": ["iPhone 15", "iPhone 16 Pro", "iPad Air"],
            "android": ["Samsung Galaxy S24", "Pixel 8", "OnePlus 12"],
            "web": ["Chrome Desktop", "Edge Desktop"],
        }
    )
    products: list[tuple[str, float]] = field(
        default_factory=lambda: [
            ("coins_pack_small", 1.99),
            ("coins_pack_medium", 4.99),
            ("coins_pack_large", 9.99),
            ("cue_starter_bundle", 3.49),
            ("pool_pass_season", 7.99),
        ]
    )

    def __post_init__(self) -> None:
        # A local RNG keeps seeded runs reproducible without changing global random state.
        self._rng = random.Random(self.seed)
        self._players: dict[str, PlayerSession] = {}

    def _now_epoch_seconds(self) -> int:
        return int(time.time())

    def register_player(self, user_id: str) -> PlayerSession:
        """Creates a new player profile if not already present."""
        # Reuse the same session so player attributes and balances persist across events.
        if user_id not in self._players:
            platform = self._rng.choice(self.platforms)
            device = self._rng.choice(self.devices_by_platform.get(platform, ["Generic Device"]))
            country = self._rng.choice(self.countries)
            self._players[user_id] = PlayerSession(
                user_id=str(user_id),
                country=country,
                platform=platform,
                device=device,
                level=self._rng.randint(1, 35),
                coins=self._rng.randint(250, 5000),
                initialized=False,
            )
        return self._players[user_id]

    def build_init_event(self, player: PlayerSession, event_time: int | None = None) -> dict:
        """Builds an event matching init.json."""
        player.initialized = True
        return {
            "event-type": "init",
            "time": event_time if event_time is not None else self._now_epoch_seconds(),
            "user-id": player.user_id,
            "country": player.country,
            "platform": player.platform,
        }

    def build_purchase_event(self, player: PlayerSession, event_time: int | None = None) -> dict:
        """Builds an event matching in_app_purchase.json (ensuring init ran first)."""
        if not player.initialized:
            raise RuntimeError(f"Cannot emit 'in-app-purchase' before 'init' for user {player.user_id}")

        product_id, price = self._rng.choice(self.products)
        player.coins += int(price * 250)
        return {
            "event-type": "in-app-purchase",
            "time": event_time if event_time is not None else self._now_epoch_seconds(),
            "purchase_value": price,
            "user-id": player.user_id,
            "product-id": product_id,
        }

    def build_match_event(
        self,
        player_a: PlayerSession,
        player_b: PlayerSession,
        event_time: int | None = None,
    ) -> dict:
        """Builds an event matching match.json (ensuring both players initialized first)."""
        for p in (player_a, player_b):
            if not p.initialized:
                raise RuntimeError(f"Cannot emit 'match' before 'init' for user {p.user_id}")

        winner_player = self._rng.choice([player_a, player_b])
        loser_player = player_b if winner_player is player_a else player_a
        stake = self._rng.randint(50, 500)

        winner_player.coins += stake
        loser_player.coins = max(0, loser_player.coins - stake)
        # Occasionally reward the winner with a level increase.
        if self._rng.random() < 0.35:
            winner_player.level += 1

        # Some match events omit player B's post-match details to vary event completeness.
        include_player_b_postmatch = self._rng.random() < 0.85
        event: dict = {
            "event-type": "match",
            "time": event_time if event_time is not None else self._now_epoch_seconds(),
            "user-a": player_a.user_id,
            "user-b": player_b.user_id,
            "user-a-postmatch-info": {
                "coin-balance-after-match": player_a.coins,
                "level-after-match": player_a.level,
                "device": player_a.device,
                "platform": player_a.platform,
            },
            "winner": winner_player.user_id,
            "game-tier": self._rng.randint(1, 5),
            "duration": self._rng.randint(30, 360),
        }

        if include_player_b_postmatch:
            event["user-b-postmatch-info"] = {
                "coin-balance-after-match": player_b.coins,
                "level-after-match": player_b.level,
                "device": player_b.device,
                "platform": player_b.platform,
            }

        return event

    def stream_events(
        self,
        num_players: int = 20,
        max_events: int | None = None,
        sleep_seconds: float = 0.0,
    ) -> Iterator[dict]:
        """Yields a stream where every player starts with 'init' before any other event."""
        player_ids = [f"player_{i:04d}" for i in range(1, num_players + 1)]
        players = [self.register_player(pid) for pid in player_ids]

        emitted = 0
        # Initialize players first, unless the event limit is reached before all can emit.
        for player in players:
            if max_events is not None and emitted >= max_events:
                return
            yield self.build_init_event(player)
            emitted += 1
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        # Subsequent events are 70% matches, 22% purchases, and 8% re-initializations.
        while max_events is None or emitted < max_events:
            roll = self._rng.random()
            if roll < 0.70 and len(players) >= 2:
                p_a, p_b = self._rng.sample(players, 2)
                event = self.build_match_event(p_a, p_b)
            elif roll < 0.92:
                buyer = self._rng.choice(players)
                event = self.build_purchase_event(buyer)
            else:
                re_init_player = self._rng.choice(players)
                event = self.build_init_event(re_init_player)

            yield event
            emitted += 1
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)