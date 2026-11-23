from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _extract_platform(event: dict[str, Any]) -> str:
    """Find a platform at the event or either player's post-match level."""
    val = (
        event.get("platform")
        or (event.get("user-a-postmatch-info") or {}).get("platform")
        or (event.get("user-b-postmatch-info") or {}).get("platform")
        or "UNKNOWN"
    )
    return str(val).strip().upper() or "UNKNOWN"


def _extract_users(event: dict[str, Any]) -> list[str]:
    """Collect the event's single user and/or the two users in a match."""
    users: list[str] = []
    if event.get("user-id"):
        users.append(str(event["user-id"]))
    for key in ("user-a", "user-b"):
        if event.get(key):
            users.append(str(event[key]))
    return users


def compute_gold_from_silver(events: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Aggregate Silver events into daily user counts and minute-level KPIs."""
    # Sets ensure that repeated events from the same user count only once per group.
    daily_users_map: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    # Each minute/country bucket accumulates both event counts and distinct users.
    minute_buckets: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {
            "purchase_count": 0,
            "total_revenue": 0.0,
            "country_revenue": 0.0,
            "match_count": 0,
            "users": set(),
        }
    )

    for ev in events:
        ts = int(ev.get("time", 0))
        # Normalize event times to UTC so daily and minute groups are consistent.
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        event_date = dt.strftime("%Y-%m-%d")
        minute_start = dt.strftime("%Y-%m-%d %H:%M:00")
        country_name = str(ev.get("country_name") or "UNKNOWN")
        platform_clean = _extract_platform(ev)
        users = _extract_users(ev)

        # Daily distinct users
        for u in users:
            daily_users_map[(event_date, country_name, platform_clean)].add(u)

        # Minute KPIs are grouped by minute and country, independent of platform.
        bucket = minute_buckets[(minute_start, country_name)]
        bucket["users"].update(users)

        event_type = ev.get("event-type")
        if event_type == "in-app-purchase":
            # Missing purchase values contribute zero revenue but still count as purchases.
            amount = float(ev.get("purchase_value") or 0.0)
            bucket["purchase_count"] += 1
            bucket["total_revenue"] += amount
            bucket["country_revenue"] += amount
        elif event_type == "match":
            bucket["match_count"] += 1

    # Sorting makes output stable across runs and independent of input order.
    daily_users = [
        {
            "event_date": d,
            "country_name": c,
            "platform_clean": p,
            "distinct_users": len(u_set),
        }
        for (d, c, p), u_set in sorted(daily_users_map.items())
    ]

    minute_kpis = [
        {
            "minute_start": m,
            "country_name": c,
            "purchase_count": data["purchase_count"],
            "total_revenue": round(data["total_revenue"], 2),
            "distinct_users": len(data["users"]),
            "country_revenue": round(data["country_revenue"], 2),
            "match_count": data["match_count"],
        }
        for (m, c), data in sorted(minute_buckets.items())
    ]

    return {"daily_users": daily_users, "minute_kpis": minute_kpis}


def main() -> None:
    """Read Silver JSONL, compute Gold aggregates, and write JSON outputs."""
    parser = argparse.ArgumentParser(description="Compute Gold outputs from Silver JSONL without JVM.")
    parser.add_argument("--input", default="data/silver/events.jsonl", help="Silver JSONL path")
    parser.add_argument("--out-dir", default="data/gold", help="Gold output directory")
    args = parser.parse_args()

    silver_path = Path(args.input)
    events = [
        json.loads(line)
        for line in silver_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    gold = compute_gold_from_silver(events)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    (out_dir / "daily_users.json").write_text(json.dumps(gold["daily_users"], indent=2), encoding="utf-8")
    (out_dir / "minute_kpis.json").write_text(json.dumps(gold["minute_kpis"], indent=2), encoding="utf-8")

    print(f"Wrote {len(gold['daily_users'])} rows -> {out_dir / 'daily_users.json'}")
    print(f"Wrote {len(gold['minute_kpis'])} rows -> {out_dir / 'minute_kpis.json'}")


if __name__ == "__main__":
    main()