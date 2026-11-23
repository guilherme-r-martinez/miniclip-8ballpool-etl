from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.spark.session import build_spark_session
from src.spark.transformations import compute_daily_distinct_users, event_schema


def run_batch(input_path: str, output_path: str | None = None) -> list[dict]:
    """Compute daily distinct users from event data and optionally save the results."""
    spark = build_spark_session(app_name="miniclip-batch-daily-users", include_kafka=False)
    df = spark.read.schema(event_schema()).json(input_path)
    # Collect the aggregate rows locally so they can be returned and optionally serialized.
    rows = [row.asDict(recursive=True) for row in compute_daily_distinct_users(df).collect()]

    # Convert Spark date values to strings so the rows can be encoded as JSON.
    for row in rows:
        if "event_date" in row and row["event_date"] is not None:
            row["event_date"] = str(row["event_date"])

    if output_path:
        out_file = Path(output_path)
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(json.dumps(rows, indent=2), encoding="utf-8")

    return rows


if __name__ == "__main__":
    # Run the batch job with paths supplied on the command line.
    parser = argparse.ArgumentParser(description="POC batch daily distinct users without Hadoop file writes.")
    parser.add_argument("--input", default="data/silver/events.jsonl", help="Path to DQ-cleaned events")
    parser.add_argument("--output", default="data/gold/daily_users.json", help="Optional JSON output path")
    args = parser.parse_args()
    result = run_batch(args.input, args.output)
    print(f"Computed {len(result)} daily user rows.")