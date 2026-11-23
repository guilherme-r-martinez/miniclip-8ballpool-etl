from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from src.spark.session import build_spark_session
from src.spark.transformations import compute_minute_kpis, event_schema


def run_stream(
    kafka_bootstrap: str,
    input_topic: str,
    checkpoint_path: str,
    output_path: str | None = None,
) -> None:
    """Read clean events from Kafka and stream minute-level KPI aggregates."""
    spark = build_spark_session(app_name="miniclip-stream-minute-kpis", include_kafka=True)

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", input_topic)
        # Process only events arriving after this streaming query starts.
        .option("startingOffsets", "latest")
        .load()
    )

    # Kafka values are bytes; decode them as JSON using the shared event schema.
    parsed_stream = raw_stream.select(
        F.from_json(F.col("value").cast("string"), event_schema()).alias("data")
    ).select("data.*")

    agg_df = compute_minute_kpis(parsed_stream)

    console_query = (
        agg_df.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", f"{checkpoint_path}/console")
        .start()
    )

    file_query = None
    if output_path:
        # Give each sink its own checkpoint so Spark can track its progress independently.
        file_query = (
            agg_df.writeStream.outputMode("append")
            .format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", f"{checkpoint_path}/parquet")
            .start()
        )

    # Keep the application alive while the active streaming queries run.
    console_query.awaitTermination()
    if file_query:
        file_query.awaitTermination()


if __name__ == "__main__":
    # Configure the Kafka source, checkpoint location, and optional Parquet sink.
    parser = argparse.ArgumentParser(description="Real-time 1-minute KPIs from clean Kafka topic.")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="clean.8ballpool.events", help="Input clean Kafka topic")
    parser.add_argument("--checkpoint", default="checkpoints/minute_kpis", help="Spark checkpoint dir")
    parser.add_argument("--output", default=None, help="Optional Parquet output path")
    args = parser.parse_args()
    run_stream(args.bootstrap, args.topic, args.checkpoint, args.output)