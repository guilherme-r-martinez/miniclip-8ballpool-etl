from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark_session(
    app_name: str = "miniclip-8ballpool-etl",
    master: str = "local[*]",
    include_kafka: bool = False,
    kafka_package: str = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
) -> SparkSession:
    """Creates a local SparkSession with optional Kafka SQL connector."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        # Keep Spark date and timestamp operations consistent across local environments.
        .config("spark.sql.session.timeZone", "UTC")
    )

    if include_kafka:
        # Load the Kafka connector only for jobs that read from or write to Kafka.
        builder = builder.config("spark.jars.packages", kafka_package)

    return builder.getOrCreate()