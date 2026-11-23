from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def event_schema() -> StructType:
    """Schema covering DQ-enriched init, match, and in-app-purchase events."""
    postmatch_schema = StructType(
        [
            StructField("coin-balance-after-match", IntegerType(), nullable=False),
            StructField("level-after-match", IntegerType(), nullable=False),
            StructField("device", StringType(), nullable=False),
            StructField("platform", StringType(), nullable=False),
        ]
    )

    return StructType(
        [
            StructField("event-type", StringType(), nullable=False),
            StructField("time", IntegerType(), nullable=False),
            StructField("user-id", StringType(), nullable=True),
            StructField("country", StringType(), nullable=True),
            StructField("country_name", StringType(), nullable=True),
            StructField("platform", StringType(), nullable=True),
            StructField("purchase_value", DoubleType(), nullable=True),
            StructField("product-id", StringType(), nullable=True),
            StructField("user-a", StringType(), nullable=True),
            StructField("user-b", StringType(), nullable=True),
            StructField("user-a-postmatch-info", postmatch_schema, nullable=True),
            StructField("user-b-postmatch-info", postmatch_schema, nullable=True),
            StructField("winner", StringType(), nullable=True),
            StructField("game-tier", IntegerType(), nullable=True),
            StructField("duration", IntegerType(), nullable=True),
        ]
    )


def with_event_time_and_platform(df: DataFrame) -> DataFrame:
    """Adds event_time timestamp, event_date, and unified uppercase platform."""
    unified_platform = F.upper(
        F.coalesce(
            F.col("platform"),
            F.col("`user-a-postmatch-info`.platform"),
            F.col("`user-b-postmatch-info`.platform"),
            F.lit("UNKNOWN"),
        )
    )

    return (
        df.withColumn("event_time", F.to_timestamp(F.from_unixtime(F.col("time"))))
        .withColumn("event_date", F.to_date(F.col("event_time")))
        .withColumn("country_name", F.coalesce(F.col("country_name"), F.lit("UNKNOWN")))
        .withColumn("platform_clean", unified_platform)
    )


def compute_daily_distinct_users(df: DataFrame) -> DataFrame:
    """Beginner batch metric: distinct users per day, country_name, and platform."""
    prepared = with_event_time_and_platform(df)

    # Expand active user IDs across init/purchase (user-id) and match (user-a, user-b)
    user_events = prepared.withColumn(
        "active_user_id",
        F.coalesce(
            F.col("`user-id`"),
            F.explode(
                F.array_remove(
                    F.array(F.col("`user-a`"), F.col("`user-b`")),
                    None,
                )
            ),
        ),
    ).filter(F.col("active_user_id").isNotNull())

    return (
        user_events.groupBy("event_date", "country_name", "platform_clean")
        .agg(F.countDistinct("active_user_id").alias("distinct_users"))
        .orderBy("event_date", "country_name", "platform_clean")
    )


def compute_minute_kpis(
    df: DataFrame,
    watermark_delay: str = "2 minutes",
    window_duration: str = "1 minute",
) -> DataFrame:
    """Pro streaming metric: 1-minute window aggregations for purchases, revenue, users, and matches."""
    prepared = with_event_time_and_platform(df).withColumn(
        "active_user_id",
        F.coalesce(
            F.col("`user-id`"),
            F.explode(
                F.array_remove(
                    F.array(F.col("`user-a`"), F.col("`user-b`")),
                    None,
                )
            ),
        ),
    )

    is_purchase = F.col("`event-type`") == "in-app-purchase"
    is_match = F.col("`event-type`") == "match"
    purchase_amount = F.when(is_purchase, F.coalesce(F.col("purchase_value"), F.lit(0.0))).otherwise(
        F.lit(0.0)
    )

    return (
        prepared.withWatermark("event_time", watermark_delay)
        .groupBy(
            F.window(F.col("event_time"), window_duration).alias("minute_window"),
            F.col("country_name"),
        )
        .agg(
            F.sum(F.when(is_purchase, 1).otherwise(0)).alias("purchase_count"),
            F.sum(purchase_amount).alias("total_revenue"),
            F.countDistinct("active_user_id").alias("distinct_users"),
            F.sum(purchase_amount).alias("country_revenue"),
            F.sum(F.when(is_match, 1).otherwise(0)).alias("match_count"),
        )
        .select(
            F.col("minute_window.start").alias("window_start"),
            F.col("minute_window.end").alias("window_end"),
            F.col("country_name"),
            F.col("purchase_count"),
            F.round(F.col("total_revenue"), 2).alias("total_revenue"),
            F.col("distinct_users"),
            F.round(F.col("country_revenue"), 2).alias("country_revenue"),
            F.col("match_count"),
        )
    )