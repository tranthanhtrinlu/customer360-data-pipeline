# -*- coding: utf-8 -*-
r"""
GOLD - Mart customer_content_profile (Delta, upsert).

Từ Delta `silver/content_events`:
  1) Pivot duration theo 5 nhóm nội dung, theo (contract, event_date) -> daily profile.
  2) most_watch_type, taste_profile theo ngày.
  3) Tổng hợp theo contract: tổng duration mỗi nhóm, active_days, activity_level.
  4) MERGE (upsert) vào Delta `gold/customer_content_profile` theo contract.

Chạy:  python -m lakehouse.gold.customer_content_profile
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .. import config
from ..common import read_delta, upsert_delta
from ..spark_session import get_spark

CONTENT_COLS = ["Giai Tri", "Phim Truyen", "The Thao", "Thieu Nhi", "Truyen Hinh"]


def build_daily_profile(events_df: DataFrame) -> DataFrame:
    pivot_df = (
        events_df.groupBy("contract", "event_date")
        .pivot("content_type", CONTENT_COLS)
        .sum("total_duration")
        .fillna(0)
    )

    return (
        pivot_df.withColumn(
            "most_watch_value",
            F.greatest(*[F.col(c) for c in CONTENT_COLS]),
        )
        .withColumn(
            "most_watch_type",
            F.when(F.col("most_watch_value") == F.col("Truyen Hinh"), "Truyen Hinh")
             .when(F.col("most_watch_value") == F.col("Phim Truyen"), "Phim Truyen")
             .when(F.col("most_watch_value") == F.col("The Thao"), "The Thao")
             .when(F.col("most_watch_value") == F.col("Thieu Nhi"), "Thieu Nhi")
             .otherwise("Giai Tri"),
        )
        .drop("most_watch_value")
        .withColumn(
            "taste_profile",
            F.concat_ws(
                "-",
                *[F.when(F.col(c) > 0, F.lit(c)) for c in CONTENT_COLS],
            ),
        )
    )


def build_profile(daily_df: DataFrame) -> DataFrame:
    w = Window.partitionBy("contract")
    with_active = (
        daily_df
        .withColumn("active_days", F.count("event_date").over(w))
        .withColumn(
            "activity_level",
            F.when(F.col("active_days") > config.HIGH_ACTIVITY_DAY_THRESHOLD, "High")
             .otherwise("Low"),
        )
    )

    return (
        with_active.groupBy("contract")
        .agg(
            F.sum("Giai Tri").alias("total_giai_tri"),
            F.sum("Phim Truyen").alias("total_phim_truyen"),
            F.sum("The Thao").alias("total_the_thao"),
            F.sum("Thieu Nhi").alias("total_thieu_nhi"),
            F.sum("Truyen Hinh").alias("total_truyen_hinh"),
            F.first("most_watch_type", ignorenulls=True).alias("most_watch_type"),
            F.first("taste_profile", ignorenulls=True).alias("taste_profile"),
            F.first("activity_level", ignorenulls=True).alias("activity_level"),
            F.max("active_days").alias("active_days"),
        )
    )


def main() -> None:
    spark = get_spark("C360_Gold_ContentProfile")
    try:
        events_df = read_delta(spark, config.SILVER_CONTENT_EVENTS)
        daily_df = build_daily_profile(events_df)
        profile_df = build_profile(daily_df)

        upsert_delta(spark, profile_df, config.GOLD_CONTENT_PROFILE, keys=config.KEY_CONTENT_PROFILE)
        print(f"[gold.content_profile] upsert {profile_df.count()} contract -> {config.GOLD_CONTENT_PROFILE}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
