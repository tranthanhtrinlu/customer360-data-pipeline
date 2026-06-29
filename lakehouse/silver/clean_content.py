# -*- coding: utf-8 -*-
r"""
SILVER (batch) - Chuẩn hóa content logs từ bronze.

Đọc Delta `bronze/content_logs`, map AppName -> content_type, lọc bản ghi rác,
ghi đè vào Delta `silver/content_events` ở mức EVENT-LEVEL:
    contract, content_type, total_duration, event_date

Tổng hợp profile (pivot, taste, activity) làm ở tầng gold.

Chạy:  python -m lakehouse.silver.clean_content
"""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from .. import config
from ..common import overwrite_delta, read_delta
from ..spark_session import get_spark


def categorize_content(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "content_type",
            F.when(F.col("AppName") == "CHANNEL", "Truyen Hinh")
             .when(F.col("AppName") == "RELAX", "Giai Tri")
             .when(F.col("AppName") == "CHILD", "Thieu Nhi")
             .when((F.col("AppName") == "FIMS") | (F.col("AppName") == "VOD"), "Phim Truyen")
             .when((F.col("AppName") == "KPLUS") | (F.col("AppName") == "SPORT"), "The Thao")
             .otherwise("Khac")
        )
        .select(
            F.col("Contract").cast("string").alias("contract"),
            "content_type",
            F.col("TotalDuration").cast("double").alias("total_duration"),
            "event_date",
        )
        .filter(F.col("contract").isNotNull())
        .filter(F.col("contract") != "0")
        .filter(F.col("content_type") != "Khac")
        .filter(F.col("total_duration").isNotNull())
    )


def main() -> None:
    spark = get_spark("C360_Silver_CleanContent")
    try:
        bronze_df = read_delta(spark, config.BRONZE_CONTENT)
        events_df = categorize_content(bronze_df)
        overwrite_delta(events_df, config.SILVER_CONTENT_EVENTS, partition_by=["event_date"])

        print(f"[silver.content] Ghi {events_df.count()} dòng -> {config.SILVER_CONTENT_EVENTS}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
