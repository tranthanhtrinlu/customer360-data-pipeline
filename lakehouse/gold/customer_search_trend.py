# -*- coding: utf-8 -*-
r"""
GOLD - Mart customer_search_trend (Delta, upsert).

Từ Delta `silver/search_clean` (event-level):
  1) Đếm số lượt theo (user_id, month_label, keyword).
  2) Lấy top-1 keyword mỗi user mỗi tháng (>= MIN_COUNT_PER_MONTH).
  3) Ghép t6/t7 thành 1 dòng mỗi user.
  4) Gán category cho keyword t6/t7, tính cờ thay đổi keyword/category.
  5) MERGE (upsert) vào Delta `gold/customer_search_trend` theo user_id.

So với CSV cũ: dữ liệu trung gian không còn là CSV mất kiểu, mà là bảng Delta
có ACID + upsert incremental.

Chạy:  python -m lakehouse.gold.customer_search_trend
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .. import config
from ..common import read_delta, upsert_delta
from ..spark_session import get_spark
from .keyword_mapping import load_mapping_rules, make_categorize_udf


def top1_keyword_per_user_month(clean_df: DataFrame) -> DataFrame:
    counts = (
        clean_df.groupBy("user_id", "month_label", "keyword")
        .agg(F.count(F.lit(1)).alias("search_count"))
    )
    w = Window.partitionBy("user_id", "month_label").orderBy(
        F.col("search_count").desc(), F.col("keyword").asc()
    )
    return (
        counts.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .filter(F.col("search_count") >= config.MIN_COUNT_PER_MONTH)
        .select("user_id", "month_label", "keyword", "search_count")
    )


def build_t6_t7(top1_df: DataFrame) -> DataFrame:
    t6 = top1_df.filter(F.col("month_label") == "t6").select(
        "user_id",
        F.col("keyword").alias("most_search_t6"),
        F.col("search_count").alias("count_t6"),
    )
    t7 = top1_df.filter(F.col("month_label") == "t7").select(
        "user_id",
        F.col("keyword").alias("most_search_t7"),
        F.col("search_count").alias("count_t7"),
    )
    return t6.join(t7, on="user_id", how="inner")


def add_categories_and_flags(df: DataFrame, categorize_udf) -> DataFrame:
    return (
        df
        .withColumn("category_t6", categorize_udf(F.col("most_search_t6")))
        .withColumn("category_t7", categorize_udf(F.col("most_search_t7")))
        .withColumn(
            "keyword_changed_flag",
            F.when(F.col("most_search_t6") == F.col("most_search_t7"), F.lit("No"))
             .otherwise(F.lit("Yes")),
        )
        .withColumn(
            "category_shift_flag",
            F.when(F.col("category_t6") == F.col("category_t7"), F.lit("Unchanged"))
             .otherwise(F.lit("Changed")),
        )
        .withColumn(
            "category_transition",
            F.when(F.col("category_t6") == F.col("category_t7"), F.lit("Unchanged"))
             .otherwise(F.concat_ws(" -> ", F.col("category_t6"), F.col("category_t7"))),
        )
        .select(
            "user_id", "most_search_t6", "count_t6", "category_t6",
            "most_search_t7", "count_t7", "category_t7",
            "keyword_changed_flag", "category_shift_flag", "category_transition",
        )
    )


def main() -> None:
    spark = get_spark("C360_Gold_SearchTrend")
    try:
        rules = load_mapping_rules(config.MAPPING_CSV_PATH)
        categorize_udf = make_categorize_udf(rules)

        clean_df = read_delta(spark, config.SILVER_SEARCH_CLEAN)
        top1_df = top1_keyword_per_user_month(clean_df)
        wide_df = build_t6_t7(top1_df)
        mart_df = add_categories_and_flags(wide_df, categorize_udf)

        upsert_delta(spark, mart_df, config.GOLD_SEARCH_TREND, keys=config.KEY_SEARCH_TREND)
        print(f"[gold.search_trend] upsert {mart_df.count()} user -> {config.GOLD_SEARCH_TREND}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
